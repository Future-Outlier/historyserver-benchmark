package benchmark

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	. "github.com/ray-project/kuberay/historyserver/test/support"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

const (
	collectorMemoryRayImage       = "rayproject/ray:2.56.0"
	collectorMemoryS3LocalPort    = 19004
	collectorMemoryBatchSize      = 1000
	collectorMemoryMaxDiskMiB     = 1024
	collectorMemoryBaseline       = 10 * time.Second
	collectorMemoryContinuousTail = 15 * time.Second
	collectorMemoryJobID          = "01000000"
)

type collectorMemoryConfig struct {
	ArmName       string        `json:"armName"`
	MatrixSHA256  string        `json:"matrixSHA256"`
	Kind          string        `json:"kind"`
	RateEventsSec int           `json:"rateEventsPerSecond"`
	Ingest        time.Duration `json:"-"`
	Idle          time.Duration `json:"-"`
	Repeat        int           `json:"repeat"`
	CPURequest    string        `json:"cpuRequest"`
	CPULimit      string        `json:"cpuLimit"`
	MemoryRequest string        `json:"memoryRequest"`
	MemoryLimit   string        `json:"memoryLimit"`
	KindNode      string        `json:"kindNode"`
	OutDir        string        `json:"outDir"`
}

func (c collectorMemoryConfig) MarshalJSON() ([]byte, error) {
	type alias collectorMemoryConfig
	return json.Marshal(struct {
		alias
		IngestSeconds float64 `json:"ingestSeconds"`
		IdleSeconds   float64 `json:"idleSeconds"`
	}{alias: alias(c), IngestSeconds: c.Ingest.Seconds(), IdleSeconds: c.Idle.Seconds()})
}

type collectorMemoryPhase struct {
	Name     string `json:"name"`
	TimeNano int64  `json:"timeNano"`
}

type collectorReplaySummary struct {
	SchemaVersion              int     `json:"schema_version"`
	TargetEventsPerSecond      int     `json:"target_events_per_second"`
	BatchSize                  int     `json:"batch_size"`
	PlannedEvents              int     `json:"planned_events"`
	SentEvents                 int     `json:"sent_events"`
	AcceptedEvents             int     `json:"accepted_events"`
	Requests                   int     `json:"requests"`
	AcceptedRequests           int     `json:"accepted_requests"`
	BodyBytes                  int64   `json:"body_bytes"`
	ExpectedJSONLBytes         int64   `json:"expected_jsonl_bytes"`
	JSONLBytesPerEvent         int     `json:"jsonl_bytes_per_event"`
	FixtureGzipRatio           float64 `json:"fixture_gzip_ratio"`
	FixtureGzipRatioDefinition string  `json:"fixture_gzip_ratio_definition"`
	WallSeconds                float64 `json:"wall_seconds"`
	AchievedEventsPerSecond    float64 `json:"achieved_events_per_second"`
	LatencyP50Millis           float64 `json:"latency_p50_ms"`
	LatencyP95Millis           float64 `json:"latency_p95_ms"`
	LatencyP99Millis           float64 `json:"latency_p99_ms"`
	LatencyMaxMillis           float64 `json:"latency_max_ms"`
	Non200Responses            int     `json:"non_200_responses"`
	Retries                    int     `json:"retries"`
}

type collectorRemoteEvidence struct {
	Prefix             string `json:"prefix"`
	Objects            int    `json:"objects"`
	StoredBytes        int64  `json:"storedBytes"`
	RawJSONLBytes      int64  `json:"rawJSONLBytes"`
	Lines              int    `json:"lines"`
	UniqueEventIDs     int    `json:"uniqueEventIDs"`
	DuplicateEventIDs  int    `json:"duplicateEventIDs"`
	MalformedLines     int    `json:"malformedLines"`
	UnexpectedEventIDs int    `json:"unexpectedEventIDs"`
}

type collectorRemotePreflight struct {
	Prefix  string `json:"prefix"`
	Objects int    `json:"objects"`
	Bytes   int64  `json:"bytes"`
	Empty   bool   `json:"empty"`
}

type collectorClockSkewSample struct {
	HostBeforeUnixNano int64 `json:"hostBeforeUnixNano"`
	NodeUnixNano       int64 `json:"nodeUnixNano"`
	HostAfterUnixNano  int64 `json:"hostAfterUnixNano"`
	RoundTripNano      int64 `json:"roundTripNano"`
	SkewNano           int64 `json:"skewNano"`
}

type collectorClockSkewEvidence struct {
	Before collectorClockSkewSample `json:"before"`
	After  collectorClockSkewSample `json:"after"`
}

type collectorRuntimeEvidence struct {
	Pod                           string `json:"pod"`
	PodUID                        string `json:"podUID"`
	Image                         string `json:"image"`
	ImageID                       string `json:"imageID"`
	ContainerID                   string `json:"containerID"`
	RestartCount                  int32  `json:"restartCount"`
	CPURequest                    string `json:"cpuRequest"`
	CPULimit                      string `json:"cpuLimit"`
	MemoryRequest                 string `json:"memoryRequest"`
	MemoryLimit                   string `json:"memoryLimit"`
	TerminationGracePeriodSeconds int64  `json:"terminationGracePeriodSeconds"`
}

type collectorRayRuntimeEvidence struct {
	Pod          string `json:"pod"`
	PodUID       string `json:"podUID"`
	Image        string `json:"image"`
	ImageID      string `json:"imageID"`
	ContainerID  string `json:"containerID"`
	RestartCount int32  `json:"restartCount"`
}

type collectorMemoryReport struct {
	SchemaVersion   int                         `json:"schemaVersion"`
	Completed       bool                        `json:"completed"`
	Config          collectorMemoryConfig       `json:"config"`
	Namespace       string                      `json:"namespace"`
	NamespaceUID    string                      `json:"namespaceUID"`
	ClusterName     string                      `json:"clusterName"`
	SessionName     string                      `json:"sessionName"`
	StartedAt       time.Time                   `json:"startedAt"`
	EndedAt         time.Time                   `json:"endedAt"`
	Phases          []collectorMemoryPhase      `json:"phases"`
	Replay          collectorReplaySummary      `json:"replay"`
	CollectorLogs   []CollectorLogStat          `json:"collectorLogs"`
	CgroupSampler   CgroupSamplerStatus         `json:"cgroupSampler"`
	MemoryDetail    collectorMemoryDetailGate   `json:"memoryDetail"`
	EventSpool      collectorEventSpoolGate     `json:"eventSpool"`
	RemotePreflight collectorRemotePreflight    `json:"remotePreflight"`
	Remote          collectorRemoteEvidence     `json:"remote"`
	Runtime         collectorRuntimeEvidence    `json:"runtime"`
	RayRuntime      collectorRayRuntimeEvidence `json:"rayRuntime"`
	ClockSkew       collectorClockSkewEvidence  `json:"clockSkew"`
	FullLifecycle   SnapshotDiff                `json:"fullLifecycle"`
	DuringIngest    SnapshotDiff                `json:"duringIngest"`
	DuringIdle      SnapshotDiff                `json:"duringIdle"`
	DuringShutdown  SnapshotDiff                `json:"duringShutdown"`
	Error           string                      `json:"error,omitempty"`
}

type collectorMemoryDetailGate struct {
	Observed        bool     `json:"observed"`
	Samples         int      `json:"samples"`
	ReadErrors      int      `json:"readErrors"`
	ReadErrorFields []string `json:"readErrorFields"`
}

type collectorEventSpoolGate struct {
	Samples        int `json:"samples"`
	InvalidSamples int `json:"invalidSamples"`
}

func loadCollectorMemoryConfig() (collectorMemoryConfig, error) {
	cfg := collectorMemoryConfig{
		ArmName:       envStr("BENCH_COLLECTOR_MEMORY_ARM", ""),
		MatrixSHA256:  envStr("BENCH_COLLECTOR_MEMORY_MATRIX_SHA256", ""),
		Kind:          envStr("BENCH_COLLECTOR_MEMORY_KIND", ""),
		RateEventsSec: envInt("BENCH_COLLECTOR_EVENT_RATE", 0),
		Ingest:        envDuration("BENCH_COLLECTOR_INGEST_DURATION", 0),
		Idle:          envDuration("BENCH_COLLECTOR_IDLE_DURATION", 0),
		Repeat:        envInt("BENCH_COLLECTOR_MEMORY_REPEAT", 0),
		CPURequest:    envStr("BENCH_COLLECTOR_CPU_REQUEST", ""),
		CPULimit:      envStr("BENCH_COLLECTOR_CPU_LIMIT", ""),
		MemoryRequest: envStr("BENCH_COLLECTOR_MEMORY_REQUEST", ""),
		MemoryLimit:   envStr("BENCH_COLLECTOR_MEMORY_LIMIT", ""),
		KindNode:      envStr("BENCH_KIND_NODE", ""),
		OutDir:        envStr("BENCH_OUT_DIR", ""),
	}
	decodedMatrixHash, hashErr := hex.DecodeString(cfg.MatrixSHA256)
	if cfg.ArmName == "" || hashErr != nil || len(decodedMatrixHash) != 32 {
		return cfg, fmt.Errorf("formal arm/matrix binding invalid: arm=%q matrixSHA256=%q", cfg.ArmName, cfg.MatrixSHA256)
	}
	if cfg.Kind != "continuous" && cfg.Kind != "ingest-idle" && cfg.Kind != "limit" {
		return cfg, fmt.Errorf("BENCH_COLLECTOR_MEMORY_KIND=%q, want continuous, ingest-idle, or limit", cfg.Kind)
	}
	if cfg.RateEventsSec <= 0 || cfg.Ingest <= 0 || cfg.Idle < 0 || cfg.Repeat < 1 || cfg.Repeat > 3 {
		return cfg, fmt.Errorf("invalid rate/duration/repeat: rate=%d ingest=%s idle=%s repeat=%d", cfg.RateEventsSec, cfg.Ingest, cfg.Idle, cfg.Repeat)
	}
	if cfg.CPURequest != "100m" || cfg.CPULimit != "2" || cfg.MemoryRequest != "128Mi" {
		return cfg, fmt.Errorf("fixed discovery resources drifted: cpu=%s/%s memory request=%s", cfg.CPURequest, cfg.CPULimit, cfg.MemoryRequest)
	}
	if cfg.MemoryLimit != "1Gi" && cfg.MemoryLimit != "192Mi" && cfg.MemoryLimit != "256Mi" && cfg.MemoryLimit != "512Mi" {
		return cfg, fmt.Errorf("unsupported memory limit %q", cfg.MemoryLimit)
	}
	if cfg.KindNode != "bench-control-plane" || cfg.OutDir == "" {
		return cfg, fmt.Errorf("formal kind node/out dir invalid: node=%q out=%q", cfg.KindNode, cfg.OutDir)
	}
	switch cfg.Kind {
	case "continuous":
		if cfg.Ingest != 90*time.Second || cfg.Idle != collectorMemoryContinuousTail || cfg.MemoryLimit != "1Gi" {
			return cfg, fmt.Errorf("continuous arm must be 90s ingest, 15s tail, 1Gi limit")
		}
	case "ingest-idle":
		if cfg.Ingest != 30*time.Second || cfg.Idle != 60*time.Second || cfg.MemoryLimit != "1Gi" {
			return cfg, fmt.Errorf("ingest-idle arm must be 30s ingest, 60s idle, 1Gi limit")
		}
	case "limit":
		if cfg.Ingest != 90*time.Second || cfg.Idle != collectorMemoryContinuousTail || cfg.RateEventsSec != 5000 || cfg.MemoryLimit == "1Gi" {
			return cfg, fmt.Errorf("limit arm must be 5000 events/s, 90s ingest, 15s tail, capped memory")
		}
	}
	return cfg, nil
}

func TestCollectorMemoryBenchmark(t *testing.T) {
	if os.Getenv("BENCH_COLLECTOR_MEMORY_RUN") != "1" {
		t.Skip("collector memory benchmark is opt-in")
	}
	cfg, err := loadCollectorMemoryConfig()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
		t.Fatalf("create out dir: %v", err)
	}
	runDir := filepath.Join(cfg.OutDir, time.Now().Format("20060102-150405.000000000"))
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatalf("create fresh run dir: %v", err)
	}

	test := With(t)
	g := NewWithT(t)
	report := &collectorMemoryReport{SchemaVersion: 1, Config: cfg, StartedAt: time.Now()}
	report.ClockSkew.Before, err = measureKindClockSkew(cfg.KindNode)
	if err != nil {
		t.Fatalf("measure pre-run host/Kind clock skew: %v", err)
	}
	if err := validateCollectorClockSkew(report.ClockSkew.Before); err != nil {
		t.Fatalf("pre-run host/Kind clock skew gate: %v", err)
	}
	cgroups := newCgroupSampler(cfg.KindNode)
	defer func() {
		cgroups.Stop()
		report.CgroupSampler = cgroups.Status()
		report.EndedAt = time.Now()
		if err := cgroups.WriteCSV(filepath.Join(runDir, "cgroup_samples.csv")); err != nil {
			t.Errorf("write legacy cgroup CSV: %v", err)
		}
		if err := cgroups.WriteMemoryDetailCSV(filepath.Join(runDir, "collector_memory_samples.csv")); err != nil {
			t.Errorf("write detailed memory CSV: %v", err)
		}
		if err := cgroups.WriteEventSpoolCSV(filepath.Join(runDir, "event_spool_samples.csv")); err != nil {
			t.Errorf("write event spool CSV: %v", err)
		}
		if err := writeCollectorMemoryReport(filepath.Join(runDir, "collector-memory-report.json"), report); err != nil {
			t.Errorf("write collector memory report: %v", err)
		}
	}()

	s3Client := ensureBenchS3Client(t, collectorMemoryS3LocalPort)
	g.Expect(ensureBenchmarkS3Bucket(s3Client)).To(Succeed())
	baseline, err := takeBucketSnapshot(s3Client, benchmarkS3BucketName)
	g.Expect(err).NotTo(HaveOccurred())
	stopWatchdog := startS3Watchdog(t, s3Client, benchmarkS3BucketName, collectorMemoryS3LocalPort)
	defer stopWatchdog()

	namespace := test.NewTestNamespace()
	report.Namespace = namespace.Name
	report.NamespaceUID = string(namespace.UID)
	cgroups.Start(test)
	rayCluster := applyCollectorMemoryCluster(test, g, namespace, cfg)
	report.ClusterName = rayCluster.Name
	cgroups.RegisterPods(test, namespace.Name)
	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	report.SessionName = GetSessionIDFromHeadPod(test, g, rayCluster)
	nodeID := GetNodeIDFromPod(test, g, HeadPod(test, rayCluster), "ray-head")
	logFollowers := startCollectorLogFollowers(test, namespace.Name)
	targetPrefix := clusterlogs.SessionDir("log", "", "", namespace.Name, rayCluster.Name, report.SessionName) +
		"/job_events/" + collectorMemoryJobID + "/"
	preIngestSnapshot, err := takeBucketSnapshot(s3Client, benchmarkS3BucketName)
	g.Expect(err).NotTo(HaveOccurred())
	report.RemotePreflight = collectorMemoryRemotePreflight(preIngestSnapshot, targetPrefix)
	g.Expect(report.RemotePreflight.Empty).To(BeTrue(),
		"synthetic event prefix must be empty before ingest; stale objects could spoof reconciliation")

	markCollectorMemoryPhase(report, "baseline")
	time.Sleep(collectorMemoryBaseline)
	ingestStartSnapshot, err := takeBucketSnapshot(s3Client, benchmarkS3BucketName)
	g.Expect(err).NotTo(HaveOccurred())
	markCollectorMemoryPhase(report, "ingest")
	replay, replayErr := runCollectorReplay(cfg, namespace.Name, headPod.Name, report.SessionName, nodeID, runDir)
	report.Replay = replay
	ingestEndSnapshot, err := takeBucketSnapshot(s3Client, benchmarkS3BucketName)
	g.Expect(err).NotTo(HaveOccurred())
	markCollectorMemoryPhase(report, "idle")
	if cfg.Idle > 0 {
		time.Sleep(cfg.Idle)
	}
	idleEndSnapshot, err := takeBucketSnapshot(s3Client, benchmarkS3BucketName)
	g.Expect(err).NotTo(HaveOccurred())
	report.Runtime, err = currentCollectorMemoryRuntime(test, namespace.Name, headPod.Name)
	g.Expect(err).NotTo(HaveOccurred())
	report.RayRuntime, err = currentRayMemoryRuntime(test, namespace.Name, headPod.Name)
	g.Expect(err).NotTo(HaveOccurred())

	markCollectorMemoryPhase(report, "shutdown")
	DeleteRayClusterAndWait(test, g, namespace.Name, rayCluster.Name)
	g.Eventually(func(gg Gomega) {
		pods, listErr := test.Client().Core().CoreV1().Pods(namespace.Name).List(test.Ctx(), metav1.ListOptions{
			LabelSelector: "test=raycluster-historyserver",
		})
		gg.Expect(listErr).NotTo(HaveOccurred())
		gg.Expect(pods.Items).To(BeEmpty())
	}, TestTimeoutMedium).Should(Succeed())
	report.CollectorLogs = logFollowers.CollectAfterTermination(30 * time.Second)
	attachCollectorCgroupMemoryEvidence(report.CollectorLogs, cgroups)
	shutdownEndSnapshot, err := takeBucketSnapshot(s3Client, benchmarkS3BucketName)
	g.Expect(err).NotTo(HaveOccurred())
	markCollectorMemoryPhase(report, "complete")

	// Scan the exact session prefix, then accept only job_events/01000000 files.
	prefix := clusterlogs.SessionDir("log", "", "", namespace.Name, rayCluster.Name, report.SessionName) + "/"
	remote, err := scanCollectorReplayObjects(s3Client, benchmarkS3BucketName, prefix, collectorMemoryJobID, replay.SentEvents)
	g.Expect(err).NotTo(HaveOccurred())
	report.Remote = remote
	expectedExactKeys := map[string]struct{}{}
	report.DuringIngest = diffSnapshots("ingest", ingestStartSnapshot, ingestEndSnapshot, prefix, expectedExactKeys)
	report.DuringIdle = diffSnapshots("idle", ingestEndSnapshot, idleEndSnapshot, prefix, expectedExactKeys)
	report.DuringShutdown = diffSnapshots("shutdown", idleEndSnapshot, shutdownEndSnapshot, prefix, expectedExactKeys)
	report.FullLifecycle = diffSnapshots("full-lifecycle", baseline, shutdownEndSnapshot, prefix, expectedExactKeys)
	cgroups.Stop()
	report.CgroupSampler = cgroups.Status()
	detail := cgroups.MemoryDetailEvidence(report.Runtime.ContainerID)
	report.MemoryDetail = collectorMemoryDetailGate{
		Observed: detail.Observed, Samples: detail.Samples, ReadErrors: detail.ReadErrors,
		ReadErrorFields: append([]string(nil), detail.ReadErrorFields...),
	}
	spoolSamples := cgroups.EventSpoolSamples(report.Runtime.PodUID)
	report.EventSpool.Samples = len(spoolSamples)
	for _, sample := range spoolSamples {
		if !sample.Valid {
			report.EventSpool.InvalidSamples++
		}
	}
	report.ClockSkew.After, err = measureKindClockSkew(cfg.KindNode)
	if err != nil {
		t.Fatalf("measure post-run host/Kind clock skew: %v", err)
	}
	if err := validateCollectorClockSkew(report.ClockSkew.After); err != nil {
		t.Fatalf("post-run host/Kind clock skew gate: %v", err)
	}

	if replayErr != nil {
		report.Error = replayErr.Error()
		t.Fatalf("collector replay: %v", replayErr)
	}
	if err := validateCollectorMemoryRunInProcess(report); err != nil {
		report.Error = err.Error()
		t.Fatalf("collector memory evidence: %v", err)
	}
	report.Completed = true
}

func measureKindClockSkew(node string) (collectorClockSkewSample, error) {
	sample := collectorClockSkewSample{HostBeforeUnixNano: time.Now().UnixNano()}
	output, err := exec.Command("docker", "exec", node, "date", "+%s%N").Output()
	sample.HostAfterUnixNano = time.Now().UnixNano()
	if err != nil {
		return sample, fmt.Errorf("read Kind node clock: %w", err)
	}
	nodeTime, err := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
	if err != nil || nodeTime <= 0 {
		return sample, fmt.Errorf("parse Kind node clock %q: %w", strings.TrimSpace(string(output)), err)
	}
	sample.NodeUnixNano = nodeTime
	sample.RoundTripNano = sample.HostAfterUnixNano - sample.HostBeforeUnixNano
	midpoint := sample.HostBeforeUnixNano + sample.RoundTripNano/2
	sample.SkewNano = sample.NodeUnixNano - midpoint
	return sample, nil
}

func validateCollectorClockSkew(sample collectorClockSkewSample) error {
	lowerBound := sample.NodeUnixNano - sample.HostAfterUnixNano
	upperBound := sample.NodeUnixNano - sample.HostBeforeUnixNano
	midpoint := sample.HostBeforeUnixNano + (sample.HostAfterUnixNano-sample.HostBeforeUnixNano)/2
	if sample.HostBeforeUnixNano <= 0 || sample.NodeUnixNano <= 0 ||
		sample.HostAfterUnixNano < sample.HostBeforeUnixNano ||
		sample.RoundTripNano != sample.HostAfterUnixNano-sample.HostBeforeUnixNano ||
		sample.SkewNano != sample.NodeUnixNano-midpoint ||
		sample.RoundTripNano <= 0 ||
		sample.RoundTripNano > 250*int64(time.Millisecond) ||
		lowerBound < -int64(time.Second) || lowerBound > int64(time.Second) ||
		upperBound < -int64(time.Second) || upperBound > int64(time.Second) {
		return fmt.Errorf("host/Kind clock evidence outside 1s gate: %#v", sample)
	}
	return nil
}

func collectorMemoryRemotePreflight(snapshot bucketSnapshot, prefix string) collectorRemotePreflight {
	evidence := collectorRemotePreflight{Prefix: prefix, Empty: true}
	for key, object := range snapshot {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		evidence.Objects++
		evidence.Bytes += object.Size
	}
	evidence.Empty = evidence.Objects == 0
	return evidence
}

func currentCollectorMemoryRuntime(test Test, namespace, podName string) (collectorRuntimeEvidence, error) {
	pod, err := test.Client().Core().CoreV1().Pods(namespace).Get(test.Ctx(), podName, metav1.GetOptions{})
	if err != nil {
		return collectorRuntimeEvidence{}, err
	}
	identity := collectorContainerIdentity(*pod)
	grace := int64(0)
	if pod.Spec.TerminationGracePeriodSeconds != nil {
		grace = *pod.Spec.TerminationGracePeriodSeconds
	}
	return collectorRuntimeEvidence{
		Pod: podName, PodUID: string(pod.UID), Image: identity.image, ImageID: identity.imageID,
		ContainerID: identity.containerID, RestartCount: identity.restartCount,
		CPURequest: identity.cpuRequest, CPULimit: identity.cpuLimit,
		MemoryRequest: identity.memoryRequest, MemoryLimit: identity.memoryLimit,
		TerminationGracePeriodSeconds: grace,
	}, nil
}

func currentRayMemoryRuntime(test Test, namespace, podName string) (collectorRayRuntimeEvidence, error) {
	pod, err := test.Client().Core().CoreV1().Pods(namespace).Get(test.Ctx(), podName, metav1.GetOptions{})
	if err != nil {
		return collectorRayRuntimeEvidence{}, err
	}
	evidence := collectorRayRuntimeEvidence{Pod: podName, PodUID: string(pod.UID)}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != "ray-head" {
			continue
		}
		evidence.Image = status.Image
		evidence.ImageID = status.ImageID
		evidence.ContainerID = bareContainerID(status.ContainerID)
		evidence.RestartCount = status.RestartCount
		return evidence, nil
	}
	return evidence, fmt.Errorf("ray-head runtime status missing from %s/%s", namespace, podName)
}

func applyCollectorMemoryCluster(test Test, g *WithT, namespace *corev1.Namespace, cfg collectorMemoryConfig) *rayv1.RayCluster {
	rayCluster := DeserializeRayClusterYAML(test, RayClusterManifestPath)
	rayCluster.Namespace = namespace.Name
	rayCluster.Spec.WorkerGroupSpecs = nil
	rayCluster.Spec.HeadGroupSpec.RayStartParams["num-cpus"] = "0"
	terminationGrace := int64(120)
	rayCluster.Spec.HeadGroupSpec.Template.Spec.TerminationGracePeriodSeconds = &terminationGrace
	benchCfg := benchConfig{
		RayImage:               collectorMemoryRayImage,
		Compression:            true,
		CollectorCPURequest:    cfg.CPURequest,
		CollectorCPU:           cfg.CPULimit,
		CollectorMemoryRequest: cfg.MemoryRequest,
		CollectorMemoryLimit:   cfg.MemoryLimit,
		CollectorEnv: strings.Join([]string{
			"RAY_COLLECTOR_EVENT_MAX_DISK_MB=" + strconv.Itoa(collectorMemoryMaxDiskMiB),
			"RAY_COLLECTOR_EVENT_INGRESS_METRICS_WINDOW=10s",
		}, ","),
	}
	applyBenchRaySettings(&rayCluster.Spec, benchCfg)
	injectBenchCollectorSettings(rayCluster.Spec.HeadGroupSpec.Template.Spec.Containers, rayCluster.Name, namespace.Name, benchCfg)
	for i := range rayCluster.Spec.HeadGroupSpec.Template.Spec.Containers {
		container := &rayCluster.Spec.HeadGroupSpec.Template.Spec.Containers[i]
		switch container.Name {
		case "ray-head":
			upsertEnv(container, "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SERVICE", "false", nil)
		case "collector":
			for j := range container.Command {
				if container.Command[j] == "--role=Head" {
					container.Command[j] = "--role=Worker"
				}
			}
			container.Command = append(container.Command, "--push-interval=1h")
		}
	}
	created, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Create(test.Ctx(), rayCluster, metav1.CreateOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Eventually(RayCluster(test, created.Namespace, created.Name), TestTimeoutLong).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))
	g.Eventually(HeadPod(test, created), TestTimeoutMedium).
		Should(WithTransform(IsPodRunningAndReady, BeTrue()))
	return created
}

func markCollectorMemoryPhase(report *collectorMemoryReport, name string) {
	report.Phases = append(report.Phases, collectorMemoryPhase{Name: name, TimeNano: time.Now().UnixNano()})
}

func runCollectorReplay(cfg collectorMemoryConfig, namespace, pod, session, nodeID, runDir string) (collectorReplaySummary, error) {
	scriptPath, err := collectorReplayScriptPath()
	if err != nil {
		return collectorReplaySummary{}, err
	}
	script, err := os.Open(scriptPath)
	if err != nil {
		return collectorReplaySummary{}, fmt.Errorf("open replay script: %w", err)
	}
	defer script.Close()
	args := []string{
		"exec", "-n", namespace, "-i", pod, "-c", "ray-head", "--", "python", "-",
		"--endpoint", "http://127.0.0.1:8084/v1/events",
		"--rate", strconv.Itoa(cfg.RateEventsSec),
		"--duration", strconv.FormatFloat(cfg.Ingest.Seconds(), 'f', 0, 64),
		"--batch-size", strconv.Itoa(collectorMemoryBatchSize),
		"--session", session,
		"--node-id-hex", nodeID,
		"--job-id-hex", collectorMemoryJobID,
	}
	cmd := exec.Command("kubectl", args...)
	cmd.Stdin = script
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	_ = os.WriteFile(filepath.Join(runDir, "replay.stdout"), []byte(stdout.String()), 0o644)
	_ = os.WriteFile(filepath.Join(runDir, "replay.stderr"), []byte(stderr.String()), 0o644)
	if err != nil {
		return collectorReplaySummary{}, fmt.Errorf("kubectl exec replay: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) == 0 {
		return collectorReplaySummary{}, fmt.Errorf("replay produced no summary")
	}
	var summary collectorReplaySummary
	var raw struct {
		SchemaVersion string `json:"schemaVersion"`
		Success       bool   `json:"success"`
		Failure       string `json:"failure"`
		Configuration struct {
			Rate             int `json:"rateEventsPerSecond"`
			BatchSize        int `json:"batchSize"`
			SerialPublishers int `json:"serialPublishers"`
			HiddenRetries    int `json:"hiddenRetries"`
		} `json:"configuration"`
		Calibration struct {
			JSONLBytesPerEvent  float64 `json:"jsonlBytesPerEvent"`
			GzipRatio           float64 `json:"gzipRatio"`
			GzipRatioDefinition string  `json:"gzipRatioDefinition"`
		} `json:"calibration"`
		ScheduledEvents         int     `json:"scheduledEvents"`
		AttemptedEvents         int     `json:"attemptedEvents"`
		AcknowledgedEvents      int     `json:"acknowledgedEvents"`
		Batches                 int     `json:"batches"`
		RequestBytes            int64   `json:"requestBytes"`
		RawJSONLBytes           int64   `json:"rawJSONLBytes"`
		ActualDurationSeconds   float64 `json:"actualDurationSeconds"`
		AchievedEventsPerSecond float64 `json:"achievedEventsPerSecond"`
		RequestLatency          struct {
			P50 float64 `json:"p50"`
			P95 float64 `json:"p95"`
			P99 float64 `json:"p99"`
			Max float64 `json:"max"`
		} `json:"requestLatencyMs"`
		StatusCounts map[string]int `json:"statusCounts"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &raw); err != nil {
		return summary, fmt.Errorf("decode replay summary: %w", err)
	}
	if !raw.Success {
		return summary, fmt.Errorf("replay summary reports failure: %s", raw.Failure)
	}
	if raw.SchemaVersion != "collector-event-replay-v1" || raw.Configuration.Rate != cfg.RateEventsSec ||
		raw.Configuration.BatchSize != collectorMemoryBatchSize || raw.Configuration.SerialPublishers != 1 ||
		raw.Configuration.HiddenRetries != 0 || raw.Calibration.JSONLBytesPerEvent != 895 ||
		raw.Calibration.GzipRatioDefinition != "compressed_bytes/raw_jsonl_bytes" ||
		raw.Calibration.GzipRatio <= 0 || raw.Calibration.GzipRatio >= 1 {
		return summary, fmt.Errorf("replay contract drift: %#v", raw)
	}
	summary = collectorReplaySummary{
		SchemaVersion: 1, TargetEventsPerSecond: raw.Configuration.Rate,
		BatchSize: raw.Configuration.BatchSize, PlannedEvents: raw.ScheduledEvents,
		SentEvents: raw.AttemptedEvents, AcceptedEvents: raw.AcknowledgedEvents,
		Requests: raw.Batches, AcceptedRequests: raw.StatusCounts["200"],
		BodyBytes: raw.RequestBytes, ExpectedJSONLBytes: raw.RawJSONLBytes,
		JSONLBytesPerEvent: int(raw.Calibration.JSONLBytesPerEvent), FixtureGzipRatio: raw.Calibration.GzipRatio,
		FixtureGzipRatioDefinition: raw.Calibration.GzipRatioDefinition,
		WallSeconds:                raw.ActualDurationSeconds, AchievedEventsPerSecond: raw.AchievedEventsPerSecond,
		LatencyP50Millis: raw.RequestLatency.P50, LatencyP95Millis: raw.RequestLatency.P95,
		LatencyP99Millis: raw.RequestLatency.P99, LatencyMaxMillis: raw.RequestLatency.Max,
		Non200Responses: raw.Batches - raw.StatusCounts["200"], Retries: 0,
	}
	return summary, nil
}

func collectorReplayScriptPath() (string, error) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok || sourceFile == "" {
		return "", fmt.Errorf("resolve collector replay script: caller source path unavailable")
	}
	scriptPath := filepath.Join(filepath.Dir(sourceFile), "sweeps", "collector_event_replay.py")
	info, err := os.Lstat(scriptPath)
	if err != nil {
		return "", fmt.Errorf("resolve collector replay script %q: %w", scriptPath, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("resolve collector replay script %q: expected regular file, got %s", scriptPath, info.Mode())
	}
	return scriptPath, nil
}

func scanCollectorReplayObjects(client *s3.S3, bucket, prefix, jobID string, sent int) (collectorRemoteEvidence, error) {
	evidence := collectorRemoteEvidence{Prefix: prefix}
	ids := map[string]struct{}{}
	expectedIDs := make(map[string]struct{}, sent)
	for index := 0; index < sent; index++ {
		expectedIDs[collectorMemoryExpectedEventID(index)] = struct{}{}
	}
	wantPrefix := "/job_events/" + jobID + "/"
	var scanErr error
	listErr := client.ListObjectsV2Pages(&s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)},
		func(page *s3.ListObjectsV2Output, _ bool) bool {
			for _, object := range page.Contents {
				key := aws.StringValue(object.Key)
				if !strings.Contains(key, wantPrefix) || (!strings.HasSuffix(key, ".jsonl") && !strings.HasSuffix(key, ".jsonl.gz")) {
					continue
				}
				evidence.Objects++
				evidence.StoredBytes += aws.Int64Value(object.Size)
				output, getErr := client.GetObject(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
				if getErr != nil {
					scanErr = getErr
					return false
				}
				var reader io.Reader = output.Body
				var gz *gzip.Reader
				if strings.HasSuffix(key, ".gz") {
					gz, getErr = gzip.NewReader(output.Body)
					if getErr != nil {
						output.Body.Close()
						scanErr = getErr
						return false
					}
					reader = gz
				}
				scanner := bufio.NewScanner(reader)
				scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
				for scanner.Scan() {
					line := append([]byte(nil), scanner.Bytes()...)
					evidence.Lines++
					evidence.RawJSONLBytes += int64(len(line) + 1)
					var event map[string]any
					if json.Unmarshal(line, &event) != nil {
						evidence.MalformedLines++
						continue
					}
					id, ok := event["eventId"].(string)
					decodedID, decodeErr := base64.StdEncoding.DecodeString(id)
					_, expected := expectedIDs[id]
					if !ok || decodeErr != nil || len(decodedID) != 16 || !expected {
						evidence.UnexpectedEventIDs++
						continue
					}
					if _, duplicate := ids[id]; duplicate {
						evidence.DuplicateEventIDs++
					} else {
						ids[id] = struct{}{}
					}
				}
				if err := scanner.Err(); err != nil && scanErr == nil {
					scanErr = err
				}
				if gz != nil {
					gz.Close()
				}
				output.Body.Close()
				if scanErr != nil {
					return false
				}
			}
			return true
		})
	if listErr != nil {
		return evidence, listErr
	}
	if scanErr != nil {
		return evidence, scanErr
	}
	evidence.UniqueEventIDs = len(ids)
	if evidence.UniqueEventIDs > sent {
		evidence.UnexpectedEventIDs += evidence.UniqueEventIDs - sent
	}
	return evidence, nil
}

func collectorMemoryExpectedEventID(index int) string {
	prefix := make([]byte, 0, 8+8+len("event")+4)
	word := make([]byte, 8)
	binary.BigEndian.PutUint64(word, 1)
	prefix = append(prefix, word...)
	binary.BigEndian.PutUint64(word, uint64(index))
	prefix = append(prefix, word...)
	prefix = append(prefix, "event"...)
	counter := make([]byte, 4)
	binary.BigEndian.PutUint32(counter, 0)
	prefix = append(prefix, counter...)
	digest := sha256.Sum256(prefix)
	return base64.StdEncoding.EncodeToString(digest[:16])
}

func validateCollectorMemoryRunInProcess(report *collectorMemoryReport) error {
	if err := validateCollectorClockSkew(report.ClockSkew.Before); err != nil {
		return fmt.Errorf("pre-run clock skew: %w", err)
	}
	if err := validateCollectorClockSkew(report.ClockSkew.After); err != nil {
		return fmt.Errorf("post-run clock skew: %w", err)
	}
	if !report.RemotePreflight.Empty || report.RemotePreflight.Prefix == "" ||
		report.RemotePreflight.Objects != 0 || report.RemotePreflight.Bytes != 0 {
		return fmt.Errorf("synthetic remote prefix was not empty before ingest: %#v", report.RemotePreflight)
	}
	if report.Replay.SchemaVersion != 1 || report.Replay.BatchSize != collectorMemoryBatchSize ||
		report.Replay.SentEvents != report.Replay.AcceptedEvents || report.Replay.Non200Responses != 0 || report.Replay.Retries != 0 {
		return fmt.Errorf("invalid replay summary: %#v", report.Replay)
	}
	if report.Replay.PlannedEvents != report.Replay.SentEvents || report.Replay.AcceptedRequests != report.Replay.Requests ||
		report.Replay.ExpectedJSONLBytes != int64(report.Replay.AcceptedEvents*report.Replay.JSONLBytesPerEvent) ||
		report.Replay.BodyBytes != report.Replay.ExpectedJSONLBytes+int64(report.Replay.Requests) {
		return fmt.Errorf("replay byte/count invariant failed: %#v", report.Replay)
	}
	rateError := absFloat(report.Replay.AchievedEventsPerSecond-float64(report.Config.RateEventsSec)) / float64(report.Config.RateEventsSec)
	if rateError > 0.05 {
		return fmt.Errorf("achieved rate %.3f differs from target %d by %.2f%%", report.Replay.AchievedEventsPerSecond, report.Config.RateEventsSec, rateError*100)
	}
	if report.Remote.Lines != report.Replay.AcceptedEvents || report.Remote.UniqueEventIDs != report.Replay.AcceptedEvents ||
		report.Remote.DuplicateEventIDs != 0 || report.Remote.MalformedLines != 0 || report.Remote.UnexpectedEventIDs != 0 ||
		report.Remote.RawJSONLBytes != report.Replay.ExpectedJSONLBytes {
		return fmt.Errorf("remote event reconciliation failed: remote=%#v replay=%#v", report.Remote, report.Replay)
	}
	if len(report.CollectorLogs) != 1 {
		return fmt.Errorf("collector log streams=%d, want 1", len(report.CollectorLogs))
	}
	collector := report.CollectorLogs[0]
	var ingressBatches, ingressEvents, ingressBytes, ingressRejected, ingressQueueFull int64
	for _, window := range collector.IngressWindows {
		ingressBatches += window.Batches
		ingressEvents += window.Events
		ingressBytes += window.Bytes
		ingressRejected += window.RejectedRequests + window.RejectedDraining + window.RejectedDiskPressure +
			window.RejectedBadRequest + window.RejectedInternal
		ingressQueueFull += window.RotationQueueFull
	}
	if !collector.GracefulShutdownComplete || !collector.LogStreamComplete || collector.RestartCount != 0 ||
		collector.DiskPressure503s != 0 || collector.RotationQueueFul != 0 || collector.UploadFailures != 0 ||
		ingressBatches != int64(report.Replay.Requests) || ingressEvents != int64(report.Replay.AcceptedEvents) ||
		ingressBytes != report.Replay.BodyBytes || ingressRejected != 0 || ingressQueueFull != 0 ||
		collector.UploadedBytes != report.Replay.ExpectedJSONLBytes ||
		collector.CPURequest != report.Config.CPURequest || collector.CPULimit != report.Config.CPULimit ||
		collector.MemoryRequest != report.Config.MemoryRequest || collector.MemoryLimit != report.Config.MemoryLimit {
		return fmt.Errorf("collector runtime/log gate failed: %#v", collector)
	}
	if report.Runtime.RestartCount != 0 || report.Runtime.ContainerID == "" || report.Runtime.ImageID == "" ||
		report.Runtime.CPURequest != report.Config.CPURequest || report.Runtime.CPULimit != report.Config.CPULimit ||
		report.Runtime.MemoryRequest != report.Config.MemoryRequest || report.Runtime.MemoryLimit != report.Config.MemoryLimit ||
		report.Runtime.ContainerID != collector.ContainerID || report.Runtime.ImageID != collector.ImageID ||
		report.Runtime.TerminationGracePeriodSeconds != 120 {
		return fmt.Errorf("pre-shutdown runtime identity gate failed: %#v", report.Runtime)
	}
	if report.RayRuntime.RestartCount != 0 || report.RayRuntime.ContainerID == "" || report.RayRuntime.ImageID == "" ||
		report.RayRuntime.PodUID != report.Runtime.PodUID {
		return fmt.Errorf("pre-shutdown Ray runtime identity gate failed: %#v", report.RayRuntime)
	}
	if !report.CgroupSampler.StreamComplete || !report.MemoryDetail.Observed || report.MemoryDetail.Samples < 4 ||
		report.MemoryDetail.ReadErrors != 0 || report.EventSpool.Samples < 4 || report.EventSpool.InvalidSamples != 0 ||
		!collector.CgroupMemoryObserved || collector.CgroupMemoryReadErrors != 0 ||
		collector.MemoryEventsOOM != 0 || collector.MemoryEventsOOMKill != 0 {
		return fmt.Errorf("cgroup/spool evidence gate failed: sampler=%#v detail=%#v spool=%#v collector=%#v",
			report.CgroupSampler, report.MemoryDetail, report.EventSpool, collector)
	}
	for _, diff := range []SnapshotDiff{report.DuringIngest, report.DuringIdle, report.DuringShutdown, report.FullLifecycle} {
		if diff.ChangedObjects != 0 || diff.DeletedObjects != 0 || len(diff.UnexpectedKeys) != 0 ||
			len(diff.UnexpectedChangedKeys) != 0 || len(diff.DeletedKeys) != 0 {
			return fmt.Errorf("bucket isolation failed for %s: %#v", diff.Label, diff)
		}
	}
	return nil
}

func writeCollectorMemoryReport(filename string, report *collectorMemoryReport) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func absFloat(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

func TestLoadCollectorMemoryConfigFailsClosed(t *testing.T) {
	base := map[string]string{
		"BENCH_COLLECTOR_MEMORY_ARM":           "continuous-rate2000-r1",
		"BENCH_COLLECTOR_MEMORY_MATRIX_SHA256": strings.Repeat("0", 64),
		"BENCH_COLLECTOR_MEMORY_KIND":          "continuous",
		"BENCH_COLLECTOR_EVENT_RATE":           "2000",
		"BENCH_COLLECTOR_INGEST_DURATION":      "90s",
		"BENCH_COLLECTOR_IDLE_DURATION":        "15s",
		"BENCH_COLLECTOR_MEMORY_REPEAT":        "1",
		"BENCH_COLLECTOR_CPU_REQUEST":          "100m",
		"BENCH_COLLECTOR_CPU_LIMIT":            "2",
		"BENCH_COLLECTOR_MEMORY_REQUEST":       "128Mi",
		"BENCH_COLLECTOR_MEMORY_LIMIT":         "1Gi",
		"BENCH_KIND_NODE":                      "bench-control-plane",
		"BENCH_OUT_DIR":                        "/private/tmp/test",
	}
	keys := make([]string, 0, len(base))
	for key := range base {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		t.Setenv(key, base[key])
	}
	if _, err := loadCollectorMemoryConfig(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	for _, attack := range []struct{ key, value string }{
		{"BENCH_COLLECTOR_INGEST_DURATION", "60s"},
		{"BENCH_COLLECTOR_CPU_LIMIT", "4"},
		{"BENCH_COLLECTOR_MEMORY_LIMIT", "2Gi"},
		{"BENCH_KIND_NODE", "other"},
	} {
		t.Run(attack.key, func(t *testing.T) {
			for _, key := range keys {
				t.Setenv(key, base[key])
			}
			t.Setenv(attack.key, attack.value)
			if _, err := loadCollectorMemoryConfig(); err == nil {
				t.Fatalf("accepted %s=%q", attack.key, attack.value)
			}
		})
	}
}

func TestValidateCollectorMemoryRunRejectsRateAndReconciliationDrift(t *testing.T) {
	report := &collectorMemoryReport{
		Config:          collectorMemoryConfig{RateEventsSec: 2000, CPURequest: "100m", CPULimit: "2", MemoryRequest: "128Mi", MemoryLimit: "1Gi"},
		RemotePreflight: collectorRemotePreflight{Prefix: "fresh-prefix/", Empty: true},
		Replay:          collectorReplaySummary{SchemaVersion: 1, BatchSize: 1000, PlannedEvents: 2000, SentEvents: 2000, AcceptedEvents: 2000, Requests: 2, AcceptedRequests: 2, BodyBytes: 20002, ExpectedJSONLBytes: 20000, JSONLBytesPerEvent: 10, AchievedEventsPerSecond: 2000},
		Remote:          collectorRemoteEvidence{Lines: 2000, UniqueEventIDs: 2000, RawJSONLBytes: 20000},
		CollectorLogs:   []CollectorLogStat{{GracefulShutdownComplete: true, LogStreamComplete: true, CPURequest: "100m", CPULimit: "2", MemoryRequest: "128Mi", MemoryLimit: "1Gi", CgroupMemoryObserved: true, UploadedBytes: 20000, IngressWindows: []CollectorIngressWindow{{Batches: 2, Events: 2000, Bytes: 20002}}}},
		Runtime:         collectorRuntimeEvidence{ImageID: "sha256:test", ContainerID: "container", CPURequest: "100m", CPULimit: "2", MemoryRequest: "128Mi", MemoryLimit: "1Gi", TerminationGracePeriodSeconds: 120},
		RayRuntime:      collectorRayRuntimeEvidence{PodUID: "pod-uid", ImageID: "sha256:ray", ContainerID: "ray-container"},
		CgroupSampler:   CgroupSamplerStatus{StreamComplete: true},
		MemoryDetail:    collectorMemoryDetailGate{Observed: true, Samples: 4},
		EventSpool:      collectorEventSpoolGate{Samples: 4},
		ClockSkew: collectorClockSkewEvidence{
			Before: collectorClockSkewSample{HostBeforeUnixNano: 1_000_000_000, NodeUnixNano: 1_050_000_000, HostAfterUnixNano: 1_100_000_000, RoundTripNano: 100_000_000},
			After:  collectorClockSkewSample{HostBeforeUnixNano: 2_000_000_000, NodeUnixNano: 2_050_000_000, HostAfterUnixNano: 2_100_000_000, RoundTripNano: 100_000_000},
		},
	}
	report.CollectorLogs[0].ImageID = "sha256:test"
	report.CollectorLogs[0].ContainerID = "container"
	report.Runtime.PodUID = "pod-uid"
	if err := validateCollectorMemoryRunInProcess(report); err != nil {
		t.Fatalf("valid report rejected: %v", err)
	}
	report.Remote.Lines--
	if err := validateCollectorMemoryRunInProcess(report); err == nil {
		t.Fatal("line loss was accepted")
	}
	report.Remote.Lines++
	report.Replay.AchievedEventsPerSecond = 1800
	if err := validateCollectorMemoryRunInProcess(report); err == nil {
		t.Fatal("rate drift was accepted")
	}
	report.Replay.AchievedEventsPerSecond = 2000
	report.RemotePreflight = collectorRemotePreflight{Prefix: "fresh-prefix/", Objects: 1, Bytes: 10, Empty: false}
	if err := validateCollectorMemoryRunInProcess(report); err == nil {
		t.Fatal("stale objects in the synthetic remote prefix were accepted")
	}
}

func TestValidateCollectorClockSkewFailsClosed(t *testing.T) {
	valid := collectorClockSkewSample{
		HostBeforeUnixNano: 1_000_000_000,
		NodeUnixNano:       1_050_000_000,
		HostAfterUnixNano:  1_100_000_000,
		RoundTripNano:      100_000_000,
		SkewNano:           0,
	}
	if err := validateCollectorClockSkew(valid); err != nil {
		t.Fatalf("valid clock sample rejected: %v", err)
	}
	for name, mutate := range map[string]func(*collectorClockSkewSample){
		"skew":       func(sample *collectorClockSkewSample) { sample.SkewNano = int64(time.Second) + 1 },
		"round-trip": func(sample *collectorClockSkewSample) { sample.RoundTripNano = int64(time.Second) + 1 },
		"negative-round-trip": func(sample *collectorClockSkewSample) {
			sample.HostAfterUnixNano = sample.HostBeforeUnixNano - 1
			sample.RoundTripNano = -1
			sample.SkewNano = sample.NodeUnixNano - sample.HostBeforeUnixNano
		},
		"inconsistent": func(sample *collectorClockSkewSample) {
			sample.HostAfterUnixNano++
		},
	} {
		t.Run(name, func(t *testing.T) {
			attack := valid
			mutate(&attack)
			if err := validateCollectorClockSkew(attack); err == nil {
				t.Fatalf("accepted invalid clock sample: %#v", attack)
			}
		})
	}
}

func TestCollectorMemoryRemotePreflightScopesExactPrefix(t *testing.T) {
	snapshot := bucketSnapshot{
		"other/session/file":                 {Size: 100},
		"target/job_events/01000000/a.jsonl": {Size: 10},
		"target/job_events/other/b.jsonl":    {Size: 20},
	}
	evidence := collectorMemoryRemotePreflight(snapshot, "target/job_events/01000000/")
	if evidence.Empty || evidence.Objects != 1 || evidence.Bytes != 10 {
		t.Fatalf("unexpected preflight evidence: %#v", evidence)
	}
}

func TestCollectorMemoryExpectedEventIDMatchesReplayFixture(t *testing.T) {
	if got := collectorMemoryExpectedEventID(0); got != "xCtuFeGiPNx6FXpwi+miSg==" {
		t.Fatalf("event 0 ID = %q", got)
	}
	if got := collectorMemoryExpectedEventID(999); got != "Cpy4rEmQTgVycnXyRcbreA==" {
		t.Fatalf("event 999 ID = %q", got)
	}
}

func TestCollectorReplayScriptPathIsSourceAnchored(t *testing.T) {
	scriptPath, err := collectorReplayScriptPath()
	if err != nil {
		t.Fatalf("resolve replay script: %v", err)
	}
	if !filepath.IsAbs(scriptPath) {
		t.Fatalf("replay script path is not absolute: %q", scriptPath)
	}
	if filepath.Base(scriptPath) != "collector_event_replay.py" {
		t.Fatalf("unexpected replay script path: %q", scriptPath)
	}
}
