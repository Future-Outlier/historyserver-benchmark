// Package benchmark contains an opt-in, single-run benchmark for the History
// Server data path: one RayCluster + one RayJob submitting BENCH_TASK_COUNT
// no-op tasks, measured end to end (collector resources -> storage footprint ->
// history server load latency/memory).
//
// It is skipped unless BENCH_RUN=1 so `go test ./...` stays fast. See README.md.
package benchmark

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"

	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clustermetadata"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	. "github.com/ray-project/kuberay/historyserver/test/support"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

const benchmarkS3BucketName = "ray-historyserver-benchmark"

type benchConfig struct {
	TaskCount                                        int           // total no-op tasks the driver submits
	WaveSize                                         int           // tasks per ray.get() wave (bounds in-flight refs)
	TaskNumCPUs                                      string        // num_cpus per task; fractional raises concurrency
	Compression                                      bool          // sets RAY_COLLECTOR_EVENT_COMPRESSION_ENABLED on the collector
	RotationIntvl                                    string        // sets RAY_COLLECTOR_EVENT_ROTATION_INTERVAL (Go duration, "" = default 5m)
	WorkerMemLimit                                   string        // overrides the ray-worker container memory limit (e.g. "4G"); low num_cpus multiplies Ray worker processes
	KindNode                                         string        // kind node container name for the cgroup sampler
	HeadStatusBuffer                                 string        // sets RAY_task_events_max_num_status_events_buffer_on_worker on the HEAD Ray container only ("" = Ray default 100000)
	RayEnv                                           string        // extra env on every ray-head/ray-worker container, "K=V,K=V"; the knob for Ray-side event constants (RAY_task_events_send_batch_size)
	RayImage                                         string        // overrides the ray-head/ray-worker image; 2.56 changes the aggregator's batch cap to 1,000 and the buffer to 100,000, which moves every collector memory number
	DrainSleepSec                                    int           // exploratory-only driver delay; formal RayJob runs keep this 0 and use shutdownAfterJobFinishes + TTL
	S3Bucket                                         string        // fixed benchmark-only bucket; shared e2e tests use a different bucket
	S3LocalPort                                      int           // local port for the benchmark's own MinIO port-forward; NOT 9000, which e2e suites on other clusters fight over
	JobTimeout                                       time.Duration // wall-clock budget for the RayJob to succeed
	WarmIterations                                   int           // requests per warm history server endpoint
	HSCPURequest                                     string        // overrides the history server CPU request; empty defaults to the finite HSCPULimit so formal CPU discovery measures request=limit
	HSCPULimit                                       string        // overrides the history server container CPU limit ("none" removes it); the shipped manifest pins 500m, which the cold load saturates
	HSMemoryRequest                                  string        // overrides the history server memory request; formal HS sizing keeps request=limit to remove eviction priority as a confounder
	HSMemoryLimit                                    string        // overrides the history server memory limit ("none" removes it); CPU discovery uses an 8Gi request=limit ceiling
	HSEnv                                            string        // extra env for the history server container, "K=V,K=V" (GOMAXPROCS, GODEBUG=gctrace=1, GOGC...)
	HSArgs                                           string        // extra CLI flags for the history server, comma separated (e.g. --session-process-timeout=30m)
	HSOnly                                           string        // comma-separated "namespace/cluster/sessionID" list: skip generating data and load these stored sessions, in order, into one history server
	HSSourceObjectCount                              int           // immutable source object inventory from the formal expected matrix
	HSSourceTotalBytes                               int64         // immutable source byte inventory from the formal expected matrix
	HSSourceTaskLogMetadataAlgorithm                 string        // immutable raw-source task-log metadata canonicalization algorithm
	HSSourceTaskLogMetadataSHA256                    string        // immutable raw-source per-attempt metadata digest
	HSSourceTaskLogMetadataAttempts                  int           // immutable raw-source canonical attempt count
	HSSourceTaskLogMetadataNil                       int           // immutable raw-source attempts without TaskLogInfo
	HSSourceTaskLogMetadataPresent                   int           // immutable raw-source attempts with TaskLogInfo
	HSSourceTaskLogMetadataStructurallyInvalid       int           // immutable raw-source malformed/conflicting TaskLogInfo count
	HSSourceTaskLogMetadataIncompleteNonNil          int           // immutable raw-source nonnil metadata that cannot resolve both streams exactly
	HSSourceTaskLogMetadataStdoutExactResolvable     int           // immutable raw-source exact stdout range count
	HSSourceTaskLogMetadataStderrExactResolvable     int           // immutable raw-source exact stderr range count
	HSSourceTaskLogMetadataLegacyWholeWorkerFallback int           // immutable raw-source nil metadata with legacy worker identity
	HSStrictCold                                     bool          // formal mode: measure only the first cold request, prohibit warm-probe recovery, and require all HS correctness evidence
	HSColdSLO                                        time.Duration // maximum valid first-attempt /enter_cluster latency in formal mode
	HSQueryConcurrency                               int           // audited warm query concurrency; formal campaigns require exactly one sequential request
	HSProtocol                                       string        // formal request protocol; empty preserves the legacy combined endpoint phase
	HSPreColdIdle                                    time.Duration // no-request baseline before the isolated cold request
	HSRequestQuietGap                                time.Duration // fixed no-request gap between cold/count/detail requests in isolated-request mode
	SkipHistoryServer                                bool          // finish after Collector shutdown and storage validation without deploying a history server
	HSSessionSettle                                  time.Duration // quiet window between (and after) multi-session loads so a 1 Hz sample can catch the retained cost, not another session's transient
	ShutdownAfterJob                                 bool          // let the operator delete the cluster via shutdownAfterJobFinishes instead of deleting it here; this is what the docs tell users to do, and it removes the drain window the manual path gives
	JobTTLSeconds                                    int32         // ttlSecondsAfterFinished; the API default is 0, i.e. delete the instant the job reports finished
	CollectorCPURequest                              string        // sets resources.requests.cpu on the collector sidecars; empty preserves the sample manifest
	CollectorCPU                                     string        // sets resources.limits.cpu on the collector sidecars; retained as the CPU-limit field for existing benchmark callers
	CollectorMemoryRequest                           string        // sets resources.requests.memory on the collector sidecars; empty preserves the sample manifest
	CollectorMemoryLimit                             string        // sets resources.limits.memory on the collector sidecars; empty preserves the sample manifest
	CollectorEnv                                     string        // extra env on the collector containers, "K=V,K=V" (GODEBUG=gctrace=1 to separate live heap from GC headroom)
	TargetTaskRate                                   int           // paces the driver to this many tasks/s (0 = submit as fast as the scheduler allows), making the event rate an independent variable
	Drivers                                          int           // concurrent Ray drivers sharing TaskCount; each has its own event buffer and its own 10k events/s drain, so the aggregate rate is not capped by one driver
	HSEnterTimeout                                   time.Duration // client budget for the first /enter_cluster attempt
	HSWarmWait                                       time.Duration // total budget for warm-probe retries when the first attempt times out
	OutDir                                           string        // report + CSV destination
	SkipCleanup                                      bool          // keep the S3 bucket contents after the run
	ExecutionIdentityFile                            string        // formal runner artifact binding the exact generated namespace name and UID before cleanup starts
	HSSourceRayJobOwned                              bool          // immutable-source lineage copied into HS-only reports; formal source must use an owned RayCluster
	HSSourceShutdownAfterJob                         bool          // immutable-source lineage: shutdownAfterJobFinishes from the observed source RayJob
	HSSourceJobTTLSeconds                            int32         // immutable-source lineage: ttlSecondsAfterFinished from the observed source RayJob
	HSSourceRayJobBackoffLimit                       *int32        // immutable-source lineage: explicit outer RayJob retry limit; nil differs from zero
	HSSourceSubmitterBackoffLimit                    *int32        // immutable-source lineage: explicit submitter Kubernetes Job retry limit; nil differs from zero
}

func loadBenchConfig() benchConfig {
	return benchConfig{
		TaskCount:                        envInt("BENCH_TASK_COUNT", 50000),
		WaveSize:                         envInt("BENCH_WAVE_SIZE", 2000),
		TaskNumCPUs:                      envStr("BENCH_TASK_NUM_CPUS", "0.2"),
		Compression:                      envBool("BENCH_COMPRESSION", false),
		RotationIntvl:                    envStr("BENCH_EVENT_ROTATION_INTERVAL", ""),
		WorkerMemLimit:                   envStr("BENCH_WORKER_MEMORY_LIMIT", ""),
		KindNode:                         envStr("BENCH_KIND_NODE", "kind-control-plane"),
		HeadStatusBuffer:                 envStr("BENCH_RAY_STATUS_BUFFER_HEAD", ""),
		RayEnv:                           envStr("BENCH_RAY_ENV", ""),
		RayImage:                         envStr("BENCH_RAY_IMAGE", ""),
		DrainSleepSec:                    envInt("BENCH_DRIVER_DRAIN_SLEEP", 0),
		S3Bucket:                         benchmarkS3BucketName,
		S3LocalPort:                      envInt("BENCH_S3_LOCAL_PORT", 9002),
		JobTimeout:                       envDuration("BENCH_JOB_TIMEOUT", 45*time.Minute),
		WarmIterations:                   envInt("BENCH_WARM_ITERATIONS", 10),
		HSCPURequest:                     envStr("BENCH_HS_CPU_REQUEST", ""),
		HSCPULimit:                       envStr("BENCH_HS_CPU_LIMIT", ""),
		HSMemoryRequest:                  envStr("BENCH_HS_MEMORY_REQUEST", ""),
		HSMemoryLimit:                    envStr("BENCH_HS_MEMORY_LIMIT", ""),
		HSEnv:                            envStr("BENCH_HS_ENV", ""),
		HSArgs:                           envStr("BENCH_HS_ARGS", ""),
		HSOnly:                           envStr("BENCH_HS_ONLY", ""),
		HSSourceObjectCount:              envInt("BENCH_HS_SOURCE_OBJECT_COUNT", 0),
		HSSourceTotalBytes:               envInt64("BENCH_HS_SOURCE_TOTAL_BYTES", 0),
		HSSourceTaskLogMetadataAlgorithm: envStr("BENCH_HS_SOURCE_TASK_LOG_METADATA_ALGORITHM", ""),
		HSSourceTaskLogMetadataSHA256:    envStr("BENCH_HS_SOURCE_TASK_LOG_METADATA_SHA256", ""),
		HSSourceTaskLogMetadataAttempts:  envInt("BENCH_HS_SOURCE_TASK_LOG_METADATA_ATTEMPTS", 0),
		HSSourceTaskLogMetadataNil:       envInt("BENCH_HS_SOURCE_TASK_LOG_METADATA_NIL", 0),
		HSSourceTaskLogMetadataPresent:   envInt("BENCH_HS_SOURCE_TASK_LOG_METADATA_PRESENT", 0),
		HSSourceTaskLogMetadataStructurallyInvalid:       envInt("BENCH_HS_SOURCE_TASK_LOG_METADATA_STRUCTURALLY_INVALID", 0),
		HSSourceTaskLogMetadataIncompleteNonNil:          envInt("BENCH_HS_SOURCE_TASK_LOG_METADATA_INCOMPLETE_NON_NIL", 0),
		HSSourceTaskLogMetadataStdoutExactResolvable:     envInt("BENCH_HS_SOURCE_TASK_LOG_METADATA_STDOUT_EXACT_RESOLVABLE", 0),
		HSSourceTaskLogMetadataStderrExactResolvable:     envInt("BENCH_HS_SOURCE_TASK_LOG_METADATA_STDERR_EXACT_RESOLVABLE", 0),
		HSSourceTaskLogMetadataLegacyWholeWorkerFallback: envInt("BENCH_HS_SOURCE_TASK_LOG_METADATA_LEGACY_WHOLE_WORKER_FALLBACK", 0),
		HSStrictCold:             envBool("BENCH_HS_STRICT_COLD", false),
		HSColdSLO:                envDuration("BENCH_HS_COLD_SLO", 120*time.Second),
		HSQueryConcurrency:       envInt("BENCH_HS_QUERY_CONCURRENCY", 1),
		HSProtocol:               envStr("BENCH_HS_PROTOCOL", ""),
		HSPreColdIdle:            envDuration("BENCH_HS_PRE_COLD_IDLE", 0),
		HSRequestQuietGap:        envDuration("BENCH_HS_REQUEST_QUIET_GAP", 0),
		SkipHistoryServer:        envBool("BENCH_SKIP_HISTORY_SERVER", false),
		HSSessionSettle:          envDuration("BENCH_HS_SESSION_SETTLE", 20*time.Second),
		ShutdownAfterJob:         envBool("BENCH_SHUTDOWN_AFTER_JOB", false),
		JobTTLSeconds:            int32(envInt("BENCH_JOB_TTL_SECONDS", 0)),
		CollectorCPURequest:      envStr("BENCH_COLLECTOR_CPU_REQUEST", ""),
		CollectorCPU:             envStr("BENCH_COLLECTOR_CPU_LIMIT", ""),
		CollectorMemoryRequest:   envStr("BENCH_COLLECTOR_MEMORY_REQUEST", ""),
		CollectorMemoryLimit:     envStr("BENCH_COLLECTOR_MEMORY_LIMIT", ""),
		CollectorEnv:             envStr("BENCH_COLLECTOR_ENV", ""),
		TargetTaskRate:           envInt("BENCH_TARGET_TASK_RATE", 0),
		Drivers:                  envInt("BENCH_DRIVERS", 1),
		HSEnterTimeout:           envDuration("BENCH_HS_ENTER_TIMEOUT", 5*time.Minute),
		HSWarmWait:               envDuration("BENCH_HS_WARM_WAIT", 15*time.Minute),
		OutDir:                   envStr("BENCH_OUT_DIR", "out"),
		SkipCleanup:              envBool("BENCH_SKIP_CLEANUP", false),
		ExecutionIdentityFile:    envStr("BENCH_EXECUTION_IDENTITY_FILE", ""),
		HSSourceRayJobOwned:      envBool("BENCH_HS_SOURCE_RAYJOB_OWNED", false),
		HSSourceShutdownAfterJob: envBool("BENCH_HS_SOURCE_SHUTDOWN_AFTER_JOB", false),
		HSSourceJobTTLSeconds:    int32(envInt("BENCH_HS_SOURCE_JOB_TTL_SECONDS", 0)),
		HSSourceRayJobBackoffLimit: envInt32Pointer(
			"BENCH_HS_SOURCE_RAYJOB_BACKOFF_LIMIT",
		),
		HSSourceSubmitterBackoffLimit: envInt32Pointer(
			"BENCH_HS_SOURCE_SUBMITTER_BACKOFF_LIMIT",
		),
	}
}

type executionNamespaceIdentity struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}

func writeExecutionNamespaceIdentity(t *testing.T, cfg benchConfig, namespace *corev1.Namespace) {
	t.Helper()
	if err := writeExecutionNamespaceIdentityFile(cfg, namespace); err != nil {
		t.Fatalf("write execution namespace identity: %v", err)
	}
}

func writeExecutionNamespaceIdentityFile(cfg benchConfig, namespace *corev1.Namespace) error {
	if cfg.ExecutionIdentityFile == "" {
		return nil
	}
	if namespace == nil || namespace.Name == "" || namespace.UID == "" {
		return fmt.Errorf("execution namespace name and UID must be present")
	}
	f, err := os.OpenFile(cfg.ExecutionIdentityFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create %s: %w", cfg.ExecutionIdentityFile, err)
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	encodeErr := encoder.Encode(executionNamespaceIdentity{
		Name: namespace.Name,
		UID:  string(namespace.UID),
	})
	closeErr := f.Close()
	if encodeErr != nil {
		return fmt.Errorf("encode %s: %w", cfg.ExecutionIdentityFile, encodeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close %s: %w", cfg.ExecutionIdentityFile, closeErr)
	}
	return nil
}

func TestWriteExecutionNamespaceIdentityFileBindsNameAndUID(t *testing.T) {
	identityFile := filepath.Join(t.TempDir(), "execution-namespace.json")
	cfg := benchConfig{ExecutionIdentityFile: identityFile}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: "test-ns-formal", UID: "11111111-1111-1111-1111-111111111111",
	}}
	if err := writeExecutionNamespaceIdentityFile(cfg, namespace); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(identityFile)
	if err != nil {
		t.Fatal(err)
	}
	var identity executionNamespaceIdentity
	if err := json.Unmarshal(raw, &identity); err != nil {
		t.Fatal(err)
	}
	if identity.Name != namespace.Name || identity.UID != string(namespace.UID) {
		t.Fatalf("identity=%#v, namespace=%s/%s", identity, namespace.Name, namespace.UID)
	}
	if err := writeExecutionNamespaceIdentityFile(cfg, namespace); err == nil {
		t.Fatal("identity artifact overwrite was accepted")
	}
	namespace.UID = ""
	if err := writeExecutionNamespaceIdentityFile(
		benchConfig{ExecutionIdentityFile: filepath.Join(t.TempDir(), "execution-namespace.json")},
		namespace,
	); err == nil {
		t.Fatal("namespace with an empty UID was accepted")
	}
}

func validateOwnedRayJobDriverPolicy(cfg benchConfig, driverScript string) error {
	if !cfg.ShutdownAfterJob {
		return nil
	}
	if cfg.DrainSleepSec != 0 {
		return fmt.Errorf("owned RayJob must not use a post-job driver drain sleep")
	}

	sleepCalls := strings.Count(driverScript, "time.sleep(")
	if cfg.TargetTaskRate > 0 {
		if sleepCalls != 1 || !strings.Contains(driverScript, "time.sleep(behind)") {
			return fmt.Errorf("paced owned RayJob must contain only its target-rate pacing sleep")
		}
		return nil
	}
	if sleepCalls != 0 {
		return fmt.Errorf("unpaced owned RayJob driver must not contain sleep code")
	}
	return nil
}

func TestHistoryServerBenchmark(t *testing.T) {
	if os.Getenv("BENCH_RUN") == "" {
		t.Skip("benchmark is opt-in: set BENCH_RUN=1 (see test/benchmark/README.md)")
	}
	cfg := loadBenchConfig()
	if cfg.S3Bucket != benchmarkS3BucketName {
		t.Fatalf("benchmark S3 bucket=%q, want fixed isolated bucket %q", cfg.S3Bucket, benchmarkS3BucketName)
	}
	runDir := filepath.Join(cfg.OutDir, time.Now().Format("20060102-150405"))
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatalf("create output dir %s: %v", runDir, err)
	}

	test := With(t)
	g := NewWithT(t)

	// Same-session mode: every cell of a comparison should read byte-identical
	// data, otherwise a difference between two history server configurations is
	// confounded with a difference between two generated sessions.
	if cfg.HSOnly != "" {
		if cfg.SkipHistoryServer {
			t.Fatal("BENCH_HS_ONLY and BENCH_SKIP_HISTORY_SERVER cannot both be set")
		}
		if cfg.HSStrictCold {
			if err := validateHSFormalConfig(cfg); err != nil {
				t.Fatalf("formal History Server config: %v", err)
			}
		}
		runHSOnly(t, test, g, cfg, runDir)
		return
	}
	if cfg.HSStrictCold {
		t.Fatal("BENCH_HS_STRICT_COLD requires BENCH_HS_ONLY")
	}

	driverScript := renderDriverScript(cfg)
	if err := validateOwnedRayJobDriverPolicy(cfg, driverScript); err != nil {
		t.Fatalf("formal RayJob driver policy: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "driver.py"), []byte(driverScript), 0o644); err != nil {
		t.Fatalf("write rendered RayJob driver: %v", err)
	}

	s3Client := ensureBenchS3Client(t, cfg.S3LocalPort)
	g.Expect(ensureBenchmarkS3Bucket(s3Client)).To(Succeed())
	// Capture the immutable baseline before creating any RayJob, RayCluster, or
	// Collector. A baseline taken after the owned RayJob starts cannot prove the
	// Collector did not mutate another session during startup.
	snapPreStart, err := takeBucketSnapshot(s3Client, cfg.S3Bucket)
	g.Expect(err).NotTo(HaveOccurred())
	stopWatchdog := startS3Watchdog(t, s3Client, cfg.S3Bucket, cfg.S3LocalPort)
	defer stopWatchdog()
	namespace := test.NewTestNamespace()
	writeExecutionNamespaceIdentity(t, cfg, namespace)

	report := &Report{Config: cfg, StartedAt: time.Now()}
	report.NamespaceUID = string(namespace.UID)
	report.Env = captureEnvInfo(test)

	sampler := newResourceSampler(test, namespace.Name, time.Second)
	cgroups := newCgroupSampler(cfg.KindNode)

	// Always dump whatever was measured, even when an assertion aborts the run:
	// a partial report is exactly what you need to debug a failed benchmark.
	defer func() {
		sampler.Stop()
		cgroups.Stop()
		attachCollectorCgroupMemoryEvidence(report.CollectorLogs, cgroups)
		report.CgroupSampler = cgroups.Status()
		report.Resources = sampler.Summarize()
		report.Cgroups = cgroups.Summarize(sampler.Marks())
		report.CollectorWindows = cgroups.AlignCollectorIngressWindows(
			report.CollectorLogs,
			report.Storage.Events.TaskLifecycleWindows,
		)
		report.CollectorIngressGates = summarizeCollectorIngressGates(
			report.CollectorLogs, report.CollectorWindows, report.CgroupSampler, report.PodTerminations)
		report.SpoolPeakMiB = cgroups.SpoolPeaks()
		if err := sampler.WriteCSV(filepath.Join(runDir, "samples.csv")); err != nil {
			t.Errorf("write samples.csv: %v", err)
		}
		if err := cgroups.WriteCSV(filepath.Join(runDir, "cgroup_samples.csv")); err != nil {
			t.Errorf("write cgroup_samples.csv: %v", err)
		}
		if err := writeCollectorResourceWindowsCSV(filepath.Join(runDir, "collector_ingress_cgroup_10s.csv"), report.CollectorWindows); err != nil {
			t.Errorf("write collector_ingress_cgroup_10s.csv: %v", err)
		}
		if err := writeCollectorIngressGatesCSV(filepath.Join(runDir, "collector_ingress_gate.csv"), report.CollectorIngressGates); err != nil {
			t.Errorf("write collector_ingress_gate.csv: %v", err)
		}
		writeReport(t, report, runDir)
	}()

	// Sampling starts before any workload exists. In owned mode the RayJob is
	// submitted first and the operator may start the driver the instant the
	// cluster reports ready, so a sampler started after that point misses the
	// ingestion peak permanently — no later analysis can recover samples that
	// were never taken. Labels are attached at read time, so containers
	// registered later still get their earlier samples labelled.
	sampler.SetPhase("setup")
	sampler.Start()
	cgroups.Start(test)

	// Phase 1: RayCluster with collector sidecars.
	//
	// Two ownership modes. Default: the harness creates the cluster and later
	// deletes it by hand. ShutdownAfterJob: the RayJob embeds the cluster spec,
	// so the operator creates the cluster AND deletes it the moment the job
	// finishes (plus TTL) — ClusterSelector mode can never exercise that path
	// because the controller refuses to delete a cluster it does not own
	// (rayjob_controller.go:429).
	// The timeline runs in BOTH modes. Without it on the control 組 there is no
	// like-for-like evidence — the control's deletion and container exits would
	// be unrecorded, so "owned exits at 137, control is fine" could not be said.
	// It also carries the RayJob's own EndTime, which is the authoritative
	// boundary for re-slicing phases offline; the harness's SetPhase calls are
	// necessarily late in owned mode, where the operator can begin deleting
	// before waitBenchJob's 5 s poll returns.
	timeline := startDeletionTimeline(test, namespace.Name, "rayjob-bench")
	defer func() {
		report.Timeline, report.PodTerminations = timeline.Stop()
		if err := timeline.WriteCSV(filepath.Join(runDir, "timeline.csv")); err != nil {
			t.Errorf("write timeline.csv: %v", err)
		}
	}()

	var rayCluster *rayv1.RayCluster
	if cfg.ShutdownAfterJob {
		ownedSpec := buildOwnedClusterSpec(test, namespace, cfg)
		job := createBenchRayJob(test, g, namespace, "", ownedSpec, cfg)
		report.RayJobLifecycle = lifecycleEvidenceFromRayJob(job)
		rayCluster = waitForOwnedCluster(test, g, namespace, job.Name)
	} else {
		rayCluster = applyBenchRayCluster(test, g, namespace, cfg)
	}
	g.Eventually(func() error {
		_, err := s3Client.HeadBucket(&s3.HeadBucketInput{Bucket: aws.String(cfg.S3Bucket)})
		return err
	}, TestTimeoutMedium).Should(Succeed(), "S3 bucket should exist")

	sessionID := GetSessionIDFromHeadPod(test, g, rayCluster)
	report.SessionID = sessionID
	report.Namespace = namespace.Name
	report.ClusterName = rayCluster.Name
	t.Logf("BENCH_HS_ONLY=%s/%s/%s  (re-measure this exact session with BENCH_SKIP_CLEANUP=1 data)",
		namespace.Name, rayCluster.Name, sessionID)
	sessionPrefix := clusterlogs.SessionDir("log", "", "", namespace.Name, rayCluster.Name, sessionID) + "/"
	markerKey := clustermetadata.EncodePath(
		utils.ClusterInfo{Namespace: namespace.Name, Name: rayCluster.Name}, "log", sessionID)

	// T0: bucket inventory before any load.
	snapT0, err := takeBucketSnapshot(s3Client, cfg.S3Bucket)
	g.Expect(err).NotTo(HaveOccurred())

	cgroups.RegisterPods(test, namespace.Name)
	logFollowers := startCollectorLogFollowers(test, namespace.Name)

	// Phase 2: the 50k-task RayJob. In owned mode it was created above (the
	// cluster could not be discovered before it existed) and may already be
	// running, so only wait here.
	sampler.SetPhase("job")
	if cfg.ShutdownAfterJob {
		report.Job = waitBenchJob(test, g, namespace.Name, "rayjob-bench", cfg)
	} else {
		report.Job = runBenchJob(test, g, namespace, rayCluster, cfg)
	}

	// T1: what rotation uploaded while the job was running.
	snapT1, err := takeBucketSnapshot(s3Client, cfg.S3Bucket)
	g.Expect(err).NotTo(HaveOccurred())

	// Phase 3: graceful deletion triggers the final flush (rotate + upload) and
	// writes the cluster-metadata session marker.
	sampler.SetPhase("flush")
	flushStart := time.Now()
	if cfg.ShutdownAfterJob {
		// The operator owns the deletion: it removes the cluster once
		// Status.EndTime + TTLSecondsAfterFinished has passed, and the default
		// TTL is zero. Deleting here too would hide exactly the race this mode
		// exists to measure, so only wait — and only NotFound counts as deleted;
		// an API error must keep retrying, not silently pass the gate.
		LogWithTimestamp(test.T(), "waiting for the operator to delete the RayCluster (ttl=%ds)", cfg.JobTTLSeconds)
		g.Eventually(func() error {
			_, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).
				Get(test.Ctx(), rayCluster.Name, metav1.GetOptions{})
			if err == nil {
				return fmt.Errorf("RayCluster %s still present", rayCluster.Name)
			}
			if k8serrors.IsNotFound(err) {
				return nil
			}
			return err
		}, TestTimeoutMedium).Should(Succeed(), "operator should delete the RayCluster after the job finishes")
	} else {
		DeleteRayClusterAndWait(test, g, namespace.Name, rayCluster.Name)
	}

	// The RayCluster CR disappears before the pods finish terminating, and the
	// collector's final flush runs inside the termination grace period. Snapshot
	// T2 only after every Ray pod is gone and the session marker — the head
	// collector writes it during drain — is visible, or the scan races ahead of
	// the upload and reports an empty session.
	g.Eventually(func(gg Gomega) {
		pods, err := test.Client().Core().CoreV1().Pods(namespace.Name).List(test.Ctx(), metav1.ListOptions{
			LabelSelector: "test=raycluster-historyserver",
		})
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(pods.Items).To(BeEmpty())
	}, TestTimeoutMedium).Should(Succeed(), "Ray pods should terminate after RayCluster deletion")
	if err := waitForObject(s3Client, cfg.S3Bucket, markerKey, TestTimeoutMedium); err != nil {
		t.Logf("session marker did not appear after flush: %v (data-loss finding if events are also missing)", err)
	}
	report.FlushDuration = time.Since(flushStart)

	// The follow-streams closed when the collector containers terminated, so
	// this now includes the drain-phase upload lines a pre-deletion scrape
	// could never see.
	report.CollectorLogs = logFollowers.CollectAfterTermination(30 * time.Second)

	// T2: what the shutdown flush added — also the SIGKILL-at-risk volume, since
	// anything in this diff would have been lost without a graceful shutdown.
	snapT2, err := takeBucketSnapshot(s3Client, cfg.S3Bucket)
	g.Expect(err).NotTo(HaveOccurred())

	// CreateDirectory writes the marker's cluster-level directory as one exact
	// zero-byte object. Allow that exact object, never its prefix: another
	// session marker under the same directory must remain unexpected.
	markerDirectoryKey := path.Dir(markerKey) + "/"
	expectedExactKeys := map[string]struct{}{markerKey: {}, markerDirectoryKey: {}}
	report.StorageIsolation = diffSnapshots(
		"full-lifecycle (T2-pre-start)", snapPreStart, snapT2, sessionPrefix, expectedExactKeys,
	)
	report.StorageDiffs = []SnapshotDiff{
		diffSnapshots("during-job (T1-T0)", snapT0, snapT1, sessionPrefix, expectedExactKeys),
		diffSnapshots("flush (T2-T1)", snapT1, snapT2, sessionPrefix, expectedExactKeys),
	}

	// Phase 4: walk the bucket and decode every event file.
	sampler.SetPhase("storage-scan")
	report.Storage = buildStorageReport(
		t, s3Client, cfg.S3Bucket, sessionPrefix, markerKey, cfg, runDir,
		report.Job.StartTime, report.Job.EndTime,
	)
	if err := validateBenchTaskValidity(report.Storage.Events.BenchTaskValidity); err != nil {
		t.Fatalf("benchmark task validity gate failed: %v", err)
	}
	if cfg.SkipHistoryServer {
		// Collector-only campaigns stop here. The deferred writer still joins the
		// captured Collector ingress with cgroup samples and emits every Collector
		// validity artifact; the formal sweep validator fails closed on those gates.
		report.Completed = true
		if !cfg.SkipCleanup {
			g.Expect(deleteBenchmarkRunObjects(s3Client, namespace.Name, rayCluster.Name, sessionID)).To(Succeed())
		}
		return
	}

	// Phase 5: history server against the flushed session.
	sampler.SetPhase("historyserver")
	ApplyHistoryServer(test, g, namespace, hsManifest(t, runDir, cfg))
	cgroups.RegisterPods(test, namespace.Name)
	hsURL := GetHistoryServerURL(test, g, namespace)
	report.HistoryServer = runHSBench(t, g, hsURL, namespace.Name, rayCluster.Name, sessionID, cfg, nil, nil, nil)
	report.HistoryServer.GC, _ = captureHSLogs(test, namespace.Name, runDir)

	// Only now has every configured phase completed. A report written by the
	// deferred dump after an assertion failure has zero values in whatever phase
	// never ran, and analysis reading it as data would manufacture
	// findings ("0 events, no marker") out of a harness failure.
	report.Completed = true

	if !cfg.SkipCleanup {
		g.Expect(deleteBenchmarkRunObjects(s3Client, namespace.Name, rayCluster.Name, sessionID)).To(Succeed())
	}
}

func TestLoadBenchConfigUsesDedicatedS3Bucket(t *testing.T) {
	cfg := loadBenchConfig()
	if cfg.S3Bucket != benchmarkS3BucketName {
		t.Fatalf("S3Bucket=%q, want %q", cfg.S3Bucket, benchmarkS3BucketName)
	}
	if cfg.S3Bucket == S3BucketName {
		t.Fatalf("benchmark bucket must differ from shared e2e bucket %q", S3BucketName)
	}
}

func TestLoadBenchConfigSkipHistoryServer(t *testing.T) {
	t.Setenv("BENCH_SKIP_HISTORY_SERVER", "true")
	if cfg := loadBenchConfig(); !cfg.SkipHistoryServer {
		t.Fatal("BENCH_SKIP_HISTORY_SERVER=true was not loaded")
	}

	t.Setenv("BENCH_SKIP_HISTORY_SERVER", "false")
	if cfg := loadBenchConfig(); cfg.SkipHistoryServer {
		t.Fatal("BENCH_SKIP_HISTORY_SERVER=false was not loaded")
	}
}

func TestLoadBenchConfigCollectorResources(t *testing.T) {
	t.Setenv("BENCH_COLLECTOR_CPU_REQUEST", "250m")
	t.Setenv("BENCH_COLLECTOR_CPU_LIMIT", "1250m")
	t.Setenv("BENCH_COLLECTOR_MEMORY_REQUEST", "384Mi")
	t.Setenv("BENCH_COLLECTOR_MEMORY_LIMIT", "1536Mi")

	cfg := loadBenchConfig()
	if cfg.CollectorCPURequest != "250m" {
		t.Fatalf("CollectorCPURequest=%q, want %q", cfg.CollectorCPURequest, "250m")
	}
	if cfg.CollectorCPU != "1250m" {
		t.Fatalf("CollectorCPU=%q, want %q", cfg.CollectorCPU, "1250m")
	}
	if cfg.CollectorMemoryRequest != "384Mi" {
		t.Fatalf("CollectorMemoryRequest=%q, want %q", cfg.CollectorMemoryRequest, "384Mi")
	}
	if cfg.CollectorMemoryLimit != "1536Mi" {
		t.Fatalf("CollectorMemoryLimit=%q, want %q", cfg.CollectorMemoryLimit, "1536Mi")
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envInt64(key string, def int64) int64 {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("invalid %s=%q: %v", key, raw, err))
	}
	return value
}

func envInt32Pointer(key string) *int32 {
	raw, present := os.LookupEnv(key)
	if !present || raw == "" {
		return nil
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		panic(fmt.Sprintf("invalid %s=%q: %v", key, raw, err))
	}
	result := int32(value)
	return &result
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
