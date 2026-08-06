// Package benchmark contains an opt-in, single-run benchmark for the History
// Server data path: one RayCluster + one RayJob submitting BENCH_TASK_COUNT
// no-op tasks, measured end to end (collector resources -> storage footprint ->
// history server load latency/memory).
//
// It is skipped unless BENCH_RUN=1 so `go test ./...` stays fast. See README.md.
package benchmark

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clustermetadata"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	. "github.com/ray-project/kuberay/historyserver/test/support"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

type benchConfig struct {
	TaskCount        int           // total no-op tasks the driver submits
	WaveSize         int           // tasks per ray.get() wave (bounds in-flight refs)
	TaskNumCPUs      string        // num_cpus per task; fractional raises concurrency
	Compression      bool          // sets RAY_COLLECTOR_EVENT_COMPRESSION_ENABLED on the collector
	RotationIntvl    string        // sets RAY_COLLECTOR_EVENT_ROTATION_INTERVAL (Go duration, "" = default 5m)
	WorkerMemLimit   string        // overrides the ray-worker container memory limit (e.g. "4G"); low num_cpus multiplies Ray worker processes
	KindNode         string        // kind node container name for the cgroup sampler
	HeadStatusBuffer string        // sets RAY_task_events_max_num_status_events_buffer_on_worker on the HEAD Ray container only ("" = Ray default 100000)
	DrainSleepSec    int           // driver's trailing sleep so the task-event queue drains before exit (final flush drains <=10k/flush)
	S3LocalPort      int           // local port for the benchmark's own MinIO port-forward; NOT 9000, which e2e suites on other clusters fight over
	JobTimeout       time.Duration // wall-clock budget for the RayJob to succeed
	WarmIterations   int           // requests per warm history server endpoint
	HSCPULimit       string        // overrides the history server container CPU limit ("none" removes it); the shipped manifest pins 500m, which the cold load saturates
	HSEnv            string        // extra env for the history server container, "K=V,K=V" (GOMAXPROCS, GODEBUG=gctrace=1, GOGC...)
	HSArgs           string        // extra CLI flags for the history server, comma separated (e.g. --session-process-timeout=30m)
	HSEnterTimeout   time.Duration // client budget for the first /enter_cluster attempt
	HSWarmWait       time.Duration // total budget for warm-probe retries when the first attempt times out
	OutDir           string        // report + CSV destination
	SkipCleanup      bool          // keep the S3 bucket contents after the run
}

func loadBenchConfig() benchConfig {
	return benchConfig{
		TaskCount:        envInt("BENCH_TASK_COUNT", 50000),
		WaveSize:         envInt("BENCH_WAVE_SIZE", 2000),
		TaskNumCPUs:      envStr("BENCH_TASK_NUM_CPUS", "0.2"),
		Compression:      envBool("BENCH_COMPRESSION", false),
		RotationIntvl:    envStr("BENCH_EVENT_ROTATION_INTERVAL", ""),
		WorkerMemLimit:   envStr("BENCH_WORKER_MEMORY_LIMIT", ""),
		KindNode:         envStr("BENCH_KIND_NODE", "kind-control-plane"),
		HeadStatusBuffer: envStr("BENCH_RAY_STATUS_BUFFER_HEAD", ""),
		DrainSleepSec:    envInt("BENCH_DRIVER_DRAIN_SLEEP", 10),
		S3LocalPort:      envInt("BENCH_S3_LOCAL_PORT", 9002),
		JobTimeout:       envDuration("BENCH_JOB_TIMEOUT", 45*time.Minute),
		WarmIterations:   envInt("BENCH_WARM_ITERATIONS", 10),
		HSCPULimit:       envStr("BENCH_HS_CPU_LIMIT", ""),
		HSEnv:            envStr("BENCH_HS_ENV", ""),
		HSArgs:           envStr("BENCH_HS_ARGS", ""),
		HSEnterTimeout:   envDuration("BENCH_HS_ENTER_TIMEOUT", 5*time.Minute),
		HSWarmWait:       envDuration("BENCH_HS_WARM_WAIT", 15*time.Minute),
		OutDir:           envStr("BENCH_OUT_DIR", "out"),
		SkipCleanup:      envBool("BENCH_SKIP_CLEANUP", false),
	}
}

func TestHistoryServerBenchmark(t *testing.T) {
	if os.Getenv("BENCH_RUN") == "" {
		t.Skip("benchmark is opt-in: set BENCH_RUN=1 (see test/benchmark/README.md)")
	}
	cfg := loadBenchConfig()
	runDir := filepath.Join(cfg.OutDir, time.Now().Format("20060102-150405"))
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatalf("create output dir %s: %v", runDir, err)
	}

	s3Client := ensureBenchS3Client(t, cfg.S3LocalPort)
	stopWatchdog := startS3Watchdog(t, s3Client, S3BucketName, cfg.S3LocalPort)
	defer stopWatchdog()
	test := With(t)
	g := NewWithT(t)
	namespace := test.NewTestNamespace()

	report := &Report{Config: cfg, StartedAt: time.Now()}
	report.Env = captureEnvInfo(test)

	sampler := newResourceSampler(test, namespace.Name, time.Second)
	cgroups := newCgroupSampler(cfg.KindNode)

	// Always dump whatever was measured, even when an assertion aborts the run:
	// a partial report is exactly what you need to debug a failed benchmark.
	defer func() {
		sampler.Stop()
		cgroups.Stop()
		report.Resources = sampler.Summarize()
		report.Cgroups = cgroups.Summarize(sampler.Marks())
		if err := sampler.WriteCSV(filepath.Join(runDir, "samples.csv")); err != nil {
			t.Logf("write samples.csv: %v", err)
		}
		if err := cgroups.WriteCSV(filepath.Join(runDir, "cgroup_samples.csv")); err != nil {
			t.Logf("write cgroup_samples.csv: %v", err)
		}
		writeReport(t, report, runDir)
	}()

	// Phase 1: RayCluster with collector sidecars.
	rayCluster := applyBenchRayCluster(test, g, namespace, cfg)
	g.Eventually(func() error {
		_, err := s3Client.HeadBucket(&s3.HeadBucketInput{Bucket: aws.String(S3BucketName)})
		return err
	}, TestTimeoutMedium).Should(Succeed(), "S3 bucket should exist")

	sessionID := GetSessionIDFromHeadPod(test, g, rayCluster)
	report.SessionID = sessionID
	sessionPrefix := clusterlogs.SessionDir("log", "", "", namespace.Name, rayCluster.Name, sessionID) + "/"
	markerKey := clustermetadata.EncodePath(
		utils.ClusterInfo{Namespace: namespace.Name, Name: rayCluster.Name}, "log", sessionID)

	// T0: bucket inventory before any load.
	snapT0, err := takeBucketSnapshot(s3Client, S3BucketName)
	g.Expect(err).NotTo(HaveOccurred())

	sampler.Start()
	cgroups.Start(test)
	cgroups.RegisterPods(test, namespace.Name)
	logFollowers := startCollectorLogFollowers(test, namespace.Name)

	// Phase 2: the 50k-task RayJob.
	sampler.SetPhase("job")
	report.Job = runBenchJob(test, g, namespace, rayCluster, cfg)

	// T1: what rotation uploaded while the job was running.
	snapT1, err := takeBucketSnapshot(s3Client, S3BucketName)
	g.Expect(err).NotTo(HaveOccurred())

	// Phase 3: graceful deletion triggers the final flush (rotate + upload) and
	// writes the cluster-metadata session marker.
	sampler.SetPhase("flush")
	flushStart := time.Now()
	DeleteRayClusterAndWait(test, g, namespace.Name, rayCluster.Name)

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
	if err := waitForObject(s3Client, S3BucketName, markerKey, TestTimeoutMedium); err != nil {
		t.Logf("session marker did not appear after flush: %v (data-loss finding if events are also missing)", err)
	}
	report.FlushDuration = time.Since(flushStart)

	// The follow-streams closed when the collector containers terminated, so
	// this now includes the drain-phase upload lines a pre-deletion scrape
	// could never see.
	report.CollectorLogs = logFollowers.CollectAfterTermination(30 * time.Second)

	// T2: what the shutdown flush added — also the SIGKILL-at-risk volume, since
	// anything in this diff would have been lost without a graceful shutdown.
	snapT2, err := takeBucketSnapshot(s3Client, S3BucketName)
	g.Expect(err).NotTo(HaveOccurred())

	// The head collector also creates the marker's parent "directory" object,
	// so the expected prefix is the marker's directory, not just the key.
	markerPrefix := markerKey[:strings.LastIndex(markerKey, "/")+1]
	report.StorageDiffs = []SnapshotDiff{
		diffSnapshots("during-job (T1-T0)", snapT0, snapT1, []string{sessionPrefix}),
		diffSnapshots("flush (T2-T1)", snapT1, snapT2, []string{sessionPrefix, markerPrefix}),
	}

	// Phase 4: walk the bucket and decode every event file.
	sampler.SetPhase("storage-scan")
	report.Storage = buildStorageReport(t, s3Client, S3BucketName, sessionPrefix, markerKey, cfg, runDir)

	// Phase 5: history server against the flushed session.
	sampler.SetPhase("historyserver")
	ApplyHistoryServer(test, g, namespace, hsManifest(t, runDir, cfg))
	cgroups.RegisterPods(test, namespace.Name)
	hsURL := GetHistoryServerURL(test, g, namespace)
	report.HistoryServer = runHSBench(t, g, hsURL, namespace.Name, rayCluster.Name, sessionID, cfg)
	report.HistoryServer.GC = captureHSLogs(test, namespace.Name, runDir)

	if !cfg.SkipCleanup {
		DeleteS3Bucket(test, g, s3Client)
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
