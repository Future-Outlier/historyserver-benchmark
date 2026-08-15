package benchmark

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayutils "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// driverTemplate submits __TASK_COUNT__ no-op tasks in waves of __WAVE_SIZE__.
// Waves bound the number of in-flight ObjectRefs so driver memory stays flat.
// __TARGET_RATE__ paces submission so the event rate becomes an independent
// variable: without it the rate is whatever the scheduler happens to deliver,
// which varied only 4,277-4,776 events/s across the whole num_cpus axis.
// Python code must avoid double quotes: the entrypoint wraps it in `python -c "..."`.
const driverTemplate = `
import ray
import sys
import time
import multiprocessing
T = __TASK_COUNT__
WAVE = __WAVE_SIZE__
TARGET = __TARGET_RATE__
DRIVERS = __DRIVERS__

def submit(share, target):
    # Each process calls ray.init() separately, so each is its own Ray driver
    # with its own CoreWorker event buffer and its own 10k-events/s drain. One
    # driver tops out near 3k tasks/s on this hardware; the aggregate does not.
    ray.init()

    @ray.remote(num_cpus=__TASK_NUM_CPUS__, max_retries=0)
    def bench_task(i):
        return i

    t0 = time.time()
    done = 0
    refs = []
    for i in range(share):
        refs.append(bench_task.remote(i))
__PACE_CODE__
        if len(refs) >= WAVE:
            ray.get(refs)
            done += len(refs)
            refs = []
            print(f'BENCH_PROGRESS {done}/{share} elapsed={time.time() - t0:.1f}s', flush=True)
    if refs:
        ray.get(refs)
        done += len(refs)
    wall = time.time() - t0
    print(f'BENCH_DRIVER_DONE tasks={done} wall_s={wall:.2f} rate_tps={done / wall:.1f}', flush=True)
__DRAIN_CODE__

t0 = time.time()
if DRIVERS <= 1:
    submit(T, TARGET)
else:
    # Distribute the remainder too: T//DRIVERS x DRIVERS silently ran fewer
    # tasks than requested (49,998 for 50k/6) and the report then counted the
    # missing two as event loss.
    base = T // DRIVERS
    shares = [base + (1 if i < T % DRIVERS else 0) for i in range(DRIVERS)]
    per_driver_target = TARGET // DRIVERS if TARGET > 0 else 0
    procs = [multiprocessing.Process(target=submit, args=(s, per_driver_target))
             for s in shares]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    failed = [p.exitcode for p in procs if p.exitcode != 0]
    if failed:
        # Without this a crashed child still let the parent print BENCH_DONE and
        # the run passed while genuinely missing that child's tasks.
        print(f'BENCH_CHILD_FAILED exitcodes={failed}', flush=True)
        sys.exit(1)
# Exploratory runs can inject a driver-side drain. Formal RayJob runs inject no
# sleep code and rely only on shutdownAfterJobFinishes + TTL.
wall = time.time() - t0__DRAIN_ADJUSTMENT__
print(f'BENCH_DONE tasks={T} wall_s={wall:.2f} rate_tps={T / wall:.1f} drivers={DRIVERS}', flush=True)
`

func renderDriverScript(cfg benchConfig) string {
	paceCode := ""
	if cfg.TargetTaskRate > 0 {
		paceCode = strings.Join([]string{
			"        if target > 0:",
			"            behind = (i + 1) / target - (time.time() - t0)",
			"            if behind > 0:",
			"                time.sleep(behind)",
		}, "\n")
	}
	drainCode := ""
	drainAdjustment := ""
	if cfg.DrainSleepSec > 0 {
		drainCode = fmt.Sprintf("    time.sleep(%d)", cfg.DrainSleepSec)
		drainAdjustment = fmt.Sprintf(" - %d", cfg.DrainSleepSec)
	}
	return strings.NewReplacer(
		"__TASK_COUNT__", strconv.Itoa(cfg.TaskCount),
		"__WAVE_SIZE__", strconv.Itoa(cfg.WaveSize),
		"__TASK_NUM_CPUS__", cfg.TaskNumCPUs,
		"__TARGET_RATE__", strconv.Itoa(cfg.TargetTaskRate),
		"__DRIVERS__", strconv.Itoa(cfg.Drivers),
		"__PACE_CODE__", paceCode,
		"__DRAIN_CODE__", drainCode,
		"__DRAIN_ADJUSTMENT__", drainAdjustment,
	).Replace(driverTemplate)
}

// JobResult captures the load-generation phase.
type JobResult struct {
	WallClock     time.Duration `json:"wallClock"`     // k8s-observed: create -> Succeeded+Complete
	StartTime     time.Time     `json:"startTime"`     // Ray dashboard/GCS job start, copied into Status.RayJobStatusInfo
	EndTime       time.Time     `json:"endTime"`       // Ray dashboard/GCS job end, copied into Status.RayJobStatusInfo
	DriverTasks   int           `json:"driverTasks"`   // parsed from BENCH_DONE (0 if log unavailable)
	DriverWallSec float64       `json:"driverWallSec"` // driver-measured seconds
	DriverRateTPS float64       `json:"driverRateTPS"` // driver-measured tasks/s
}

// RayJobLifecycleEvidence records the lifecycle fields from the RayJob object
// returned by the Kubernetes API, rather than trusting only the input config or
// the local builder.
type RayJobLifecycleEvidence struct {
	OwnedCluster             bool   `json:"ownedCluster"`
	ShutdownAfterJobFinishes bool   `json:"shutdownAfterJobFinishes"`
	TTLSecondsAfterFinished  int32  `json:"ttlSecondsAfterFinished"`
	RayJobBackoffLimit       *int32 `json:"rayJobBackoffLimit"`
	SubmitterBackoffLimit    *int32 `json:"submitterBackoffLimit"`
}

func copyInt32Pointer(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func int32Pointer(value int32) *int32 {
	return &value
}

func lifecycleEvidenceFromRayJob(job *rayv1.RayJob) RayJobLifecycleEvidence {
	if job == nil {
		return RayJobLifecycleEvidence{}
	}
	return RayJobLifecycleEvidence{
		OwnedCluster:             job.Spec.RayClusterSpec != nil,
		ShutdownAfterJobFinishes: job.Spec.ShutdownAfterJobFinishes,
		TTLSecondsAfterFinished:  job.Spec.TTLSecondsAfterFinished,
		RayJobBackoffLimit:       copyInt32Pointer(job.Spec.BackoffLimit),
		SubmitterBackoffLimit: func() *int32 {
			if job.Spec.SubmitterConfig == nil {
				return nil
			}
			return copyInt32Pointer(job.Spec.SubmitterConfig.BackoffLimit)
		}(),
	}
}

// runBenchJob submits the benchmark RayJob against the existing cluster and
// blocks until it succeeds (or fails fast on a terminal failure status).
func runBenchJob(test Test, g *WithT, namespace *corev1.Namespace, rayCluster *rayv1.RayCluster, cfg benchConfig) JobResult {
	createBenchRayJob(test, g, namespace, rayCluster.Name, nil, cfg)
	return waitBenchJob(test, g, namespace.Name, "rayjob-bench", cfg)
}

// createBenchRayJob submits the benchmark RayJob. With ownedSpec nil it targets
// an existing cluster via ClusterSelector; with ownedSpec set it embeds the
// cluster spec, which is the ONLY mode where shutdownAfterJobFinishes does
// anything — on a selected cluster the controller returns before ever checking
// that flag (rayjob_controller.go:429).
func createBenchRayJob(test Test, g *WithT, namespace *corev1.Namespace, rayClusterName string, ownedSpec *rayv1.RayClusterSpec, cfg benchConfig) *rayv1.RayJob {
	rayJob := buildBenchRayJob(namespace.Name, rayClusterName, ownedSpec, cfg)

	created, err := test.Client().Ray().RayV1().
		RayJobs(namespace.Name).
		Create(test.Ctx(), rayJob, metav1.CreateOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayJob %s/%s (%d tasks, wave %d, owned=%v)",
		created.Namespace, created.Name, cfg.TaskCount, cfg.WaveSize, ownedSpec != nil)
	return created
}

func buildBenchRayJob(namespace, rayClusterName string, ownedSpec *rayv1.RayClusterSpec, cfg benchConfig) *rayv1.RayJob {
	zeroBackoff := int32(0)
	rayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rayjob-bench",
			Namespace: namespace,
		},
		Spec: rayv1.RayJobSpec{
			Entrypoint:   fmt.Sprintf("python -c \"%s\"", renderDriverScript(cfg)),
			BackoffLimit: &zeroBackoff,
			SubmitterConfig: &rayv1.SubmitterConfig{
				BackoffLimit: &zeroBackoff,
			},
		},
	}
	if ownedSpec != nil {
		rayJob.Spec.RayClusterSpec = ownedSpec
		rayJob.Spec.ShutdownAfterJobFinishes = true
		rayJob.Spec.TTLSecondsAfterFinished = cfg.JobTTLSeconds
	} else {
		rayJob.Spec.ClusterSelector = map[string]string{"ray.io/cluster": rayClusterName}
	}
	return rayJob
}

func waitBenchJob(test Test, g *WithT, namespace, name string, cfg benchConfig) JobResult {
	start := time.Now()
	var completedJob *rayv1.RayJob
	g.Eventually(func(gg Gomega) {
		job, err := RayJob(test, namespace, name)()
		gg.Expect(err).NotTo(HaveOccurred())
		if job.Status.JobStatus == rayv1.JobStatusFailed {
			StopTrying(fmt.Sprintf("driver failed: %s", job.Status.Message)).Now()
		}
		if job.Status.JobDeploymentStatus == rayv1.JobDeploymentStatusFailed {
			StopTrying(fmt.Sprintf("job deployment failed: %s", job.Status.Message)).Now()
		}
		gg.Expect(job.Status.JobStatus).To(Equal(rayv1.JobStatusSucceeded))
		gg.Expect(job.Status.JobDeploymentStatus).To(Equal(rayv1.JobDeploymentStatusComplete))
		// Top-level times drive the RayJob controller lifecycle and TTL.
		gg.Expect(job.Status.StartTime).NotTo(BeNil())
		gg.Expect(job.Status.EndTime).NotTo(BeNil())
		// RayJobStatusInfo times originate in Ray Dashboard/GCS and therefore
		// share the workload clock used by task lifecycle transitions.
		gg.Expect(job.Status.RayJobStatusInfo.StartTime).NotTo(BeNil())
		gg.Expect(job.Status.RayJobStatusInfo.EndTime).NotTo(BeNil())
		completedJob = job.DeepCopy()
	}, cfg.JobTimeout, 5*time.Second).Should(Succeed())

	if completedJob == nil || completedJob.Status.RayJobStatusInfo.StartTime == nil ||
		completedJob.Status.RayJobStatusInfo.EndTime == nil {
		test.T().Fatalf("completed RayJob %s/%s is missing Ray job start/end timestamps", namespace, name)
	}
	res := JobResult{
		WallClock: time.Since(start),
		StartTime: completedJob.Status.RayJobStatusInfo.StartTime.Time,
		EndTime:   completedJob.Status.RayJobStatusInfo.EndTime.Time,
	}
	parseDriverLog(test, namespace, name, &res)
	LogWithTimestamp(test.T(), "RayJob done: wall=%s driver_rate=%.1f tasks/s",
		res.WallClock.Round(time.Second), res.DriverRateTPS)
	return res
}

var benchDoneRe = regexp.MustCompile(`BENCH_DONE tasks=(\d+) wall_s=([0-9.]+) rate_tps=([0-9.]+)`)

// parseDriverLog reads the submitter pod log (which streams driver output) and
// extracts the driver-side timing. Best effort: the k8s-observed wall clock in
// JobResult already covers the failure of this path.
func parseDriverLog(test Test, namespace, rayJobName string, res *JobResult) {
	pods, err := test.Client().Core().CoreV1().Pods(namespace).List(test.Ctx(), metav1.ListOptions{
		LabelSelector: "job-name=" + rayJobName,
	})
	if err != nil || len(pods.Items) == 0 {
		LogWithTimestamp(test.T(), "No submitter pod found for RayJob %s: %v", rayJobName, err)
		return
	}
	raw, err := test.Client().Core().CoreV1().Pods(namespace).
		GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{}).DoRaw(test.Ctx())
	if err != nil {
		LogWithTimestamp(test.T(), "Failed to read submitter log: %v", err)
		return
	}
	m := benchDoneRe.FindStringSubmatch(string(raw))
	if m == nil {
		LogWithTimestamp(test.T(), "BENCH_DONE marker not found in submitter log")
		return
	}
	res.DriverTasks, _ = strconv.Atoi(m[1])
	res.DriverWallSec, _ = strconv.ParseFloat(m[2], 64)
	res.DriverRateTPS, _ = strconv.ParseFloat(m[3], 64)
}

// CollectorLogStat summarizes one collector container's log, surfacing
// backpressure and upload behavior that resource metrics cannot show.
type CollectorLogStat struct {
	Pod                      string   `json:"pod"`
	Role                     string   `json:"role"`
	Image                    string   `json:"image"`
	ImageID                  string   `json:"imageID"`
	ContainerID              string   `json:"containerID"`
	RestartCount             int32    `json:"restartCount"`
	CPURequest               string   `json:"cpuRequest"`
	CPULimit                 string   `json:"cpuLimit"`
	MemoryRequest            string   `json:"memoryRequest"`
	MemoryLimit              string   `json:"memoryLimit"`
	CgroupMemoryObserved     bool     `json:"cgroupMemoryObserved"`
	CgroupMemoryMax          string   `json:"cgroupMemoryMax"`
	CgroupMemoryMaxBytes     int64    `json:"cgroupMemoryMaxBytes"`
	MemoryEventsOOM          int64    `json:"memoryEventsOOM"`
	MemoryEventsOOMKill      int64    `json:"memoryEventsOOMKill"`
	CgroupMemoryReadErrors   int      `json:"cgroupMemoryReadErrors"`
	CgroupMemoryErrorFields  []string `json:"cgroupMemoryErrorFields"`
	LogStreamComplete        bool     `json:"logStreamComplete"`
	LogStreamTimedOut        bool     `json:"logStreamTimedOut"`
	LogStreamError           string   `json:"logStreamError,omitempty"`
	GracefulShutdownComplete bool     `json:"gracefulShutdownComplete"`
	Uploads                  int      `json:"uploads"`
	UploadedBytes            int64    `json:"uploadedBytes"`
	// Legacy text-log counter. The HTTP response itself still has no standalone
	// log line, so a zero here is not evidence that no rejection occurred. Use
	// IngressWindows.RejectedDiskPressure for the fail-closed verdict.
	DiskPressure503s  int                          `json:"diskPressure503s"`
	RotationQueueFul  int                          `json:"rotationQueueFull"`
	UploadFailures    int                          `json:"uploadFailures"`
	UploadTimeline    []UploadPoint                `json:"uploadTimeline"` // reconstructs the local-disk sawtooth
	FinalCgroupMemory *FinalCgroupMemoryEvidence   `json:"finalCgroupMemory,omitempty"`
	MainReturn        *CollectorMainReturnEvidence `json:"mainReturn,omitempty"`
	IngressWindows    []CollectorIngressWindow     `json:"ingressWindows,omitempty"`
	// GC is populated when BENCH_COLLECTOR_ENV sets GODEBUG=gctrace=1. It is the
	// only way to tell the collector's live heap apart from GC headroom, which
	// cgroup anon lumps together.
	GC *GCStats `json:"gc,omitempty"`
}

// CollectorIngressWindow is one receive-time counter bucket emitted by a
// collector. WindowStartUnixNano comes from the collector process, not the
// RayEvent timestamp, so it can be joined directly to cgroup wall-clock rows.
type CollectorIngressWindow struct {
	WindowStartUnixNano  int64  `json:"windowStartUnixNano"`
	WindowSeconds        int64  `json:"windowSeconds"`
	NodeID               string `json:"nodeID"`
	Batches              int64  `json:"batches"`
	Events               int64  `json:"events"`
	Bytes                int64  `json:"bytes"`
	RejectedRequests     int64  `json:"rejectedRequests"`
	RejectedDraining     int64  `json:"rejectedDraining"`
	RejectedDiskPressure int64  `json:"rejectedDiskPressure"`
	RejectedBadRequest   int64  `json:"rejectedBadRequest"`
	RejectedInternal     int64  `json:"rejectedInternal"`
	RotationQueueFull    int64  `json:"rotationQueueFull"`
}

// UploadPoint is one upload occurrence parsed from the collector log.
type UploadPoint struct {
	Time     string `json:"time"` // logrus timestamp, kept verbatim
	UnixNano int64  `json:"unixNano"`
	Bytes    int64  `json:"bytes"`
}

type FinalCgroupMemoryEvidence struct {
	Time             string `json:"time"`
	UnixNano         int64  `json:"unixNano"`
	CurrentBytes     uint64 `json:"currentBytes"`
	PeakBytes        uint64 `json:"peakBytes"`
	EventsMax        uint64 `json:"eventsMax"`
	EventsOOM        uint64 `json:"eventsOOM"`
	EventsOOMKill    uint64 `json:"eventsOOMKill"`
	PSIFullTotalUsec uint64 `json:"psiFullTotalUsec"`
}

type CollectorMainReturnEvidence struct {
	Time     string `json:"time"`
	UnixNano int64  `json:"unixNano"`
	ExitCode int    `json:"exitCode"`
	Reason   string `json:"reason"`
}

// Matches logrus text format: time="..." level=info msg="Uploaded N bytes to ..."
var uploadedRe = regexp.MustCompile(`(?:time="([^"]+)".*?)?Uploaded (\d+) bytes to`)
var uploadedUnixNanoRe = regexp.MustCompile(`at_unix_nano=(\d+)`)
var finalCgroupMemoryRe = regexp.MustCompile(
	`(?:time="([^"]+)".*?)?FINAL_CGROUP_MEMORY at_unix_nano=(\d+) current_bytes=(\d+) peak_bytes=(\d+) events_max=(\d+) events_oom=(\d+) events_oom_kill=(\d+) psi_full_total_usec=(\d+)`,
)
var collectorMainReturnRe = regexp.MustCompile(
	`(?:time="([^"]+)".*?)?COLLECTOR_MAIN_RETURN at_unix_nano=(\d+) exit_code=(\d+) reason=([^\s"]+)`,
)

var collectorIngressWindowRe = regexp.MustCompile(
	`collector_ingress_window window_start_unix_nano=(\d+) window_seconds=(\d+) ray_node_id=([^\s"]+) batches=(\d+) events=(\d+) bytes=(\d+) rejected_requests=(\d+) rejected_draining=(\d+) rejected_disk_pressure=(\d+) rejected_bad_request=(\d+) rejected_internal=(\d+) rotation_queue_full=(\d+)`,
)

// parseCollectorLog counts the backpressure/upload signals in one collector log.
func parseCollectorLog(pod, log string) CollectorLogStat {
	stat := CollectorLogStat{
		Pod:                      pod,
		GracefulShutdownComplete: strings.Contains(log, "Graceful shutdown complete"),
		DiskPressure503s:         strings.Count(log, "under disk pressure"),
		RotationQueueFul:         strings.Count(log, "rotation queue full"),
		UploadFailures:           strings.Count(log, "Failed to upload"),
	}
	for _, line := range strings.Split(log, "\n") {
		if m := collectorMainReturnRe.FindStringSubmatch(line); m != nil {
			exact, exactErr := strconv.ParseInt(m[2], 10, 64)
			exitCode, exitErr := strconv.Atoi(m[3])
			if exactErr == nil && exitErr == nil {
				stat.MainReturn = &CollectorMainReturnEvidence{
					Time: m[1], UnixNano: exact, ExitCode: exitCode, Reason: m[4],
				}
			}
		}
		if m := finalCgroupMemoryRe.FindStringSubmatch(line); m != nil {
			values := make([]uint64, 7)
			valid := true
			for i := range values {
				value, parseErr := strconv.ParseUint(m[i+2], 10, 64)
				if parseErr != nil {
					valid = false
					break
				}
				values[i] = value
			}
			if valid && values[0] <= uint64(^uint64(0)>>1) {
				stat.FinalCgroupMemory = &FinalCgroupMemoryEvidence{
					Time: m[1], UnixNano: int64(values[0]), CurrentBytes: values[1], PeakBytes: values[2],
					EventsMax: values[3], EventsOOM: values[4], EventsOOMKill: values[5],
					PSIFullTotalUsec: values[6],
				}
			}
		}
		m := uploadedRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		stat.Uploads++
		n, err := strconv.ParseInt(m[2], 10, 64)
		if err != nil {
			continue
		}
		stat.UploadedBytes += n
		point := UploadPoint{Time: m[1], Bytes: n}
		if exact := uploadedUnixNanoRe.FindStringSubmatch(line); exact != nil {
			point.UnixNano, _ = strconv.ParseInt(exact[1], 10, 64)
		}
		stat.UploadTimeline = append(stat.UploadTimeline, point)
	}
	// A request that straddles a flush boundary can produce two log rows for the
	// same key. Merge them here so every artifact has exactly one pod/node/window
	// row and no received batch is lost.
	type ingressKey struct {
		start, seconds int64
		nodeID         string
	}
	ingress := map[ingressKey]*CollectorIngressWindow{}
	for _, m := range collectorIngressWindowRe.FindAllStringSubmatch(log, -1) {
		start, errStart := strconv.ParseInt(m[1], 10, 64)
		seconds, errSeconds := strconv.ParseInt(m[2], 10, 64)
		batches, errBatches := strconv.ParseInt(m[4], 10, 64)
		events, errEvents := strconv.ParseInt(m[5], 10, 64)
		bytes, errBytes := strconv.ParseInt(m[6], 10, 64)
		rejected, errRejected := strconv.ParseInt(m[7], 10, 64)
		rejectedDraining, errRejectedDraining := strconv.ParseInt(m[8], 10, 64)
		rejectedDisk, errRejectedDisk := strconv.ParseInt(m[9], 10, 64)
		rejectedBadRequest, errRejectedBadRequest := strconv.ParseInt(m[10], 10, 64)
		rejectedInternal, errRejectedInternal := strconv.ParseInt(m[11], 10, 64)
		queueFull, errQueueFull := strconv.ParseInt(m[12], 10, 64)
		if errStart != nil || errSeconds != nil || errBatches != nil || errEvents != nil || errBytes != nil ||
			errRejected != nil || errRejectedDraining != nil || errRejectedDisk != nil || errRejectedBadRequest != nil ||
			errRejectedInternal != nil || errQueueFull != nil || seconds <= 0 {
			continue
		}
		key := ingressKey{start: start, seconds: seconds, nodeID: m[3]}
		row := ingress[key]
		if row == nil {
			row = &CollectorIngressWindow{
				WindowStartUnixNano: start,
				WindowSeconds:       seconds,
				NodeID:              m[3],
			}
			ingress[key] = row
		}
		row.Batches += batches
		row.Events += events
		row.Bytes += bytes
		row.RejectedRequests += rejected
		row.RejectedDraining += rejectedDraining
		row.RejectedDiskPressure += rejectedDisk
		row.RejectedBadRequest += rejectedBadRequest
		row.RejectedInternal += rejectedInternal
		row.RotationQueueFull += queueFull
	}
	for _, row := range ingress {
		stat.IngressWindows = append(stat.IngressWindows, *row)
	}
	sort.Slice(stat.IngressWindows, func(i, j int) bool {
		if stat.IngressWindows[i].WindowStartUnixNano != stat.IngressWindows[j].WindowStartUnixNano {
			return stat.IngressWindows[i].WindowStartUnixNano < stat.IngressWindows[j].WindowStartUnixNano
		}
		return stat.IngressWindows[i].NodeID < stat.IngressWindows[j].NodeID
	})
	return stat
}

func TestParseCollectorIngressWindows(t *testing.T) {
	log := `time="2026-08-08T01:00:10Z" level=info msg="collector_ingress_window window_start_unix_nano=100000000000 window_seconds=10 ray_node_id=node-a batches=2 events=1500 bytes=6144 rejected_requests=0 rejected_draining=0 rejected_disk_pressure=0 rejected_bad_request=0 rejected_internal=0 rotation_queue_full=0"
time="2026-08-08T01:00:11Z" level=info msg="collector_ingress_window window_start_unix_nano=100000000000 window_seconds=10 ray_node_id=node-a batches=1 events=500 bytes=2048 rejected_requests=1 rejected_draining=0 rejected_disk_pressure=1 rejected_bad_request=0 rejected_internal=0 rotation_queue_full=1"
time="2026-08-08T01:00:20Z" level=info msg="collector_ingress_window window_start_unix_nano=110000000000 window_seconds=10 ray_node_id=node-a batches=1 events=100 bytes=512 rejected_requests=0 rejected_draining=0 rejected_disk_pressure=0 rejected_bad_request=0 rejected_internal=0 rotation_queue_full=0"
time="2026-08-08T01:00:21Z" level=info msg="Graceful shutdown complete"`

	stat := parseCollectorLog("ray-head/collector", log)
	if len(stat.IngressWindows) != 2 {
		t.Fatalf("got %d ingress windows, want 2: %#v", len(stat.IngressWindows), stat.IngressWindows)
	}
	if got := stat.IngressWindows[0]; got.Batches != 3 || got.Events != 2000 || got.Bytes != 8192 {
		t.Fatalf("merged first window = %#v, want batches=3 events=2000 bytes=8192", got)
	}
	if got := stat.IngressWindows[0]; got.RejectedRequests != 1 || got.RejectedDiskPressure != 1 || got.RotationQueueFull != 1 {
		t.Fatalf("merged first window pressure counters = %#v, want rejected=1 disk=1 queue=1", got)
	}
	if got := stat.IngressWindows[1].WindowStartUnixNano; got != 110000000000 {
		t.Fatalf("second window start = %d, want 110000000000", got)
	}
	if !stat.GracefulShutdownComplete {
		t.Fatal("collector graceful-shutdown marker was not recorded")
	}
	if parseCollectorLog("ray-head/collector", "collector stopped unexpectedly").GracefulShutdownComplete {
		t.Fatal("collector without the exact graceful-shutdown marker passed")
	}
}

func TestParseCollectorUploadTimelineKeepsExactCompletionTime(t *testing.T) {
	log := `time="2026-08-13T23:06:23Z" level=info msg="Uploaded 53700000 bytes to bucket/key at_unix_nano=1786662383420000000"`
	stat := parseCollectorLog("ray-head/collector", log)
	if stat.Uploads != 1 || stat.UploadedBytes != 53700000 || len(stat.UploadTimeline) != 1 {
		t.Fatalf("upload summary=%#v", stat)
	}
	point := stat.UploadTimeline[0]
	if point.Time != "2026-08-13T23:06:23Z" || point.UnixNano != 1786662383420000000 || point.Bytes != 53700000 {
		t.Fatalf("upload point=%#v", point)
	}
	legacy := parseCollectorLog("ray-head/collector", `time="2026-08-13T23:06:23Z" level=info msg="Uploaded 1 bytes to old/key"`)
	if len(legacy.UploadTimeline) != 1 || legacy.UploadTimeline[0].UnixNano != 0 {
		t.Fatalf("legacy upload timestamp was invented: %#v", legacy.UploadTimeline)
	}
}

func TestParseCollectorFinalCgroupMemoryEvidence(t *testing.T) {
	log := `time="2026-08-13T23:06:23.420Z" level=info msg="FINAL_CGROUP_MEMORY at_unix_nano=1786662383420000000 current_bytes=100 peak_bytes=200 events_max=0 events_oom=0 events_oom_kill=0 psi_full_total_usec=7"
time="2026-08-13T23:06:23.421Z" level=info msg="COLLECTOR_MAIN_RETURN at_unix_nano=1786662383421000000 exit_code=0 reason=Completed"`
	stat := parseCollectorLog("ray-head/collector", log)
	got := stat.FinalCgroupMemory
	if got == nil || got.Time != "2026-08-13T23:06:23.420Z" || got.UnixNano != 1786662383420000000 ||
		got.CurrentBytes != 100 || got.PeakBytes != 200 || got.EventsMax != 0 || got.EventsOOM != 0 || got.EventsOOMKill != 0 || got.PSIFullTotalUsec != 7 {
		t.Fatalf("final cgroup memory=%#v", got)
	}
	if parseCollectorLog("ray-head/collector", "FINAL_CGROUP_MEMORY malformed").FinalCgroupMemory != nil {
		t.Fatal("malformed final cgroup evidence was accepted")
	}
	mainReturn := stat.MainReturn
	if mainReturn == nil || mainReturn.Time != "2026-08-13T23:06:23.421Z" ||
		mainReturn.UnixNano != 1786662383421000000 || mainReturn.ExitCode != 0 || mainReturn.Reason != "Completed" {
		t.Fatalf("main return=%#v", mainReturn)
	}
}

// collectorLogFollowers streams collector logs with follow=true. A post-hoc
// GetLogs cannot see the drain phase: most uploads happen during pod
// termination, and by then the pods are gone. Streams end naturally when the
// containers terminate.
type collectorLogFollowers struct {
	mu      sync.Mutex
	bufs    map[string]*strings.Builder
	states  map[string]*collectorLogFollowState
	cancels map[string]context.CancelFunc
	wg      sync.WaitGroup
	done    chan struct{}
}

type collectorLogFollowState struct {
	role          string
	image         string
	imageID       string
	containerID   string
	restartCount  int32
	cpuRequest    string
	cpuLimit      string
	memoryRequest string
	memoryLimit   string
	completed     bool
	timedOut      bool
	err           string
}

// startCollectorLogFollowers must be called while the Ray pods are Running.
func startCollectorLogFollowers(test Test, namespace string) *collectorLogFollowers {
	f := &collectorLogFollowers{
		bufs:    map[string]*strings.Builder{},
		states:  map[string]*collectorLogFollowState{},
		cancels: map[string]context.CancelFunc{},
		done:    make(chan struct{}),
	}
	pods, err := test.Client().Core().CoreV1().Pods(namespace).List(test.Ctx(), metav1.ListOptions{
		LabelSelector: "test=raycluster-historyserver",
	})
	if err != nil {
		LogWithTimestamp(test.T(), "Failed to list Ray pods for collector log following: %v", err)
		close(f.done)
		return f
	}
	for _, pod := range pods.Items {
		podName := pod.Name
		role := pod.Labels[rayutils.RayNodeTypeLabelKey]
		identity := collectorContainerIdentity(pod)
		buf := &strings.Builder{}
		streamCtx, cancel := context.WithCancel(context.Background())
		f.bufs[podName] = buf
		f.states[podName] = &collectorLogFollowState{
			role: role, image: identity.image, imageID: identity.imageID,
			containerID: identity.containerID, restartCount: identity.restartCount,
			cpuRequest: identity.cpuRequest, cpuLimit: identity.cpuLimit,
			memoryRequest: identity.memoryRequest, memoryLimit: identity.memoryLimit,
		}
		f.cancels[podName] = cancel
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			defer cancel()
			stream, err := test.Client().Core().CoreV1().Pods(namespace).
				GetLogs(podName, &corev1.PodLogOptions{Container: "collector", Follow: true}).
				Stream(streamCtx)
			if err != nil {
				f.setLogStreamError(podName, fmt.Sprintf("open stream: %v", err))
				LogWithTimestamp(test.T(), "collector log follow failed for %s: %v", podName, err)
				return
			}
			defer stream.Close()
			chunk := make([]byte, 32*1024)
			for {
				n, err := stream.Read(chunk)
				if n > 0 {
					f.mu.Lock()
					buf.Write(chunk[:n])
					f.mu.Unlock()
				}
				if err == io.EOF {
					f.mu.Lock()
					f.states[podName].completed = true
					f.mu.Unlock()
					return
				}
				if err != nil {
					f.setLogStreamError(podName, fmt.Sprintf("read stream: %v", err))
					return
				}
			}
		}()
	}
	go func() {
		f.wg.Wait()
		close(f.done)
	}()
	return f
}

type collectorContainerRuntimeIdentity struct {
	image         string
	imageID       string
	containerID   string
	restartCount  int32
	cpuRequest    string
	cpuLimit      string
	memoryRequest string
	memoryLimit   string
}

func collectorContainerIdentity(pod corev1.Pod) collectorContainerRuntimeIdentity {
	identity := collectorContainerRuntimeIdentity{}
	for _, container := range pod.Spec.Containers {
		if container.Name != "collector" {
			continue
		}
		identity.cpuRequest = container.Resources.Requests.Cpu().String()
		identity.cpuLimit = container.Resources.Limits.Cpu().String()
		identity.memoryRequest = container.Resources.Requests.Memory().String()
		identity.memoryLimit = container.Resources.Limits.Memory().String()
		break
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "collector" {
			identity.image = status.Image
			identity.imageID = status.ImageID
			identity.containerID = bareContainerID(status.ContainerID)
			identity.restartCount = status.RestartCount
			break
		}
	}
	return identity
}

func (f *collectorLogFollowers) setLogStreamError(pod, message string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if state := f.states[pod]; state != nil {
		state.err = message
	}
}

// CollectAfterTermination waits for the streams to close (containers gone),
// then parses everything captured — including the drain-phase upload lines.
// parseCollectorGC extracts gctrace stats from a collector's own log stream.
func parseCollectorGC(log string) *GCStats {
	matches := gcTraceRe.FindAllStringSubmatch(log, -1)
	if len(matches) == 0 {
		return nil
	}
	last := matches[len(matches)-1]
	st := &GCStats{Cycles: len(matches)}
	st.FinalPercent, _ = strconv.ParseFloat(last[2], 64)
	st.GOMAXPROCS, _ = strconv.Atoi(last[6])
	for _, m := range matches {
		if v, err := strconv.ParseFloat(m[4], 64); err == nil && v > st.PeakHeapMB {
			st.PeakHeapMB = v
		}
	}
	return st
}

func (f *collectorLogFollowers) CollectAfterTermination(timeout time.Duration) []CollectorLogStat {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-f.done:
	case <-timer.C:
		var cancels []context.CancelFunc
		f.mu.Lock()
		for pod, state := range f.states {
			if !state.completed && state.err == "" {
				state.timedOut = true
			}
			if cancel := f.cancels[pod]; cancel != nil {
				cancels = append(cancels, cancel)
			}
		}
		f.mu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}

		// Cancellation unblocks the HTTP response-body Read. Give every follower
		// a bounded chance to publish its final error and stop before snapshotting
		// the buffers; otherwise a late write could be omitted from this artifact.
		cancelWait := 5 * time.Second
		if timeout > 0 && timeout < cancelWait {
			cancelWait = timeout
		}
		cancelTimer := time.NewTimer(cancelWait)
		select {
		case <-f.done:
		case <-cancelTimer.C:
		}
		cancelTimer.Stop()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	stats := make([]CollectorLogStat, 0, len(f.bufs))
	for pod, buf := range f.bufs {
		st := parseCollectorLog(pod, buf.String())
		if state := f.states[pod]; state != nil {
			st.Role = state.role
			st.Image = state.image
			st.ImageID = state.imageID
			st.ContainerID = state.containerID
			st.RestartCount = state.restartCount
			st.CPURequest = state.cpuRequest
			st.CPULimit = state.cpuLimit
			st.MemoryRequest = state.memoryRequest
			st.MemoryLimit = state.memoryLimit
			st.LogStreamComplete = state.completed
			st.LogStreamTimedOut = state.timedOut
			st.LogStreamError = state.err
		}
		st.GC = parseCollectorGC(buf.String())
		stats = append(stats, st)
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].Pod < stats[j].Pod })
	return stats
}

func TestCollectorLogFollowersRecordsCompleteStreamAndRole(t *testing.T) {
	done := make(chan struct{})
	close(done)
	f := &collectorLogFollowers{
		bufs: map[string]*strings.Builder{
			"head-pod": {},
		},
		states: map[string]*collectorLogFollowState{
			"head-pod": {
				role: string(rayv1.HeadNode), image: "collector:v0.1.0",
				imageID: "sha256:collector-image", containerID: "head-container",
				restartCount: 0, cpuRequest: "150m", cpuLimit: "1200m",
				memoryRequest: "160Mi", memoryLimit: "192Mi", completed: true,
			},
		},
		cancels: map[string]context.CancelFunc{},
		done:    done,
	}

	stats := f.CollectAfterTermination(time.Second)
	if len(stats) != 1 || stats[0].Role != string(rayv1.HeadNode) || stats[0].Image != "collector:v0.1.0" ||
		stats[0].ImageID != "sha256:collector-image" || stats[0].ContainerID != "head-container" ||
		stats[0].RestartCount != 0 || !stats[0].LogStreamComplete ||
		stats[0].CPURequest != "150m" || stats[0].CPULimit != "1200m" ||
		stats[0].MemoryRequest != "160Mi" || stats[0].MemoryLimit != "192Mi" ||
		stats[0].LogStreamTimedOut || stats[0].LogStreamError != "" {
		t.Fatalf("unexpected completed follower status: %#v", stats)
	}
}

func TestCollectorContainerIdentityUsesCurrentStatus(t *testing.T) {
	pod := corev1.Pod{
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "collector",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("150m"), corev1.ResourceMemory: resource.MustParse("160Mi")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1200m"), corev1.ResourceMemory: resource.MustParse("192Mi")},
			},
		}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "ray-head", ContainerID: "containerd://ray", RestartCount: 0},
			{Name: "collector", Image: "collector:v0.1.0", ImageID: "sha256:image-id", ContainerID: "containerd://collector-id", RestartCount: 2},
		}},
	}
	identity := collectorContainerIdentity(pod)
	if identity.image != "collector:v0.1.0" || identity.imageID != "sha256:image-id" ||
		identity.containerID != "collector-id" || identity.restartCount != 2 ||
		identity.cpuRequest != "150m" || identity.cpuLimit != "1200m" ||
		identity.memoryRequest != "160Mi" || identity.memoryLimit != "192Mi" {
		t.Fatalf("unexpected collector identity: %#v", identity)
	}
}

func TestCollectorLogFollowersTimeoutCancelsAndWaitsBeforeSnapshot(t *testing.T) {
	done := make(chan struct{})
	cancelled := false
	f := &collectorLogFollowers{
		bufs: map[string]*strings.Builder{
			"worker-pod": {},
		},
		states: map[string]*collectorLogFollowState{
			"worker-pod": {role: string(rayv1.WorkerNode)},
		},
		cancels: map[string]context.CancelFunc{
			"worker-pod": func() {
				cancelled = true
				close(done)
			},
		},
		done: done,
	}

	stats := f.CollectAfterTermination(time.Millisecond)
	if !cancelled {
		t.Fatal("timeout did not cancel the collector log stream")
	}
	if len(stats) != 1 || stats[0].LogStreamComplete || !stats[0].LogStreamTimedOut {
		t.Fatalf("timeout was not preserved in follower status: %#v", stats)
	}
}

func TestCollectorLogFollowersPreservesReadError(t *testing.T) {
	done := make(chan struct{})
	close(done)
	f := &collectorLogFollowers{
		bufs: map[string]*strings.Builder{"worker-pod": {}},
		states: map[string]*collectorLogFollowState{
			"worker-pod": {role: string(rayv1.WorkerNode), err: "read stream: unexpected EOF"},
		},
		cancels: map[string]context.CancelFunc{},
		done:    done,
	}

	stats := f.CollectAfterTermination(time.Second)
	if len(stats) != 1 || stats[0].LogStreamComplete || stats[0].LogStreamError != "read stream: unexpected EOF" {
		t.Fatalf("read error was not preserved in follower status: %#v", stats)
	}
}
