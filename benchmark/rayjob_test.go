package benchmark

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// driverTemplate submits __TASK_COUNT__ no-op tasks in waves of __WAVE_SIZE__.
// Waves bound the number of in-flight ObjectRefs so driver memory stays flat.
// Python code must avoid double quotes: the entrypoint wraps it in `python -c "..."`.
const driverTemplate = `
import ray
import time
ray.init()
T = __TASK_COUNT__
WAVE = __WAVE_SIZE__

@ray.remote(num_cpus=__TASK_NUM_CPUS__)
def bench_task(i):
    return i

t0 = time.time()
done = 0
refs = []
for i in range(T):
    refs.append(bench_task.remote(i))
    if len(refs) >= WAVE:
        ray.get(refs)
        done += len(refs)
        refs = []
        print(f'BENCH_PROGRESS {done}/{T} elapsed={time.time() - t0:.1f}s', flush=True)
if refs:
    ray.get(refs)
    done += len(refs)
wall = time.time() - t0
print(f'BENCH_DONE tasks={done} wall_s={wall:.2f} rate_tps={done / wall:.1f}', flush=True)
# Drain the task-event pipeline before the driver exits: the status queue is
# drained at <=10k events per 1s flush and the shutdown flush is a single
# <=10k batch, so a backlog of B events needs roughly B/10k seconds here.
time.sleep(__DRAIN_SLEEP__)
`

// JobResult captures the load-generation phase.
type JobResult struct {
	WallClock     time.Duration `json:"wallClock"`     // k8s-observed: create -> Succeeded+Complete
	DriverTasks   int           `json:"driverTasks"`   // parsed from BENCH_DONE (0 if log unavailable)
	DriverWallSec float64       `json:"driverWallSec"` // driver-measured seconds
	DriverRateTPS float64       `json:"driverRateTPS"` // driver-measured tasks/s
}

// runBenchJob submits the benchmark RayJob against the existing cluster and
// blocks until it succeeds (or fails fast on a terminal failure status).
func runBenchJob(test Test, g *WithT, namespace *corev1.Namespace, rayCluster *rayv1.RayCluster, cfg benchConfig) JobResult {
	script := strings.NewReplacer(
		"__TASK_COUNT__", strconv.Itoa(cfg.TaskCount),
		"__WAVE_SIZE__", strconv.Itoa(cfg.WaveSize),
		"__TASK_NUM_CPUS__", cfg.TaskNumCPUs,
		"__DRAIN_SLEEP__", strconv.Itoa(cfg.DrainSleepSec),
	).Replace(driverTemplate)

	rayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rayjob-bench",
			Namespace: namespace.Name,
		},
		Spec: rayv1.RayJobSpec{
			Entrypoint: fmt.Sprintf("python -c \"%s\"", script),
			ClusterSelector: map[string]string{
				"ray.io/cluster": rayCluster.Name,
			},
		},
	}

	created, err := test.Client().Ray().RayV1().
		RayJobs(namespace.Name).
		Create(test.Ctx(), rayJob, metav1.CreateOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayJob %s/%s (%d tasks, wave %d)",
		created.Namespace, created.Name, cfg.TaskCount, cfg.WaveSize)

	start := time.Now()
	g.Eventually(func(gg Gomega) {
		job, err := RayJob(test, created.Namespace, created.Name)()
		gg.Expect(err).NotTo(HaveOccurred())
		if job.Status.JobStatus == rayv1.JobStatusFailed {
			StopTrying(fmt.Sprintf("driver failed: %s", job.Status.Message)).Now()
		}
		if job.Status.JobDeploymentStatus == rayv1.JobDeploymentStatusFailed {
			StopTrying(fmt.Sprintf("job deployment failed: %s", job.Status.Message)).Now()
		}
		gg.Expect(job.Status.JobStatus).To(Equal(rayv1.JobStatusSucceeded))
		gg.Expect(job.Status.JobDeploymentStatus).To(Equal(rayv1.JobDeploymentStatusComplete))
	}, cfg.JobTimeout, 5*time.Second).Should(Succeed())

	res := JobResult{WallClock: time.Since(start)}
	parseDriverLog(test, namespace.Name, created.Name, &res)
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
	Pod              string        `json:"pod"`
	Uploads          int           `json:"uploads"`
	UploadedBytes    int64         `json:"uploadedBytes"`
	DiskPressure503s int           `json:"diskPressure503s"`
	RotationQueueFul int           `json:"rotationQueueFull"`
	UploadFailures   int           `json:"uploadFailures"`
	UploadTimeline   []UploadPoint `json:"uploadTimeline"` // reconstructs the local-disk sawtooth
}

// UploadPoint is one upload occurrence parsed from the collector log.
type UploadPoint struct {
	Time  string `json:"time"` // logrus timestamp, kept verbatim
	Bytes int64  `json:"bytes"`
}

// Matches logrus text format: time="..." level=info msg="Uploaded N bytes to ..."
var uploadedRe = regexp.MustCompile(`(?:time="([^"]+)".*?)?Uploaded (\d+) bytes to`)

// parseCollectorLog counts the backpressure/upload signals in one collector log.
func parseCollectorLog(pod, log string) CollectorLogStat {
	stat := CollectorLogStat{
		Pod:              pod,
		DiskPressure503s: strings.Count(log, "under disk pressure"),
		RotationQueueFul: strings.Count(log, "rotation queue full"),
		UploadFailures:   strings.Count(log, "Failed to upload"),
	}
	for _, m := range uploadedRe.FindAllStringSubmatch(log, -1) {
		stat.Uploads++
		n, err := strconv.ParseInt(m[2], 10, 64)
		if err != nil {
			continue
		}
		stat.UploadedBytes += n
		stat.UploadTimeline = append(stat.UploadTimeline, UploadPoint{Time: m[1], Bytes: n})
	}
	return stat
}

// collectorLogFollowers streams collector logs with follow=true. A post-hoc
// GetLogs cannot see the drain phase: most uploads happen during pod
// termination, and by then the pods are gone. Streams end naturally when the
// containers terminate.
type collectorLogFollowers struct {
	mu   sync.Mutex
	bufs map[string]*strings.Builder
	wg   sync.WaitGroup
}

// startCollectorLogFollowers must be called while the Ray pods are Running.
func startCollectorLogFollowers(test Test, namespace string) *collectorLogFollowers {
	f := &collectorLogFollowers{bufs: map[string]*strings.Builder{}}
	pods, err := test.Client().Core().CoreV1().Pods(namespace).List(test.Ctx(), metav1.ListOptions{
		LabelSelector: "test=raycluster-historyserver",
	})
	if err != nil {
		LogWithTimestamp(test.T(), "Failed to list Ray pods for collector log following: %v", err)
		return f
	}
	for _, pod := range pods.Items {
		podName := pod.Name
		buf := &strings.Builder{}
		f.bufs[podName] = buf
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			stream, err := test.Client().Core().CoreV1().Pods(namespace).
				GetLogs(podName, &corev1.PodLogOptions{Container: "collector", Follow: true}).
				Stream(context.Background())
			if err != nil {
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
				if err != nil {
					return
				}
			}
		}()
	}
	return f
}

// CollectAfterTermination waits for the streams to close (containers gone),
// then parses everything captured — including the drain-phase upload lines.
func (f *collectorLogFollowers) CollectAfterTermination(timeout time.Duration) []CollectorLogStat {
	done := make(chan struct{})
	go func() {
		f.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	stats := make([]CollectorLogStat, 0, len(f.bufs))
	for pod, buf := range f.bufs {
		stats = append(stats, parseCollectorLog(pod, buf.String()))
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].Pod < stats[j].Pod })
	return stats
}
