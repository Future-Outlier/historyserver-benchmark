package benchmark

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// EnvInfo records where the numbers were produced; kind-on-macOS CPU figures
// are VM-noisy, so a report without this context is not comparable.
type EnvInfo struct {
	Nodes []NodeInfo `json:"nodes"`
}

type NodeInfo struct {
	Name           string `json:"name"`
	KubeletVersion string `json:"kubeletVersion"`
	OSImage        string `json:"osImage"`
	CPU            string `json:"cpu"`
	Memory         string `json:"memory"`
}

// Report is the single artifact of a benchmark run (also serialized to JSON).
type Report struct {
	StartedAt                time.Time                `json:"startedAt"`
	Config                   benchConfig              `json:"config"`
	Env                      EnvInfo                  `json:"env"`
	Namespace                string                   `json:"namespace,omitempty"`
	NamespaceUID             string                   `json:"namespaceUID,omitempty"`
	ExecutionNamespace       string                   `json:"executionNamespace,omitempty"`
	ExecutionNamespaceUID    string                   `json:"executionNamespaceUID,omitempty"`
	ClusterName              string                   `json:"clusterName,omitempty"`
	SessionID                string                   `json:"sessionID"`
	Job                      JobResult                `json:"job"`
	RayJobLifecycle          RayJobLifecycleEvidence  `json:"rayJobLifecycle"`
	FlushDuration            time.Duration            `json:"flushDuration"` // cluster deletion incl. final collector upload
	CollectorLogs            []CollectorLogStat       `json:"collectorLogs"`
	StorageDiffs             []SnapshotDiff           `json:"storageDiffs"`     // bucket deltas: during-job vs shutdown flush
	StorageIsolation         SnapshotDiff             `json:"storageIsolation"` // whole lifecycle: before any Collector exists through final flush
	Storage                  StorageReport            `json:"storage"`
	HistoryServer            HSBenchResult            `json:"historyServer"`
	HistoryServerPhases      []HSPhaseTimestamp       `json:"historyServerPhases,omitempty"`
	HSCPUCheckpoints         []HSCPUCheckpoint        `json:"hsCPUCheckpoints,omitempty"`
	HSRequestIsolation       HSRequestIsolation       `json:"hsRequestIsolation"`
	HSPodEvidence            HSPodEvidence            `json:"hsPodEvidence"`
	HSValidation             HSValidation             `json:"hsValidation"`
	SourceSessionFingerprint SourceSessionFingerprint `json:"sourceSessionFingerprint"`
	// One entry per session when several are loaded into the same server, in
	// load order; HistoryServer repeats the first for readers that expect one.
	HistoryServerSessions []HSBenchResult           `json:"historyServerSessions,omitempty"`
	Resources             []ResourceUsage           `json:"resources"`                       // kubelet summary API (working_set, k8s semantics)
	SpoolPeakMiB          map[string]float64        `json:"spoolPeakMiB,omitempty"`          // collector on-disk backlog per pod UID: the 200MB budget, not the heap, is what backpressure defends
	CgroupSampler         CgroupSamplerStatus       `json:"cgroupSampler"`                   // fail-closed start/completion/error state for the raw cgroup stream
	Cgroups               []CgroupUsage             `json:"cgroups"`                         // direct cgroup v2 reads (anon/peak, 0.5s sleep plus scan time)
	CollectorWindows      []CollectorResourceWindow `json:"collectorWindows,omitempty"`      // receive-time events/bytes paired with the same collector's cgroup data in fixed 10s windows
	CollectorIngressGates []CollectorIngressGate    `json:"collectorIngressGates,omitempty"` // fail-closed per-collector NodeID, coverage, rejection, and queue-pressure verdict
	Timeline              []TimelineEvent           `json:"timeline,omitempty"`              // owned-mode deletion sequence (job end -> cluster delete -> pod SIGTERM)
	PodTerminations       []PodTermination          `json:"podTerminations,omitempty"`       // last observed container exits; 0-in-grace vs 137 is the durability verdict
	// Completed is set only when every configured phase ran. Collector-only runs
	// deliberately omit the History Server phase; aborted runs still leave zero
	// values in untouched fields, so every consumer must gate on this value and
	// Config.SkipHistoryServer.
	Completed bool `json:"completed"`
}

func captureEnvInfo(test Test) EnvInfo {
	info := EnvInfo{}
	nodes, err := test.Client().Core().CoreV1().Nodes().List(test.Ctx(), metav1.ListOptions{})
	if err != nil {
		return info
	}
	for _, n := range nodes.Items {
		info.Nodes = append(info.Nodes, NodeInfo{
			Name:           n.Name,
			KubeletVersion: n.Status.NodeInfo.KubeletVersion,
			OSImage:        n.Status.NodeInfo.OSImage,
			CPU:            n.Status.Allocatable.Cpu().String(),
			Memory:         n.Status.Allocatable.Memory().String(),
		})
	}
	return info
}

// writeReport renders bench-report.md / bench-report.json into runDir and logs
// the markdown so `go test -v` output alone is enough to read the results.
func writeReport(t *testing.T, r *Report, runDir string) {
	md := renderMarkdown(r)
	if err := os.WriteFile(filepath.Join(runDir, "bench-report.md"), []byte(md), 0o644); err != nil {
		t.Errorf("write bench-report.md: %v", err)
	}
	if data, err := json.MarshalIndent(r, "", "  "); err != nil {
		t.Errorf("marshal bench-report.json: %v", err)
	} else {
		if err := os.WriteFile(filepath.Join(runDir, "bench-report.json"), data, 0o644); err != nil {
			t.Errorf("write bench-report.json: %v", err)
		}
	}
	t.Logf("benchmark report written to %s\n%s", runDir, md)
}

func renderMarkdown(r *Report) string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }

	w("# History Server Benchmark Report")
	w("")
	w("- Date: %s", r.StartedAt.Format(time.RFC3339))
	w("- Tasks: %d (wave %d, num_cpus=%s), compression=%v",
		r.Config.TaskCount, r.Config.WaveSize, r.Config.TaskNumCPUs, r.Config.Compression)
	w("- Storage bucket: `%s`", r.Config.S3Bucket)
	for _, n := range r.Env.Nodes {
		w("- Node %s: %s, %s, cpu=%s, mem=%s", n.Name, n.KubeletVersion, n.OSImage, n.CPU, n.Memory)
	}
	w("- Session: `%s`", r.SessionID)
	w("")

	w("## Load generation")
	w("")
	w("| metric | value |")
	w("|---|---|")
	targetTaskRate := "unpaced (0)"
	if r.Config.TargetTaskRate > 0 {
		targetTaskRate = fmt.Sprintf("%d tasks/s", r.Config.TargetTaskRate)
	}
	achievedTaskRate := "not measured"
	if r.Job.DriverTasks > 0 {
		achievedTaskRate = fmt.Sprintf("%.1f tasks/s", r.Job.DriverRateTPS)
	}
	w("| configured task num_cpus | %s |", r.Config.TaskNumCPUs)
	w("| configured wave size | %d tasks |", r.Config.WaveSize)
	w("| configured drivers | %d |", r.Config.Drivers)
	w("| configured target task rate | %s |", targetTaskRate)
	w("| achieved driver task rate | %s |", achievedTaskRate)
	w("| configured post-job driver drain sleep | %ds |", r.Config.DrainSleepSec)
	w("| configured skip History Server | %v |", r.Config.SkipHistoryServer)
	w("| configured shutdownAfterJobFinishes / TTL | %v / %ds |",
		r.Config.ShutdownAfterJob, r.Config.JobTTLSeconds)
	w("| observed RayJob owned / shutdownAfterJobFinishes / TTL | %v / %v / %ds |",
		r.RayJobLifecycle.OwnedCluster, r.RayJobLifecycle.ShutdownAfterJobFinishes,
		r.RayJobLifecycle.TTLSecondsAfterFinished)
	w("| observed RayJob / submitter backoffLimit | %s / %s |",
		formatOptionalInt32(r.RayJobLifecycle.RayJobBackoffLimit),
		formatOptionalInt32(r.RayJobLifecycle.SubmitterBackoffLimit))
	w("| RayJob wall clock (k8s-observed) | %s |", r.Job.WallClock.Round(time.Second))
	if r.Job.DriverTasks > 0 {
		w("| driver-measured wall | %.1fs |", r.Job.DriverWallSec)
	}
	w("| flush (cluster deletion incl. final upload) | %s |", r.FlushDuration.Round(time.Second))
	w("")

	w("## Collector")
	w("")
	if len(r.CollectorLogs) > 0 {
		w("| pod | role | image | image ID | container ID | CPU req/lim | memory req/lim | cgroup memory.max | oom/oom_kill | restarts | log complete | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |")
		w("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
		for _, c := range r.CollectorLogs {
			w("| %s | %s | %s | %s | %s | %s/%s | %s/%s | %s | %d/%d | %d | %v | %d | %s | %d | %d | %d |",
				c.Pod, c.Role, c.Image, c.ImageID, c.ContainerID,
				c.CPURequest, c.CPULimit, c.MemoryRequest, c.MemoryLimit, c.CgroupMemoryMax,
				c.MemoryEventsOOM, c.MemoryEventsOOMKill, c.RestartCount, c.LogStreamComplete,
				c.Uploads, formatBytes(c.UploadedBytes), c.DiskPressure503s, c.RotationQueueFul, c.UploadFailures)
		}
		w("")
	}

	w("## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)")
	w("")
	w("| class | phase | samples | peak working set (MiB) | avg cores | peak cores |")
	w("|---|---|---|---|---|---|")
	for _, u := range r.Resources {
		w("| %s | %s | %d | %.1f | %.3f | %.3f |",
			u.Class, u.Phase, u.Samples, u.PeakWorkingSetMiB, u.AvgCores, u.PeakCores)
	}
	w("")

	w("## Cgroup sampler status")
	w("")
	w("- started=%v, stream complete=%v, ended-before-stop=%v, start error=%q, stream error=%q",
		r.CgroupSampler.Started, r.CgroupSampler.StreamComplete, r.CgroupSampler.StreamEndedBeforeStop,
		r.CgroupSampler.StartError, r.CgroupSampler.StreamError)
	w("")
	if len(r.Cgroups) > 0 {
		w("## Container resources — cgroup v2 direct (anon = anonymous memory, 0.5s sleep + scan time; lifetime peak = kernel memory.peak)")
		w("")
		w("| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |")
		w("|---|---|---|---|---|---|---|---|")
		for _, u := range r.Cgroups {
			lifetime := ""
			if u.LifetimePeakMiB > 0 {
				lifetime = fmt.Sprintf("%.1f", u.LifetimePeakMiB)
			}
			w("| %s | %s | %d | %.1f | %.1f | %.3f | %.3f | %s |",
				u.Container, u.Phase, u.Samples, u.PeakAnonMiB, u.PeakCurrentMiB, u.AvgCores, u.PeakCores, lifetime)
		}
		w("")
	}

	if len(r.CollectorWindows) > 0 {
		w("## Collector receive-time load aligned with cgroup resources (10s wall-clock windows)")
		w("")
		w("| window start | submitted-to-worker attempts | finished attempts | backlog delta | pod | Ray NodeID | lifecycle bound | batches | events/s | request KiB/s | rejected | queue full | cgroup errors | CPU intervals | required | max gap s | CPU coverage | cadence valid | avg cores | peak cores | peak anon MiB | peak current MiB | valid for sizing |")
		w("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
		for _, row := range r.CollectorWindows {
			w("| %s | %d | %d | %+d | %s | %s | %v | %d | %.1f | %.1f | %d | %d | %d | %d | %d | %.2f | %.0f%% | %v | %.3f | %.3f | %.1f | %.1f | %v |",
				time.Unix(0, row.WindowStartUnixNano).UTC().Format(time.RFC3339), row.SubmittedToWorkerAttempts,
				row.FinishedAttempts, row.BacklogDelta, row.Pod, row.NodeID,
				row.ContainerLifecycleBound, row.Batches, row.EventsPerSecond, row.RequestBytesPerSecond/1024,
				row.RejectedRequests, row.RotationQueueFull, row.CgroupReadErrors, row.CPUIntervals, row.RequiredCPUIntervals,
				row.MaxCPUIntervalSeconds, 100*row.CPUCoverageRatio, row.CPUSamplingValid, row.AvgCores, row.PeakCores,
				float64(row.PeakAnonBytes)/(1<<20), float64(row.PeakCurrentBytes)/(1<<20), row.ValidForSizing)
		}
		w("")
		w("- `submitted-to-worker/finished` are job-wide attempt-0 lifecycle counts repeated on both Collector rows for the same wall-clock bucket; do not add the head and worker values together. `submitted-to-worker` is a Ray scheduler transition, not the instant Python called `.remote()`. Collector events/resources remain per pod and per Ray NodeID.")
		w("- Task counts use Ray transition time; Collector events/resources use HTTP receive time. Raw task buckets are in `task_lifecycle_10s.csv`; paired rows are in `collector_ingress_cgroup_10s.csv`.")
		w("- A valid 10s Collector row needs >=80%% CPU coverage, at least 4 intervals, max interval <=2s, and the same non-restarted collector container lifecycle.")
		w("")
	}
	if len(r.CollectorIngressGates) > 0 {
		w("### Collector ingress validity gate")
		w("")
		w("| pod | role | container ID | restarts | lifecycle bound | windows | valid windows | cgroup errors | peak windows | peak cadence valid | peak events/s | worst peak CPU coverage | max peak gap s | log complete | graceful shutdown | cgroup complete | valid | problems |")
		w("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
		for _, gate := range r.CollectorIngressGates {
			w("| %s | %s | %s | %d | %v | %d | %d | %d | %d | %d | %.1f | %.0f%% | %.2f | %v | %v | %v | %v | %s |",
				gate.Pod, gate.Role, gate.ContainerID, gate.RestartCount, gate.LifecycleBound,
				gate.Windows, gate.ValidWindows, gate.CgroupReadErrors, gate.PeakWindows, gate.PeakSamplingValidWindows,
				gate.PeakEventsPerSecond, 100*gate.PeakWindowCPUCoverage,
				gate.PeakWindowMaxCPUIntervalSeconds, gate.LogStreamComplete, gate.GracefulShutdownComplete, gate.CgroupSamplerComplete,
				gate.Valid, strings.Join(gate.Problems, "; "))
		}
		w("")
		w("- Machine-readable gate: `collector_ingress_gate.csv` and `collectorIngressGates` in `bench-report.json`.")
		w("")
	}

	if len(r.StorageDiffs) > 0 {
		w("## Storage delta (bucket snapshots)")
		w("")
		w("| window | added objs | added bytes | changed objs | changed bytes | deleted objs |")
		w("|---|---|---|---|---|---|")
		storageDiffs := append([]SnapshotDiff{r.StorageIsolation}, r.StorageDiffs...)
		for _, d := range storageDiffs {
			w("| %s | %d | %s | %d | %s | %d |",
				d.Label, d.AddedObjects, formatBytes(d.AddedBytes),
				d.ChangedObjects, formatBytes(d.ChangedBytes), d.DeletedObjects)
		}
		w("")
		for _, d := range storageDiffs {
			if len(d.UnexpectedKeys) > 0 {
				w("- WARNING: %s added keys outside the current session/marker: %v", d.Label, d.UnexpectedKeys)
			}
			if len(d.UnexpectedChangedKeys) > 0 {
				w("- WARNING: %s changed keys outside the current session/marker: %v", d.Label, d.UnexpectedChangedKeys)
			}
			if len(d.DeletedKeys) > 0 {
				w("- WARNING: %s deleted keys: %v", d.Label, d.DeletedKeys)
			}
		}
		w("")
	}

	w("## Storage footprint (session prefix)")
	w("")
	w("| category | bytes | share |")
	w("|---|---|---|")
	cats := make([]string, 0, len(r.Storage.Categories))
	for c := range r.Storage.Categories {
		cats = append(cats, c)
	}
	sort.Strings(cats)
	for _, c := range cats {
		share := 0.0
		if r.Storage.TotalBytes > 0 {
			share = 100 * float64(r.Storage.Categories[c]) / float64(r.Storage.TotalBytes)
		}
		w("| %s | %s | %.1f%% |", c, formatBytes(r.Storage.Categories[c]), share)
	}
	w("| **total** | **%s** | (%d objects) |", formatBytes(r.Storage.TotalBytes), r.Storage.ObjectCount)
	w("")
	w("- Session marker present: %v", r.Storage.MarkerPresent)
	w("")

	e := r.Storage.Events
	w("## Event statistics")
	w("")
	w("| metric | value |")
	w("|---|---|")
	w("| total events | %d |", e.TotalEvents)
	w("| task-scoped events (TASK_* + ACTOR_TASK_*) | %d |", e.TaskScopedEvents)
	w("| events per task (k) | %.2f |", e.EventsPerTask)
	w("| raw JSONL bytes | %s |", formatBytes(e.RawJSONLBytes))
	w("| stored event bytes | %s |", formatBytes(e.StoredEventBytes))
	w("| avg raw bytes/event | %.0f |", e.AvgRawBytesPerEvent)
	w("| compression ratio (stored/raw) | %.3f |", e.CompressionRatio)
	w("| distinct taskDefinitionEvent taskIds (all jobs) | %d |", e.DistinctTaskDefIDs)
	// BenchTaskIDs, not BenchJobTaskIDs: the per-job count is taken over ALL
	// definition events in one job, so the driver's own definitions pad the
	// total and can hide missing bench tasks; and with several drivers the work
	// spans several jobs, which a single-job count cannot see. This row is the
	// loss verdict, so it must use the name-filtered, cross-job set.
	w("| distinct `bench_task` taskIds (loss metric) | **%d / %d expected** |", e.BenchTaskIDs, e.ExpectedTasks)
	v := e.BenchTaskValidity
	verdict := "FAIL"
	if v.Valid {
		verdict = "PASS"
	}
	w("| benchmark task validity | **%s** |", verdict)
	w("| distinct `bench_task` attempts | %d / %d expected |", v.ObservedAttempts, v.ExpectedTaskIDs)
	w("| `bench_task` attempt_number = 0 | %d / %d expected |", v.AttemptZero, v.ExpectedTaskIDs)
	w("| `bench_task` latest state = FINISHED | %d / %d expected |", v.FinishedAttempts, v.ExpectedTaskIDs)
	if len(v.Problems) > 0 {
		w("| task validity problems | %s |", strings.Join(v.Problems, "; "))
	}
	w("| distinct taskIds in largest single job `%s` | %d |", e.BenchJobID, e.BenchJobTaskIDs)
	metadata := e.TaskLogMetadata
	w("| task-log metadata canonical SHA-256 | %s |", metadata.SHA256)
	w("| task-log metadata nil / present / incomplete nonnil / structurally invalid | %d / %d / %d / %d |",
		metadata.Counts.Nil, metadata.Counts.Present, metadata.Counts.IncompleteNonNil,
		metadata.Counts.StructurallyInvalid)
	w("| exact stdout / stderr ranges | %d / %d |",
		metadata.Counts.StdoutExactResolvable, metadata.Counts.StderrExactResolvable)
	w("| legacy whole-worker fallback | %d (not task-exact) |", metadata.Counts.LegacyWholeWorkerFallback)
	w("")
	w("- `incompleteNonNil` is expected-unavailable for exact task-log resolution and may return 404; it is not eligible for worker-log fallback.")
	w("- `legacyWholeWorkerFallback` means nil TaskLogInfo plus node/worker identity. It can expose only a whole worker file, is not task-exact, and is not reachable from the normal frontend task-log tabs.")
	w("")
	if len(e.PerNode) > 0 {
		w("### Per-node attribution (whose aggregator emitted the events)")
		w("")
		w("| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |")
		w("|---|---|---|---|---|---|")
		for _, n := range e.PerNode {
			w("| %s | %d | %s | %d | %d | %.1f |",
				n.NodeID, n.Events, formatBytes(n.RawBytes), n.DistinctTaskIDs, n.Peak1sEvents, n.Peak10sEventsPerSec)
		}
		w("")
	}
	types := make([]string, 0, len(e.CountByType))
	for typ := range e.CountByType {
		types = append(types, typ)
	}
	sort.Strings(types)
	w("| event type | count |")
	w("|---|---|")
	for _, typ := range types {
		w("| %s | %d |", typ, e.CountByType[typ])
	}
	w("")

	w("## History server")
	w("")
	if r.Config.SkipHistoryServer {
		w("- Skipped by `BENCH_SKIP_HISTORY_SERVER=true` (Collector-only run).")
		return b.String()
	}

	h := r.HistoryServer
	w("| metric | value |")
	w("|---|---|")
	w("| GET /clusters p50 / p95 / max | %s / %s / %s (errors: %d) |",
		h.ListClusters.P50.Round(time.Millisecond), h.ListClusters.P95.Round(time.Millisecond),
		h.ListClusters.Max.Round(time.Millisecond), h.ListClusters.Errors)
	w("| /enter_cluster cold load | %s (HTTP %d) |", h.EnterColdLatency.Round(time.Millisecond), h.EnterStatus)
	w("| /enter_cluster attempts | %d |", h.EnterAttempts)
	w("")
	if r.Config.HSStrictCold {
		pod := r.HSPodEvidence
		validation := r.HSValidation
		fingerprint := r.SourceSessionFingerprint
		w("### Formal History Server validity")
		w("")
		w("- Scope: replay=%v, task-list=%v, `/logs/file`=%v. This sizing run does not call or size the log-file endpoint.",
			validation.Scope.Replay, validation.Scope.TaskList, validation.Scope.LogsFile)
		w("- A nonnil but incomplete TaskLogInfo is expected-unavailable and can return 404; it is not a worker-log fallback. A nil TaskLogInfo with node/worker identity is only a legacy whole-worker fallback, not a task-exact range.")
		w("")
		w("| evidence | value |")
		w("|---|---|")
		w("| execution namespace | %s |", r.ExecutionNamespace)
		w("| actual CPU request / limit | %s / %s |", pod.CPURequest, pod.CPULimit)
		w("| actual memory request / limit | %s / %s |", pod.MemoryRequest, pod.MemoryLimit)
		w("| pod UID / container ID | %s / %s |", pod.PodUID, pod.ContainerID)
		w("| restarts / OOMKilled | %d / %v |", pod.RestartCount, pod.OOMKilled)
		w("| cgroup memory.max / oom / oom_kill | %s / %d / %d |",
			pod.CgroupMemoryMax, pod.MemoryEventsOOM, pod.MemoryEventsOOMKill)
		w("| source fingerprint algorithm | %s |", fingerprint.Algorithm)
		w("| source fingerprint start=end | %v |", fingerprint.Start != "" && fingerprint.Start == fingerprint.End)
		w("| source objects / bytes | %d / %d |", fingerprint.ObjectCount, fingerprint.TotalBytes)
		w("| benchmark-task count query HTTP / num_filtered | %d / %d |",
			validation.TaskCountQuery.HTTPStatus, validation.TaskCountQuery.NumFiltered)
		w("| Q=1 detailed warm query rows / metadata hash / projection match | %d / %s / %v |",
			validation.WarmTaskQuery.Rows, validation.WarmTaskQuery.TaskLogMetadata.SHA256,
			validation.WarmTaskQuery.ProjectionMatches)
		w("| production replay attempts / metadata hash / errors | %d / %s / %d |",
			validation.FullReplay.ObservedAttempts, validation.FullReplay.TaskLogMetadata.SHA256,
			validation.FullReplay.TotalErrors)
		w("| lifetime memory.peak | %d bytes |", validation.LifetimeMemoryPeakBytes)
		w("| measurement valid / cold SLO met | **%v** / **%v** |",
			validation.MeasurementValid, validation.MeetsColdSLO)
		w("| pod / logs / replay valid | %v / %v / %v |",
			pod.Valid, validation.Logs.Valid, validation.FullReplay.Valid)
		if len(validation.Problems) > 0 {
			w("| validity problems | %s |", strings.Join(validation.Problems, "; "))
		}
		w("")
	}
	if len(h.WarmEndpoints) > 0 {
		w("| warm endpoint | p50 | p95 | max | resp bytes | errors |")
		w("|---|---|---|---|---|---|")
		for _, ep := range h.WarmEndpoints {
			w("| %s | %s | %s | %s | %s | %d |",
				ep.Endpoint, ep.P50.Round(time.Millisecond), ep.P95.Round(time.Millisecond),
				ep.Max.Round(time.Millisecond), formatBytes(ep.LastBytes), ep.Errors)
		}
		w("")
	}
	for _, note := range h.Notes {
		w("- NOTE: %s", note)
	}
	return b.String()
}

func formatOptionalInt32(value *int32) string {
	if value == nil {
		return "null"
	}
	return fmt.Sprintf("%d", *value)
}

func TestRenderMarkdownIncludesFormalHSEvidence(t *testing.T) {
	report := validHSFormalReportFixture()
	markdown := renderMarkdown(&report)
	for _, fragment := range []string{
		"### Formal History Server validity",
		"| /enter_cluster attempts | 1 |",
		"| actual CPU request / limit | 1 / 1 |",
		"| benchmark-task count query HTTP / num_filtered | 200 / 50000 |",
		"Scope: replay=true, task-list=true, `/logs/file`=false",
		"| Q=1 detailed warm query rows / metadata hash / projection match | 10000 / " + strings.Repeat("f", 64) + " / true |",
		"| production replay attempts / metadata hash / errors | 50000 / " + strings.Repeat("e", 64) + " / 0 |",
		"| measurement valid / cold SLO met | **true** / **true** |",
		"| pod / logs / replay valid | true / true / true |",
	} {
		if !strings.Contains(markdown, fragment) {
			t.Fatalf("formal report is missing %q:\n%s", fragment, markdown)
		}
	}
}

func TestRenderMarkdownDistinguishesPacingFromPostJobDrain(t *testing.T) {
	tests := []struct {
		name       string
		targetRate int
		skipHS     bool
		wantTarget string
	}{
		{name: "paced Collector-only", targetRate: 1000, skipHS: true, wantTarget: "1000 tasks/s"},
		{name: "unpaced full run", targetRate: 0, skipHS: false, wantTarget: "unpaced (0)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := &Report{
				Config: benchConfig{
					TaskNumCPUs:       "0.5",
					WaveSize:          2000,
					Drivers:           1,
					TargetTaskRate:    tt.targetRate,
					DrainSleepSec:     0,
					SkipHistoryServer: tt.skipHS,
					ShutdownAfterJob:  true,
					JobTTLSeconds:     30,
				},
				Job: JobResult{
					DriverTasks:   50000,
					DriverRateTPS: 987.6,
				},
				RayJobLifecycle: RayJobLifecycleEvidence{
					OwnedCluster:             true,
					ShutdownAfterJobFinishes: true,
					TTLSecondsAfterFinished:  30,
					RayJobBackoffLimit:       int32Pointer(0),
					SubmitterBackoffLimit:    int32Pointer(0),
				},
			}
			markdown := renderMarkdown(report)
			for _, fragment := range []string{
				"| configured task num_cpus | 0.5 |",
				"| configured wave size | 2000 tasks |",
				"| configured drivers | 1 |",
				"| configured target task rate | " + tt.wantTarget + " |",
				"| achieved driver task rate | 987.6 tasks/s |",
				"| configured post-job driver drain sleep | 0s |",
				fmt.Sprintf("| configured skip History Server | %v |", tt.skipHS),
				"| configured shutdownAfterJobFinishes / TTL | true / 30s |",
				"| observed RayJob owned / shutdownAfterJobFinishes / TTL | true / true / 30s |",
				"| observed RayJob / submitter backoffLimit | 0 / 0 |",
			} {
				if !strings.Contains(markdown, fragment) {
					t.Fatalf("report is missing %q:\n%s", fragment, markdown)
				}
			}
			if tt.skipHS && !strings.Contains(markdown,
				"Skipped by `BENCH_SKIP_HISTORY_SERVER=true` (Collector-only run).") {
				t.Fatalf("Collector-only report does not explain the skipped History Server phase:\n%s", markdown)
			}
		})
	}
}

func formatBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
