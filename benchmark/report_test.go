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
	StartedAt     time.Time          `json:"startedAt"`
	Config        benchConfig        `json:"config"`
	Env           EnvInfo            `json:"env"`
	SessionID     string             `json:"sessionID"`
	Job           JobResult          `json:"job"`
	FlushDuration time.Duration      `json:"flushDuration"` // cluster deletion incl. final collector upload
	CollectorLogs []CollectorLogStat `json:"collectorLogs"`
	StorageDiffs  []SnapshotDiff     `json:"storageDiffs"` // bucket deltas: during-job vs shutdown flush
	Storage       StorageReport      `json:"storage"`
	HistoryServer HSBenchResult      `json:"historyServer"`
	Resources     []ResourceUsage    `json:"resources"` // kubelet summary API (working_set, k8s semantics)
	Cgroups       []CgroupUsage      `json:"cgroups"`   // direct cgroup v2 reads (anon/peak, 1s)
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
		t.Logf("write bench-report.md: %v", err)
	}
	if data, err := json.MarshalIndent(r, "", "  "); err == nil {
		if err := os.WriteFile(filepath.Join(runDir, "bench-report.json"), data, 0o644); err != nil {
			t.Logf("write bench-report.json: %v", err)
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
	for _, n := range r.Env.Nodes {
		w("- Node %s: %s, %s, cpu=%s, mem=%s", n.Name, n.KubeletVersion, n.OSImage, n.CPU, n.Memory)
	}
	w("- Session: `%s`", r.SessionID)
	w("")

	w("## Load generation")
	w("")
	w("| metric | value |")
	w("|---|---|")
	w("| RayJob wall clock (k8s-observed) | %s |", r.Job.WallClock.Round(time.Second))
	if r.Job.DriverTasks > 0 {
		w("| driver-measured wall | %.1fs |", r.Job.DriverWallSec)
		w("| driver-measured rate | %.1f tasks/s |", r.Job.DriverRateTPS)
	}
	w("| flush (cluster deletion incl. final upload) | %s |", r.FlushDuration.Round(time.Second))
	w("")

	w("## Collector")
	w("")
	if len(r.CollectorLogs) > 0 {
		w("| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |")
		w("|---|---|---|---|---|---|")
		for _, c := range r.CollectorLogs {
			w("| %s | %d | %s | %d | %d | %d |",
				c.Pod, c.Uploads, formatBytes(c.UploadedBytes), c.DiskPressure503s, c.RotationQueueFul, c.UploadFailures)
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

	if len(r.Cgroups) > 0 {
		w("## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)")
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

	if len(r.StorageDiffs) > 0 {
		w("## Storage delta (bucket snapshots)")
		w("")
		w("| window | added objs | added bytes | changed objs | changed bytes | deleted objs |")
		w("|---|---|---|---|---|---|")
		for _, d := range r.StorageDiffs {
			w("| %s | %d | %s | %d | %s | %d |",
				d.Label, d.AddedObjects, formatBytes(d.AddedBytes),
				d.ChangedObjects, formatBytes(d.ChangedBytes), d.DeletedObjects)
		}
		w("")
		for _, d := range r.StorageDiffs {
			if len(d.UnexpectedKeys) > 0 {
				w("- WARNING: %s added keys outside expected prefixes: %v", d.Label, d.UnexpectedKeys)
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
	w("| distinct taskIds in benchmark job `%s` | **%d / %d expected** |", e.BenchJobID, e.BenchJobTaskIDs, e.ExpectedTasks)
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

	h := r.HistoryServer
	w("## History server")
	w("")
	w("| metric | value |")
	w("|---|---|")
	w("| GET /clusters p50 / p95 / max | %s / %s / %s (errors: %d) |",
		h.ListClusters.P50.Round(time.Millisecond), h.ListClusters.P95.Round(time.Millisecond),
		h.ListClusters.Max.Round(time.Millisecond), h.ListClusters.Errors)
	w("| /enter_cluster cold load | %s (HTTP %d) |", h.EnterColdLatency.Round(time.Millisecond), h.EnterStatus)
	w("")
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
