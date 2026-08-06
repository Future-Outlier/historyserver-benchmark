package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// sampledContainers is the set of container names worth recording.
var sampledContainers = map[string]bool{
	"collector":     true,
	"ray-head":      true,
	"ray-worker":    true,
	"historyserver": true,
}

// resourceSample is one kubelet stats/summary observation of one container.
//
// CPUStatTime is the timestamp kubelet attached to the stat, NOT our poll
// time: kubelet refreshes its cAdvisor cache only every ~10s, so consecutive
// polls often return the same stat. Deltas must be computed between distinct
// stat timestamps or peak rates get inflated by up to refresh/poll ratio.
type resourceSample struct {
	Time            time.Time
	Phase           string
	Pod             string
	Container       string
	CPUStatTime     time.Time
	CPUUsageNanoSec uint64 // cumulative usageCoreNanoSeconds
	WorkingSetBytes uint64
}

// statsSummary mirrors the subset of the kubelet /stats/summary payload we read.
type statsSummary struct {
	Pods []struct {
		PodRef struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"podRef"`
		Containers []struct {
			Name string `json:"name"`
			CPU  *struct {
				Time                 string  `json:"time"`
				UsageCoreNanoSeconds *uint64 `json:"usageCoreNanoSeconds"`
			} `json:"cpu"`
			Memory *struct {
				WorkingSetBytes *uint64 `json:"workingSetBytes"`
			} `json:"memory"`
		} `json:"containers"`
	} `json:"pods"`
}

// resourceSampler polls the kubelet summary API for container CPU/memory.
// It needs no metrics-server or Prometheus: kind's kubelet serves
// /api/v1/nodes/{node}/proxy/stats/summary out of the box. Effective
// resolution is bounded by kubelet housekeeping (~10s); the cgroupSampler
// covers the 1s-resolution needs.
type resourceSampler struct {
	test     Test
	ns       string
	interval time.Duration

	mu      sync.Mutex
	phase   string
	marks   []phaseMark
	samples []resourceSample
	errs    int

	nodes    []string
	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
	started  bool
}

func newResourceSampler(test Test, namespace string, interval time.Duration) *resourceSampler {
	return &resourceSampler{
		test:     test,
		ns:       namespace,
		interval: interval,
		phase:    "baseline",
		marks:    []phaseMark{{Name: "baseline", At: time.Now()}},
		done:     make(chan struct{}),
	}
}

func (s *resourceSampler) Start() {
	nodes, err := s.test.Client().Core().CoreV1().Nodes().List(s.test.Ctx(), metav1.ListOptions{})
	if err != nil {
		LogWithTimestamp(s.test.T(), "sampler: failed to list nodes, sampling disabled: %v", err)
		close(s.done)
		return
	}
	for _, n := range nodes.Items {
		s.nodes = append(s.nodes, n.Name)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.started = true
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.pollOnce(ctx)
			}
		}
	}()
}

func (s *resourceSampler) SetPhase(phase string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.phase = phase
	s.marks = append(s.marks, phaseMark{Name: phase, At: time.Now()})
}

// Marks returns the phase timeline, shared with the cgroup sampler so both
// summaries bucket samples identically.
func (s *resourceSampler) Marks() []phaseMark {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]phaseMark, len(s.marks))
	copy(out, s.marks)
	return out
}

func (s *resourceSampler) Stop() {
	s.stopOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		if s.started {
			<-s.done
		}
	})
}

func (s *resourceSampler) pollOnce(ctx context.Context) {
	now := time.Now()
	for _, node := range s.nodes {
		raw, err := s.test.Client().Core().CoreV1().RESTClient().Get().
			AbsPath("/api/v1/nodes/" + node + "/proxy/stats/summary").
			DoRaw(ctx)
		if err != nil {
			s.mu.Lock()
			s.errs++
			n := s.errs
			s.mu.Unlock()
			if n%30 == 1 {
				LogWithTimestamp(s.test.T(), "sampler: stats/summary on %s failed (%d so far): %v", node, n, err)
			}
			continue
		}
		var summary statsSummary
		if err := json.Unmarshal(raw, &summary); err != nil {
			continue
		}
		s.mu.Lock()
		phase := s.phase
		for _, pod := range summary.Pods {
			if pod.PodRef.Namespace != s.ns {
				continue
			}
			for _, c := range pod.Containers {
				if !sampledContainers[c.Name] {
					continue
				}
				sample := resourceSample{
					Time:      now,
					Phase:     phase,
					Pod:       pod.PodRef.Name,
					Container: c.Name,
				}
				if c.CPU != nil && c.CPU.UsageCoreNanoSeconds != nil {
					sample.CPUUsageNanoSec = *c.CPU.UsageCoreNanoSeconds
					if t, err := time.Parse(time.RFC3339, c.CPU.Time); err == nil {
						sample.CPUStatTime = t
					}
				}
				if c.Memory != nil && c.Memory.WorkingSetBytes != nil {
					sample.WorkingSetBytes = *c.Memory.WorkingSetBytes
				}
				s.samples = append(s.samples, sample)
			}
		}
		s.mu.Unlock()
	}
}

// ResourceUsage aggregates one container class within one phase.
type ResourceUsage struct {
	Class             string  `json:"class"` // e.g. "collector (worker)"
	Phase             string  `json:"phase"`
	Samples           int     `json:"samples"`
	PeakWorkingSetMiB float64 `json:"peakWorkingSetMiB"`
	AvgCores          float64 `json:"avgCores"`  // mean over the phase
	PeakCores         float64 `json:"peakCores"` // max between distinct kubelet refreshes
}

// classify collapses pods into stable classes so head/worker collectors are
// reported separately even though worker pod names are random.
func classify(pod, container string) string {
	if container == "collector" {
		if strings.Contains(pod, "-head-") {
			return "collector (head)"
		}
		return "collector (worker)"
	}
	return container
}

// Summarize derives per-class per-phase usage. CPU deltas are taken only
// between samples whose kubelet stat timestamp differs — repeated polls of a
// stale stat contribute nothing instead of distorting rates.
func (s *resourceSampler) Summarize() []ResourceUsage {
	s.mu.Lock()
	defer s.mu.Unlock()

	type seriesKey struct{ pod, container string }
	series := make(map[seriesKey][]resourceSample)
	for _, sample := range s.samples {
		k := seriesKey{sample.Pod, sample.Container}
		series[k] = append(series[k], sample)
	}

	type aggKey struct{ class, phase string }
	type agg struct {
		samples     int
		peakWS      uint64
		cpuNanoSum  uint64
		wallNanoSum int64
		peakCores   float64
	}
	aggs := make(map[aggKey]*agg)

	for k, list := range series {
		class := classify(k.pod, k.container)
		lastDistinct := -1 // index of last sample with a fresh kubelet stat
		for i, sample := range list {
			ak := aggKey{class, sample.Phase}
			a := aggs[ak]
			if a == nil {
				a = &agg{}
				aggs[ak] = a
			}
			a.samples++
			if sample.WorkingSetBytes > a.peakWS {
				a.peakWS = sample.WorkingSetBytes
			}
			if sample.CPUStatTime.IsZero() {
				continue
			}
			if lastDistinct >= 0 {
				prev := list[lastDistinct]
				if sample.CPUStatTime.Equal(prev.CPUStatTime) {
					continue // stale kubelet cache, not a new observation
				}
				// A delta spanning a phase boundary would attribute the old
				// phase's CPU to the new phase (fatal for short phases like
				// flush) — drop it and restart accounting inside the new phase.
				if sample.Phase != prev.Phase {
					lastDistinct = i
					continue
				}
				dt := sample.CPUStatTime.Sub(prev.CPUStatTime).Nanoseconds()
				if dt > 0 && sample.CPUUsageNanoSec >= prev.CPUUsageNanoSec {
					dcpu := sample.CPUUsageNanoSec - prev.CPUUsageNanoSec
					a.cpuNanoSum += dcpu
					a.wallNanoSum += dt
					if cores := float64(dcpu) / float64(dt); cores > a.peakCores {
						a.peakCores = cores
					}
				}
			}
			lastDistinct = i
		}
	}

	var out []ResourceUsage
	for k, a := range aggs {
		u := ResourceUsage{
			Class:             k.class,
			Phase:             k.phase,
			Samples:           a.samples,
			PeakWorkingSetMiB: float64(a.peakWS) / (1024 * 1024),
			PeakCores:         a.peakCores,
		}
		if a.wallNanoSum > 0 {
			u.AvgCores = float64(a.cpuNanoSum) / float64(a.wallNanoSum)
		}
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Class != out[j].Class {
			return out[i].Class < out[j].Class
		}
		return out[i].Phase < out[j].Phase
	})
	return out
}

// WriteCSV dumps the raw series for offline analysis/plotting.
func (s *resourceSampler) WriteCSV(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintln(f, "time,phase,pod,container,cpu_stat_time,cpu_usage_core_nanoseconds,working_set_bytes"); err != nil {
		return err
	}
	for _, sm := range s.samples {
		statTime := ""
		if !sm.CPUStatTime.IsZero() {
			statTime = sm.CPUStatTime.Format(time.RFC3339Nano)
		}
		if _, err := fmt.Fprintf(f, "%s,%s,%s,%s,%s,%d,%d\n",
			sm.Time.Format(time.RFC3339Nano), sm.Phase, sm.Pod, sm.Container,
			statTime, sm.CPUUsageNanoSec, sm.WorkingSetBytes); err != nil {
			return err
		}
	}
	return nil
}
