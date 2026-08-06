package benchmark

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// cgroupScript runs inside the kind node. Every second it walks all pod
// container cgroups (systemd driver, cgroup v2) and emits one line per
// container: <unixNano> <cgroupDir> <memory.current> <memory.peak> <anon> <cpu usage_usec>.
//
// Reading the kernel's accounting directly bypasses the kubelet/cAdvisor 10s
// housekeeping cadence and adds two things the Summary API cannot provide:
// anon (pure heap, no page-cache inflation) and memory.peak (kernel-recorded
// lifetime maximum — immune to sampling gaps).
const cgroupScript = `
base=/sys/fs/cgroup/kubelet.slice/kubelet-kubepods.slice
while true; do
  ts=$(date +%s%N)
  for d in "$base"/kubelet-kubepods-pod*.slice/cri-containerd-*.scope \
           "$base"/kubelet-kubepods-*.slice/kubelet-kubepods-*-pod*.slice/cri-containerd-*.scope; do
    [ -d "$d" ] || continue
    cur=$(cat "$d/memory.current" 2>/dev/null) || continue
    peak=$(cat "$d/memory.peak" 2>/dev/null)
    anon=$(awk '$1=="anon"{print $2}' "$d/memory.stat" 2>/dev/null)
    cpu=$(awk '$1=="usage_usec"{print $2}' "$d/cpu.stat" 2>/dev/null)
    echo "$ts $d $cur ${peak:-0} ${anon:-0} ${cpu:-0}"
  done
  sleep 1
done
`

var criScopeRe = regexp.MustCompile(`cri-containerd-([0-9a-f]+)\.scope`)

type cgroupSample struct {
	TimeNano     int64
	ContainerID  string
	CurrentBytes int64
	PeakBytes    int64
	AnonBytes    int64
	CPUUsageUsec int64
}

// phaseMark records when a benchmark phase began, for attributing samples.
type phaseMark struct {
	Name string
	At   time.Time
}

// cgroupSampler streams 1s cgroup readings from the kind node via a single
// long-lived `docker exec`, and maps container IDs to pod/container names
// through the k8s API (RegisterPods must be called while pods are alive).
type cgroupSampler struct {
	nodeName string

	mu      sync.Mutex
	samples []cgroupSample
	labels  map[string]string // containerID -> "pod/container"
	errs    []string

	cmd      *exec.Cmd
	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
	started  bool
}

func newCgroupSampler(kindNodeName string) *cgroupSampler {
	return &cgroupSampler{
		nodeName: kindNodeName,
		labels:   map[string]string{},
		done:     make(chan struct{}),
	}
}

func (c *cgroupSampler) Start(test Test) {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.cmd = exec.CommandContext(ctx, "docker", "exec", c.nodeName, "sh", "-c", cgroupScript)
	stdout, err := c.cmd.StdoutPipe()
	if err != nil {
		LogWithTimestamp(test.T(), "cgroup sampler: stdout pipe failed, disabled: %v", err)
		close(c.done)
		return
	}
	if err := c.cmd.Start(); err != nil {
		LogWithTimestamp(test.T(), "cgroup sampler: docker exec failed, disabled: %v", err)
		close(c.done)
		return
	}
	c.started = true
	go func() {
		defer close(c.done)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		for scanner.Scan() {
			c.ingestLine(scanner.Text())
		}
	}()
}

func (c *cgroupSampler) ingestLine(line string) {
	fields := strings.Fields(line)
	if len(fields) != 6 {
		return
	}
	m := criScopeRe.FindStringSubmatch(fields[1])
	if m == nil {
		return
	}
	ts, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return
	}
	parse := func(s string) int64 { n, _ := strconv.ParseInt(s, 10, 64); return n }
	c.mu.Lock()
	c.samples = append(c.samples, cgroupSample{
		TimeNano:     ts,
		ContainerID:  m[1],
		CurrentBytes: parse(fields[2]),
		PeakBytes:    parse(fields[3]),
		AnonBytes:    parse(fields[4]),
		CPUUsageUsec: parse(fields[5]),
	})
	c.mu.Unlock()
}

// RegisterPods records containerID -> pod/container labels for every pod in
// the namespace. Call it whenever new pods of interest are Running (after the
// RayCluster is ready, after the history server is ready): once a pod is gone
// its container IDs cannot be resolved anymore.
func (c *cgroupSampler) RegisterPods(test Test, namespace string) {
	pods, err := test.Client().Core().CoreV1().Pods(namespace).List(test.Ctx(), metav1.ListOptions{})
	if err != nil {
		LogWithTimestamp(test.T(), "cgroup sampler: list pods for labeling failed: %v", err)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, pod := range pods.Items {
		for _, st := range pod.Status.ContainerStatuses {
			id := st.ContainerID
			if idx := strings.LastIndex(id, "://"); idx >= 0 {
				id = id[idx+3:]
			}
			if id != "" {
				c.labels[id] = pod.Name + "/" + st.Name
			}
		}
	}
}

func (c *cgroupSampler) Stop() {
	c.stopOnce.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		if c.started {
			<-c.done
			_ = c.cmd.Wait()
		}
	})
}

// CgroupUsage aggregates one labeled container within one phase.
// LifetimePeakMiB is filled only on the per-container "lifetime" row: it is
// the kernel's memory.peak (monotonic since container start), so slicing it
// per phase would be meaningless.
type CgroupUsage struct {
	Container       string  `json:"container"` // "pod/container"
	Phase           string  `json:"phase"`
	Samples         int     `json:"samples"`
	PeakAnonMiB     float64 `json:"peakAnonMiB"`
	PeakCurrentMiB  float64 `json:"peakCurrentMiB"`
	AvgCores        float64 `json:"avgCores"`
	PeakCores       float64 `json:"peakCores"`
	LifetimePeakMiB float64 `json:"lifetimePeakMiB,omitempty"`
}

// Summarize buckets samples into phases (via marks) per labeled container.
// Unlabeled containers (infra pods etc.) are skipped.
func (c *cgroupSampler) Summarize(marks []phaseMark) []CgroupUsage {
	c.mu.Lock()
	defer c.mu.Unlock()

	phaseAt := func(t time.Time) string {
		name := "baseline"
		for _, m := range marks {
			if t.Before(m.At) {
				break
			}
			name = m.Name
		}
		return name
	}

	bySeries := map[string][]cgroupSample{}
	for _, s := range c.samples {
		label, ok := c.labels[s.ContainerID]
		if !ok {
			continue
		}
		bySeries[label] = append(bySeries[label], s)
	}

	type key struct{ label, phase string }
	type agg struct {
		samples              int
		peakAnon, peakCur    int64
		cpuUsecSum, wallNano int64
		peakCores            float64
	}
	aggs := map[key]*agg{}
	lifetimePeak := map[string]int64{}

	for label, list := range bySeries {
		sort.Slice(list, func(i, j int) bool { return list[i].TimeNano < list[j].TimeNano })
		for i, s := range list {
			ph := phaseAt(time.Unix(0, s.TimeNano))
			k := key{label, ph}
			a := aggs[k]
			if a == nil {
				a = &agg{}
				aggs[k] = a
			}
			a.samples++
			if s.AnonBytes > a.peakAnon {
				a.peakAnon = s.AnonBytes
			}
			if s.CurrentBytes > a.peakCur {
				a.peakCur = s.CurrentBytes
			}
			if s.PeakBytes > lifetimePeak[label] {
				lifetimePeak[label] = s.PeakBytes
			}
			if i == 0 {
				continue
			}
			prev := list[i-1]
			// Same cross-phase rule as the kubelet sampler: never attribute a
			// delta that spans a phase boundary.
			if phaseAt(time.Unix(0, prev.TimeNano)) != ph {
				continue
			}
			dt := s.TimeNano - prev.TimeNano
			dcpu := s.CPUUsageUsec - prev.CPUUsageUsec
			if dt <= 0 || dcpu < 0 {
				continue
			}
			a.cpuUsecSum += dcpu
			a.wallNano += dt
			if cores := float64(dcpu) * 1000 / float64(dt); cores > a.peakCores {
				a.peakCores = cores
			}
		}
	}

	var out []CgroupUsage
	for k, a := range aggs {
		u := CgroupUsage{
			Container:      k.label,
			Phase:          k.phase,
			Samples:        a.samples,
			PeakAnonMiB:    float64(a.peakAnon) / (1 << 20),
			PeakCurrentMiB: float64(a.peakCur) / (1 << 20),
			PeakCores:      a.peakCores,
		}
		if a.wallNano > 0 {
			u.AvgCores = float64(a.cpuUsecSum) * 1000 / float64(a.wallNano)
		}
		out = append(out, u)
	}
	for label, peak := range lifetimePeak {
		out = append(out, CgroupUsage{
			Container:       label,
			Phase:           "lifetime",
			LifetimePeakMiB: float64(peak) / (1 << 20),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Container != out[j].Container {
			return out[i].Container < out[j].Container
		}
		return out[i].Phase < out[j].Phase
	})
	return out
}

// WriteCSV dumps the raw 1s series for offline analysis.
func (c *cgroupSampler) WriteCSV(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintln(f, "time_nano,container,anon_bytes,current_bytes,peak_bytes,cpu_usage_usec"); err != nil {
		return err
	}
	for _, s := range c.samples {
		label := c.labels[s.ContainerID]
		if label == "" {
			continue
		}
		if _, err := fmt.Fprintf(f, "%d,%s,%d,%d,%d,%d\n",
			s.TimeNano, label, s.AnonBytes, s.CurrentBytes, s.PeakBytes, s.CPUUsageUsec); err != nil {
			return err
		}
	}
	return nil
}
