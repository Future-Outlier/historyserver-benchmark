package benchmark

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// cgroupScript runs inside the kind node. Every half-second it walks all pod
// container cgroups (systemd driver, cgroup v2) and emits one line per
// container: <unixNano> <cgroupDir> <memory.current> <memory.peak> <anon>
// <cpu usage_usec> <nr_throttled> <throttled_usec> <nr_periods>. A second
// CGROUP_MEMORY record carries memory.max and the oom/oom_kill counters from
// memory.events without changing the long-standing CPU sample schema.
//
// Reading the kernel's accounting directly bypasses the kubelet/cAdvisor 10s
// housekeeping cadence and adds two things the Summary API cannot provide:
// anon (anonymous resident memory, without file-backed page-cache inflation)
// and memory.peak (kernel-recorded lifetime maximum — immune to sampling gaps).
const cgroupScript = `
base=/sys/fs/cgroup/kubelet.slice/kubelet-kubepods.slice
is_uint() {
  case "$1" in
    ''|*[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}
emit_cgroup_error() {
  echo "$ts CGROUP_ERROR $d $1"
}
while true; do
  ts=$(date +%s%N)
  for d in "$base"/kubelet-kubepods-pod*.slice/cri-containerd-*.scope \
           "$base"/kubelet-kubepods-*.slice/kubelet-kubepods-*-pod*.slice/cri-containerd-*.scope; do
    [ -d "$d" ] || continue
    IFS= read -r cur < "$d/memory.current" 2>/dev/null || cur=
    if ! is_uint "$cur"; then emit_cgroup_error memory.current; continue; fi
    IFS= read -r peak < "$d/memory.peak" 2>/dev/null || peak=
    if ! is_uint "$peak"; then emit_cgroup_error memory.peak; continue; fi

    anon=
    while IFS=' ' read -r key value rest; do
      if [ "$key" = anon ]; then anon=$value; break; fi
    done < "$d/memory.stat" 2>/dev/null
    if ! is_uint "$anon"; then emit_cgroup_error memory.stat.anon; continue; fi

    cpu=; thr=; thrus=; per=
    while IFS=' ' read -r key value rest; do
      case "$key" in
        usage_usec) cpu=$value ;;
        nr_throttled) thr=$value ;;
        throttled_usec) thrus=$value ;;
        nr_periods) per=$value ;;
      esac
    done < "$d/cpu.stat" 2>/dev/null
    if ! is_uint "$cpu"; then emit_cgroup_error cpu.stat.usage_usec; continue; fi
    if ! is_uint "$thr"; then emit_cgroup_error cpu.stat.nr_throttled; continue; fi
    if ! is_uint "$thrus"; then emit_cgroup_error cpu.stat.throttled_usec; continue; fi
    if ! is_uint "$per"; then emit_cgroup_error cpu.stat.nr_periods; continue; fi
    echo "$ts $d $cur $peak $anon $cpu $thr $thrus $per"

    IFS= read -r memmax < "$d/memory.max" 2>/dev/null || memmax=
    case "$memmax" in
      max) ;;
      *) if ! is_uint "$memmax"; then emit_cgroup_error memory.max; continue; fi ;;
    esac
    ev_oom=; ev_oom_kill=
    while IFS=' ' read -r key value rest; do
      case "$key" in
        oom) ev_oom=$value ;;
        oom_kill) ev_oom_kill=$value ;;
      esac
    done < "$d/memory.events" 2>/dev/null
    if ! is_uint "$ev_oom"; then emit_cgroup_error memory.events.oom; continue; fi
    if ! is_uint "$ev_oom_kill"; then emit_cgroup_error memory.events.oom_kill; continue; fi
    echo "$ts CGROUP_MEMORY $d $memmax $ev_oom $ev_oom_kill"
  done
  # Spool backlog: the collector writes events to an emptyDir and uploads in the
  # background, so this - not its heap - is what fills up under load and what the
  # 503 backpressure defends. One line per volume, tagged so the parser can tell
  # it apart from a cgroup line.
  for v in /var/lib/kubelet/pods/*/volumes/kubernetes.io~empty-dir/historyserver; do
    [ -d "$v" ] || continue
    sz=$(du -sk "$v" 2>/dev/null | awk '{print $1 * 1024}')
    echo "$ts SPOOL $v ${sz:-0}"
  done
  sleep 0.5
done
`

var (
	criScopeRe        = regexp.MustCompile(`cri-containerd-([0-9a-f]+)\.scope`)
	bareContainerIDRe = regexp.MustCompile(`^[0-9a-f]{64}$`)
	podUIDRe          = regexp.MustCompile(`/pods/([0-9a-f-]+)/volumes/`)
)

type spoolSample struct {
	TimeNano int64
	PodUID   string
	Bytes    int64
}

type cgroupSample struct {
	TimeNano      int64
	NrThrottled   int64
	ThrottledUsec int64
	NrPeriods     int64
	ContainerID   string
	CurrentBytes  int64
	PeakBytes     int64
	AnonBytes     int64
	CPUUsageUsec  int64
}

type cgroupReadError struct {
	TimeNano    int64
	ContainerID string
	Field       string
}

type cgroupMemorySample struct {
	TimeNano       int64
	ContainerID    string
	MemoryMax      string
	MemoryMaxBytes int64
	OOM            int64
	OOMKill        int64
}

type cgroupMemoryEvidence struct {
	Observed        bool
	MemoryMax       string
	MemoryMaxBytes  int64
	OOM             int64
	OOMKill         int64
	ReadErrors      int
	ReadErrorFields []string
}

// phaseMark records when a benchmark phase began, for attributing samples.
type phaseMark struct {
	Name string
	At   time.Time
}

// cgroupSampler streams sub-second-target cgroup readings from the kind node via a single
// long-lived `docker exec`, and maps container IDs to pod/container names
// through the k8s API (RegisterPods must be called while pods are alive).
type cgroupSampler struct {
	nodeName string

	mu            sync.Mutex
	samples       []cgroupSample
	memorySamples []cgroupMemorySample
	readErrors    []cgroupReadError
	labels        map[string]string // containerID -> "pod/container"

	startAttempted        bool
	started               bool
	stopRequested         bool
	streamEnded           bool
	streamEndedBeforeStop bool
	startError            string
	streamError           string

	cmd      *exec.Cmd
	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
	spool    []spoolSample
}

// CgroupSamplerStatus makes a truncated or never-started raw stream explicit.
// StreamComplete is true only after an intentional Stop observed a clean EOF.
type CgroupSamplerStatus struct {
	StartAttempted        bool   `json:"startAttempted"`
	Started               bool   `json:"started"`
	StopRequested         bool   `json:"stopRequested"`
	StreamEnded           bool   `json:"streamEnded"`
	StreamEndedBeforeStop bool   `json:"streamEndedBeforeStop"`
	StreamComplete        bool   `json:"streamComplete"`
	StartError            string `json:"startError,omitempty"`
	StreamError           string `json:"streamError,omitempty"`
}

func newCgroupSampler(kindNodeName string) *cgroupSampler {
	return &cgroupSampler{
		nodeName: kindNodeName,
		labels:   map[string]string{},
		done:     make(chan struct{}),
	}
}

func (c *cgroupSampler) Start(test Test) {
	c.mu.Lock()
	c.startAttempted = true
	c.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.cmd = exec.CommandContext(ctx, "docker", "exec", c.nodeName, "sh", "-c", cgroupScript)
	stdout, err := c.cmd.StdoutPipe()
	if err != nil {
		c.recordStartError(err)
		LogWithTimestamp(test.T(), "cgroup sampler: stdout pipe failed, disabled: %v", err)
		close(c.done)
		return
	}
	if err := c.cmd.Start(); err != nil {
		c.recordStartError(err)
		LogWithTimestamp(test.T(), "cgroup sampler: docker exec failed, disabled: %v", err)
		close(c.done)
		return
	}
	c.mu.Lock()
	c.started = true
	c.mu.Unlock()
	go func() {
		defer close(c.done)
		c.consumeStream(stdout)
	}()
}

func (c *cgroupSampler) recordStartError(err error) {
	c.mu.Lock()
	c.startError = err.Error()
	c.mu.Unlock()
}

func (c *cgroupSampler) consumeStream(reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		c.ingestLine(scanner.Text())
	}
	c.mu.Lock()
	c.streamEnded = true
	c.streamEndedBeforeStop = !c.stopRequested
	if err := scanner.Err(); err != nil {
		c.streamError = err.Error()
	}
	c.mu.Unlock()
}

func (c *cgroupSampler) Status() CgroupSamplerStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	status := CgroupSamplerStatus{
		StartAttempted:        c.startAttempted,
		Started:               c.started,
		StopRequested:         c.stopRequested,
		StreamEnded:           c.streamEnded,
		StreamEndedBeforeStop: c.streamEndedBeforeStop,
		StartError:            c.startError,
		StreamError:           c.streamError,
	}
	status.StreamComplete = status.Started && status.StopRequested && status.StreamEnded &&
		!status.StreamEndedBeforeStop && status.StartError == "" && status.StreamError == ""
	return status
}

// CaptureCPUCheckpoint reads the target cgroup counter directly. The streaming
// sampler remains the lifetime record, while these boundary reads make a
// sub-500ms single HTTP request measurable without increasing global sampling
// frequency or issuing another request to the History Server.
func (c *cgroupSampler) CaptureCPUCheckpoint(label, containerID string) HSCPUCheckpoint {
	checkpoint := HSCPUCheckpoint{Label: label, ContainerID: containerID}
	if !bareContainerIDRe.MatchString(containerID) {
		checkpoint.Error = "container ID is not 64 lowercase hex characters"
		return checkpoint
	}
	script := fmt.Sprintf(`
base=/sys/fs/cgroup/kubelet.slice/kubelet-kubepods.slice
for d in "$base"/kubelet-kubepods-pod*.slice/cri-containerd-%[1]s.scope \
         "$base"/kubelet-kubepods-*.slice/kubelet-kubepods-*-pod*.slice/cri-containerd-%[1]s.scope; do
  [ -d "$d" ] || continue
  cpu=; thr=; thrus=; per=
  while IFS=' ' read -r key value rest; do
    case "$key" in
      usage_usec) cpu=$value ;;
      nr_throttled) thr=$value ;;
      throttled_usec) thrus=$value ;;
      nr_periods) per=$value ;;
    esac
  done < "$d/cpu.stat" || exit 2
  ts=$(date +%%s%%N)
  echo "$ts $cpu $thr $thrus $per"
  exit 0
done
exit 3
`, containerID)
	raw, err := exec.Command("docker", "exec", c.nodeName, "sh", "-c", script).Output()
	if err != nil {
		checkpoint.Error = err.Error()
		return checkpoint
	}
	fields := strings.Fields(string(raw))
	if len(fields) != 5 {
		checkpoint.Error = fmt.Sprintf("checkpoint field count=%d, want 5", len(fields))
		return checkpoint
	}
	values := []*int64{
		&checkpoint.TimeNano,
		&checkpoint.CPUUsageUsec,
		&checkpoint.NrThrottled,
		&checkpoint.ThrottledUsec,
		&checkpoint.NrPeriods,
	}
	for i, destination := range values {
		value, parseErr := strconv.ParseInt(fields[i], 10, 64)
		if parseErr != nil || value < 0 {
			checkpoint.Error = fmt.Sprintf("checkpoint field %d is invalid", i)
			return checkpoint
		}
		*destination = value
	}
	return checkpoint
}

// EvaluateQuietWindow proves that no benchmark HTTP request leaked into a
// declared quiet phase. Only complete cgroup intervals after the mandatory
// five-second wait count toward the gate. Memory is evidence, not a gate.
func (c *cgroupSampler) EvaluateQuietWindow(name, containerID string, start, end time.Time) HSIsolationWindow {
	return c.evaluateQuietWindow(name, containerID, start, end, false)
}

func (c *cgroupSampler) EvaluateStartupBaseline(containerID string, start, end time.Time) HSIsolationWindow {
	return c.evaluateQuietWindow("startup-baseline", containerID, start, end, true)
}

func (c *cgroupSampler) evaluateQuietWindow(name, containerID string, start, end time.Time, fullBaseline bool) HSIsolationWindow {
	c.mu.Lock()
	all := append([]cgroupSample(nil), c.samples...)
	c.mu.Unlock()
	window := HSIsolationWindow{
		Name: name, Kind: "quiet", StartLabel: name + "-start", EndLabel: name + "-end",
		WallDurationNano: end.Sub(start).Nanoseconds(), Valid: true, Problems: []string{},
	}
	if !bareContainerIDRe.MatchString(containerID) {
		window.Problems = append(window.Problems, "target container ID is invalid")
	}
	if end.Sub(start) < hsQuietMinWait || end.Sub(start) > hsQuietMaxWait {
		window.Problems = append(window.Problems, fmt.Sprintf("quiet duration=%s, want [%s,%s]",
			end.Sub(start), hsQuietMinWait, hsQuietMaxWait))
	}
	gateStart := start.Add(hsQuietMinWait).UnixNano()
	if fullBaseline {
		gateStart = start.UnixNano()
		if end.Sub(start) < hsIsolatedPreColdIdle {
			window.Problems = append(window.Problems, fmt.Sprintf("startup baseline duration=%s, want >=%s", end.Sub(start), hsIsolatedPreColdIdle))
		}
	}
	endNano := end.UnixNano()
	var samples []cgroupSample
	for _, sample := range all {
		if sample.ContainerID == containerID && sample.TimeNano >= gateStart && sample.TimeNano <= endNano {
			samples = append(samples, sample)
		}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].TimeNano < samples[j].TimeNano })
	window.SampleCount = len(samples)
	if len(samples) > 0 {
		window.MemoryStartBytes = samples[0].CurrentBytes
		window.MemoryEndBytes = samples[len(samples)-1].CurrentBytes
		window.MemoryMinBytes = samples[0].CurrentBytes
		window.MemoryMaxBytes = samples[0].CurrentBytes
		for _, sample := range samples[1:] {
			window.MemoryMinBytes = min(window.MemoryMinBytes, sample.CurrentBytes)
			window.MemoryMaxBytes = max(window.MemoryMaxBytes, sample.CurrentBytes)
		}
	}
	if len(samples) > 0 {
		if samples[0].TimeNano-gateStart > hsQuietMaxSampleGap.Nanoseconds() ||
			endNano-samples[len(samples)-1].TimeNano > hsQuietMaxSampleGap.Nanoseconds() {
			window.Problems = append(window.Problems, "quiet sample coverage has a boundary gap")
		}
	}
	if len(samples) < hsQuietMinIntervals+1 {
		window.Problems = append(window.Problems, fmt.Sprintf("complete cgroup intervals=%d, want at least %d",
			max(0, len(samples)-1), hsQuietMinIntervals))
	} else {
		consecutiveQuiet := 0
		maximumConsecutiveQuiet := 0
		for i := 1; i < len(samples); i++ {
			gap := samples[i].TimeNano - samples[i-1].TimeNano
			if gap <= 0 {
				window.Problems = append(window.Problems, "cgroup sample timestamps are not increasing")
				continue
			}
			window.IntervalCount++
			window.MaxSampleGapNano = max(window.MaxSampleGapNano, gap)
			cpuDelta := samples[i].CPUUsageUsec - samples[i-1].CPUUsageUsec
			intervalMillicores := float64(cpuDelta) * 1e6 / float64(gap)
			qualifies := cpuDelta >= 0 && gap <= hsQuietMaxSampleGap.Nanoseconds() && intervalMillicores <= hsQuietMaxAvgMillicores
			window.Intervals = append(window.Intervals, HSQuietInterval{StartNano: samples[i-1].TimeNano,
				EndNano: samples[i].TimeNano, CPUUsageUsec: cpuDelta, AvgMillicores: intervalMillicores, Qualifies: qualifies})
			if qualifies {
				consecutiveQuiet++
				maximumConsecutiveQuiet = max(maximumConsecutiveQuiet, consecutiveQuiet)
			} else {
				consecutiveQuiet = 0
			}
		}
		first, last := samples[0], samples[len(samples)-1]
		observedNano := last.TimeNano - first.TimeNano
		window.CPUUsageUsec = last.CPUUsageUsec - first.CPUUsageUsec
		window.NrThrottledDelta = last.NrThrottled - first.NrThrottled
		window.ThrottledUsec = last.ThrottledUsec - first.ThrottledUsec
		if observedNano > 0 && window.CPUUsageUsec >= 0 {
			window.AvgMillicores = float64(window.CPUUsageUsec) * 1e6 / float64(observedNano)
		} else {
			window.Problems = append(window.Problems, "cgroup CPU counters are not monotonic")
		}
		if window.MaxSampleGapNano > hsQuietMaxSampleGap.Nanoseconds() {
			window.Problems = append(window.Problems, fmt.Sprintf("maximum cgroup sample gap=%s, want <=%s",
				time.Duration(window.MaxSampleGapNano), hsQuietMaxSampleGap))
		}
		if fullBaseline && maximumConsecutiveQuiet != window.IntervalCount {
			window.Problems = append(window.Problems, "startup baseline contains a non-quiet complete interval")
		} else if !fullBaseline && consecutiveQuiet < hsQuietMinIntervals {
			window.Problems = append(window.Problems, fmt.Sprintf("trailing consecutive whole intervals <=%.1fm: %d, want at least %d",
				hsQuietMaxAvgMillicores, consecutiveQuiet, hsQuietMinIntervals))
		}
	}
	window.Valid = len(window.Problems) == 0
	return window
}

func (c *cgroupSampler) ingestLine(line string) {
	fields := strings.Fields(line)
	if len(fields) == 4 && fields[1] == "SPOOL" {
		c.ingestSpool(fields)
		return
	}
	if len(fields) == 4 && fields[1] == "CGROUP_ERROR" {
		c.ingestCgroupReadError(fields)
		return
	}
	if len(fields) >= 2 && fields[1] == "CGROUP_MEMORY" {
		c.ingestCgroupMemory(fields)
		return
	}
	if len(fields) < 2 {
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
	if len(fields) != 9 {
		// A formal sample must contain every field emitted by cgroupScript. In
		// particular, do not accept the old six-column format and silently turn
		// missing cpu.stat fields into zeroes.
		c.recordCgroupReadError(ts, m[1], "sample.field_count")
		return
	}

	fieldNames := [...]string{
		"memory.current",
		"memory.peak",
		"memory.stat.anon",
		"cpu.stat.usage_usec",
		"cpu.stat.nr_throttled",
		"cpu.stat.throttled_usec",
		"cpu.stat.nr_periods",
	}
	var values [len(fieldNames)]int64
	for i, fieldName := range fieldNames {
		value, parseErr := strconv.ParseInt(fields[i+2], 10, 64)
		if parseErr != nil {
			c.recordCgroupReadError(ts, m[1], fieldName)
			return
		}
		values[i] = value
	}

	sample := cgroupSample{
		TimeNano:      ts,
		ContainerID:   m[1],
		CurrentBytes:  values[0],
		PeakBytes:     values[1],
		AnonBytes:     values[2],
		CPUUsageUsec:  values[3],
		NrThrottled:   values[4],
		ThrottledUsec: values[5],
		NrPeriods:     values[6],
	}
	c.mu.Lock()
	c.samples = append(c.samples, sample)
	c.mu.Unlock()
}

func (c *cgroupSampler) ingestCgroupMemory(fields []string) {
	if len(fields) < 3 {
		return
	}
	m := criScopeRe.FindStringSubmatch(fields[2])
	if m == nil {
		return
	}
	ts, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return
	}
	if len(fields) != 6 {
		c.recordCgroupReadError(ts, m[1], "memory.sample.field_count")
		return
	}
	memoryMax := fields[3]
	var memoryMaxBytes int64
	if memoryMax != "max" {
		memoryMaxBytes, err = strconv.ParseInt(memoryMax, 10, 64)
		if err != nil || memoryMaxBytes < 0 {
			c.recordCgroupReadError(ts, m[1], "memory.max")
			return
		}
	}
	oom, err := strconv.ParseInt(fields[4], 10, 64)
	if err != nil || oom < 0 {
		c.recordCgroupReadError(ts, m[1], "memory.events.oom")
		return
	}
	oomKill, err := strconv.ParseInt(fields[5], 10, 64)
	if err != nil || oomKill < 0 {
		c.recordCgroupReadError(ts, m[1], "memory.events.oom_kill")
		return
	}
	c.mu.Lock()
	c.memorySamples = append(c.memorySamples, cgroupMemorySample{
		TimeNano:       ts,
		ContainerID:    m[1],
		MemoryMax:      memoryMax,
		MemoryMaxBytes: memoryMaxBytes,
		OOM:            oom,
		OOMKill:        oomKill,
	})
	c.mu.Unlock()
}

func (c *cgroupSampler) recordCgroupReadError(timestamp int64, containerID, field string) {
	c.mu.Lock()
	c.readErrors = append(c.readErrors, cgroupReadError{
		TimeNano:    timestamp,
		ContainerID: containerID,
		Field:       field,
	})
	c.mu.Unlock()
}

func (c *cgroupSampler) ingestCgroupReadError(fields []string) {
	m := criScopeRe.FindStringSubmatch(fields[2])
	if m == nil {
		return
	}
	ts, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return
	}
	c.recordCgroupReadError(ts, m[1], fields[3])
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
			id := bareContainerID(st.ContainerID)
			if id != "" {
				c.labels[id] = pod.Name + "/" + st.Name
			}
		}
	}
}

func (c *cgroupSampler) MemoryEvidence(containerID string) cgroupMemoryEvidence {
	c.mu.Lock()
	defer c.mu.Unlock()
	evidence := cgroupMemoryEvidence{}
	var latestTimeNano int64
	for _, sample := range c.memorySamples {
		if sample.ContainerID != containerID {
			continue
		}
		if !evidence.Observed || sample.TimeNano >= latestTimeNano {
			evidence.Observed = true
			latestTimeNano = sample.TimeNano
			evidence.MemoryMax = sample.MemoryMax
			evidence.MemoryMaxBytes = sample.MemoryMaxBytes
			evidence.OOM = sample.OOM
			evidence.OOMKill = sample.OOMKill
		}
	}
	fieldSet := map[string]struct{}{}
	for _, readError := range c.readErrors {
		if readError.ContainerID != containerID {
			continue
		}
		evidence.ReadErrors++
		fieldSet[readError.Field] = struct{}{}
	}
	for field := range fieldSet {
		evidence.ReadErrorFields = append(evidence.ReadErrorFields, field)
	}
	sort.Strings(evidence.ReadErrorFields)
	return evidence
}

func attachCollectorCgroupMemoryEvidence(logs []CollectorLogStat, sampler *cgroupSampler) {
	for i := range logs {
		evidence := sampler.MemoryEvidence(logs[i].ContainerID)
		logs[i].CgroupMemoryObserved = evidence.Observed
		logs[i].CgroupMemoryMax = evidence.MemoryMax
		logs[i].CgroupMemoryMaxBytes = evidence.MemoryMaxBytes
		logs[i].MemoryEventsOOM = evidence.OOM
		logs[i].MemoryEventsOOMKill = evidence.OOMKill
		logs[i].CgroupMemoryReadErrors = evidence.ReadErrors
		logs[i].CgroupMemoryErrorFields = append([]string(nil), evidence.ReadErrorFields...)
	}
}

func bareContainerID(id string) string {
	if idx := strings.LastIndex(id, "://"); idx >= 0 {
		return id[idx+3:]
	}
	return id
}

// ingestSpool records the collector's on-disk backlog, keyed by pod UID because
// the volume path carries the UID rather than a container ID.
func (c *cgroupSampler) ingestSpool(fields []string) {
	m := podUIDRe.FindStringSubmatch(fields[2])
	if m == nil {
		return
	}
	ts, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return
	}
	bytes, _ := strconv.ParseInt(fields[3], 10, 64)
	c.mu.Lock()
	c.spool = append(c.spool, spoolSample{TimeNano: ts, PodUID: m[1], Bytes: bytes})
	c.mu.Unlock()
}

// SpoolPeaks returns the highest observed backlog per pod UID, in MiB.
func (c *cgroupSampler) SpoolPeaks() map[string]float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	peaks := map[string]float64{}
	for _, s := range c.spool {
		if mib := float64(s.Bytes) / (1024 * 1024); mib > peaks[s.PodUID] {
			peaks[s.PodUID] = mib
		}
	}
	return peaks
}

func (c *cgroupSampler) Stop() {
	c.stopOnce.Do(func() {
		c.mu.Lock()
		c.stopRequested = true
		started := c.started
		c.mu.Unlock()
		if c.cancel != nil {
			c.cancel()
		}
		if started {
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
	Container         string  `json:"container"` // "pod/container"
	Phase             string  `json:"phase"`
	Samples           int     `json:"samples"`
	PeakAnonMiB       float64 `json:"peakAnonMiB"`
	PeakCurrentMiB    float64 `json:"peakCurrentMiB"`
	AvgCores          float64 `json:"avgCores"`
	PeakCores         float64 `json:"peakCores"`
	LifetimePeakMiB   float64 `json:"lifetimePeakMiB,omitempty"`
	LifetimePeakBytes int64   `json:"lifetimePeakBytes,omitempty"`
}

// CollectorResourceWindow joins one collector's receive-time ingress counters
// to that same collector container's direct cgroup samples in the identical
// wall-clock window. CPUObservedSeconds makes sampling coverage explicit: CPU
// deltas that cross a window boundary are intentionally not attributed.
type CollectorResourceWindow struct {
	WindowStartUnixNano       int64    `json:"windowStartUnixNano"`
	WindowEndUnixNano         int64    `json:"windowEndUnixNano"`
	SubmittedToWorkerAttempts int64    `json:"submittedToWorkerAttempts"`
	FinishedAttempts          int64    `json:"finishedAttempts"`
	BacklogDelta              int64    `json:"backlogDelta"`
	Pod                       string   `json:"pod"`
	NodeID                    string   `json:"nodeID"`
	NodeIDKnown               bool     `json:"nodeIDKnown"`
	CollectorContainerID      string   `json:"collectorContainerID"`
	CgroupContainerIDs        []string `json:"cgroupContainerIDs"`
	ContainerLifecycleBound   bool     `json:"containerLifecycleBound"`
	Batches                   int64    `json:"batches"`
	Events                    int64    `json:"events"`
	RequestBytes              int64    `json:"requestBytes"`
	RejectedRequests          int64    `json:"rejectedRequests"`
	RejectedDraining          int64    `json:"rejectedDraining"`
	RejectedDiskPressure      int64    `json:"rejectedDiskPressure"`
	RejectedBadRequest        int64    `json:"rejectedBadRequest"`
	RejectedInternal          int64    `json:"rejectedInternal"`
	RotationQueueFull         int64    `json:"rotationQueueFull"`
	EventsPerSecond           float64  `json:"eventsPerSecond"`
	RequestBytesPerSecond     float64  `json:"requestBytesPerSecond"`
	CgroupSamples             int      `json:"cgroupSamples"`
	CgroupReadErrors          int      `json:"cgroupReadErrors"`
	CgroupReadErrorFields     []string `json:"cgroupReadErrorFields"`
	CgroupDataComplete        bool     `json:"cgroupDataComplete"`
	CPUIntervals              int      `json:"cpuIntervals"`
	RequiredCPUIntervals      int      `json:"requiredCPUIntervals"`
	CPUObservedSeconds        float64  `json:"cpuObservedSeconds"`
	CPUCoverageRatio          float64  `json:"cpuCoverageRatio"`
	MaxCPUIntervalSeconds     float64  `json:"maxCPUIntervalSeconds"`
	CPUSamplingValid          bool     `json:"cpuSamplingValid"`
	CPUUsageUsec              int64    `json:"cpuUsageUsec"`
	AvgCores                  float64  `json:"avgCores"`
	PeakCores                 float64  `json:"peakCores"`
	PeakAnonBytes             int64    `json:"peakAnonBytes"`
	PeakCurrentBytes          int64    `json:"peakCurrentBytes"`
	NrThrottledDelta          int64    `json:"nrThrottledDelta"`
	ThrottledUsecDelta        int64    `json:"throttledUsecDelta"`
	ValidForSizing            bool     `json:"validForSizing"`
}

const (
	collectorMinCPUCoverage  = 0.8
	collectorMaxCPUSampleGap = 2 * time.Second
)

// A valid window needs enough intervals that the coverage threshold cannot be
// satisfied by only a few widely separated samples. For a W-second window,
// coverage C, and maximum accepted gap G, ceil(C*W/G) is the minimum possible
// interval count. At W=10s, C=0.8, G=2s, this is 4 intervals.
func requiredCollectorCPUIntervals(windowSeconds int64) int {
	if windowSeconds <= 0 {
		return 0
	}
	return int(math.Ceil(collectorMinCPUCoverage * float64(windowSeconds) /
		collectorMaxCPUSampleGap.Seconds()))
}

// AlignCollectorIngressWindows produces one row per pod/Ray-NodeID/10-second
// ingress window. It deliberately uses receive timestamps rather than event
// creation timestamps, which can precede the collector work by an arbitrary
// queueing delay.
func (c *cgroupSampler) AlignCollectorIngressWindows(
	logs []CollectorLogStat,
	taskWindows []TaskLifecycleWindow,
) []CollectorResourceWindow {
	c.mu.Lock()
	defer c.mu.Unlock()
	tasksByWindow := make(map[int64]TaskLifecycleWindow, len(taskWindows))
	for _, window := range taskWindows {
		tasksByWindow[window.WindowStartUnixNano] = window
	}

	byContainer := map[string][]cgroupSample{}
	for _, sample := range c.samples {
		label := c.labels[sample.ContainerID]
		if label == "" {
			continue
		}
		byContainer[label] = append(byContainer[label], sample)
	}
	readErrorsByContainer := map[string][]cgroupReadError{}
	for _, readError := range c.readErrors {
		label := c.labels[readError.ContainerID]
		if label == "" {
			continue
		}
		readErrorsByContainer[label] = append(readErrorsByContainer[label], readError)
	}
	for label := range byContainer {
		sort.Slice(byContainer[label], func(i, j int) bool {
			return byContainer[label][i].TimeNano < byContainer[label][j].TimeNano
		})
	}

	var rows []CollectorResourceWindow
	for _, collector := range logs {
		containerLabel := collector.Pod + "/collector"
		series := byContainer[containerLabel]
		containerReadErrors := readErrorsByContainer[containerLabel]
		for _, ingress := range collector.IngressWindows {
			windowNano := ingress.WindowSeconds * int64(time.Second)
			if windowNano <= 0 {
				continue
			}
			start := ingress.WindowStartUnixNano
			end := start + windowNano
			tasks := tasksByWindow[start]
			row := CollectorResourceWindow{
				WindowStartUnixNano:       start,
				WindowEndUnixNano:         end,
				SubmittedToWorkerAttempts: tasks.SubmittedToWorkerAttempts,
				FinishedAttempts:          tasks.FinishedAttempts,
				BacklogDelta:              tasks.BacklogDelta,
				Pod:                       collector.Pod,
				NodeID:                    ingress.NodeID,
				NodeIDKnown:               ingress.NodeID != "" && ingress.NodeID != "unknown",
				CollectorContainerID:      collector.ContainerID,
				CgroupContainerIDs:        []string{},
				CgroupReadErrorFields:     []string{},
				Batches:                   ingress.Batches,
				Events:                    ingress.Events,
				RequestBytes:              ingress.Bytes,
				RejectedRequests:          ingress.RejectedRequests,
				RejectedDraining:          ingress.RejectedDraining,
				RejectedDiskPressure:      ingress.RejectedDiskPressure,
				RejectedBadRequest:        ingress.RejectedBadRequest,
				RejectedInternal:          ingress.RejectedInternal,
				RotationQueueFull:         ingress.RotationQueueFull,
				EventsPerSecond:           float64(ingress.Events) / float64(ingress.WindowSeconds),
				RequestBytesPerSecond:     float64(ingress.Bytes) / float64(ingress.WindowSeconds),
				RequiredCPUIntervals:      requiredCollectorCPUIntervals(ingress.WindowSeconds),
			}

			containerIDs := map[string]struct{}{}
			readErrorFields := map[string]struct{}{}
			for _, readError := range containerReadErrors {
				if readError.TimeNano < start || readError.TimeNano >= end {
					continue
				}
				row.CgroupReadErrors++
				readErrorFields[readError.Field] = struct{}{}
			}
			for field := range readErrorFields {
				row.CgroupReadErrorFields = append(row.CgroupReadErrorFields, field)
			}
			sort.Strings(row.CgroupReadErrorFields)
			row.CgroupDataComplete = row.CgroupReadErrors == 0
			for _, sample := range series {
				// The sample at the right boundary is excluded from memory peaks,
				// but can be the endpoint of a CPU interval, so retain its identity.
				if sample.TimeNano >= start && sample.TimeNano <= end {
					containerIDs[sample.ContainerID] = struct{}{}
				}
				if sample.TimeNano < start || sample.TimeNano >= end {
					continue
				}
				row.CgroupSamples++
				if sample.AnonBytes > row.PeakAnonBytes {
					row.PeakAnonBytes = sample.AnonBytes
				}
				if sample.CurrentBytes > row.PeakCurrentBytes {
					row.PeakCurrentBytes = sample.CurrentBytes
				}
			}
			for containerID := range containerIDs {
				row.CgroupContainerIDs = append(row.CgroupContainerIDs, containerID)
			}
			sort.Strings(row.CgroupContainerIDs)
			row.ContainerLifecycleBound = collector.ContainerID != "" && collector.RestartCount == 0 &&
				len(row.CgroupContainerIDs) == 1 && row.CgroupContainerIDs[0] == collector.ContainerID

			var observedNano int64
			for i := 1; i < len(series); i++ {
				prev, sample := series[i-1], series[i]
				// Keep only intervals wholly contained in this event window.
				// A pod/container label can cover several container IDs after a restart;
				// cumulative CPU counters are comparable only within one cgroup.
				if prev.ContainerID != sample.ContainerID || prev.TimeNano < start || sample.TimeNano > end {
					continue
				}
				dt := sample.TimeNano - prev.TimeNano
				dCPU := sample.CPUUsageUsec - prev.CPUUsageUsec
				if dt <= 0 || dCPU < 0 {
					continue
				}
				row.CPUUsageUsec += dCPU
				observedNano += dt
				row.CPUIntervals++
				if seconds := float64(dt) / float64(time.Second); seconds > row.MaxCPUIntervalSeconds {
					row.MaxCPUIntervalSeconds = seconds
				}
				if cores := float64(dCPU) * 1000 / float64(dt); cores > row.PeakCores {
					row.PeakCores = cores
				}
				if delta := sample.NrThrottled - prev.NrThrottled; delta > 0 {
					row.NrThrottledDelta += delta
				}
				if delta := sample.ThrottledUsec - prev.ThrottledUsec; delta > 0 {
					row.ThrottledUsecDelta += delta
				}
			}
			row.CPUObservedSeconds = float64(observedNano) / float64(time.Second)
			row.CPUCoverageRatio = row.CPUObservedSeconds / float64(ingress.WindowSeconds)
			if observedNano > 0 {
				row.AvgCores = float64(row.CPUUsageUsec) * 1000 / float64(observedNano)
			}
			row.CPUSamplingValid = row.CPUIntervals >= row.RequiredCPUIntervals &&
				row.MaxCPUIntervalSeconds <= collectorMaxCPUSampleGap.Seconds()
			row.ValidForSizing = row.NodeIDKnown && row.ContainerLifecycleBound && row.CgroupDataComplete &&
				row.CPUSamplingValid &&
				row.CPUCoverageRatio >= collectorMinCPUCoverage &&
				row.RejectedRequests == 0 && row.RotationQueueFull == 0
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].WindowStartUnixNano != rows[j].WindowStartUnixNano {
			return rows[i].WindowStartUnixNano < rows[j].WindowStartUnixNano
		}
		if rows[i].Pod != rows[j].Pod {
			return rows[i].Pod < rows[j].Pod
		}
		return rows[i].NodeID < rows[j].NodeID
	})
	return rows
}

func writeCollectorResourceWindowsCSV(path string, rows []CollectorResourceWindow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintln(f, "window_start_unix_nano,window_end_unix_nano,submitted_to_worker_attempts,finished_attempts,backlog_delta,pod,ray_node_id,node_id_known,collector_container_id,cgroup_container_ids,container_lifecycle_bound,batches,events,request_bytes,rejected_requests,rejected_draining,rejected_disk_pressure,rejected_bad_request,rejected_internal,rotation_queue_full,events_per_second,request_bytes_per_second,cgroup_samples,cgroup_read_errors,cgroup_read_error_fields,cgroup_data_complete,cpu_intervals,required_cpu_intervals,cpu_observed_seconds,cpu_coverage_ratio,max_cpu_interval_seconds,cpu_sampling_valid,cpu_usage_usec,avg_cores,peak_cores,peak_anon_bytes,peak_current_bytes,nr_throttled_delta,throttled_usec_delta,valid_for_sizing"); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(f, "%d,%d,%d,%d,%d,%s,%s,%t,%s,%q,%t,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.6f,%.6f,%d,%d,%q,%t,%d,%d,%.6f,%.6f,%.6f,%t,%d,%.6f,%.6f,%d,%d,%d,%d,%t\n",
			row.WindowStartUnixNano, row.WindowEndUnixNano, row.SubmittedToWorkerAttempts, row.FinishedAttempts,
			row.BacklogDelta, row.Pod, row.NodeID, row.NodeIDKnown,
			row.CollectorContainerID, strings.Join(row.CgroupContainerIDs, ";"), row.ContainerLifecycleBound,
			row.Batches, row.Events, row.RequestBytes, row.RejectedRequests, row.RejectedDraining,
			row.RejectedDiskPressure, row.RejectedBadRequest, row.RejectedInternal, row.RotationQueueFull,
			row.EventsPerSecond, row.RequestBytesPerSecond, row.CgroupSamples, row.CgroupReadErrors,
			strings.Join(row.CgroupReadErrorFields, ";"), row.CgroupDataComplete, row.CPUIntervals,
			row.RequiredCPUIntervals, row.CPUObservedSeconds, row.CPUCoverageRatio, row.MaxCPUIntervalSeconds,
			row.CPUSamplingValid, row.CPUUsageUsec, row.AvgCores, row.PeakCores, row.PeakAnonBytes,
			row.PeakCurrentBytes, row.NrThrottledDelta, row.ThrottledUsecDelta, row.ValidForSizing); err != nil {
			return err
		}
	}
	return nil
}

// CollectorIngressGate is a fail-closed, per-collector verdict for whether its
// receive-rate/resource rows can be used for sizing. First/last partial windows
// remain in the raw CSV, but the peak event-rate window must have at least 80%
// CPU interval coverage.
type CollectorIngressGate struct {
	Pod                             string   `json:"pod"`
	Role                            string   `json:"role"`
	NodeIDs                         []string `json:"nodeIDs"`
	ContainerID                     string   `json:"containerID"`
	RestartCount                    int32    `json:"restartCount"`
	CgroupContainerIDs              []string `json:"cgroupContainerIDs"`
	LifecycleBound                  bool     `json:"lifecycleBound"`
	TerminationObserved             bool     `json:"terminationObserved"`
	TerminationSource               string   `json:"terminationSource"`
	TerminationContainerID          string   `json:"terminationContainerID"`
	TerminationRestartCount         int32    `json:"terminationRestartCount"`
	TerminationExitCode             int32    `json:"terminationExitCode"`
	Windows                         int      `json:"windows"`
	CoveredWindows                  int      `json:"coveredWindows"`
	SamplingValidWindows            int      `json:"samplingValidWindows"`
	ValidWindows                    int      `json:"validWindows"`
	CgroupReadErrors                int      `json:"cgroupReadErrors"`
	CgroupReadErrorFields           []string `json:"cgroupReadErrorFields"`
	NodeIDKnown                     bool     `json:"nodeIDKnown"`
	LogStreamComplete               bool     `json:"logStreamComplete"`
	GracefulShutdownComplete        bool     `json:"gracefulShutdownComplete"`
	CgroupSamplerComplete           bool     `json:"cgroupSamplerComplete"`
	PeakEventsPerSecond             float64  `json:"peakEventsPerSecond"`
	PeakWindows                     int      `json:"peakWindows"`
	PeakSamplingValidWindows        int      `json:"peakSamplingValidWindows"`
	PeakWindowCPUCoverage           float64  `json:"peakWindowCPUCoverage"`
	PeakWindowMaxCPUIntervalSeconds float64  `json:"peakWindowMaxCPUIntervalSeconds"`
	RejectedRequests                int64    `json:"rejectedRequests"`
	RotationQueueFull               int64    `json:"rotationQueueFull"`
	Valid                           bool     `json:"valid"`
	Problems                        []string `json:"problems"`
}

func summarizeCollectorIngressGates(
	logs []CollectorLogStat,
	rows []CollectorResourceWindow,
	cgroupStatus CgroupSamplerStatus,
	terminations []PodTermination,
) []CollectorIngressGate {
	rowsByPod := map[string][]CollectorResourceWindow{}
	for _, row := range rows {
		rowsByPod[row.Pod] = append(rowsByPod[row.Pod], row)
	}

	roleCounts := map[string]int{}
	for _, collector := range logs {
		roleCounts[collector.Role]++
	}
	topologyProblems := make([]string, 0, 3)
	if len(logs) != 2 {
		topologyProblems = append(topologyProblems, fmt.Sprintf("expected exactly 2 collector pods, found %d", len(logs)))
	}
	for _, role := range []string{string(rayv1.HeadNode), string(rayv1.WorkerNode)} {
		if roleCounts[role] != 1 {
			topologyProblems = append(topologyProblems,
				fmt.Sprintf("expected exactly one %s collector, found %d", role, roleCounts[role]))
		}
	}
	cgroupProblems := cgroupSamplerProblems(cgroupStatus)

	terminationsByPod := map[string][]PodTermination{}
	for _, termination := range terminations {
		if termination.Container == "collector" {
			terminationsByPod[termination.Pod] = append(terminationsByPod[termination.Pod], termination)
		}
	}

	// Missing roles need explicit invalid rows. Returning an empty gate slice for
	// zero discovered pods would make an `all(gate.valid)` consumer pass vacuously.
	collectors := append([]CollectorLogStat(nil), logs...)
	for _, role := range []string{string(rayv1.HeadNode), string(rayv1.WorkerNode)} {
		if roleCounts[role] == 0 {
			collectors = append(collectors, CollectorLogStat{
				Pod:            "<missing-" + role + "-collector>",
				Role:           role,
				LogStreamError: "collector pod was not discovered",
			})
		}
	}

	gates := make([]CollectorIngressGate, 0, len(collectors))
	for _, collector := range collectors {
		gate := CollectorIngressGate{
			Pod:                      collector.Pod,
			Role:                     collector.Role,
			NodeIDs:                  []string{},
			ContainerID:              collector.ContainerID,
			RestartCount:             collector.RestartCount,
			CgroupContainerIDs:       []string{},
			CgroupReadErrorFields:    []string{},
			Problems:                 []string{},
			NodeIDKnown:              true,
			LifecycleBound:           true,
			GracefulShutdownComplete: collector.GracefulShutdownComplete,
			CgroupSamplerComplete:    cgroupStatus.StreamComplete,
		}
		podRows := rowsByPod[collector.Pod]
		gate.Windows = len(podRows)
		nodeIDs := map[string]struct{}{}
		cgroupContainerIDs := map[string]struct{}{}
		cgroupReadErrorFields := map[string]struct{}{}
		for _, row := range podRows {
			if nodeID := strings.TrimSpace(row.NodeID); nodeID != "" {
				nodeIDs[nodeID] = struct{}{}
			}
			if !row.NodeIDKnown {
				gate.NodeIDKnown = false
			}
			if row.CPUCoverageRatio >= collectorMinCPUCoverage {
				gate.CoveredWindows++
			}
			if row.CPUSamplingValid {
				gate.SamplingValidWindows++
			}
			if row.ValidForSizing {
				gate.ValidWindows++
			}
			gate.CgroupReadErrors += row.CgroupReadErrors
			for _, field := range row.CgroupReadErrorFields {
				cgroupReadErrorFields[field] = struct{}{}
			}
			if !row.ContainerLifecycleBound {
				gate.LifecycleBound = false
			}
			for _, containerID := range row.CgroupContainerIDs {
				cgroupContainerIDs[containerID] = struct{}{}
			}
			gate.RejectedRequests += row.RejectedRequests
			gate.RotationQueueFull += row.RotationQueueFull
			if row.EventsPerSecond > gate.PeakEventsPerSecond {
				gate.PeakEventsPerSecond = row.EventsPerSecond
			}
		}
		for nodeID := range nodeIDs {
			gate.NodeIDs = append(gate.NodeIDs, nodeID)
		}
		sort.Strings(gate.NodeIDs)
		for containerID := range cgroupContainerIDs {
			gate.CgroupContainerIDs = append(gate.CgroupContainerIDs, containerID)
		}
		sort.Strings(gate.CgroupContainerIDs)
		for field := range cgroupReadErrorFields {
			gate.CgroupReadErrorFields = append(gate.CgroupReadErrorFields, field)
		}
		sort.Strings(gate.CgroupReadErrorFields)
		for _, row := range podRows {
			if row.EventsPerSecond != gate.PeakEventsPerSecond {
				continue
			}
			gate.PeakWindows++
			if gate.PeakWindows == 1 || row.CPUCoverageRatio < gate.PeakWindowCPUCoverage {
				gate.PeakWindowCPUCoverage = row.CPUCoverageRatio
			}
			if row.MaxCPUIntervalSeconds > gate.PeakWindowMaxCPUIntervalSeconds {
				gate.PeakWindowMaxCPUIntervalSeconds = row.MaxCPUIntervalSeconds
			}
			if row.CPUSamplingValid {
				gate.PeakSamplingValidWindows++
			}
		}
		gate.LogStreamComplete = collector.LogStreamComplete && !collector.LogStreamTimedOut && collector.LogStreamError == ""
		if collector.Role != string(rayv1.HeadNode) && collector.Role != string(rayv1.WorkerNode) {
			gate.Problems = append(gate.Problems,
				fmt.Sprintf("missing or invalid ray.io/node-type role %q", collector.Role))
		}
		if !gate.LogStreamComplete {
			switch {
			case collector.LogStreamTimedOut:
				gate.Problems = append(gate.Problems, "collector log stream timed out before completion")
			case collector.LogStreamError != "":
				gate.Problems = append(gate.Problems, "collector log stream failed: "+collector.LogStreamError)
			default:
				gate.Problems = append(gate.Problems, "collector log stream did not complete")
			}
		}
		if collector.ContainerID == "" {
			gate.LifecycleBound = false
			gate.Problems = append(gate.Problems, "collector container ID was not captured before log following")
		}
		if collector.RestartCount != 0 {
			gate.LifecycleBound = false
			gate.Problems = append(gate.Problems,
				fmt.Sprintf("collector restartCount=%d, expected 0", collector.RestartCount))
		}
		if collector.MemoryLimit != "" && collector.MemoryLimit != "0" {
			expectedMemoryMax, err := resource.ParseQuantity(collector.MemoryLimit)
			switch {
			case err != nil:
				gate.Problems = append(gate.Problems,
					fmt.Sprintf("invalid collector memory limit %q", collector.MemoryLimit))
			case !collector.CgroupMemoryObserved:
				gate.Problems = append(gate.Problems, "collector cgroup memory.max/memory.events were not observed")
			case collector.CgroupMemoryMaxBytes != expectedMemoryMax.Value():
				gate.Problems = append(gate.Problems,
					fmt.Sprintf("collector cgroup memory.max=%q (%d), want %s",
						collector.CgroupMemoryMax, collector.CgroupMemoryMaxBytes, collector.MemoryLimit))
			}
			if collector.CgroupMemoryReadErrors != 0 {
				gate.Problems = append(gate.Problems,
					fmt.Sprintf("collector cgroup memory evidence has %d read errors: %s",
						collector.CgroupMemoryReadErrors, strings.Join(collector.CgroupMemoryErrorFields, ";")))
			}
			if collector.MemoryEventsOOM != 0 || collector.MemoryEventsOOMKill != 0 {
				gate.Problems = append(gate.Problems,
					fmt.Sprintf("collector memory.events oom=%d oom_kill=%d",
						collector.MemoryEventsOOM, collector.MemoryEventsOOMKill))
			}
		}
		if gate.Windows == 0 {
			gate.NodeIDKnown = false
			gate.LifecycleBound = false
			gate.Problems = append(gate.Problems, "no ingress windows")
		}
		if !gate.NodeIDKnown {
			gate.Problems = append(gate.Problems, "missing or unknown Ray NodeID")
		}
		if len(gate.NodeIDs) != 1 {
			gate.Problems = append(gate.Problems,
				fmt.Sprintf("expected exactly one Ray NodeID, found %d", len(gate.NodeIDs)))
		}
		if gate.CoveredWindows == 0 {
			gate.Problems = append(gate.Problems, "no window has at least 80% cgroup CPU coverage")
		}
		if gate.SamplingValidWindows == 0 {
			gate.Problems = append(gate.Problems, "no window satisfies cgroup CPU sample gap and interval-count requirements")
		}
		if gate.ValidWindows == 0 {
			gate.Problems = append(gate.Problems, "no ingress window is valid for sizing")
		}
		if gate.CgroupReadErrors > 0 {
			gate.Problems = append(gate.Problems,
				fmt.Sprintf("cgroup sampler had %d required-field read errors: %s",
					gate.CgroupReadErrors, strings.Join(gate.CgroupReadErrorFields, ";")))
		}
		if gate.PeakWindows > 0 && gate.PeakWindowCPUCoverage < collectorMinCPUCoverage {
			gate.Problems = append(gate.Problems, "one or more maximum event-rate windows have less than 80% cgroup CPU coverage")
		}
		if gate.PeakWindows > 0 && gate.PeakSamplingValidWindows != gate.PeakWindows {
			gate.Problems = append(gate.Problems,
				fmt.Sprintf("only %d/%d maximum event-rate windows satisfy CPU sample cadence", gate.PeakSamplingValidWindows, gate.PeakWindows))
		}
		if gate.RejectedRequests > 0 {
			gate.Problems = append(gate.Problems, "collector rejected HTTP requests")
		}
		if gate.RotationQueueFull > 0 {
			gate.Problems = append(gate.Problems, "collector rotation queue was full")
		}
		shutdownEvidenceValid := collector.GracefulShutdownComplete
		podTerminations := terminationsByPod[collector.Pod]
		if len(podTerminations) > 1 {
			gate.LifecycleBound = false
			gate.Problems = append(gate.Problems,
				fmt.Sprintf("expected at most one collector termination record, found %d", len(podTerminations)))
		} else if len(podTerminations) == 1 {
			termination := podTerminations[0]
			gate.TerminationObserved = termination.Observed
			gate.TerminationSource = termination.Source
			gate.TerminationContainerID = termination.ContainerID
			gate.TerminationRestartCount = termination.RestartCount
			gate.TerminationExitCode = termination.ExitCode
			if termination.Observed {
				terminationValid := true
				if termination.Source != "current" {
					terminationValid = false
					gate.Problems = append(gate.Problems, "collector termination is not from the current run")
				}
				if termination.ContainerID == "" || termination.ContainerID != collector.ContainerID {
					terminationValid = false
					gate.Problems = append(gate.Problems,
						fmt.Sprintf("termination container ID %q does not match followed container ID %q",
							termination.ContainerID, collector.ContainerID))
				}
				if termination.RestartCount != 0 {
					terminationValid = false
					gate.Problems = append(gate.Problems,
						fmt.Sprintf("termination restartCount=%d, expected 0", termination.RestartCount))
				}
				if termination.ExitCode != 0 {
					terminationValid = false
					gate.Problems = append(gate.Problems,
						fmt.Sprintf("collector exitCode=%d, expected 0", termination.ExitCode))
				}
				if terminationValid {
					shutdownEvidenceValid = true
				} else {
					gate.LifecycleBound = false
				}
			}
		}
		if !shutdownEvidenceValid {
			gate.LifecycleBound = false
			gate.Problems = append(gate.Problems,
				"neither the collector graceful-shutdown marker nor a clean current-run termination was observed")
		}
		if !gate.LifecycleBound {
			gate.Problems = append(gate.Problems, "cgroup windows are not bound to the followed collector container lifecycle")
		}
		gate.Problems = append(gate.Problems, cgroupProblems...)
		gate.Problems = append(gate.Problems, topologyProblems...)
		gates = append(gates, gate)
	}

	// A Ray node owns one collector sidecar, so head and worker must not report
	// the same Ray NodeID. Attach the failure to every colliding collector.
	indicesByNodeID := map[string][]int{}
	for i, gate := range gates {
		if gate.NodeIDKnown && len(gate.NodeIDs) == 1 {
			indicesByNodeID[gate.NodeIDs[0]] = append(indicesByNodeID[gate.NodeIDs[0]], i)
		}
	}
	for nodeID, indices := range indicesByNodeID {
		if len(indices) < 2 {
			continue
		}
		for _, i := range indices {
			gates[i].Problems = append(gates[i].Problems,
				fmt.Sprintf("Ray NodeID %q is shared by %d collectors", nodeID, len(indices)))
		}
	}
	for i := range gates {
		gates[i].Valid = len(gates[i].Problems) == 0
	}
	sort.Slice(gates, func(i, j int) bool { return gates[i].Pod < gates[j].Pod })
	return gates
}

func cgroupSamplerProblems(status CgroupSamplerStatus) []string {
	var problems []string
	if !status.StartAttempted {
		problems = append(problems, "cgroup sampler was never started")
	} else if !status.Started {
		problems = append(problems, "cgroup sampler did not start")
	}
	if status.StartError != "" {
		problems = append(problems, "cgroup sampler start failed: "+status.StartError)
	}
	if status.StreamEndedBeforeStop {
		problems = append(problems, "cgroup sampler stream ended before benchmark stop")
	}
	if status.StreamError != "" {
		problems = append(problems, "cgroup sampler stream failed: "+status.StreamError)
	}
	if !status.StreamComplete {
		problems = append(problems, "cgroup sampler stream did not complete cleanly")
	}
	return problems
}

func writeCollectorIngressGatesCSV(path string, gates []CollectorIngressGate) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintln(f, "pod,role,node_ids,container_id,restart_count,cgroup_container_ids,lifecycle_bound,termination_observed,termination_source,termination_container_id,termination_restart_count,termination_exit_code,windows,covered_windows,sampling_valid_windows,valid_windows,cgroup_read_errors,cgroup_read_error_fields,node_id_known,log_stream_complete,graceful_shutdown_complete,cgroup_sampler_complete,peak_events_per_second,peak_windows,peak_sampling_valid_windows,peak_window_cpu_coverage,peak_window_max_cpu_interval_seconds,rejected_requests,rotation_queue_full,valid,problems"); err != nil {
		return err
	}
	for _, gate := range gates {
		if _, err := fmt.Fprintf(f, "%s,%s,%q,%s,%d,%q,%t,%t,%s,%s,%d,%d,%d,%d,%d,%d,%d,%q,%t,%t,%t,%t,%.6f,%d,%d,%.6f,%.6f,%d,%d,%t,%q\n",
			gate.Pod, gate.Role, strings.Join(gate.NodeIDs, ";"), gate.ContainerID, gate.RestartCount,
			strings.Join(gate.CgroupContainerIDs, ";"), gate.LifecycleBound, gate.TerminationObserved,
			gate.TerminationSource, gate.TerminationContainerID, gate.TerminationRestartCount,
			gate.TerminationExitCode, gate.Windows, gate.CoveredWindows, gate.SamplingValidWindows,
			gate.ValidWindows, gate.CgroupReadErrors, strings.Join(gate.CgroupReadErrorFields, ";"),
			gate.NodeIDKnown, gate.LogStreamComplete, gate.GracefulShutdownComplete, gate.CgroupSamplerComplete,
			gate.PeakEventsPerSecond, gate.PeakWindows, gate.PeakSamplingValidWindows,
			gate.PeakWindowCPUCoverage, gate.PeakWindowMaxCPUIntervalSeconds,
			gate.RejectedRequests, gate.RotationQueueFull, gate.Valid,
			strings.Join(gate.Problems, "; ")); err != nil {
			return err
		}
	}
	return nil
}

func TestAlignCollectorIngressWindows(t *testing.T) {
	const containerID = "collector-container"
	start := time.Unix(100, 0).UnixNano()
	sampler := &cgroupSampler{
		labels: map[string]string{containerID: "raycluster-head-abc/collector"},
		samples: []cgroupSample{
			{TimeNano: start + int64(time.Second), ContainerID: containerID, CPUUsageUsec: 1_000, AnonBytes: 10, CurrentBytes: 20, NrThrottled: 1, ThrottledUsec: 100},
			{TimeNano: start + 3*int64(time.Second), ContainerID: containerID, CPUUsageUsec: 201_000, AnonBytes: 30, CurrentBytes: 40, NrThrottled: 2, ThrottledUsec: 300},
			{TimeNano: start + 5*int64(time.Second), ContainerID: containerID, CPUUsageUsec: 401_000, AnonBytes: 25, CurrentBytes: 35, NrThrottled: 3, ThrottledUsec: 500},
			{TimeNano: start + 7*int64(time.Second), ContainerID: containerID, CPUUsageUsec: 601_000, AnonBytes: 20, CurrentBytes: 30, NrThrottled: 4, ThrottledUsec: 700},
			{TimeNano: start + 9*int64(time.Second), ContainerID: containerID, CPUUsageUsec: 801_000, AnonBytes: 15, CurrentBytes: 25, NrThrottled: 5, ThrottledUsec: 900},
		},
	}
	logs := []CollectorLogStat{
		{
			Pod: "raycluster-head-abc", ContainerID: containerID,
			IngressWindows: []CollectorIngressWindow{
				{
					WindowStartUnixNano: start,
					WindowSeconds:       10,
					NodeID:              "node-a",
					Batches:             3,
					Events:              2_000,
					Bytes:               8_192,
				},
			},
		},
	}

	rows := sampler.AlignCollectorIngressWindows(logs, []TaskLifecycleWindow{{
		WindowStartUnixNano:       start,
		WindowEndUnixNano:         start + 10*int64(time.Second),
		SubmittedToWorkerAttempts: 1_200,
		FinishedAttempts:          1_100,
		BacklogDelta:              100,
	}})
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1: %#v", len(rows), rows)
	}
	row := rows[0]
	if row.EventsPerSecond != 200 || row.CgroupSamples != 5 || row.CPUObservedSeconds != 8 || row.CPUUsageUsec != 800_000 {
		t.Fatalf("unexpected aligned row: %#v", row)
	}
	if !row.NodeIDKnown || !row.ContainerLifecycleBound || row.CPUCoverageRatio != 0.8 ||
		row.CPUIntervals != 4 || row.RequiredCPUIntervals != 4 || row.MaxCPUIntervalSeconds != 2 ||
		!row.CPUSamplingValid || !row.ValidForSizing {
		t.Fatalf("unexpected row validity: %#v", row)
	}
	if row.AvgCores != 0.1 || row.PeakAnonBytes != 30 || row.PeakCurrentBytes != 40 {
		t.Fatalf("unexpected CPU/memory summary: %#v", row)
	}
	if row.NrThrottledDelta != 4 || row.ThrottledUsecDelta != 800 {
		t.Fatalf("unexpected throttle deltas: %#v", row)
	}
	if row.SubmittedToWorkerAttempts != 1_200 || row.FinishedAttempts != 1_100 || row.BacklogDelta != 100 {
		t.Fatalf("task lifecycle window was not joined by exact wall-clock bucket: %#v", row)
	}
}

func TestAlignCollectorIngressWindowsRejectsSparseSamplesDespiteCoverage(t *testing.T) {
	const containerID = "collector-container"
	start := time.Unix(100, 0).UnixNano()
	sampler := &cgroupSampler{
		labels: map[string]string{containerID: "collector-pod/collector"},
		samples: []cgroupSample{
			{TimeNano: start + int64(time.Second), ContainerID: containerID, CPUUsageUsec: 1_000},
			{TimeNano: start + 5*int64(time.Second), ContainerID: containerID, CPUUsageUsec: 401_000},
			{TimeNano: start + 9*int64(time.Second), ContainerID: containerID, CPUUsageUsec: 801_000},
		},
	}
	logs := []CollectorLogStat{{
		Pod: "collector-pod", ContainerID: containerID,
		IngressWindows: []CollectorIngressWindow{{
			WindowStartUnixNano: start, WindowSeconds: 10, NodeID: "node-a", Events: 1_000,
		}},
	}}

	row := sampler.AlignCollectorIngressWindows(logs, nil)[0]
	if row.CPUCoverageRatio != 0.8 || row.CPUIntervals != 2 || row.RequiredCPUIntervals != 4 ||
		row.MaxCPUIntervalSeconds != 4 || row.CPUSamplingValid || row.ValidForSizing {
		t.Fatalf("sparse samples were not rejected: %#v", row)
	}
}

func TestRequiredCollectorCPUIntervals(t *testing.T) {
	for _, tc := range []struct {
		windowSeconds int64
		want          int
	}{{0, 0}, {10, 4}, {20, 8}} {
		if got := requiredCollectorCPUIntervals(tc.windowSeconds); got != tc.want {
			t.Fatalf("requiredCollectorCPUIntervals(%d)=%d, want %d", tc.windowSeconds, got, tc.want)
		}
	}
}

type scannerErrorReader struct{ err error }

func (r scannerErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestCgroupSamplerStatusRecordsScannerError(t *testing.T) {
	sampler := newCgroupSampler("unused")
	sampler.startAttempted, sampler.started, sampler.stopRequested = true, true, true
	sampler.consumeStream(scannerErrorReader{err: errors.New("synthetic scanner failure")})

	status := sampler.Status()
	if status.StreamComplete || status.StreamError != "synthetic scanner failure" || !status.StreamEnded {
		t.Fatalf("scanner failure was not observable: %#v", status)
	}
}

func TestCgroupScriptHasValidPOSIXShellSyntax(t *testing.T) {
	cmd := exec.Command("sh", "-n")
	cmd.Stdin = strings.NewReader(cgroupScript)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("cgroup sampler shell syntax: %v\n%s", err, out)
	}
}

func TestCgroupMemoryEvidenceUsesLatestSampleAndFailsClosedOnReadErrors(t *testing.T) {
	const (
		containerID = "0123456789abcdef"
		path        = "/sys/fs/cgroup/kubelet.slice/kubelet-kubepods.slice/cri-containerd-" + containerID + ".scope"
	)
	sampler := newCgroupSampler("unused")
	sampler.ingestLine("20 CGROUP_MEMORY " + path + " 8589934592 2 1")
	sampler.ingestLine("10 CGROUP_MEMORY " + path + " 4294967296 0 0")
	sampler.ingestLine("30 CGROUP_ERROR " + path + " memory.events.oom")

	evidence := sampler.MemoryEvidence(containerID)
	if !evidence.Observed || evidence.MemoryMax != "8589934592" ||
		evidence.MemoryMaxBytes != 8<<30 || evidence.OOM != 2 || evidence.OOMKill != 1 {
		t.Fatalf("latest memory evidence was not preserved: %#v", evidence)
	}
	if evidence.ReadErrors != 1 || len(evidence.ReadErrorFields) != 1 ||
		evidence.ReadErrorFields[0] != "memory.events.oom" {
		t.Fatalf("cgroup read error was not bound to the container: %#v", evidence)
	}
}

func TestCgroupMemorySampleRejectsMalformedRequiredFields(t *testing.T) {
	const (
		containerID = "abcdef0123456789"
		path        = "/sys/fs/cgroup/kubelet.slice/kubelet-kubepods.slice/cri-containerd-" + containerID + ".scope"
	)
	sampler := newCgroupSampler("unused")
	for _, line := range []string{
		"1 CGROUP_MEMORY " + path + " invalid 0 0",
		"2 CGROUP_MEMORY " + path + " 1024 invalid 0",
		"3 CGROUP_MEMORY " + path + " 1024 0 invalid",
		"4 CGROUP_MEMORY " + path + " 1024 0",
	} {
		sampler.ingestLine(line)
	}

	evidence := sampler.MemoryEvidence(containerID)
	if evidence.Observed || evidence.ReadErrors != 4 {
		t.Fatalf("malformed memory samples did not fail closed: %#v", evidence)
	}
	wantFields := []string{
		"memory.events.oom",
		"memory.events.oom_kill",
		"memory.max",
		"memory.sample.field_count",
	}
	if strings.Join(evidence.ReadErrorFields, ",") != strings.Join(wantFields, ",") {
		t.Fatalf("read error fields=%v, want %v", evidence.ReadErrorFields, wantFields)
	}
}

func TestIngestCgroupReadError(t *testing.T) {
	sampler := newCgroupSampler("unused")
	sampler.ingestLine("123 CGROUP_ERROR /sys/fs/cgroup/cri-containerd-abc123.scope memory.peak")
	if len(sampler.readErrors) != 1 || sampler.readErrors[0].TimeNano != 123 ||
		sampler.readErrors[0].ContainerID != "abc123" || sampler.readErrors[0].Field != "memory.peak" {
		t.Fatalf("cgroup read error was not parsed: %#v", sampler.readErrors)
	}
}

func TestIngestCgroupSampleRequiresAndParsesEveryField(t *testing.T) {
	sampler := newCgroupSampler("unused")
	sampler.ingestLine("123 /sys/fs/cgroup/cri-containerd-abc123.scope 1 2 3 4 5 6 7")
	if len(sampler.readErrors) != 0 || len(sampler.samples) != 1 {
		t.Fatalf("complete sample was not accepted: samples=%#v errors=%#v", sampler.samples, sampler.readErrors)
	}
	want := cgroupSample{
		TimeNano: 123, ContainerID: "abc123", CurrentBytes: 1, PeakBytes: 2, AnonBytes: 3,
		CPUUsageUsec: 4, NrThrottled: 5, ThrottledUsec: 6, NrPeriods: 7,
	}
	if got := sampler.samples[0]; got != want {
		t.Fatalf("parsed sample=%#v, want %#v", got, want)
	}
}

func TestIngestCgroupSampleRejectsIncompleteOrMalformedRequiredFields(t *testing.T) {
	tests := []struct {
		name      string
		line      string
		wantField string
	}{
		{
			name:      "old format misses cpu stat fields",
			line:      "123 /sys/fs/cgroup/cri-containerd-abc123.scope 1 2 3 4",
			wantField: "sample.field_count",
		},
		{
			name:      "malformed memory peak",
			line:      "123 /sys/fs/cgroup/cri-containerd-abc123.scope 1 invalid 3 4 5 6 7",
			wantField: "memory.peak",
		},
		{
			name:      "overflowing cpu usage",
			line:      "123 /sys/fs/cgroup/cri-containerd-abc123.scope 1 2 3 9223372036854775808 5 6 7",
			wantField: "cpu.stat.usage_usec",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sampler := newCgroupSampler("unused")
			sampler.ingestLine(tc.line)
			if len(sampler.samples) != 0 {
				t.Fatalf("invalid required field produced a sample: %#v", sampler.samples)
			}
			if len(sampler.readErrors) != 1 || sampler.readErrors[0].ContainerID != "abc123" ||
				sampler.readErrors[0].TimeNano != 123 || sampler.readErrors[0].Field != tc.wantField {
				t.Fatalf("invalid required field was not recorded: %#v", sampler.readErrors)
			}
		})
	}
}

func TestCgroupSamplerStatusRejectsCleanEOFEarly(t *testing.T) {
	sampler := newCgroupSampler("unused")
	sampler.startAttempted, sampler.started = true, true
	sampler.consumeStream(strings.NewReader(""))

	status := sampler.Status()
	if status.StreamComplete || !status.StreamEndedBeforeStop || status.StreamError != "" {
		t.Fatalf("premature clean EOF was not rejected: %#v", status)
	}
}

func TestCgroupSamplerStatusAcceptsCleanEOFAfterStop(t *testing.T) {
	sampler := newCgroupSampler("unused")
	sampler.startAttempted, sampler.started, sampler.stopRequested = true, true, true
	sampler.consumeStream(strings.NewReader(""))

	status := sampler.Status()
	if !status.StreamComplete || status.StreamEndedBeforeStop || status.StreamError != "" {
		t.Fatalf("intentional clean EOF was not accepted: %#v", status)
	}
}

func TestCgroupReadErrorMakesWindowInvalid(t *testing.T) {
	const containerID = "collector-container"
	start := time.Unix(100, 0).UnixNano()
	sampler := &cgroupSampler{
		labels: map[string]string{containerID: "collector-pod/collector"},
		samples: []cgroupSample{
			{TimeNano: start + int64(time.Second), ContainerID: containerID, CPUUsageUsec: 1_000},
			{TimeNano: start + 3*int64(time.Second), ContainerID: containerID, CPUUsageUsec: 201_000},
			{TimeNano: start + 5*int64(time.Second), ContainerID: containerID, CPUUsageUsec: 401_000},
			{TimeNano: start + 7*int64(time.Second), ContainerID: containerID, CPUUsageUsec: 601_000},
			{TimeNano: start + 9*int64(time.Second), ContainerID: containerID, CPUUsageUsec: 801_000},
		},
		readErrors: []cgroupReadError{{
			TimeNano: start + 4*int64(time.Second), ContainerID: containerID, Field: "memory.peak",
		}},
	}
	logs := []CollectorLogStat{{
		Pod: "collector-pod", ContainerID: containerID,
		IngressWindows: []CollectorIngressWindow{{
			WindowStartUnixNano: start, WindowSeconds: 10, NodeID: "node-a", Events: 1_000,
		}},
	}}

	row := sampler.AlignCollectorIngressWindows(logs, nil)[0]
	if row.CgroupReadErrors != 1 || row.CgroupDataComplete || row.ValidForSizing ||
		len(row.CgroupReadErrorFields) != 1 || row.CgroupReadErrorFields[0] != "memory.peak" {
		t.Fatalf("required-field read failure passed open: %#v", row)
	}
}

func completeCgroupSamplerStatus() CgroupSamplerStatus {
	return CgroupSamplerStatus{
		StartAttempted: true, Started: true, StopRequested: true, StreamEnded: true, StreamComplete: true,
	}
}

func validCollectorLog(pod, role, containerID string) CollectorLogStat {
	return CollectorLogStat{
		Pod: pod, Role: role, ContainerID: containerID, RestartCount: 0,
		LogStreamComplete: true, GracefulShutdownComplete: true,
	}
}

func validCollectorRow(pod, nodeID, containerID string, rate float64) CollectorResourceWindow {
	return CollectorResourceWindow{
		Pod: pod, NodeID: nodeID, NodeIDKnown: true,
		CollectorContainerID: containerID, CgroupContainerIDs: []string{containerID}, ContainerLifecycleBound: true,
		EventsPerSecond: rate, CgroupDataComplete: true, CPUIntervals: 8, RequiredCPUIntervals: 4,
		CPUObservedSeconds: 8, CPUCoverageRatio: 0.8, MaxCPUIntervalSeconds: 1,
		CPUSamplingValid: true, ValidForSizing: true,
	}
}

func validCollectorTermination(pod, containerID string) PodTermination {
	return PodTermination{
		Pod: pod, Container: "collector", ContainerID: containerID, RestartCount: 0,
		Observed: true, Source: "current", ExitCode: 0,
	}
}

func validCappedCollectorLog(pod, role, containerID string) CollectorLogStat {
	log := validCollectorLog(pod, role, containerID)
	log.CPURequest = "150m"
	log.CPULimit = "1200m"
	log.MemoryRequest = "160Mi"
	log.MemoryLimit = "192Mi"
	log.CgroupMemoryObserved = true
	log.CgroupMemoryMax = strconv.FormatInt(192<<20, 10)
	log.CgroupMemoryMaxBytes = 192 << 20
	return log
}

func TestAttachCollectorCgroupMemoryEvidence(t *testing.T) {
	logs := []CollectorLogStat{{Pod: "head-pod", ContainerID: "head-container"}}
	sampler := &cgroupSampler{
		memorySamples: []cgroupMemorySample{{
			TimeNano: 1, ContainerID: "head-container", MemoryMax: strconv.FormatInt(192<<20, 10),
			MemoryMaxBytes: 192 << 20, OOM: 0, OOMKill: 0,
		}},
		readErrors: []cgroupReadError{{TimeNano: 1, ContainerID: "head-container", Field: "memory.current"}},
	}

	attachCollectorCgroupMemoryEvidence(logs, sampler)
	if !logs[0].CgroupMemoryObserved || logs[0].CgroupMemoryMaxBytes != 192<<20 ||
		logs[0].MemoryEventsOOM != 0 || logs[0].MemoryEventsOOMKill != 0 ||
		logs[0].CgroupMemoryReadErrors != 1 || len(logs[0].CgroupMemoryErrorFields) != 1 ||
		logs[0].CgroupMemoryErrorFields[0] != "memory.current" {
		t.Fatalf("unexpected collector cgroup memory evidence: %#v", logs[0])
	}
}

func TestCollectorIngressGateRejectsInvalidMemoryLimitEvidence(t *testing.T) {
	baseLogs := []CollectorLogStat{
		validCappedCollectorLog("head-pod", "head", "head-container"),
		validCappedCollectorLog("worker-pod", "worker", "worker-container"),
	}
	rows := []CollectorResourceWindow{
		validCollectorRow("head-pod", "node-head", "head-container", 100),
		validCollectorRow("worker-pod", "node-worker", "worker-container", 200),
	}
	terminations := []PodTermination{
		validCollectorTermination("head-pod", "head-container"),
		validCollectorTermination("worker-pod", "worker-container"),
	}
	for _, gate := range summarizeCollectorIngressGates(baseLogs, rows, completeCgroupSamplerStatus(), terminations) {
		if !gate.Valid {
			t.Fatalf("valid capped Collector evidence was rejected: %#v", gate)
		}
	}

	for _, testCase := range []struct {
		name   string
		mutate func(*CollectorLogStat)
		want   string
	}{
		{name: "not observed", mutate: func(log *CollectorLogStat) { log.CgroupMemoryObserved = false }, want: "not observed"},
		{name: "wrong max", mutate: func(log *CollectorLogStat) { log.CgroupMemoryMaxBytes = 256 << 20 }, want: "memory.max"},
		{name: "oom", mutate: func(log *CollectorLogStat) { log.MemoryEventsOOM = 1 }, want: "oom=1"},
		{name: "read error", mutate: func(log *CollectorLogStat) {
			log.CgroupMemoryReadErrors = 1
			log.CgroupMemoryErrorFields = []string{"memory.events.oom"}
		}, want: "read errors"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			logs := append([]CollectorLogStat(nil), baseLogs...)
			testCase.mutate(&logs[0])
			gates := summarizeCollectorIngressGates(logs, rows, completeCgroupSamplerStatus(), terminations)
			if len(gates) != 2 {
				t.Fatalf("got %d gates, want 2", len(gates))
			}
			var head CollectorIngressGate
			for _, gate := range gates {
				if gate.Role == "head" {
					head = gate
				}
			}
			if head.Valid || !strings.Contains(strings.Join(head.Problems, ";"), testCase.want) {
				t.Fatalf("invalid memory evidence passed or lost reason: %#v", head)
			}
		})
	}
}

func TestCollectorIngressGateAcceptsExactHeadWorkerTopology(t *testing.T) {
	logs := []CollectorLogStat{
		validCollectorLog("head-pod", "head", "head-container"),
		validCollectorLog("worker-pod", "worker", "worker-container"),
	}
	rows := []CollectorResourceWindow{
		validCollectorRow("head-pod", "node-head", "head-container", 100),
		validCollectorRow("worker-pod", "node-worker", "worker-container", 200),
	}
	terminations := []PodTermination{
		validCollectorTermination("head-pod", "head-container"),
		validCollectorTermination("worker-pod", "worker-container"),
	}

	gates := summarizeCollectorIngressGates(logs, rows, completeCgroupSamplerStatus(), terminations)
	if len(gates) != 2 {
		t.Fatalf("got %d gates, want 2: %#v", len(gates), gates)
	}
	for _, gate := range gates {
		if !gate.Valid || !gate.LogStreamComplete || !gate.CgroupSamplerComplete || !gate.LifecycleBound ||
			len(gate.NodeIDs) != 1 || len(gate.Problems) != 0 {
			t.Fatalf("collector gate unexpectedly invalid: %#v", gate)
		}
	}
}

func TestCollectorIngressGateAcceptsGracefulMarkerWhenTerminationPollMisses(t *testing.T) {
	logs := []CollectorLogStat{
		validCollectorLog("head-pod", "head", "head-container"),
		validCollectorLog("worker-pod", "worker", "worker-container"),
	}
	rows := []CollectorResourceWindow{
		validCollectorRow("head-pod", "node-head", "head-container", 100),
		validCollectorRow("worker-pod", "node-worker", "worker-container", 200),
	}
	terminations := []PodTermination{
		{Pod: "head-pod", Container: "collector"},
		{Pod: "worker-pod", Container: "collector"},
	}

	for _, gate := range summarizeCollectorIngressGates(logs, rows, completeCgroupSamplerStatus(), terminations) {
		if !gate.Valid || !gate.GracefulShutdownComplete || gate.TerminationObserved || !gate.LifecycleBound {
			t.Fatalf("graceful marker did not replace the missed Kubernetes termination sample: %#v", gate)
		}
	}
}

func TestCollectorIngressGateRejectsMissingShutdownEvidence(t *testing.T) {
	logs := []CollectorLogStat{
		validCollectorLog("head-pod", "head", "head-container"),
		validCollectorLog("worker-pod", "worker", "worker-container"),
	}
	for i := range logs {
		logs[i].GracefulShutdownComplete = false
	}
	rows := []CollectorResourceWindow{
		validCollectorRow("head-pod", "node-head", "head-container", 100),
		validCollectorRow("worker-pod", "node-worker", "worker-container", 200),
	}
	terminations := []PodTermination{
		{Pod: "head-pod", Container: "collector"},
		{Pod: "worker-pod", Container: "collector"},
	}

	for _, gate := range summarizeCollectorIngressGates(logs, rows, completeCgroupSamplerStatus(), terminations) {
		if gate.Valid || gate.LifecycleBound || !hasProblem(gate.Problems, "neither the collector graceful-shutdown marker") {
			t.Fatalf("collector without shutdown evidence passed open: %#v", gate)
		}
	}
}

func TestCollectorIngressGateFailsClosedForNodeIDAndLogStreamFailures(t *testing.T) {
	logs := []CollectorLogStat{
		validCollectorLog("head-pod", "head", "head-container"),
		validCollectorLog("worker-pod", "worker", "worker-container"),
	}
	logs[0].LogStreamComplete, logs[0].LogStreamTimedOut = false, true
	logs[1].LogStreamComplete, logs[1].LogStreamError = false, "read stream: unexpected EOF"
	rows := []CollectorResourceWindow{
		validCollectorRow("head-pod", "node-a", "head-container", 100),
		validCollectorRow("head-pod", "node-b", "head-container", 90),
		validCollectorRow("worker-pod", "unknown", "worker-container", 200),
	}
	rows[2].NodeIDKnown, rows[2].CPUCoverageRatio, rows[2].ValidForSizing = false, 0.4, false
	rows[2].RejectedRequests, rows[2].RotationQueueFull = 1, 1
	terminations := []PodTermination{
		validCollectorTermination("head-pod", "head-container"),
		validCollectorTermination("worker-pod", "worker-container"),
	}

	gates := summarizeCollectorIngressGates(logs, rows, completeCgroupSamplerStatus(), terminations)
	if len(gates) != 2 {
		t.Fatalf("got %d gates, want 2: %#v", len(gates), gates)
	}
	byPod := map[string]CollectorIngressGate{}
	for _, gate := range gates {
		byPod[gate.Pod] = gate
	}
	head := byPod["head-pod"]
	if head.Valid || head.LogStreamComplete || len(head.NodeIDs) != 2 || !hasProblem(head.Problems, "timed out") || !hasProblem(head.Problems, "exactly one Ray NodeID") {
		t.Fatalf("head collector did not fail closed: %#v", head)
	}
	worker := byPod["worker-pod"]
	if worker.Valid || worker.LogStreamComplete || worker.NodeIDKnown || worker.PeakWindowCPUCoverage != 0.4 ||
		worker.RejectedRequests != 1 || worker.RotationQueueFull != 1 || !hasProblem(worker.Problems, "unexpected EOF") {
		t.Fatalf("worker collector did not fail closed: %#v", worker)
	}
}

func TestCollectorIngressGateRejectsSharedNodeID(t *testing.T) {
	logs := []CollectorLogStat{
		validCollectorLog("head-pod", "head", "head-container"),
		validCollectorLog("worker-pod", "worker", "worker-container"),
	}
	rows := []CollectorResourceWindow{
		validCollectorRow("head-pod", "same-node", "head-container", 100),
		validCollectorRow("worker-pod", "same-node", "worker-container", 100),
	}
	terminations := []PodTermination{
		validCollectorTermination("head-pod", "head-container"),
		validCollectorTermination("worker-pod", "worker-container"),
	}

	for _, gate := range summarizeCollectorIngressGates(logs, rows, completeCgroupSamplerStatus(), terminations) {
		if gate.Valid || !hasProblem(gate.Problems, "shared by 2 collectors") {
			t.Fatalf("shared NodeID did not fail collector %q closed: %#v", gate.Pod, gate)
		}
	}
}

func TestCollectorIngressGateRejectsMissingFormalTopology(t *testing.T) {
	logs := []CollectorLogStat{validCollectorLog("head-pod", "head", "head-container")}
	rows := []CollectorResourceWindow{validCollectorRow("head-pod", "node-head", "head-container", 100)}
	terminations := []PodTermination{validCollectorTermination("head-pod", "head-container")}

	gates := summarizeCollectorIngressGates(logs, rows, completeCgroupSamplerStatus(), terminations)
	if len(gates) != 2 {
		t.Fatalf("got %d gates, want real head plus explicit missing-worker row: %#v", len(gates), gates)
	}
	for _, gate := range gates {
		if gate.Valid || !hasProblem(gate.Problems, "expected exactly 2 collector pods") ||
			!hasProblem(gate.Problems, "expected exactly one worker collector") {
			t.Fatalf("missing topology did not fail gate closed: %#v", gate)
		}
	}
}

func TestCollectorIngressGateChecksAllEqualPeakWindows(t *testing.T) {
	logs := []CollectorLogStat{
		validCollectorLog("head-pod", "head", "head-container"),
		validCollectorLog("worker-pod", "worker", "worker-container"),
	}
	headGood := validCollectorRow("head-pod", "node-head", "head-container", 100)
	headBad := validCollectorRow("head-pod", "node-head", "head-container", 100)
	headBad.CPUCoverageRatio, headBad.ValidForSizing = 0.4, false
	worker := validCollectorRow("worker-pod", "node-worker", "worker-container", 80)
	terminations := []PodTermination{
		validCollectorTermination("head-pod", "head-container"),
		validCollectorTermination("worker-pod", "worker-container"),
	}

	for _, rows := range [][]CollectorResourceWindow{
		{headGood, headBad, worker},
		{headBad, headGood, worker},
	} {
		gates := summarizeCollectorIngressGates(logs, rows, completeCgroupSamplerStatus(), terminations)
		byPod := map[string]CollectorIngressGate{}
		for _, gate := range gates {
			byPod[gate.Pod] = gate
		}
		head := byPod["head-pod"]
		if head.Valid || head.PeakWindows != 2 || head.PeakWindowCPUCoverage != 0.4 ||
			!hasProblem(head.Problems, "one or more maximum event-rate windows") {
			t.Fatalf("equal-rate peak coverage was order-dependent or passed open: %#v", head)
		}
	}
}

func TestCollectorIngressGateRejectsRestartedOrMismatchedLifecycle(t *testing.T) {
	logs := []CollectorLogStat{
		validCollectorLog("head-pod", "head", "old-head-container"),
		validCollectorLog("worker-pod", "worker", "worker-container"),
	}
	rows := []CollectorResourceWindow{
		validCollectorRow("head-pod", "node-head", "old-head-container", 100),
		validCollectorRow("worker-pod", "node-worker", "worker-container", 100),
	}
	terminations := []PodTermination{
		validCollectorTermination("head-pod", "new-head-container"),
		validCollectorTermination("worker-pod", "worker-container"),
	}
	terminations[0].RestartCount = 1

	gates := summarizeCollectorIngressGates(logs, rows, completeCgroupSamplerStatus(), terminations)
	for _, gate := range gates {
		if gate.Pod == "head-pod" && (gate.Valid || gate.LifecycleBound ||
			!hasProblem(gate.Problems, "does not match followed container ID") ||
			!hasProblem(gate.Problems, "termination restartCount=1")) {
			t.Fatalf("old log and new termination lifecycle passed open: %#v", gate)
		}
	}
}

func TestCollectorIngressGateRejectsIncompleteCgroupSampler(t *testing.T) {
	logs := []CollectorLogStat{
		validCollectorLog("head-pod", "head", "head-container"),
		validCollectorLog("worker-pod", "worker", "worker-container"),
	}
	rows := []CollectorResourceWindow{
		validCollectorRow("head-pod", "node-head", "head-container", 100),
		validCollectorRow("worker-pod", "node-worker", "worker-container", 100),
	}
	terminations := []PodTermination{
		validCollectorTermination("head-pod", "head-container"),
		validCollectorTermination("worker-pod", "worker-container"),
	}
	status := completeCgroupSamplerStatus()
	status.StreamComplete, status.StreamError = false, "scanner failed"

	for _, gate := range summarizeCollectorIngressGates(logs, rows, status, terminations) {
		if gate.Valid || gate.CgroupSamplerComplete || !hasProblem(gate.Problems, "scanner failed") {
			t.Fatalf("incomplete cgroup stream passed open: %#v", gate)
		}
	}
}

func TestWriteCollectorIngressGatesCSVIncludesTopologyAndStreamFields(t *testing.T) {
	path := t.TempDir() + "/collector_ingress_gate.csv"
	gates := []CollectorIngressGate{{
		Pod:               "head-pod",
		Role:              "head",
		NodeIDs:           []string{"node-head"},
		NodeIDKnown:       true,
		LogStreamComplete: true,
		Valid:             true,
		Problems:          []string{},
	}}
	if err := writeCollectorIngressGatesCSV(path, gates); err != nil {
		t.Fatalf("write collector gate CSV: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read collector gate CSV: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, "pod,role,node_ids,container_id,restart_count,cgroup_container_ids,lifecycle_bound") ||
		!strings.Contains(text, "head-pod,head,\"node-head\"") {
		t.Fatalf("collector gate CSV is missing formal topology fields:\n%s", text)
	}
	records, err := csv.NewReader(strings.NewReader(text)).ReadAll()
	if err != nil || len(records) != 2 || len(records[0]) != len(records[1]) {
		t.Fatalf("collector gate CSV schema mismatch: records=%#v err=%v", records, err)
	}
}

func TestWriteCollectorResourceWindowsCSVIncludesCadenceAndReadErrors(t *testing.T) {
	path := t.TempDir() + "/collector_ingress_cgroup_10s.csv"
	rows := []CollectorResourceWindow{{
		Pod: "head-pod", NodeID: "node-head", CollectorContainerID: "head-container",
		CgroupContainerIDs: []string{"head-container"}, CgroupReadErrorFields: []string{"memory.peak"},
		CgroupReadErrors: 1, CPUIntervals: 4, RequiredCPUIntervals: 4, MaxCPUIntervalSeconds: 2,
		SubmittedToWorkerAttempts: 200, FinishedAttempts: 180, BacklogDelta: 20,
	}}
	if err := writeCollectorResourceWindowsCSV(path, rows); err != nil {
		t.Fatalf("write collector resource CSV: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read collector resource CSV: %v", err)
	}
	records, err := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if err != nil || len(records) != 2 || len(records[0]) != len(records[1]) {
		t.Fatalf("collector resource CSV schema mismatch: records=%#v err=%v", records, err)
	}
	header := strings.Join(records[0], ",")
	if !strings.Contains(header, "submitted_to_worker_attempts,finished_attempts,backlog_delta") ||
		!strings.Contains(header, "cgroup_read_errors,cgroup_read_error_fields,cgroup_data_complete") ||
		!strings.Contains(header, "cpu_intervals,required_cpu_intervals") {
		t.Fatalf("collector resource CSV missing fail-closed fields: %s", header)
	}
	if records[1][2] != "200" || records[1][3] != "180" || records[1][4] != "20" {
		t.Fatalf("collector resource CSV lost task lifecycle values: %#v", records[1])
	}
}

func hasProblem(problems []string, substring string) bool {
	for _, problem := range problems {
		if strings.Contains(problem, substring) {
			return true
		}
	}
	return false
}

// Summarize buckets samples into phases (via marks) per labeled container.
// Unlabeled containers (infra pods etc.) are skipped.
// A nil marks slice means the caller ran a single unphased measurement.
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
			// A pod/container label survives a container restart, but cpu.stat is
			// cumulative only within one cgroup. Never subtract across IDs.
			if prev.ContainerID != s.ContainerID {
				continue
			}
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
			Container:         label,
			Phase:             "lifetime",
			LifetimePeakMiB:   float64(peak) / (1 << 20),
			LifetimePeakBytes: peak,
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

func TestCgroupSummarizeDoesNotSubtractAcrossContainerRestart(t *testing.T) {
	const label = "collector-pod/collector"
	start := time.Unix(100, 0).UnixNano()
	sampler := &cgroupSampler{
		labels: map[string]string{"old": label, "new": label},
		samples: []cgroupSample{
			{TimeNano: start + int64(time.Second), ContainerID: "old", CPUUsageUsec: 100_000},
			{TimeNano: start + 2*int64(time.Second), ContainerID: "old", CPUUsageUsec: 200_000},
			{TimeNano: start + 3*int64(time.Second), ContainerID: "new", CPUUsageUsec: 500_000},
			{TimeNano: start + 4*int64(time.Second), ContainerID: "new", CPUUsageUsec: 700_000},
		},
	}

	rows := sampler.Summarize(nil)
	var usage *CgroupUsage
	for i := range rows {
		if rows[i].Container == label && rows[i].Phase == "baseline" {
			usage = &rows[i]
			break
		}
	}
	if usage == nil {
		t.Fatalf("baseline cgroup summary missing: %#v", rows)
	}
	if math.Abs(usage.AvgCores-0.15) > 1e-9 || math.Abs(usage.PeakCores-0.2) > 1e-9 {
		t.Fatalf("cross-container CPU delta was included: %#v", *usage)
	}
}

// WriteCSV dumps the raw series for offline analysis; timestamps, not the target
// 0.5s sleep, are authoritative because each scan adds variable work.
func (c *cgroupSampler) WriteCSV(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	// The throttle counters are cumulative, like cpu_usage_usec; consumers take
	// deltas. Without them a slow run under a CPU limit cannot be attributed:
	// "more work" and "held back by the quota" look identical in usage alone.
	if _, err := fmt.Fprintln(f, "time_nano,container,anon_bytes,current_bytes,peak_bytes,cpu_usage_usec,nr_throttled,throttled_usec,nr_periods"); err != nil {
		return err
	}
	for _, s := range c.samples {
		label := c.labels[s.ContainerID]
		if label == "" {
			continue
		}
		if _, err := fmt.Fprintf(f, "%d,%s,%d,%d,%d,%d,%d,%d,%d\n",
			s.TimeNano, label, s.AnonBytes, s.CurrentBytes, s.PeakBytes, s.CPUUsageUsec,
			s.NrThrottled, s.ThrottledUsec, s.NrPeriods); err != nil {
			return err
		}
	}
	return nil
}
