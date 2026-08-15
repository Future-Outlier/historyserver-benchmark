package benchmark

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	awss3 "github.com/aws/aws-sdk-go/service/s3"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	. "github.com/ray-project/kuberay/historyserver/test/support"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// EndpointStats summarizes repeated timed GETs against one endpoint.
type EndpointStats struct {
	Endpoint  string        `json:"endpoint"`
	P50       time.Duration `json:"p50"`
	P95       time.Duration `json:"p95"`
	Max       time.Duration `json:"max"`
	LastBytes int64         `json:"lastBytes"`
	Errors    int           `json:"errors"`
}

// HSBenchResult captures the history server phase.
type HSBenchResult struct {
	ListClusters EndpointStats `json:"listClusters"` // GET /clusters (uncached full scan per request)
	// EnterColdLatency is a load time ONLY when EnterMeasured is true. When the
	// probe budget expires without a 200 it is just how long we waited, which is
	// a property of the budget, not of the server.
	EnterColdLatency time.Duration   `json:"enterColdLatency"`
	EnterMeasured    bool            `json:"enterMeasured"`
	EnterStatus      int             `json:"enterStatus"`
	EnterAttempts    int             `json:"enterAttempts"`
	WarmEndpoints    []EndpointStats `json:"warmEndpoints"` // snapshot-backed reads after the cold load
	GC               *GCStats        `json:"gc,omitempty"`  // only when GODEBUG=gctrace=1 was injected
	Notes            []string        `json:"notes"`
}

// HSPhaseTimestamp is an observation-only boundary for aligning the raw
// cgroup series with the formal History Server lifecycle. Recording a mark must
// never add a request, retry, probe, sleep, or cache access.
type HSPhaseTimestamp struct {
	Phase    string `json:"phase"`
	TimeNano int64  `json:"timeNano"`
}

type HSCPUCheckpoint struct {
	Label         string `json:"label"`
	TimeNano      int64  `json:"timeNano"`
	ContainerID   string `json:"containerID"`
	CPUUsageUsec  int64  `json:"cpuUsageUsec"`
	NrThrottled   int64  `json:"nrThrottled"`
	ThrottledUsec int64  `json:"throttledUsec"`
	NrPeriods     int64  `json:"nrPeriods"`
	Error         string `json:"error,omitempty"`
}

type HSIsolationWindow struct {
	Name             string            `json:"name"`
	Kind             string            `json:"kind"`
	StartLabel       string            `json:"startLabel"`
	EndLabel         string            `json:"endLabel"`
	WallDurationNano int64             `json:"wallDurationNano"`
	CPUUsageUsec     int64             `json:"cpuUsageUsec"`
	AvgMillicores    float64           `json:"avgMillicores"`
	NrThrottledDelta int64             `json:"nrThrottledDelta"`
	ThrottledUsec    int64             `json:"throttledUsec"`
	SampleCount      int               `json:"sampleCount"`
	IntervalCount    int               `json:"intervalCount"`
	MaxSampleGapNano int64             `json:"maxSampleGapNano"`
	MemoryStartBytes int64             `json:"memoryStartBytes"`
	MemoryEndBytes   int64             `json:"memoryEndBytes"`
	MemoryMinBytes   int64             `json:"memoryMinBytes"`
	MemoryMaxBytes   int64             `json:"memoryMaxBytes"`
	Intervals        []HSQuietInterval `json:"intervals,omitempty"`
	Valid            bool              `json:"valid"`
	Problems         []string          `json:"problems"`
}

type HSQuietInterval struct {
	StartNano     int64   `json:"startNano"`
	EndNano       int64   `json:"endNano"`
	CPUUsageUsec  int64   `json:"cpuUsageUsec"`
	AvgMillicores float64 `json:"avgMillicores"`
	Qualifies     bool    `json:"qualifies"`
}

type HSRequestIsolation struct {
	Protocol              string                     `json:"protocol"`
	QuietGapRequiredNano  int64                      `json:"quietGapRequiredNano"`
	QuietMaxAvgMillicores float64                    `json:"quietMaxAvgMillicores"`
	CheckpointCount       int                        `json:"checkpointCount"`
	Requests              []HSHTTPRequestObservation `json:"requests"`
	Windows               []HSIsolationWindow        `json:"windows"`
	Valid                 bool                       `json:"valid"`
	Problems              []string                   `json:"problems"`
}

type HSHTTPRequestObservation struct {
	Name                    string   `json:"name"`
	Endpoint                string   `json:"endpoint"`
	StartedAtNano           int64    `json:"startedAtNano"`
	CompletedAtNano         int64    `json:"completedAtNano"`
	Status                  int      `json:"status"`
	ResponseBytes           int64    `json:"responseBytes"`
	Attempts                int      `json:"attempts"`
	Redirects               int      `json:"redirects"`
	AcceptEncoding          string   `json:"acceptEncoding"`
	ResponseContentEncoding string   `json:"responseContentEncoding"`
	Valid                   bool     `json:"valid"`
	Problems                []string `json:"problems"`
}

func newHSHTTPRequestObservation(name string) HSHTTPRequestObservation {
	return HSHTTPRequestObservation{Name: name, Problems: []string{}}
}

type hsIsolationHooks struct {
	Sampler     *cgroupSampler
	ContainerID string
	Checkpoints *[]HSCPUCheckpoint
	Isolation   *HSRequestIsolation
	TestMinWait time.Duration
	TestMaxWait time.Duration
	TestGate    func(string) bool
}

const (
	hsIsolatedRequestProtocol = "isolated-request-v1"
	hsIsolatedPreColdIdle     = 10 * time.Second
	hsIsolatedQuietGap        = 8 * time.Second
	hsQuietMinWait            = 5 * time.Second
	hsQuietMaxWait            = 30 * time.Second
	hsQuietMaxSampleGap       = time.Second
	hsQuietMinIntervals       = 3
	hsQuietMaxAvgMillicores   = 50.0
)

var formalHSPhaseSequence = []string{
	"startup",
	"cold-load",
	"endpoint-test",
	"after-endpoint-test",
	"arm-end",
}

var isolatedHSPhaseSequence = []string{
	"startup-baseline",
	"cold-request",
	"post-cold-quiet",
	"count-request",
	"post-count-quiet",
	"detail-request",
	"post-detail-quiet",
	"arm-end",
}

// The exact resources block the shipped manifest carries. Anchoring on the whole
// block (not just the number) keeps the rewrite honest if upstream changes it.
const shippedResources = `        resources:
          limits:
            cpu: "500m"`

const shippedHistoryServerBucketEnv = `          - name: S3_BUCKET
            value: "ray-historyserver"`

const shippedHistoryServerImagePullPolicy = `        imagePullPolicy: IfNotPresent`

var activeHistoryServerEntrypoint = regexp.MustCompile(`(?m)^        (?:command|args):[[:space:]]*$`)

func patchHistoryServerS3Bucket(raw string) (string, error) {
	if strings.Count(raw, shippedHistoryServerBucketEnv) != 1 {
		return "", fmt.Errorf("history server manifest must contain exactly one expected S3_BUCKET entry")
	}
	benchmarkEnv := strings.Replace(
		shippedHistoryServerBucketEnv,
		`value: "ray-historyserver"`,
		fmt.Sprintf("value: %q", benchmarkS3BucketName),
		1,
	)
	return strings.Replace(raw, shippedHistoryServerBucketEnv, benchmarkEnv, 1), nil
}

func patchHistoryServerArgs(raw, args string) (string, error) {
	if args == "" {
		return raw, nil
	}
	if strings.Count(raw, shippedHistoryServerImagePullPolicy) != 1 {
		return "", fmt.Errorf("history server manifest must contain exactly one expected imagePullPolicy entry")
	}
	if activeHistoryServerEntrypoint.MatchString(raw) {
		return "", fmt.Errorf("history server manifest already contains an active command or args block")
	}
	var block strings.Builder
	block.WriteString(shippedHistoryServerImagePullPolicy)
	block.WriteString("\n        args:\n")
	for _, rawArg := range strings.Split(args, ",") {
		arg := strings.TrimSpace(rawArg)
		if arg == "" || strings.ContainsAny(arg, "\r\n") {
			return "", fmt.Errorf("invalid empty or multiline History Server argument %q", rawArg)
		}
		fmt.Fprintf(&block, "          - %q\n", arg)
	}
	return strings.Replace(raw, shippedHistoryServerImagePullPolicy, block.String(), 1), nil
}

// hsManifest always returns a benchmark-owned manifest copy so S3_BUCKET cannot
// fall back to the shared e2e bucket. It also applies requested CPU, memory,
// environment, and argument overrides.
//
// The CPU limit matters twice over: it is the CFS quota, and since Go 1.25 the
// runtime also derives GOMAXPROCS from it (never from requests), rounding up but
// never below 2 - so "500m" means GOMAXPROCS=2. BENCH_HS_ENV separates the two.
func hsManifest(t *testing.T, runDir string, cfg benchConfig) string {
	raw, err := os.ReadFile(HistoryServerManifestPath)
	if err != nil {
		t.Fatalf("read %s: %v", HistoryServerManifestPath, err)
	}
	patched, err := patchHistoryServerS3Bucket(string(raw))
	if err != nil {
		t.Fatalf("patch %s benchmark bucket: %v", HistoryServerManifestPath, err)
	}

	if cfg.HSCPURequest != "" || cfg.HSCPULimit != "" ||
		cfg.HSMemoryRequest != "" || cfg.HSMemoryLimit != "" {
		if strings.Count(patched, shippedResources) != 1 {
			t.Fatalf("%s no longer contains the expected resources block; update the benchmark",
				HistoryServerManifestPath)
		}
		// "none" removes the ceiling entirely: no CFS quota, and GOMAXPROCS then
		// follows the node's core count.
		cpuRequest := cfg.HSCPURequest
		if cpuRequest == "" {
			if cfg.HSCPULimit != "" && cfg.HSCPULimit != "none" {
				// On an idle Kind node, changing only the limit proves available CPU,
				// not the CPU guaranteed under contention. Formal discovery therefore
				// defaults request to the tested finite limit.
				cpuRequest = cfg.HSCPULimit
			}
		}
		cpuLimit := cfg.HSCPULimit
		if cpuLimit == "" {
			cpuLimit = "500m"
		}
		memoryRequest := cfg.HSMemoryRequest
		if memoryRequest == "" && cfg.HSMemoryLimit != "" && cfg.HSMemoryLimit != "none" {
			memoryRequest = cfg.HSMemoryLimit
		}

		var resources strings.Builder
		resources.WriteString("        resources:")
		if cpuRequest != "" || memoryRequest != "" {
			resources.WriteString("\n          requests:")
			if cpuRequest != "" {
				fmt.Fprintf(&resources, "\n            cpu: %q", cpuRequest)
			}
			if memoryRequest != "" {
				fmt.Fprintf(&resources, "\n            memory: %q", memoryRequest)
			}
		}
		if cpuLimit != "none" || (cfg.HSMemoryLimit != "" && cfg.HSMemoryLimit != "none") {
			resources.WriteString("\n          limits:")
			if cpuLimit != "none" {
				fmt.Fprintf(&resources, "\n            cpu: %q", cpuLimit)
			}
			if cfg.HSMemoryLimit != "" && cfg.HSMemoryLimit != "none" {
				fmt.Fprintf(&resources, "\n            memory: %q", cfg.HSMemoryLimit)
			}
		}
		patched = strings.Replace(patched, shippedResources, resources.String(), 1)
	}

	if cfg.HSArgs != "" {
		patched, err = patchHistoryServerArgs(patched, cfg.HSArgs)
		if err != nil {
			t.Fatalf("patch %s History Server arguments: %v", HistoryServerManifestPath, err)
		}
	}

	if cfg.HSEnv != "" {
		const envAnchor = "        env:\n"
		if strings.Count(patched, envAnchor) != 1 {
			t.Fatalf("%s no longer has exactly one env block; update the benchmark", HistoryServerManifestPath)
		}
		var b strings.Builder
		b.WriteString(envAnchor)
		for _, kv := range strings.Split(cfg.HSEnv, ",") {
			name, value, ok := strings.Cut(strings.TrimSpace(kv), "=")
			if !ok {
				t.Fatalf("BENCH_HS_ENV entry %q is not name=value", kv)
			}
			if name == "S3_BUCKET" {
				t.Fatalf("BENCH_HS_ENV cannot override the fixed benchmark S3_BUCKET")
			}
			fmt.Fprintf(&b, "          - name: %s\n            value: %q\n", name, value)
		}
		patched = strings.Replace(patched, envAnchor, b.String(), 1)
	}

	path := filepath.Join(runDir, "historyserver-patched.yaml")
	if err := os.WriteFile(path, []byte(patched), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	t.Logf("history server manifest patched: bucket=%q cpu request=%q limit=%q memory request=%q limit=%q env=%q",
		benchmarkS3BucketName, cfg.HSCPURequest, cfg.HSCPULimit, cfg.HSMemoryRequest, cfg.HSMemoryLimit, cfg.HSEnv)
	return path
}

// runHSBench measures the three user-facing costs: listing clusters, cold
// loading the benchmark session, and warm snapshot reads.
func runHSBench(
	t *testing.T,
	g *WithT,
	hsURL, namespace, clusterName, sessionID string,
	cfg benchConfig,
	validation *HSValidation,
	markPhase func(string),
	isolationHooks *hsIsolationHooks,
) HSBenchResult {
	res := HSBenchResult{}
	client := CreateHTTPClientWithCookieJar(g)
	// The default 30s would abort large cold loads and 50k-task responses.
	client.Timeout = cfg.HSEnterTimeout
	if cfg.HSProtocol == hsIsolatedRequestProtocol {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.DisableKeepAlives = true
		transport.DisableCompression = true
		client.Transport = transport
		client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
			return fmt.Errorf("redirects are forbidden by %s", hsIsolatedRequestProtocol)
		}
	}

	if !cfg.HSStrictCold {
		// Listing has no cache on the server (each request rescans
		// cluster-metadata), so every iteration is equally "cold". Formal mode
		// omits this pre-work: its first measured workload must be the first
		// /enter_cluster request against a fresh process and empty session cache.
		res.ListClusters = timeEndpoint(client, hsURL, "/clusters", 5)
	}

	// Cold session load: this is the number to compare against "cold start"
	// claims — boot itself does zero storage I/O.
	enterURL := fmt.Sprintf("%s/enter_cluster/%s/raycluster/%s/%s", hsURL, namespace, clusterName, sessionID)
	if cfg.HSProtocol == hsIsolatedRequestProtocol {
		markPhase("startup-baseline")
		if !waitHSQuietWindow(cfg.HSPreColdIdle, "startup-baseline", isolationHooks) {
			res.Notes = append(res.Notes, "startup quiet gate failed; cold request was not sent")
			return res
		}
		markPhase("cold-request")
		captureHSCheckpoint("before-cold-request", isolationHooks)
	} else if markPhase != nil {
		markPhase("cold-load")
	}
	start := time.Now()
	res.EnterAttempts++
	var coldObservation *HSHTTPRequestObservation
	if isolationHooks != nil && isolationHooks.Isolation != nil {
		isolationHooks.Isolation.Requests = append(isolationHooks.Isolation.Requests,
			newHSHTTPRequestObservation("cold"))
		coldObservation = &isolationHooks.Isolation.Requests[len(isolationHooks.Isolation.Requests)-1]
	}
	status, _, dur, err := timedGETObserved(client, enterURL, coldObservation)
	res.EnterColdLatency = dur
	if status != http.StatusOK || err != nil {
		if cfg.HSStrictCold {
			res.EnterStatus = status
			res.Notes = append(res.Notes, fmt.Sprintf(
				"strict cold first attempt failed: status=%d err=%v after %s; no retry was sent",
				status, err, dur.Round(time.Millisecond)))
			return res
		}
		// The request failing does NOT stop the load: the singleflight winner
		// keeps running server-side and caches the snapshot for the next caller
		// (session_loader.go). Re-attempt with short timeouts until the warm hit
		// lands, which upper-bounds the TRUE load duration.
		res.Notes = append(res.Notes, fmt.Sprintf(
			"first enter_cluster attempt: status=%d err=%v after %s; probing for warm hit",
			status, err, time.Since(start).Round(time.Second)))
		client.Timeout = 60 * time.Second
		deadline := start.Add(cfg.HSWarmWait)
		for time.Now().Before(deadline) {
			res.EnterAttempts++
			status, _, _, err = timedGET(client, enterURL)
			if status == http.StatusOK && err == nil {
				break
			}
			time.Sleep(10 * time.Second)
		}
		client.Timeout = cfg.HSEnterTimeout
		if err != nil && status != http.StatusOK {
			res.Notes = append(res.Notes, fmt.Sprintf("warm-probe gave up: last status=%d err=%v", status, err))
		}
	}
	if !cfg.HSStrictCold {
		res.EnterColdLatency = time.Since(start)
	}
	if status == http.StatusOK && res.EnterColdLatency > dur+time.Second {
		res.Notes = append(res.Notes, fmt.Sprintf(
			"cold-load measured via warm-probe (upper bound, 10s granularity): %s",
			res.EnterColdLatency.Round(time.Second)))
	}
	res.EnterStatus = status
	res.EnterMeasured = status == http.StatusOK && err == nil
	if !res.EnterMeasured {
		res.Notes = append(res.Notes, fmt.Sprintf(
			"NOT A MEASUREMENT: enter_cluster never returned 200 within %s, so enterColdLatency is the probe budget, not a load time",
			cfg.HSWarmWait))
		return res
	}
	t.Logf("enter_cluster cold load took %s", res.EnterColdLatency.Round(time.Millisecond))
	if cfg.HSStrictCold {
		if validation == nil {
			res.Notes = append(res.Notes, "strict cold validation target was nil")
			return res
		}
		if cfg.HSProtocol == hsIsolatedRequestProtocol {
			captureHSCheckpoint("after-cold-response", isolationHooks)
			markPhase("post-cold-quiet")
			if !waitHSQuietWindow(cfg.HSRequestQuietGap, "post-cold-quiet", isolationHooks) {
				res.Notes = append(res.Notes, "post-cold quiet gate failed; endpoint requests were not sent")
				return res
			}
			markPhase("count-request")
			captureHSCheckpoint("before-count-request", isolationHooks)
		} else if markPhase != nil {
			markPhase("endpoint-test")
		}
		if cfg.HSProtocol == hsIsolatedRequestProtocol {
			isolationHooks.Isolation.Requests = append(isolationHooks.Isolation.Requests,
				newHSHTTPRequestObservation("count"))
			validation.TaskCountQuery = executeFormalTaskQueryObserved(
				client, hsURL, 0, cfg.TaskCount, 0, cfg.HSQueryConcurrency, false,
				&isolationHooks.Isolation.Requests[len(isolationHooks.Isolation.Requests)-1],
			)
			captureHSCheckpoint("after-count-response", isolationHooks)
			markPhase("post-count-quiet")
			if !waitHSQuietWindow(cfg.HSRequestQuietGap, "post-count-quiet", isolationHooks) {
				res.Notes = append(res.Notes, "post-count quiet gate failed; detail request was not sent")
				return res
			}
			markPhase("detail-request")
			captureHSCheckpoint("before-detail-request", isolationHooks)
		} else {
			validation.TaskCountQuery = executeFormalTaskQuery(
				client, hsURL, 0, cfg.TaskCount, 0, cfg.HSQueryConcurrency, false,
			)
		}
		warmLimit := formalWarmTaskLimit(cfg.TaskCount)
		if cfg.HSProtocol == hsIsolatedRequestProtocol {
			isolationHooks.Isolation.Requests = append(isolationHooks.Isolation.Requests,
				newHSHTTPRequestObservation("detail"))
			validation.WarmTaskQuery = executeFormalTaskQueryObserved(
				client, hsURL, warmLimit, cfg.TaskCount,
				warmLimit, cfg.HSQueryConcurrency, true,
				&isolationHooks.Isolation.Requests[len(isolationHooks.Isolation.Requests)-1],
			)
			captureHSCheckpoint("after-detail-response", isolationHooks)
			markPhase("post-detail-quiet")
			if !waitHSQuietWindow(cfg.HSRequestQuietGap, "post-detail-quiet", isolationHooks) {
				res.Notes = append(res.Notes, "post-detail quiet gate failed")
				return res
			}
			markPhase("arm-end")
		} else {
			validation.WarmTaskQuery = executeFormalTaskQuery(
				client, hsURL, warmLimit, cfg.TaskCount,
				warmLimit, cfg.HSQueryConcurrency, true,
			)
		}
		if cfg.HSProtocol != hsIsolatedRequestProtocol && markPhase != nil {
			markPhase("after-endpoint-test")
		}
		return res
	}

	warm := warmEndpoints(cfg.TaskCount)
	for _, ep := range warm {
		res.WarmEndpoints = append(res.WarmEndpoints, timeEndpoint(client, hsURL, ep, cfg.WarmIterations))
	}
	return res
}

func captureHSCheckpoint(label string, hooks *hsIsolationHooks) {
	if hooks == nil || hooks.Sampler == nil || hooks.Checkpoints == nil {
		return
	}
	*hooks.Checkpoints = append(*hooks.Checkpoints,
		hooks.Sampler.CaptureCPUCheckpoint(label, hooks.ContainerID))
}

func waitHSQuietWindow(minDuration time.Duration, name string, hooks *hsIsolationHooks) bool {
	if hooks != nil && hooks.TestGate != nil {
		return hooks.TestGate(name)
	}
	if hooks == nil || hooks.Sampler == nil || hooks.Isolation == nil {
		time.Sleep(minDuration)
		return false
	}
	start := time.Now()
	maxWait := hsQuietMaxWait
	if hooks.TestMaxWait > 0 {
		maxWait = hooks.TestMaxWait
	}
	deadline := start.Add(maxWait)
	for {
		minimum := minDuration
		if hooks.TestMinWait > 0 {
			minimum = hooks.TestMinWait
		} else if minimum < hsQuietMinWait {
			minimum = hsQuietMinWait
		}
		if remaining := start.Add(minimum).Sub(time.Now()); remaining > 0 {
			time.Sleep(remaining)
		}
		var window HSIsolationWindow
		if name == "startup-baseline" {
			window = hooks.Sampler.EvaluateStartupBaseline(hooks.ContainerID, start, time.Now())
		} else {
			window = hooks.Sampler.EvaluateQuietWindow(name, hooks.ContainerID, start, time.Now())
		}
		if window.Valid || !time.Now().Before(deadline) {
			hooks.Isolation.Windows = append(hooks.Isolation.Windows, window)
			return window.Valid
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func warmEndpoints(taskCount int) []string {
	taskLimit := formalWarmTaskLimit(taskCount)
	return []string{
		fmt.Sprintf("%s?limit=%d", EndpointTasks, taskLimit),
		EndpointTasksSummarize,
		"/api/jobs/",
		EndpointNodes + "?view=summary",
		"/events",
	}
}

func formalWarmTaskLimit(taskCount int) int {
	if taskCount > utils.RayMaxLimitFromAPIServer {
		return utils.RayMaxLimitFromAPIServer
	}
	return taskCount
}

func timeEndpoint(client *http.Client, base, endpoint string, iterations int) EndpointStats {
	stats := EndpointStats{Endpoint: endpoint}
	var durations []time.Duration
	for i := 0; i < iterations; i++ {
		status, bytes, dur, err := timedGET(client, base+endpoint)
		if err != nil || status != http.StatusOK {
			stats.Errors++
			continue
		}
		stats.LastBytes = bytes
		durations = append(durations, dur)
	}
	if len(durations) > 0 {
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		stats.P50 = durations[len(durations)/2]
		stats.P95 = durations[(len(durations)*95)/100]
		stats.Max = durations[len(durations)-1]
	}
	return stats
}

func timedGET(client *http.Client, url string) (status int, bytes int64, dur time.Duration, err error) {
	return timedGETObserved(client, url, nil)
}

func timedGETObserved(client *http.Client, url string, observation *HSHTTPRequestObservation) (status int, bytes int64, dur time.Duration, err error) {
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, 0, 0, err
	}
	request.Header.Set("Accept-Encoding", "identity")
	start := time.Now()
	if observation != nil {
		observation.Endpoint = request.URL.RequestURI()
		observation.StartedAtNano = start.UnixNano()
		observation.Attempts = 1
		observation.AcceptEncoding = "identity"
	}
	resp, err := client.Do(request)
	if err != nil {
		if observation != nil {
			observation.CompletedAtNano = time.Now().UnixNano()
			observation.Problems = append(observation.Problems, err.Error())
		}
		return 0, 0, time.Since(start), err
	}
	defer resp.Body.Close()
	n, copyErr := io.Copy(io.Discard, resp.Body)
	dur = time.Since(start)
	if observation != nil {
		observation.CompletedAtNano = time.Now().UnixNano()
		observation.Status = resp.StatusCode
		observation.ResponseBytes = n
		observation.ResponseContentEncoding = resp.Header.Get("Content-Encoding")
		if encoding := observation.ResponseContentEncoding; encoding != "" && encoding != "identity" {
			observation.Problems = append(observation.Problems, "response Content-Encoding is "+encoding)
		}
		if copyErr != nil {
			observation.Problems = append(observation.Problems, copyErr.Error())
		}
		if resp.StatusCode != http.StatusOK {
			observation.Problems = append(observation.Problems, fmt.Sprintf("HTTP status=%d, want 200", resp.StatusCode))
		}
		observation.Valid = len(observation.Problems) == 0 && n > 0
	}
	if copyErr != nil {
		return resp.StatusCode, n, dur, copyErr
	}
	return resp.StatusCode, n, dur, nil
}

// settle waits out a quiet window while poking a session that is already cached,
// so the process keeps allocating a little. Without that traffic Go may run no GC
// at all during the window — its forced-collection period is two minutes — and the
// last gctrace line then reports a heap captured mid-load, i.e. a transient, not
// the retained cost. The request is a cache hit (session_loader.go LRU, cap 100,
// TTL 0) so it adds work without adding data.
func settle(test Test, hsURL, namespace, cluster, session string, d time.Duration) {
	deadline := time.Now().Add(d)
	client := &http.Client{Timeout: 30 * time.Second}
	for time.Now().Before(deadline) {
		time.Sleep(5 * time.Second)
		req := fmt.Sprintf("%s/enter_cluster/%s/raycluster/%s/%s", hsURL, namespace, cluster, session)
		resp, err := client.Get(req)
		if err != nil {
			LogWithTimestamp(test.T(), "settle probe: %v", err)
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// GCStats summarizes GODEBUG=gctrace=1 output from the history server. The
// percentage in a gctrace line is cumulative GC CPU share since process start,
// so the last line is what the whole cold load cost in collection.
type GCStats struct {
	Cycles       int     `json:"cycles"`
	FinalPercent float64 `json:"finalPercent"`
	PeakHeapMB   float64 `json:"peakHeapMB"`
	GOMAXPROCS   int     `json:"gomaxprocs"`
}

// gctrace lines look like:
// gc 12 @3.1s 7%: 0.1+45+0.2 ms clock, 0.5+12/44/0+1.0 ms cpu, 812->830->421 MB, 850 MB goal, 0 MB stacks, 0 MB globals, 4 P
var gcTraceRe = regexp.MustCompile(`gc (\d+) @[\d.]+s (\d+)%:.*?, (\d+)->(\d+)->(\d+) MB.*?, (\d+) P`)

// captureHSLogs writes the history server's container log next to the report and
// extracts gctrace stats if GODEBUG=gctrace=1 was set.
func captureHSLogs(test Test, namespace, runDir string) (*GCStats, HSLogValidation) {
	invalid := func(problem string) HSLogValidation {
		return HSLogValidation{
			ErrorCounts: map[string]int{},
			Problems:    []string{problem},
		}
	}
	pods, err := test.Client().Core().CoreV1().Pods(namespace).List(test.Ctx(), metav1.ListOptions{
		LabelSelector: "app=historyserver",
	})
	if err != nil {
		problem := fmt.Sprintf("list History Server pod for logs: %v", err)
		test.T().Error(problem)
		return nil, invalid(problem)
	}
	if len(pods.Items) != 1 {
		problem := fmt.Sprintf("found %d History Server pods for logs, want exactly 1", len(pods.Items))
		test.T().Error(problem)
		return nil, invalid(problem)
	}
	raw, err := test.Client().Core().CoreV1().Pods(namespace).
		GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{}).DoRaw(test.Ctx())
	if err != nil {
		problem := fmt.Sprintf("read History Server log: %v", err)
		test.T().Error(problem)
		return nil, invalid(problem)
	}
	validation := validateHSLogs(raw)
	if err := os.WriteFile(filepath.Join(runDir, "historyserver.log"), raw, 0o644); err != nil {
		test.T().Errorf("write historyserver.log: %v", err)
		validation.Problems = append(validation.Problems, fmt.Sprintf("write historyserver.log: %v", err))
		validation.Valid = false
	}

	matches := gcTraceRe.FindAllStringSubmatch(string(raw), -1)
	if len(matches) == 0 {
		return nil, validation
	}
	last := matches[len(matches)-1]
	stats := &GCStats{Cycles: len(matches)}
	stats.FinalPercent, _ = strconv.ParseFloat(last[2], 64)
	stats.GOMAXPROCS, _ = strconv.Atoi(last[6])
	for _, m := range matches {
		if v, err := strconv.ParseFloat(m[4], 64); err == nil && v > stats.PeakHeapMB {
			stats.PeakHeapMB = v
		}
	}
	return stats, validation
}

// runHSOnly measures a history server against a session that already exists in
// the bucket, instead of generating a fresh one. Every cell of a CPU or runtime
// comparison then reads byte-identical data, so the difference between two cells
// is the configuration and not the session.
//
// The history server resolves everything from object-storage paths, so the
// RayCluster the session came from does not need to exist, and this can run in a
// throwaway namespace. Set BENCH_HS_ONLY=<namespace>/<cluster>/<sessionID> from a
// prior run that used BENCH_SKIP_CLEANUP=1.
// Several sessions may be given, comma separated. They load one after another
// into the SAME server, which is the only way to separate the fixed cost of
// running a history server from the marginal cost of one more session: a
// regression over session sizes can only extrapolate an intercept, and here that
// extrapolation lands above the smallest measurement. The LRU holds 100 sessions
// with no TTL (session_loader.go:18-23), so an earlier session is still resident
// when the next one loads.
func runHSOnly(t *testing.T, test Test, g *WithT, cfg benchConfig, runDir string) {
	specs := strings.Split(cfg.HSOnly, ",")
	type target struct{ namespace, cluster, session string }
	targets := make([]target, 0, len(specs))
	for _, spec := range specs {
		parsed, err := parseHSSourceSpec(strings.TrimSpace(spec))
		if err != nil {
			t.Fatalf("BENCH_HS_ONLY entry %q is unsafe: %v", spec, err)
		}
		targets = append(targets, target{parsed.namespace, parsed.cluster, parsed.session})
	}
	sessionNamespace, clusterName, sessionID := targets[0].namespace, targets[0].cluster, targets[0].session

	report := &Report{Config: cfg, StartedAt: time.Now()}
	report.Env = captureEnvInfo(test)
	report.Namespace, report.ClusterName, report.SessionID = sessionNamespace, clusterName, sessionID
	report.HSValidation.ExpectedTaskAttempts = cfg.TaskCount
	if cfg.HSStrictCold {
		report.HSValidation.Scope = HSBenchmarkScope{Replay: true, TaskList: true, LogsFile: false}
	}

	namespace := test.NewTestNamespace()
	writeExecutionNamespaceIdentity(t, cfg, namespace)
	report.ExecutionNamespace = namespace.Name
	report.ExecutionNamespaceUID = string(namespace.UID)
	report.RayJobLifecycle = RayJobLifecycleEvidence{
		OwnedCluster:             cfg.HSSourceRayJobOwned,
		ShutdownAfterJobFinishes: cfg.HSSourceShutdownAfterJob,
		TTLSecondsAfterFinished:  cfg.HSSourceJobTTLSeconds,
		RayJobBackoffLimit:       copyInt32Pointer(cfg.HSSourceRayJobBackoffLimit),
		SubmitterBackoffLimit:    copyInt32Pointer(cfg.HSSourceSubmitterBackoffLimit),
	}
	cgroups := newCgroupSampler(cfg.KindNode)
	// One phase, named to match the full-run reports so derive.py and the charts
	// read both modes the same way.
	marks := []phaseMark{{Name: "historyserver", At: time.Now()}}
	markHSPhase := func(phase string) {
		report.HistoryServerPhases = append(report.HistoryServerPhases, HSPhaseTimestamp{
			Phase:    phase,
			TimeNano: time.Now().UnixNano(),
		})
	}
	defer func() {
		if cfg.HSStrictCold && (len(report.HistoryServerPhases) == 0 ||
			report.HistoryServerPhases[len(report.HistoryServerPhases)-1].Phase != "arm-end") {
			markHSPhase("arm-end")
		}
		cgroups.Stop()
		report.CgroupSampler = cgroups.Status()
		report.Cgroups = cgroups.Summarize(marks)
		report.SpoolPeakMiB = cgroups.SpoolPeaks()
		if cfg.HSStrictCold {
			// Re-read final status: a pod can be Ready when the cold load starts
			// and later restart or become OOMKilled. The final evidence must not
			// preserve that stale initial success state.
			report.HSPodEvidence = captureHSPodEvidence(test, namespace.Name)
			memory := cgroups.MemoryEvidence(report.HSPodEvidence.ContainerID)
			finalizeHSPodEvidence(&report.HSPodEvidence, cfg, memory)
			label := report.HSPodEvidence.PodName + "/" + report.HSPodEvidence.ContainerName
			for _, usage := range report.Cgroups {
				if usage.Container == label && usage.Phase == "lifetime" {
					report.HSValidation.LifetimeMemoryPeakBytes = usage.LifetimePeakBytes
					break
				}
			}
			if cfg.HSProtocol == hsIsolatedRequestProtocol {
				finalizeHSRequestIsolation(report)
			}
			finalizeHSValidation(report)
		}
		if err := cgroups.WriteCSV(filepath.Join(runDir, "cgroup_samples.csv")); err != nil {
			t.Errorf("write cgroup_samples.csv: %v", err)
		}
		writeReport(t, report, runDir)
		if cfg.HSStrictCold && !report.HSValidation.Valid {
			t.Errorf("formal History Server validity gate failed: %s",
				strings.Join(report.HSValidation.Problems, "; "))
		}
	}()
	cgroups.Start(test)

	var readOnlyS3Client *awss3.S3
	if cfg.HSStrictCold {
		readOnlyS3Client = existingBenchS3ReadClient(t, cfg.S3LocalPort, cfg.S3Bucket)
		digest, objectCount, totalBytes, err := takeSourceSessionFingerprint(
			readOnlyS3Client, cfg.S3Bucket, sessionNamespace, clusterName, sessionID,
		)
		if err != nil {
			t.Errorf("fingerprint source session before campaign arm: %v", err)
			return
		}
		report.SourceSessionFingerprint = SourceSessionFingerprint{
			Algorithm:   sourceFingerprintAlgorithm,
			Bucket:      cfg.S3Bucket,
			Start:       digest,
			ObjectCount: objectCount,
			TotalBytes:  totalBytes,
		}
	}

	if cfg.HSStrictCold && cfg.HSProtocol != hsIsolatedRequestProtocol {
		markHSPhase("startup")
	}
	ApplyHistoryServer(test, g, namespace, hsManifest(t, runDir, cfg))
	cgroups.RegisterPods(test, namespace.Name)
	hsURL := GetHistoryServerURL(test, g, namespace)
	report.HSPodEvidence = captureHSPodEvidence(test, namespace.Name)

	for i, tg := range targets {
		// A settle window before each load: the sampler runs at 1 Hz, so without
		// it the "memory after session i" reading can land mid-load of session
		// i+1 and attribute one session's transient to the other's total.
		if i > 0 && !cfg.HSStrictCold {
			marks = append(marks, phaseMark{Name: fmt.Sprintf("settle%d", i), At: time.Now()})
			settle(test, hsURL, targets[0].namespace, targets[0].cluster, targets[0].session, cfg.HSSessionSettle)
			marks = append(marks, phaseMark{Name: fmt.Sprintf("session%d", i+1), At: time.Now()})
		}
		var phaseRecorder func(string)
		if cfg.HSStrictCold {
			phaseRecorder = markHSPhase
		}
		res := runHSBench(
			t, g, hsURL, tg.namespace, tg.cluster, tg.session, cfg,
			&report.HSValidation, phaseRecorder, func() *hsIsolationHooks {
				if cfg.HSProtocol != hsIsolatedRequestProtocol {
					return nil
				}
				report.HSRequestIsolation = HSRequestIsolation{Protocol: hsIsolatedRequestProtocol,
					QuietGapRequiredNano: cfg.HSRequestQuietGap.Nanoseconds(), QuietMaxAvgMillicores: hsQuietMaxAvgMillicores,
					Valid: true, Problems: []string{}}
				return &hsIsolationHooks{Sampler: cgroups, ContainerID: report.HSPodEvidence.ContainerID,
					Checkpoints: &report.HSCPUCheckpoints, Isolation: &report.HSRequestIsolation}
			}(),
		)
		LogWithTimestamp(test.T(), "BENCH_HS_SESSION %d/%d %s status=%d measured=%v %.1fs",
			i+1, len(targets), tg.session, res.EnterStatus, res.EnterMeasured, res.EnterColdLatency.Seconds())
		if i == 0 {
			report.HistoryServer = res
		}
		report.HistoryServerSessions = append(report.HistoryServerSessions, res)
	}
	if !cfg.HSStrictCold {
		// Trailing settle so the last session's retained cost is sampled after its
		// transient has been collected, not only at its peak.
		marks = append(marks, phaseMark{Name: "settled", At: time.Now()})
		settle(test, hsURL, targets[0].namespace, targets[0].cluster, targets[0].session, cfg.HSSessionSettle)
	}

	report.HistoryServer.GC, report.HSValidation.Logs = captureHSLogs(test, namespace.Name, runDir)
	if cfg.HSStrictCold {
		var projection map[taskAttemptKey]taskLogMetadataRecord
		report.HSValidation.FullReplay, projection = strictReplaySourceSession(
			readOnlyS3Client, cfg.S3Bucket, sessionNamespace, clusterName, sessionID, cfg.TaskCount,
			expectedTaskLogMetadataFromConfig(cfg),
		)
		validateWarmTaskProjection(&report.HSValidation.WarmTaskQuery, projection)
		endDigest, objectCount, totalBytes, err := takeSourceSessionFingerprint(
			readOnlyS3Client, cfg.S3Bucket, sessionNamespace, clusterName, sessionID,
		)
		if err != nil {
			t.Errorf("fingerprint source session after campaign arm: %v", err)
			return
		}
		report.SourceSessionFingerprint.End = endDigest
		if objectCount != report.SourceSessionFingerprint.ObjectCount ||
			totalBytes != report.SourceSessionFingerprint.TotalBytes {
			t.Errorf("source session object inventory changed: start=(%d,%d) end=(%d,%d)",
				report.SourceSessionFingerprint.ObjectCount, report.SourceSessionFingerprint.TotalBytes,
				objectCount, totalBytes)
			return
		}
	}
	report.Completed = true
}

func finalizeHSRequestIsolation(report *Report) {
	isolation := &report.HSRequestIsolation
	checkpoints := report.HSCPUCheckpoints
	isolation.CheckpointCount = len(checkpoints)
	byLabel := make(map[string]HSCPUCheckpoint, len(checkpoints))
	for _, checkpoint := range checkpoints {
		if checkpoint.Error != "" {
			isolation.Problems = append(isolation.Problems, checkpoint.Label+": "+checkpoint.Error)
		}
		if _, duplicate := byLabel[checkpoint.Label]; duplicate {
			isolation.Problems = append(isolation.Problems, "duplicate checkpoint "+checkpoint.Label)
		}
		byLabel[checkpoint.Label] = checkpoint
	}
	expectedEndpoints := map[string]string{
		"cold":   fmt.Sprintf("/enter_cluster/%s/raycluster/%s/%s", report.Namespace, report.ClusterName, report.SessionID),
		"count":  formalTaskEndpoint(0, false),
		"detail": formalTaskEndpoint(formalWarmTaskLimit(report.Config.TaskCount), true),
	}
	for _, request := range isolation.Requests {
		if !request.Valid || request.Attempts != 1 || request.Redirects != 0 || request.AcceptEncoding != "identity" ||
			request.Status != http.StatusOK || request.ResponseBytes <= 0 || request.CompletedAtNano <= request.StartedAtNano {
			isolation.Problems = append(isolation.Problems, "invalid isolated HTTP observation: "+request.Name)
		}
		if request.Endpoint != expectedEndpoints[request.Name] {
			isolation.Problems = append(isolation.Problems, "isolated HTTP endpoint differs: "+request.Name)
		}
		start, end := byLabel["before-"+request.Name+"-request"], byLabel["after-"+request.Name+"-response"]
		window := HSIsolationWindow{Name: request.Name, Kind: "request", StartLabel: start.Label, EndLabel: end.Label,
			WallDurationNano: end.TimeNano - start.TimeNano, CPUUsageUsec: end.CPUUsageUsec - start.CPUUsageUsec,
			NrThrottledDelta: end.NrThrottled - start.NrThrottled, ThrottledUsec: end.ThrottledUsec - start.ThrottledUsec,
			Valid: true, Problems: []string{}}
		if start.Error != "" || end.Error != "" || start.ContainerID == "" || start.ContainerID != end.ContainerID ||
			window.WallDurationNano <= 0 || window.CPUUsageUsec < 0 || start.TimeNano > request.StartedAtNano ||
			request.CompletedAtNano > end.TimeNano {
			window.Problems = append(window.Problems, "request checkpoint pair is invalid")
		}
		if window.WallDurationNano > 0 && window.CPUUsageUsec >= 0 {
			window.AvgMillicores = float64(window.CPUUsageUsec) * 1e6 / float64(window.WallDurationNano)
		}
		window.Valid = len(window.Problems) == 0
		isolation.Windows = append(isolation.Windows, window)
		if !window.Valid {
			isolation.Problems = append(isolation.Problems, "invalid request CPU window: "+request.Name)
		}
	}
	for index := 1; index < len(checkpoints); index++ {
		if checkpoints[index].ContainerID != checkpoints[0].ContainerID || checkpoints[index].TimeNano <= checkpoints[index-1].TimeNano ||
			checkpoints[index].CPUUsageUsec < checkpoints[index-1].CPUUsageUsec ||
			checkpoints[index].NrThrottled < checkpoints[index-1].NrThrottled ||
			checkpoints[index].ThrottledUsec < checkpoints[index-1].ThrottledUsec {
			isolation.Problems = append(isolation.Problems, "checkpoint identity/time/counter sequence is invalid")
		}
	}
	for index := 1; index < len(isolation.Requests); index++ {
		if isolation.Requests[index].StartedAtNano <= isolation.Requests[index-1].CompletedAtNano {
			isolation.Problems = append(isolation.Problems, "isolated HTTP requests overlap")
		}
	}
	for _, window := range isolation.Windows {
		if !window.Valid {
			isolation.Problems = append(isolation.Problems, "invalid isolation window: "+window.Name)
		}
	}
	isolation.Valid = len(isolation.Problems) == 0 && len(isolation.Requests) == 3 && len(checkpoints) == 6
}
