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

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	WarmEndpoints    []EndpointStats `json:"warmEndpoints"` // snapshot-backed reads after the cold load
	GC               *GCStats        `json:"gc,omitempty"`  // only when GODEBUG=gctrace=1 was injected
	Notes            []string        `json:"notes"`
}

// The exact resources block the shipped manifest carries. Anchoring on the whole
// block (not just the number) keeps the rewrite honest if upstream changes it.
const shippedResources = `        resources:
          limits:
            cpu: "500m"`

// hsManifest returns the manifest path to deploy, rewriting a copy when
// BENCH_HS_CPU_LIMIT or BENCH_HS_ENV is set.
//
// The CPU limit matters twice over: it is the CFS quota, and since Go 1.25 the
// runtime also derives GOMAXPROCS from it (never from requests), rounding up but
// never below 2 - so "500m" means GOMAXPROCS=2. BENCH_HS_ENV separates the two.
func hsManifest(t *testing.T, runDir string, cfg benchConfig) string {
	if cfg.HSCPULimit == "" && cfg.HSEnv == "" && cfg.HSArgs == "" {
		return ""
	}
	raw, err := os.ReadFile(HistoryServerManifestPath)
	if err != nil {
		t.Fatalf("read %s: %v", HistoryServerManifestPath, err)
	}
	patched := string(raw)

	if cfg.HSCPULimit != "" {
		if strings.Count(patched, shippedResources) != 1 {
			t.Fatalf("%s no longer contains the expected resources block; update the benchmark",
				HistoryServerManifestPath)
		}
		// "none" removes the ceiling entirely: no CFS quota, and GOMAXPROCS then
		// follows the node's core count.
		replacement := "        resources:\n          requests:\n            cpu: \"500m\""
		if cfg.HSCPULimit != "none" {
			replacement += fmt.Sprintf("\n          limits:\n            cpu: %q", cfg.HSCPULimit)
		}
		patched = strings.Replace(patched, shippedResources, replacement, 1)
	}

	if cfg.HSArgs != "" {
		const argAnchor = "        - --ray-root-dir=log\n"
		if strings.Count(patched, argAnchor) != 1 {
			t.Fatalf("%s no longer has the expected command block; update the benchmark", HistoryServerManifestPath)
		}
		var b strings.Builder
		b.WriteString(argAnchor)
		for _, a := range strings.Split(cfg.HSArgs, ",") {
			fmt.Fprintf(&b, "        - %s\n", strings.TrimSpace(a))
		}
		patched = strings.Replace(patched, argAnchor, b.String(), 1)
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
			fmt.Fprintf(&b, "          - name: %s\n            value: %q\n", name, value)
		}
		patched = strings.Replace(patched, envAnchor, b.String(), 1)
	}

	path := filepath.Join(runDir, "historyserver-patched.yaml")
	if err := os.WriteFile(path, []byte(patched), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	t.Logf("history server manifest patched: cpu=%q env=%q", cfg.HSCPULimit, cfg.HSEnv)
	return path
}

// runHSBench measures the three user-facing costs: listing clusters, cold
// loading the benchmark session, and warm snapshot reads.
func runHSBench(t *testing.T, g *WithT, hsURL, namespace, clusterName, sessionID string, cfg benchConfig) HSBenchResult {
	res := HSBenchResult{}
	client := CreateHTTPClientWithCookieJar(g)
	// The default 30s would abort large cold loads and 50k-task responses.
	client.Timeout = cfg.HSEnterTimeout

	// Listing has no cache on the server (each request rescans cluster-metadata),
	// so every iteration is equally "cold".
	res.ListClusters = timeEndpoint(client, hsURL, "/clusters", 5)

	// Cold session load: this is the number to compare against "cold start"
	// claims — boot itself does zero storage I/O.
	enterURL := fmt.Sprintf("%s/enter_cluster/%s/raycluster/%s/%s", hsURL, namespace, clusterName, sessionID)
	start := time.Now()
	status, _, dur, err := timedGET(client, enterURL)
	if status != http.StatusOK {
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
			status, _, _, err = timedGET(client, enterURL)
			if status == http.StatusOK {
				break
			}
			time.Sleep(10 * time.Second)
		}
		client.Timeout = cfg.HSEnterTimeout
		if err != nil && status != http.StatusOK {
			res.Notes = append(res.Notes, fmt.Sprintf("warm-probe gave up: last status=%d err=%v", status, err))
		}
	}
	res.EnterColdLatency = time.Since(start)
	if status == http.StatusOK && res.EnterColdLatency > dur+time.Second {
		res.Notes = append(res.Notes, fmt.Sprintf(
			"cold-load measured via warm-probe (upper bound, 10s granularity): %s",
			res.EnterColdLatency.Round(time.Second)))
	}
	res.EnterStatus = status
	res.EnterMeasured = status == http.StatusOK
	if status != http.StatusOK {
		res.Notes = append(res.Notes, fmt.Sprintf(
			"NOT A MEASUREMENT: enter_cluster never returned 200 within %s, so enterColdLatency is the probe budget, not a load time",
			cfg.HSWarmWait))
		return res
	}
	t.Logf("enter_cluster cold load took %s", res.EnterColdLatency.Round(time.Millisecond))

	warm := []string{
		fmt.Sprintf("%s?limit=%d", EndpointTasks, cfg.TaskCount),
		EndpointTasksSummarize,
		"/api/jobs/",
		EndpointNodes + "?view=summary",
		"/events",
	}
	for _, ep := range warm {
		res.WarmEndpoints = append(res.WarmEndpoints, timeEndpoint(client, hsURL, ep, cfg.WarmIterations))
	}
	return res
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
	start := time.Now()
	resp, err := client.Get(url)
	if err != nil {
		return 0, 0, time.Since(start), err
	}
	defer resp.Body.Close()
	n, copyErr := io.Copy(io.Discard, resp.Body)
	dur = time.Since(start)
	if copyErr != nil {
		return resp.StatusCode, n, dur, copyErr
	}
	return resp.StatusCode, n, dur, nil
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
func captureHSLogs(test Test, namespace, runDir string) *GCStats {
	pods, err := test.Client().Core().CoreV1().Pods(namespace).List(test.Ctx(), metav1.ListOptions{
		LabelSelector: "app=historyserver",
	})
	if err != nil || len(pods.Items) == 0 {
		LogWithTimestamp(test.T(), "no history server pod to read logs from: %v", err)
		return nil
	}
	raw, err := test.Client().Core().CoreV1().Pods(namespace).
		GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{}).DoRaw(test.Ctx())
	if err != nil {
		LogWithTimestamp(test.T(), "failed to read history server log: %v", err)
		return nil
	}
	if err := os.WriteFile(filepath.Join(runDir, "historyserver.log"), raw, 0o644); err != nil {
		LogWithTimestamp(test.T(), "write historyserver.log: %v", err)
	}

	matches := gcTraceRe.FindAllStringSubmatch(string(raw), -1)
	if len(matches) == 0 {
		return nil
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
	return stats
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
func runHSOnly(t *testing.T, test Test, g *WithT, cfg benchConfig, runDir string) {
	parts := strings.Split(cfg.HSOnly, "/")
	if len(parts) != 3 {
		t.Fatalf("BENCH_HS_ONLY must be <namespace>/<cluster>/<sessionID>, got %q", cfg.HSOnly)
	}
	sessionNamespace, clusterName, sessionID := parts[0], parts[1], parts[2]

	report := &Report{Config: cfg, StartedAt: time.Now()}
	report.Env = captureEnvInfo(test)
	report.Namespace, report.ClusterName, report.SessionID = sessionNamespace, clusterName, sessionID

	namespace := test.NewTestNamespace()
	cgroups := newCgroupSampler(cfg.KindNode)
	// One phase, named to match the full-run reports so derive.py and the charts
	// read both modes the same way.
	marks := []phaseMark{{Name: "historyserver", At: time.Now()}}
	defer func() {
		cgroups.Stop()
		report.Cgroups = cgroups.Summarize(marks)
		if err := cgroups.WriteCSV(filepath.Join(runDir, "cgroup_samples.csv")); err != nil {
			t.Logf("write cgroup_samples.csv: %v", err)
		}
		writeReport(t, report, runDir)
	}()
	cgroups.Start(test)

	ApplyHistoryServer(test, g, namespace, hsManifest(t, runDir, cfg))
	cgroups.RegisterPods(test, namespace.Name)
	hsURL := GetHistoryServerURL(test, g, namespace)
	report.HistoryServer = runHSBench(t, g, hsURL, sessionNamespace, clusterName, sessionID, cfg)
	report.HistoryServer.GC = captureHSLogs(test, namespace.Name, runDir)
}
