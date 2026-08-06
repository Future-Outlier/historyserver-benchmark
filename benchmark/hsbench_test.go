package benchmark

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	. "github.com/ray-project/kuberay/historyserver/test/support"
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
	ListClusters     EndpointStats   `json:"listClusters"`     // GET /clusters (uncached full scan per request)
	EnterColdLatency time.Duration   `json:"enterColdLatency"` // GET /enter_cluster: session cold load
	EnterStatus      int             `json:"enterStatus"`
	WarmEndpoints    []EndpointStats `json:"warmEndpoints"` // snapshot-backed reads after the cold load
	Notes            []string        `json:"notes"`
}

// hsManifest returns the manifest path to deploy. With BENCH_HS_CPU_LIMIT set it
// writes a copy carrying a different CPU limit: the shipped manifest pins 500m,
// and the cold load saturates it for its entire duration.
func hsManifest(t *testing.T, runDir string, cfg benchConfig) string {
	if cfg.HSCPULimit == "" {
		return ""
	}
	raw, err := os.ReadFile(HistoryServerManifestPath)
	if err != nil {
		t.Fatalf("read %s: %v", HistoryServerManifestPath, err)
	}
	const shipped = `cpu: "500m"`
	if strings.Count(string(raw), shipped) != 1 {
		t.Fatalf("%s no longer contains exactly one %s; update the benchmark", HistoryServerManifestPath, shipped)
	}
	path := filepath.Join(runDir, "historyserver-patched.yaml")
	patched := strings.Replace(string(raw), shipped, fmt.Sprintf("cpu: %q", cfg.HSCPULimit), 1)
	if err := os.WriteFile(path, []byte(patched), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	t.Logf("history server CPU limit overridden: 500m -> %s", cfg.HSCPULimit)
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
	if status != http.StatusOK {
		res.Notes = append(res.Notes,
			fmt.Sprintf("enter_cluster never succeeded within %s; warm reads below are expected to 503", cfg.HSWarmWait))
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
