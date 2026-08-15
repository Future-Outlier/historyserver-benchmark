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

// The exact resources block the shipped manifest carries. Anchoring on the whole
// block (not just the number) keeps the rewrite honest if upstream changes it.
const shippedResources = `        resources:
          limits:
            cpu: "500m"`

const shippedHistoryServerBucketEnv = `          - name: S3_BUCKET
            value: "ray-historyserver"`

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
) HSBenchResult {
	res := HSBenchResult{}
	client := CreateHTTPClientWithCookieJar(g)
	// The default 30s would abort large cold loads and 50k-task responses.
	client.Timeout = cfg.HSEnterTimeout

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
	start := time.Now()
	res.EnterAttempts++
	status, _, dur, err := timedGET(client, enterURL)
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
		validation.TaskCountQuery = executeFormalTaskQuery(
			client, hsURL, 0, cfg.TaskCount, 0, cfg.HSQueryConcurrency, false,
		)
		warmLimit := formalWarmTaskLimit(cfg.TaskCount)
		validation.WarmTaskQuery = executeFormalTaskQuery(
			client, hsURL, warmLimit, cfg.TaskCount,
			warmLimit, cfg.HSQueryConcurrency, true,
		)
		return res
	}

	warm := warmEndpoints(cfg.TaskCount)
	for _, ep := range warm {
		res.WarmEndpoints = append(res.WarmEndpoints, timeEndpoint(client, hsURL, ep, cfg.WarmIterations))
	}
	return res
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
	defer func() {
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
		res := runHSBench(t, g, hsURL, tg.namespace, tg.cluster, tg.session, cfg, &report.HSValidation)
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
