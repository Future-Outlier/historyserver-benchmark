package benchmark

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
)

func TestNewHSHTTPRequestObservationSerializesEmptyProblemsArray(t *testing.T) {
	raw, err := json.Marshal(newHSHTTPRequestObservation("cold"))
	if err != nil {
		t.Fatalf("marshal observation: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode observation JSON: %v", err)
	}
	problems, present := decoded["problems"]
	items, isArray := problems.([]any)
	if !present || !isArray || items == nil || len(items) != 0 {
		t.Fatalf("problems must be a present empty JSON array: %s", raw)
	}
}

func TestRunHSBenchAbortsBeforeRequestAfterInvalidQuietGate(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		failGate     string
		wantRequests int
	}{
		{name: "startup", failGate: "startup-baseline", wantRequests: 0},
		{name: "post cold", failGate: "post-cold-quiet", wantRequests: 1},
		{name: "post count", failGate: "post-count-quiet", wantRequests: 2},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				w.Header().Set("Content-Type", "application/json")
				if strings.HasPrefix(r.URL.Path, "/enter_cluster/") {
					_, _ = io.WriteString(w, `{}`)
					return
				}
				if r.URL.Query().Get("limit") == "0" {
					_, _ = io.WriteString(w, `{"result":true,"data":{"result":{"result":[],"num_filtered":1}}}`)
					return
				}
				_, _ = io.WriteString(w, `{"result":true,"data":{"result":{"result":[],"num_filtered":1}}}`)
			}))
			defer server.Close()
			isolation := HSRequestIsolation{}
			hooks := &hsIsolationHooks{Isolation: &isolation, TestGate: func(name string) bool { return name != testCase.failGate }}
			validation := HSValidation{}
			runHSBench(t, gomega.NewWithT(t), server.URL, "ns", "cluster", "session", benchConfig{
				TaskCount: 1, HSStrictCold: true, HSProtocol: hsIsolatedRequestProtocol,
				HSEnterTimeout: time.Second, HSQueryConcurrency: 1,
			}, &validation, func(string) {}, hooks)
			if requests != testCase.wantRequests {
				t.Fatalf("requests=%d, want %d", requests, testCase.wantRequests)
			}
		})
	}
}

func TestEvaluateQuietWindowRequiresThreeConsecutiveLowCPUIntervals(t *testing.T) {
	const containerID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	start := time.Unix(100, 0)
	sampler := newCgroupSampler("unused")
	for index, usage := range []int64{0, 10_000, 20_000, 90_000, 100_000} {
		sampler.samples = append(sampler.samples, cgroupSample{
			TimeNano:    start.Add(5*time.Second + time.Duration(index)*time.Second).UnixNano(),
			ContainerID: containerID, CPUUsageUsec: usage, CurrentBytes: 100 + int64(index),
		})
	}
	window := sampler.EvaluateQuietWindow("gap", containerID, start, start.Add(10*time.Second))
	if window.Valid || !strings.Contains(strings.Join(window.Problems, ";"), "consecutive whole intervals") {
		t.Fatalf("burst masked by average: %#v", window)
	}
	// Four consecutive 10m intervals satisfy the fail-closed gate.
	for index := range sampler.samples {
		sampler.samples[index].CPUUsageUsec = int64(index) * 10_000
	}
	window = sampler.EvaluateQuietWindow("gap", containerID, start, start.Add(10*time.Second))
	if !window.Valid || window.IntervalCount != 4 || window.MemoryStartBytes != 100 || window.MemoryEndBytes != 104 {
		t.Fatalf("valid quiet window rejected: %#v", window)
	}
}

func TestEvaluateQuietWindowRequiresQuietSuffixAndFullStartupBaseline(t *testing.T) {
	const containerID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	start := time.Unix(200, 0)
	sampler := newCgroupSampler("unused")
	// Three quiet intervals followed by one busy interval must not authorize the
	// next request. The same samples also invalidate the full startup baseline.
	for index, usage := range []int64{0, 10_000, 20_000, 30_000, 100_000} {
		sampler.samples = append(sampler.samples, cgroupSample{TimeNano: start.Add(time.Duration(index) * time.Second).UnixNano(),
			ContainerID: containerID, CPUUsageUsec: usage})
	}
	if got := sampler.EvaluateStartupBaseline(containerID, start, start.Add(4*time.Second)); got.Valid {
		t.Fatalf("short/busy startup baseline accepted: %#v", got)
	}
	// Shift the same pattern past the post-gap minimum wait.
	for index := range sampler.samples {
		sampler.samples[index].TimeNano += int64(5 * time.Second)
	}
	if got := sampler.EvaluateQuietWindow("post-cold-quiet", containerID, start, start.Add(9*time.Second)); got.Valid {
		t.Fatalf("busy suffix accepted: %#v", got)
	}
}

func TestValidateHSFormalConfigIsolatedProfile(t *testing.T) {
	zero := int32(0)
	cfg := benchConfig{
		TaskCount: 1_000, S3Bucket: benchmarkS3BucketName, HSOnly: "source-ns/source-cluster/session_1000",
		HSSourceObjectCount: 10, HSSourceTotalBytes: 100,
		HSSourceTaskLogMetadataAlgorithm: taskLogMetadataAlgorithm,
		HSSourceTaskLogMetadataSHA256:    strings.Repeat("a", 64), HSSourceTaskLogMetadataAttempts: 1_000,
		HSSourceTaskLogMetadataNil: 1_000, HSSourceTaskLogMetadataLegacyWholeWorkerFallback: 1_000,
		HSArgs:       "--session-cache-size=1,--session-cache-max-bytes=2147483648,--session-cache-ttl=0s,--session-process-timeout=10m",
		HSStrictCold: true, HSColdSLO: 120 * time.Second, HSEnterTimeout: 12 * time.Minute,
		S3LocalPort: 19003, WarmIterations: 1, HSQueryConcurrency: 1,
		ExecutionIdentityFile: filepath.Join(t.TempDir(), "execution-namespace.json"),
		HSSourceRayJobOwned:   true, HSSourceShutdownAfterJob: true, HSSourceJobTTLSeconds: 30,
		HSSourceRayJobBackoffLimit: &zero, HSSourceSubmitterBackoffLimit: &zero,
	}
	cfg.HSCPURequest, cfg.HSCPULimit = "1", "2"
	cfg.HSMemoryRequest, cfg.HSMemoryLimit = "1Gi", "12Gi"
	cfg.HSEnv = "GOMAXPROCS=2,GODEBUG=gctrace=1"
	cfg.HSProtocol = hsIsolatedRequestProtocol
	cfg.HSPreColdIdle = hsIsolatedPreColdIdle
	cfg.HSRequestQuietGap = hsIsolatedQuietGap
	if err := validateHSFormalConfig(cfg); err != nil {
		t.Fatalf("valid isolated profile: %v", err)
	}
	cfg.HSEnv += ",GOGC=off"
	if err := validateHSFormalConfig(cfg); err == nil {
		t.Fatal("extra History Server env was accepted")
	}
}

func TestFinalizeHSRequestIsolationRejectsEndpointAndCheckpointEscape(t *testing.T) {
	report := &Report{Namespace: "ns", ClusterName: "cluster", SessionID: "session_1", Config: benchConfig{TaskCount: 1}}
	labels := []string{"before-cold-request", "after-cold-response", "before-count-request", "after-count-response", "before-detail-request", "after-detail-response"}
	for index, label := range labels {
		report.HSCPUCheckpoints = append(report.HSCPUCheckpoints, HSCPUCheckpoint{Label: label,
			TimeNano: int64(10 + index*10), ContainerID: strings.Repeat("c", 64), CPUUsageUsec: int64(index * 10)})
	}
	report.HSRequestIsolation.Requests = []HSHTTPRequestObservation{
		{Name: "cold", Endpoint: "/wrong", StartedAtNano: 5, CompletedAtNano: 15, Status: 200, ResponseBytes: 1, Attempts: 1, AcceptEncoding: "identity", Valid: true},
		{Name: "count", Endpoint: formalTaskEndpoint(0, false), StartedAtNano: 31, CompletedAtNano: 35, Status: 200, ResponseBytes: 1, Attempts: 1, AcceptEncoding: "identity", Valid: true},
		{Name: "detail", Endpoint: formalTaskEndpoint(1, true), StartedAtNano: 51, CompletedAtNano: 55, Status: 200, ResponseBytes: 1, Attempts: 1, AcceptEncoding: "identity", Valid: true},
	}
	finalizeHSRequestIsolation(report)
	problems := strings.Join(report.HSRequestIsolation.Problems, ";")
	if report.HSRequestIsolation.Valid || !strings.Contains(problems, "endpoint differs") ||
		!strings.Contains(problems, "request CPU window") {
		t.Fatalf("binding attack accepted: %#v", report.HSRequestIsolation)
	}
}
