package benchmark

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"

	. "github.com/ray-project/kuberay/historyserver/test/support"
)

func TestWarmTaskQueryUsesLegalWorstCaseLimit(t *testing.T) {
	tests := []struct {
		name      string
		taskCount int
		want      string
	}{
		{name: "below cap", taskCount: 1000, want: "/api/v0/tasks?limit=1000"},
		{name: "at cap", taskCount: 10000, want: "/api/v0/tasks?limit=10000"},
		{name: "above cap", taskCount: 100000, want: "/api/v0/tasks?limit=10000"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := warmEndpoints(test.taskCount)
			if len(got) == 0 || got[0] != test.want {
				t.Fatalf("warm task endpoint = %v, want first endpoint %q", got, test.want)
			}
		})
	}
}

func TestHSManifestUsesDedicatedS3BucketWithoutOtherOverrides(t *testing.T) {
	path := hsManifest(t, t.TempDir(), benchConfig{})
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read patched History Server manifest: %v", err)
	}
	manifest := string(raw)
	benchmarkEntry := "          - name: S3_BUCKET\n            value: \"" + benchmarkS3BucketName + "\""
	if strings.Count(manifest, benchmarkEntry) != 1 {
		t.Fatalf("dedicated benchmark S3_BUCKET count=%d, want 1", strings.Count(manifest, benchmarkEntry))
	}
	if strings.Contains(manifest, shippedHistoryServerBucketEnv) {
		t.Fatalf("patched manifest still contains the shared e2e bucket entry")
	}
}

func TestPatchHistoryServerS3BucketFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
	}{
		{name: "missing", raw: "kind: Deployment\n"},
		{name: "duplicate", raw: shippedHistoryServerBucketEnv + "\n" + shippedHistoryServerBucketEnv},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := patchHistoryServerS3Bucket(test.raw); err == nil {
				t.Fatal("ambiguous History Server S3 bucket manifest was accepted")
			}
		})
	}
}

func TestPatchHistoryServerArgsUsesContainerArgs(t *testing.T) {
	const args = "--session-cache-size=1,--session-cache-max-bytes=2147483648,--session-cache-ttl=0s,--session-process-timeout=10m"
	path := hsManifest(t, t.TempDir(), benchConfig{HSArgs: args})
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read patched History Server manifest: %v", err)
	}
	manifest := string(raw)
	want := `        imagePullPolicy: IfNotPresent
        args:
          - "--session-cache-size=1"
          - "--session-cache-max-bytes=2147483648"
          - "--session-cache-ttl=0s"
          - "--session-process-timeout=10m"`
	if strings.Count(manifest, want) != 1 {
		t.Fatalf("patched manifest does not contain the exact args block:\n%s", manifest)
	}
}

func TestPatchHistoryServerArgsFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
		args string
	}{
		{name: "missing anchor", raw: "kind: Deployment\n", args: "--session-cache-size=1"},
		{name: "duplicate anchor", raw: shippedHistoryServerImagePullPolicy + "\n" + shippedHistoryServerImagePullPolicy, args: "--session-cache-size=1"},
		{name: "existing args", raw: shippedHistoryServerImagePullPolicy + "\n        args:\n          - \"--other=true\"", args: "--session-cache-size=1"},
		{name: "existing command", raw: shippedHistoryServerImagePullPolicy + "\n        command:\n          - \"other\"", args: "--session-cache-size=1"},
		{name: "empty argument", raw: shippedHistoryServerImagePullPolicy, args: "--session-cache-size=1,,--session-cache-ttl=0s"},
		{name: "multiline argument", raw: shippedHistoryServerImagePullPolicy, args: "--session-cache-size=1\n--session-cache-ttl=0s"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := patchHistoryServerArgs(test.raw, test.args); err == nil {
				t.Fatal("invalid History Server argument patch was accepted")
			}
		})
	}
}

func TestHSManifestCPUResources(t *testing.T) {
	tests := []struct {
		name        string
		request     string
		limit       string
		wantRequest string
		wantLimit   string
	}{
		{
			name:        "finite limit defaults request to limit",
			limit:       "2",
			wantRequest: "2",
			wantLimit:   "2",
		},
		{
			name:        "explicit asymmetric resources remain possible",
			request:     "1",
			limit:       "2",
			wantRequest: "1",
			wantLimit:   "2",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := hsManifest(t, t.TempDir(), benchConfig{
				HSCPURequest: test.request,
				HSCPULimit:   test.limit,
			})
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read patched History Server manifest: %v", err)
			}
			manifest := string(raw)
			request := "requests:\n            cpu: \"" + test.wantRequest + "\""
			limit := "limits:\n            cpu: \"" + test.wantLimit + "\""
			if !strings.Contains(manifest, request) || !strings.Contains(manifest, limit) {
				t.Fatalf("patched resources do not match request=%s limit=%s:\n%s",
					test.wantRequest, test.wantLimit, manifest)
			}
		})
	}
}

func TestHSManifestFormalCPUAndMemoryResources(t *testing.T) {
	path := hsManifest(t, t.TempDir(), benchConfig{
		HSCPURequest:    "2",
		HSCPULimit:      "2",
		HSMemoryRequest: "8Gi",
		HSMemoryLimit:   "8Gi",
	})
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read patched History Server manifest: %v", err)
	}
	manifest := string(raw)
	for _, fragment := range []string{
		"requests:\n            cpu: \"2\"\n            memory: \"8Gi\"",
		"limits:\n            cpu: \"2\"\n            memory: \"8Gi\"",
	} {
		if !strings.Contains(manifest, fragment) {
			t.Fatalf("patched resources are missing %q:\n%s", fragment, manifest)
		}
	}
}

func TestValidateHSFormalConfig(t *testing.T) {
	valid := benchConfig{
		TaskCount:                        5_000,
		S3Bucket:                         benchmarkS3BucketName,
		HSCPURequest:                     "1",
		HSCPULimit:                       "1000m",
		HSMemoryRequest:                  "8Gi",
		HSMemoryLimit:                    "8192Mi",
		HSArgs:                           "--session-cache-size=1,--session-cache-max-bytes=2147483648,--session-cache-ttl=0s,--session-process-timeout=10m",
		HSOnly:                           "source-ns/source-cluster/session_5000",
		HSSourceObjectCount:              143,
		HSSourceTotalBytes:               2_500_000,
		HSSourceTaskLogMetadataAlgorithm: taskLogMetadataAlgorithm,
		HSSourceTaskLogMetadataSHA256:    strings.Repeat("a", 64),
		HSSourceTaskLogMetadataAttempts:  5_000,
		HSSourceTaskLogMetadataNil:       30,
		HSSourceTaskLogMetadataPresent:   4_970,
		HSSourceTaskLogMetadataStructurallyInvalid:       0,
		HSSourceTaskLogMetadataIncompleteNonNil:          4_970,
		HSSourceTaskLogMetadataStdoutExactResolvable:     0,
		HSSourceTaskLogMetadataStderrExactResolvable:     0,
		HSSourceTaskLogMetadataLegacyWholeWorkerFallback: 30,
		HSStrictCold:                  true,
		HSColdSLO:                     120 * time.Second,
		HSEnterTimeout:                12 * time.Minute,
		S3LocalPort:                   19003,
		HSWarmWait:                    0,
		HSSessionSettle:               0,
		WarmIterations:                1,
		HSQueryConcurrency:            1,
		ExecutionIdentityFile:         filepath.Join(t.TempDir(), "execution-namespace.json"),
		HSSourceRayJobOwned:           true,
		HSSourceShutdownAfterJob:      true,
		HSSourceJobTTLSeconds:         30,
		HSSourceRayJobBackoffLimit:    int32Pointer(0),
		HSSourceSubmitterBackoffLimit: int32Pointer(0),
	}
	if err := validateHSFormalConfig(valid); err != nil {
		t.Fatalf("valid formal config rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*benchConfig)
	}{
		{name: "malformed source", mutate: func(cfg *benchConfig) { cfg.HSOnly = "other/cluster" }},
		{name: "multiple sources", mutate: func(cfg *benchConfig) { cfg.HSOnly += ",other/cluster/session" }},
		{name: "dot namespace", mutate: func(cfg *benchConfig) { cfg.HSOnly = "./source-cluster/session_5000" }},
		{name: "parent namespace", mutate: func(cfg *benchConfig) { cfg.HSOnly = "../source-cluster/session_5000" }},
		{name: "dot cluster", mutate: func(cfg *benchConfig) { cfg.HSOnly = "source-ns/./session_5000" }},
		{name: "parent cluster", mutate: func(cfg *benchConfig) { cfg.HSOnly = "source-ns/../session_5000" }},
		{name: "parent session", mutate: func(cfg *benchConfig) { cfg.HSOnly = "source-ns/source-cluster/.." }},
		{name: "non-Ray session", mutate: func(cfg *benchConfig) { cfg.HSOnly = "source-ns/source-cluster/not-a-session" }},
		{name: "overlong session", mutate: func(cfg *benchConfig) { cfg.HSOnly = "source-ns/source-cluster/session_" + strings.Repeat("a", 248) }},
		{name: "non-positive task count", mutate: func(cfg *benchConfig) { cfg.TaskCount = 0 }},
		{name: "missing source objects", mutate: func(cfg *benchConfig) { cfg.HSSourceObjectCount = 0 }},
		{name: "missing source bytes", mutate: func(cfg *benchConfig) { cfg.HSSourceTotalBytes = 0 }},
		{name: "missing metadata hash", mutate: func(cfg *benchConfig) { cfg.HSSourceTaskLogMetadataSHA256 = "" }},
		{name: "metadata attempts differ", mutate: func(cfg *benchConfig) { cfg.HSSourceTaskLogMetadataAttempts-- }},
		{name: "metadata structurally invalid", mutate: func(cfg *benchConfig) { cfg.HSSourceTaskLogMetadataStructurallyInvalid = 1 }},
		{name: "asymmetric CPU", mutate: func(cfg *benchConfig) { cfg.HSCPULimit = "2" }},
		{name: "zero memory", mutate: func(cfg *benchConfig) { cfg.HSMemoryRequest, cfg.HSMemoryLimit = "0", "0" }},
		{name: "wrong server args", mutate: func(cfg *benchConfig) { cfg.HSArgs = "--session-cache-size=1" }},
		{name: "wrong cache byte budget", mutate: func(cfg *benchConfig) {
			cfg.HSArgs = strings.Replace(cfg.HSArgs, "2147483648", "0", 1)
		}},
		{name: "wrong cache TTL", mutate: func(cfg *benchConfig) {
			cfg.HSArgs = strings.Replace(cfg.HSArgs, "--session-cache-ttl=0s", "--session-cache-ttl=1m", 1)
		}},
		{name: "client timeout not 12m", mutate: func(cfg *benchConfig) { cfg.HSEnterTimeout = 130 * time.Second }},
		{name: "wrong S3 port", mutate: func(cfg *benchConfig) { cfg.S3LocalPort = 9002 }},
		{name: "missing namespace identity", mutate: func(cfg *benchConfig) { cfg.ExecutionIdentityFile = "" }},
		{name: "unowned source", mutate: func(cfg *benchConfig) { cfg.HSSourceRayJobOwned = false }},
		{name: "source shutdown disabled", mutate: func(cfg *benchConfig) { cfg.HSSourceShutdownAfterJob = false }},
		{name: "wrong source TTL", mutate: func(cfg *benchConfig) { cfg.HSSourceJobTTLSeconds = 0 }},
		{name: "missing source RayJob backoff", mutate: func(cfg *benchConfig) { cfg.HSSourceRayJobBackoffLimit = nil }},
		{name: "source RayJob retries", mutate: func(cfg *benchConfig) { cfg.HSSourceRayJobBackoffLimit = int32Pointer(1) }},
		{name: "missing source submitter backoff", mutate: func(cfg *benchConfig) { cfg.HSSourceSubmitterBackoffLimit = nil }},
		{name: "source submitter retries", mutate: func(cfg *benchConfig) { cfg.HSSourceSubmitterBackoffLimit = int32Pointer(2) }},
		{name: "wrong S3 bucket", mutate: func(cfg *benchConfig) { cfg.S3Bucket = S3BucketName }},
		{name: "warm retry budget", mutate: func(cfg *benchConfig) { cfg.HSWarmWait = time.Second }},
		{name: "hidden enter probe settle", mutate: func(cfg *benchConfig) { cfg.HSSessionSettle = time.Second }},
		{name: "warm iterations", mutate: func(cfg *benchConfig) { cfg.WarmIterations = 2 }},
		{name: "query concurrency", mutate: func(cfg *benchConfig) { cfg.HSQueryConcurrency = 2 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.mutate(&cfg)
			if err := validateHSFormalConfig(cfg); err == nil {
				t.Fatal("invalid formal config was accepted")
			}
		})
	}
}

func TestExecuteFormalTaskQueryDecodesSnakeCaseTaskLogMetadata(t *testing.T) {
	const expectedTasks = 50_000
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != EndpointTasks || r.URL.Query().Get("filter_keys") != "task_name" ||
			r.URL.Query().Get("filter_predicates") != "=" ||
			r.URL.Query().Get("filter_values") != benchTaskName {
			http.Error(w, "unexpected query", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("limit") == "0" {
			_, _ = io.WriteString(w, `{"result":true,"data":{"result":{"result":[],"num_filtered":50000}}}`)
			return
		}
		_, _ = io.WriteString(w, `{"result":true,"data":{"result":{"num_filtered":50000,"result":[`+
			`{"task_id":"a","attempt_number":0,"name":"bench_task","state":"FINISHED","node_id":"n","worker_id":"w",`+
			`"task_log_info":{"stdout_file":"","stderr_file":"","stdout_start":0,"stdout_end":3,"stderr_start":0,"stderr_end":4}},`+
			`{"task_id":"b","attempt_number":0,"name":"bench_task","state":"FINISHED","node_id":"n","worker_id":"w",`+
			`"task_log_info":{"stdout_file":"worker.out","stderr_file":"worker.err","stdout_start":9007199254740993,"stdout_end":9007199254740999,"stderr_start":9007199254741001,"stderr_end":9007199254741007}}]}}}`)
	}))
	defer server.Close()

	count := executeFormalTaskQuery(server.Client(), server.URL, 0, expectedTasks, 0, 1, false)
	if !count.Valid || count.HTTPStatus != http.StatusOK || count.NumFiltered != expectedTasks {
		t.Fatalf("count query rejected: %#v", count)
	}
	warm := executeFormalTaskQuery(server.Client(), server.URL, 2, expectedTasks, 2, 1, true)
	if !warm.Valid || warm.Rows != 2 || warm.TaskLogMetadata.Counts.Present != 2 ||
		warm.TaskLogMetadata.Counts.IncompleteNonNil != 1 ||
		warm.TaskLogMetadata.Counts.StdoutExactResolvable != 1 ||
		warm.TaskLogMetadata.Counts.StderrExactResolvable != 1 {
		t.Fatalf("detailed warm query rejected: %#v", warm)
	}
	if len(warm.records) != 2 || warm.records[0].TaskLogInfo.StdoutEnd != 3 ||
		warm.records[1].TaskLogInfo.StdoutFile != "worker.out" ||
		warm.records[1].TaskLogInfo.StdoutStart != 9007199254740993 ||
		warm.records[1].TaskLogInfo.StdoutEnd != 9007199254740999 ||
		warm.records[1].TaskLogInfo.StderrStart != 9007199254741001 ||
		warm.records[1].TaskLogInfo.StderrEnd != 9007199254741007 {
		t.Fatalf("snake_case TaskLogInfo fields were not decoded exactly: %#v", warm.records)
	}
	projection := map[taskAttemptKey]taskLogMetadataRecord{}
	for _, record := range warm.records {
		projection[taskAttemptKey{TaskID: record.TaskID, Attempt: record.Attempt}] = record
	}
	validateWarmTaskProjection(&warm, projection)
	if !warm.Valid || !warm.ProjectionMatches || warm.ExpectedProjectionSHA256 != warm.TaskLogMetadata.SHA256 {
		t.Fatalf("matching production projection rejected: %#v", warm)
	}
}

func TestRunHSBenchStrictColdDoesNotPrewarmOrRetry(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if strings.HasPrefix(r.URL.Path, "/enter_cluster/") {
			http.Error(w, "synthetic cold failure", http.StatusInternalServerError)
			return
		}
		t.Fatalf("strict cold sent unexpected prewarm/query request: %s", r.URL.String())
	}))
	defer server.Close()

	validation := HSValidation{}
	var phases []string
	result := runHSBench(t, gomega.NewWithT(t), server.URL, "ns", "cluster", "session", benchConfig{
		TaskCount:          5_000,
		HSStrictCold:       true,
		HSEnterTimeout:     time.Second,
		HSQueryConcurrency: 1,
	}, &validation, func(phase string) { phases = append(phases, phase) }, nil)
	if requests != 1 || result.EnterAttempts != 1 || result.EnterMeasured ||
		result.EnterStatus != http.StatusInternalServerError {
		t.Fatalf("strict cold retry/prewarm contract violated: requests=%d result=%#v", requests, result)
	}
	if !reflect.DeepEqual(phases, []string{"cold-load"}) {
		t.Fatalf("failed strict cold emitted phases %v, want only cold-load", phases)
	}
}

func TestRunHSBenchStrictColdPhaseMarksDoNotAddRequests(t *testing.T) {
	requests := make([]string, 0, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.String())
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/enter_cluster/") {
			_, _ = io.WriteString(w, `{}`)
			return
		}
		if r.URL.Path != EndpointTasks {
			http.Error(w, "unexpected endpoint", http.StatusBadRequest)
			return
		}
		if r.URL.Query().Get("limit") == "0" {
			_, _ = io.WriteString(w, `{"result":true,"data":{"result":{"result":[],"num_filtered":1}}}`)
			return
		}
		_, _ = io.WriteString(w, `{"result":true,"data":{"result":{"num_filtered":1,"result":[{"task_id":"a","attempt_number":0,"name":"bench_task","state":"FINISHED","node_id":"n","worker_id":"w","task_log_info":null}]}}}`)
	}))
	defer server.Close()

	validation := HSValidation{}
	var phases []string
	result := runHSBench(t, gomega.NewWithT(t), server.URL, "ns", "cluster", "session", benchConfig{
		TaskCount:          1,
		HSStrictCold:       true,
		HSEnterTimeout:     time.Second,
		HSQueryConcurrency: 1,
	}, &validation, func(phase string) { phases = append(phases, phase) }, nil)
	if !result.EnterMeasured || result.EnterAttempts != 1 {
		t.Fatalf("strict cold result invalid: %#v", result)
	}
	if len(requests) != 3 {
		t.Fatalf("phase recording changed request count: %d requests=%v", len(requests), requests)
	}
	if !reflect.DeepEqual(phases, []string{"cold-load", "endpoint-test", "after-endpoint-test"}) {
		t.Fatalf("strict cold phases=%v", phases)
	}
}
