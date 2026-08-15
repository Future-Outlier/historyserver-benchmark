package benchmark

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"

	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	historyserverpkg "github.com/ray-project/kuberay/historyserver/pkg/historyserver"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	storageS3 "github.com/ray-project/kuberay/historyserver/pkg/storage/s3"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	. "github.com/ray-project/kuberay/historyserver/test/support"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

const (
	// The name is a stable artifact contract. The implementation hashes a
	// canonical JSON array sorted by key; each record contains key, listed size,
	// ETag, and a SHA-256 of the bytes returned by GetObject.
	sourceFingerprintAlgorithm = "s3-key-size-etag-content-sha256-v1"
)

type SourceSessionFingerprint struct {
	Algorithm   string `json:"algorithm"`
	Bucket      string `json:"bucket"`
	Start       string `json:"start"`
	End         string `json:"end"`
	ObjectCount int    `json:"objectCount"`
	TotalBytes  int64  `json:"totalBytes"`
}

type HSPodEvidence struct {
	ExecutionNamespace    string   `json:"executionNamespace"`
	PodName               string   `json:"podName"`
	PodUID                string   `json:"podUID"`
	ContainerName         string   `json:"containerName"`
	ContainerID           string   `json:"containerID"`
	Image                 string   `json:"image"`
	ImageID               string   `json:"imageID"`
	Ready                 bool     `json:"ready"`
	Running               bool     `json:"running"`
	RestartCount          int32    `json:"restartCount"`
	CPURequest            string   `json:"cpuRequest"`
	CPULimit              string   `json:"cpuLimit"`
	MemoryRequest         string   `json:"memoryRequest"`
	MemoryLimit           string   `json:"memoryLimit"`
	OOMKilled             bool     `json:"oomKilled"`
	TerminationReasons    []string `json:"terminationReasons"`
	CgroupObserved        bool     `json:"cgroupObserved"`
	CgroupMemoryMax       string   `json:"cgroupMemoryMax"`
	CgroupMemoryMaxBytes  int64    `json:"cgroupMemoryMaxBytes"`
	MemoryEventsOOM       int64    `json:"memoryEventsOOM"`
	MemoryEventsOOMKill   int64    `json:"memoryEventsOOMKill"`
	CgroupReadErrors      int      `json:"cgroupReadErrors"`
	CgroupReadErrorFields []string `json:"cgroupReadErrorFields"`
	Valid                 bool     `json:"valid"`
	Problems              []string `json:"problems"`
}

type HSTaskQueryValidation struct {
	Endpoint                 string                 `json:"endpoint"`
	Concurrency              int                    `json:"concurrency"`
	Limit                    int                    `json:"limit"`
	HTTPStatus               int                    `json:"httpStatus"`
	Latency                  time.Duration          `json:"latency"`
	ResponseResult           bool                   `json:"responseResult"`
	Rows                     int                    `json:"rows"`
	NumFiltered              int                    `json:"numFiltered"`
	DistinctTaskIDs          int                    `json:"distinctTaskIDs"`
	AttemptZero              int                    `json:"attemptZero"`
	Finished                 int                    `json:"finished"`
	TaskLogMetadata          TaskLogMetadataSummary `json:"taskLogMetadata"`
	ExpectedProjectionSHA256 string                 `json:"expectedProjectionSHA256"`
	ProjectionMatches        bool                   `json:"projectionMatches"`
	Valid                    bool                   `json:"valid"`
	Problems                 []string               `json:"problems"`
	records                  []taskLogMetadataRecord
}

type HSReplayValidation struct {
	Status           string                 `json:"status"`
	ExpectedAttempts int                    `json:"expectedAttempts"`
	ObservedAttempts int                    `json:"observedAttempts"`
	DistinctTaskIDs  int                    `json:"distinctTaskIDs"`
	AttemptZero      int                    `json:"attemptZero"`
	Finished         int                    `json:"finished"`
	TaskLogMetadata  TaskLogMetadataSummary `json:"taskLogMetadata"`
	ErrorCounts      map[string]int         `json:"errorCounts"`
	TotalErrors      int                    `json:"totalErrors"`
	Valid            bool                   `json:"valid"`
	Problems         []string               `json:"problems"`
}

type HSLogValidation struct {
	ErrorCounts map[string]int `json:"errorCounts"`
	TotalErrors int            `json:"totalErrors"`
	Valid       bool           `json:"valid"`
	Problems    []string       `json:"problems"`
}

type HSValidation struct {
	ExpectedTaskAttempts    int                   `json:"expectedTaskAttempts"`
	TaskCountQuery          HSTaskQueryValidation `json:"taskCountQuery"`
	WarmTaskQuery           HSTaskQueryValidation `json:"warmTaskQuery"`
	FullReplay              HSReplayValidation    `json:"fullReplay"`
	Logs                    HSLogValidation       `json:"logs"`
	LifetimeMemoryPeakBytes int64                 `json:"lifetimeMemoryPeakBytes"`
	MeasurementValid        bool                  `json:"measurementValid"`
	MeetsColdSLO            bool                  `json:"meetsColdSLO"`
	Valid                   bool                  `json:"valid"`
	Problems                []string              `json:"problems"`
	Scope                   HSBenchmarkScope      `json:"scope"`
}

type HSBenchmarkScope struct {
	Replay   bool `json:"replay"`
	TaskList bool `json:"taskList"`
	LogsFile bool `json:"logsFile"`
}

type sourceFingerprintRecord struct {
	Key           string `json:"key"`
	Size          int64  `json:"size"`
	ETag          string `json:"etag"`
	ContentSHA256 string `json:"contentSha256"`
}

func fingerprintRecords(records []sourceFingerprintRecord) (string, error) {
	if len(records) == 0 {
		return "", fmt.Errorf("source session contains no objects")
	}
	sorted := append([]sourceFingerprintRecord(nil), records...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })
	for i := 1; i < len(sorted); i++ {
		if sorted[i-1].Key == sorted[i].Key {
			return "", fmt.Errorf("duplicate source object key %q", sorted[i].Key)
		}
	}
	raw, err := json.Marshal(sorted)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// takeSourceSessionFingerprint is deliberately read-only. It binds object
// identity (key, size, ETag) and the actual bytes so a same-size rewrite cannot
// silently change the workload between formal arms.
func takeSourceSessionFingerprint(
	s3Client *awss3.S3,
	bucket, namespace, clusterName, sessionID string,
) (digest string, objectCount int, totalBytes int64, err error) {
	prefix := clusterlogs.SessionDir("log", "", "", namespace, clusterName, sessionID) + "/"
	var objects []*awss3.Object
	err = s3Client.ListObjectsV2Pages(&awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}, func(page *awss3.ListObjectsV2Output, _ bool) bool {
		objects = append(objects, page.Contents...)
		return true
	})
	if err != nil {
		return "", 0, 0, fmt.Errorf("list source session %s: %w", prefix, err)
	}

	records := make([]sourceFingerprintRecord, 0, len(objects))
	for _, object := range objects {
		key := aws.StringValue(object.Key)
		result, getErr := s3Client.GetObject(&awss3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if getErr != nil {
			return "", 0, 0, fmt.Errorf("read source object %s: %w", key, getErr)
		}
		h := sha256.New()
		readBytes, copyErr := io.Copy(h, result.Body)
		closeErr := result.Body.Close()
		if copyErr != nil {
			return "", 0, 0, fmt.Errorf("hash source object %s: %w", key, copyErr)
		}
		if closeErr != nil {
			return "", 0, 0, fmt.Errorf("close source object %s: %w", key, closeErr)
		}
		expectedSize := aws.Int64Value(object.Size)
		if readBytes != expectedSize {
			return "", 0, 0, fmt.Errorf("source object %s read %d bytes, listed size is %d", key, readBytes, expectedSize)
		}
		records = append(records, sourceFingerprintRecord{
			Key:           key,
			Size:          expectedSize,
			ETag:          aws.StringValue(object.ETag),
			ContentSHA256: hex.EncodeToString(h.Sum(nil)),
		})
		totalBytes += expectedSize
	}
	digest, err = fingerprintRecords(records)
	return digest, len(records), totalBytes, err
}

func existingBenchS3ReadClient(t *testing.T, localPort int, bucket string) *awss3.S3 {
	t.Helper()
	if bucket != benchmarkS3BucketName {
		t.Fatalf("read-only replay bucket=%q, want %q", bucket, benchmarkS3BucketName)
	}
	stop := spawnMinioForward(t, localPort)
	t.Cleanup(stop)
	endpoint := fmt.Sprintf("http://localhost:%d", localPort)
	client, err := NewS3Client(endpoint)
	if err != nil {
		t.Fatalf("create read-only benchmark S3 client: %v", err)
	}
	deadline := time.Now().Add(TestTimeoutMedium)
	for {
		_, err = client.HeadBucket(&awss3.HeadBucketInput{Bucket: aws.String(bucket)})
		if err == nil {
			return client
		}
		if time.Now().After(deadline) {
			t.Fatalf("existing MinIO bucket %s is not readable through %s: %v", bucket, endpoint, err)
		}
		time.Sleep(time.Second)
	}
}

type taskAPIRecord struct {
	TaskID        string                `json:"task_id"`
	AttemptNumber int                   `json:"attempt_number"`
	Name          string                `json:"name"`
	State         string                `json:"state"`
	NodeID        string                `json:"node_id"`
	WorkerID      string                `json:"worker_id"`
	TaskLogInfo   *taskLogInfoAPIRecord `json:"task_log_info"`
}

type taskLogInfoAPIRecord struct {
	StdoutFile  string `json:"stdout_file"`
	StderrFile  string `json:"stderr_file"`
	StdoutStart int64  `json:"stdout_start"`
	StdoutEnd   int64  `json:"stdout_end"`
	StderrStart int64  `json:"stderr_start"`
	StderrEnd   int64  `json:"stderr_end"`
}

func (record *taskLogInfoAPIRecord) canonical() *eventtypes.TaskLogInfo {
	if record == nil {
		return nil
	}
	return &eventtypes.TaskLogInfo{
		StdoutFile:  record.StdoutFile,
		StderrFile:  record.StderrFile,
		StdoutStart: record.StdoutStart,
		StdoutEnd:   record.StdoutEnd,
		StderrStart: record.StderrStart,
		StderrEnd:   record.StderrEnd,
	}
}

type taskAPIResponse struct {
	Result bool `json:"result"`
	Data   struct {
		Result struct {
			Result      []taskAPIRecord `json:"result"`
			NumFiltered int             `json:"num_filtered"`
		} `json:"result"`
	} `json:"data"`
}

func formalTaskEndpoint(limit int, detail bool) string {
	values := url.Values{}
	values.Set("limit", fmt.Sprintf("%d", limit))
	values.Set("detail", fmt.Sprintf("%t", detail))
	values.Add("filter_keys", "task_name")
	values.Add("filter_predicates", "=")
	values.Add("filter_values", benchTaskName)
	return EndpointTasks + "?" + values.Encode()
}

func executeFormalTaskQuery(
	client *http.Client,
	hsURL string,
	limit, expectedTasks, expectedRows, concurrency int,
	detail bool,
) HSTaskQueryValidation {
	return executeFormalTaskQueryObserved(client, hsURL, limit, expectedTasks, expectedRows, concurrency, detail, nil)
}

func executeFormalTaskQueryObserved(
	client *http.Client,
	hsURL string,
	limit, expectedTasks, expectedRows, concurrency int,
	detail bool,
	observation *HSHTTPRequestObservation,
) HSTaskQueryValidation {
	result := HSTaskQueryValidation{
		Endpoint:    formalTaskEndpoint(limit, detail),
		Concurrency: concurrency,
		Limit:       limit,
		Problems:    []string{},
	}
	request, requestErr := http.NewRequest(http.MethodGet, hsURL+result.Endpoint, nil)
	if requestErr != nil {
		result.Problems = append(result.Problems, fmt.Sprintf("build request: %v", requestErr))
		return result
	}
	request.Header.Set("Accept-Encoding", "identity")
	start := time.Now()
	if observation != nil {
		observation.Endpoint = result.Endpoint
		observation.StartedAtNano = start.UnixNano()
		observation.Attempts = 1
		observation.AcceptEncoding = "identity"
	}
	resp, err := client.Do(request)
	result.Latency = time.Since(start)
	if err != nil {
		if observation != nil {
			observation.CompletedAtNano = time.Now().UnixNano()
			observation.Problems = append(observation.Problems, fmt.Sprintf("request failed: %v", err))
		}
		result.Problems = append(result.Problems, fmt.Sprintf("request failed: %v", err))
		return result
	}
	defer resp.Body.Close()
	result.HTTPStatus = resp.StatusCode
	raw, readErr := io.ReadAll(resp.Body)
	result.Latency = time.Since(start)
	if observation != nil {
		observation.CompletedAtNano = time.Now().UnixNano()
		observation.Status = resp.StatusCode
		observation.ResponseBytes = int64(len(raw))
		observation.ResponseContentEncoding = resp.Header.Get("Content-Encoding")
		if encoding := observation.ResponseContentEncoding; encoding != "" && encoding != "identity" {
			observation.Problems = append(observation.Problems, "response Content-Encoding is "+encoding)
		}
	}
	if readErr != nil {
		if observation != nil {
			observation.Problems = append(observation.Problems, fmt.Sprintf("read response: %v", readErr))
		}
		result.Problems = append(result.Problems, fmt.Sprintf("read response: %v", readErr))
		return result
	}
	if resp.StatusCode != http.StatusOK {
		if observation != nil {
			observation.Problems = append(observation.Problems, fmt.Sprintf("HTTP status=%d, want 200", resp.StatusCode))
		}
		result.Problems = append(result.Problems, fmt.Sprintf("HTTP status=%d, want 200", resp.StatusCode))
		return result
	}

	var decoded taskAPIResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		if observation != nil {
			observation.Problems = append(observation.Problems, fmt.Sprintf("decode response: %v", err))
		}
		result.Problems = append(result.Problems, fmt.Sprintf("decode response: %v", err))
		return result
	}
	result.ResponseResult = decoded.Result
	result.Rows = len(decoded.Data.Result.Result)
	result.NumFiltered = decoded.Data.Result.NumFiltered
	if !result.ResponseResult {
		result.Problems = append(result.Problems, "response result=false")
	}
	if result.NumFiltered != expectedTasks {
		result.Problems = append(result.Problems,
			fmt.Sprintf("num_filtered=%d, want %d", result.NumFiltered, expectedTasks))
	}
	if result.Rows != expectedRows {
		result.Problems = append(result.Problems,
			fmt.Sprintf("rows=%d, want %d", result.Rows, expectedRows))
	}
	if concurrency != 1 {
		result.Problems = append(result.Problems, fmt.Sprintf("concurrency=%d, formal query requires 1", concurrency))
	}

	seen := map[string]struct{}{}
	for _, task := range decoded.Data.Result.Result {
		if task.TaskID != "" {
			seen[task.TaskID] = struct{}{}
		}
		if task.AttemptNumber == 0 {
			result.AttemptZero++
		}
		if task.State == string(eventtypes.FINISHED) {
			result.Finished++
		}
		if detail {
			result.records = append(result.records, canonicalTaskLogMetadataRecord(
				task.TaskID, task.AttemptNumber, task.NodeID, task.WorkerID, task.TaskLogInfo.canonical(),
			))
		}
	}
	result.DistinctTaskIDs = len(seen)
	if detail && expectedRows > 0 {
		for label, got := range map[string]int{
			"distinct task IDs": result.DistinctTaskIDs,
			"attempt zero":      result.AttemptZero,
			"FINISHED":          result.Finished,
		} {
			if got != expectedRows {
				result.Problems = append(result.Problems,
					fmt.Sprintf("%s=%d, want %d", label, got, expectedRows))
			}
		}
		result.TaskLogMetadata = summarizeTaskLogMetadata(result.records)
		if err := validateTaskLogMetadataSummary(result.TaskLogMetadata, expectedRows); err != nil {
			result.Problems = append(result.Problems, fmt.Sprintf("task-log metadata: %v", err))
		}
	}
	result.Valid = len(result.Problems) == 0
	if observation != nil {
		observation.Valid = len(observation.Problems) == 0 && result.Valid && observation.Status == http.StatusOK && observation.ResponseBytes > 0
	}
	return result
}

func validateWarmTaskProjection(
	query *HSTaskQueryValidation,
	full map[taskAttemptKey]taskLogMetadataRecord,
) {
	if query == nil {
		return
	}
	expectedRecords := make([]taskLogMetadataRecord, 0, len(query.records))
	seen := map[taskAttemptKey]struct{}{}
	projectionMatches := true
	for _, actual := range query.records {
		key := taskAttemptKey{TaskID: actual.TaskID, Attempt: actual.Attempt}
		if _, duplicate := seen[key]; duplicate {
			query.Problems = append(query.Problems,
				fmt.Sprintf("warm projection contains duplicate task attempt %s/%d", key.TaskID, key.Attempt))
			projectionMatches = false
			continue
		}
		seen[key] = struct{}{}
		expected, exists := full[key]
		if !exists {
			query.Problems = append(query.Problems,
				fmt.Sprintf("warm task attempt %s/%d is absent from production replay", key.TaskID, key.Attempt))
			projectionMatches = false
			continue
		}
		expectedRecords = append(expectedRecords, expected)
		if !taskLogMetadataRecordsEqual(actual, expected) {
			query.Problems = append(query.Problems,
				fmt.Sprintf("warm task attempt %s/%d differs from production replay", key.TaskID, key.Attempt))
			projectionMatches = false
		}
	}
	expectedSummary := summarizeTaskLogMetadata(expectedRecords)
	query.ExpectedProjectionSHA256 = expectedSummary.SHA256
	if !taskLogMetadataSummariesEqual(query.TaskLogMetadata, expectedSummary) {
		query.Problems = append(query.Problems, "warm task-log metadata summary differs from production replay projection")
		projectionMatches = false
	}
	query.ProjectionMatches = projectionMatches && len(expectedRecords) == len(query.records)
	query.Valid = len(query.Problems) == 0 && query.ProjectionMatches
}

func taskLogMetadataRecordsEqual(left, right taskLogMetadataRecord) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

var strictReplayErrorPatterns = map[string]string{
	"decompress":             "Failed to decompress event file",
	"getContent":             "Failed to get content for event file",
	"read":                   "Failed to read events for file",
	"decode":                 "Failed to decode events for file",
	"store":                  "Failed to store events for file",
	"taskLifecycleUnmarshal": "failed to unmarshal task lifecycle event",
	"emptyLifecycle":         "TASK_LIFECYCLE_EVENT must have at least one state transition",
	"logEvents":              "Incomplete Log Events read",
	"listObjects":            "Failed to list objects",
	"getObject":              "Failed to get object",
	"readObject":             "Failed to read all data from object",
	// History Server's legacy S3 reader can try CreateBucket after a failed
	// HeadBucket. Formal arms prove that the existing-bucket path was actually
	// used by rejecting every creation-path diagnostic in the pod log.
	"bucketCreateMissing": "does not exist, creating",
	"bucketCreateAttempt": "Attempting to create bucket",
	"bucketCreateSuccess": "Successfully created bucket",
}

type replayErrorHook struct {
	mu     sync.Mutex
	counts map[string]int
}

func newReplayErrorHook() *replayErrorHook {
	return &replayErrorHook{counts: map[string]int{}}
}

func (h *replayErrorHook) Levels() []logrus.Level {
	return []logrus.Level{logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel}
}

func (h *replayErrorHook) Fire(entry *logrus.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for key, pattern := range strictReplayErrorPatterns {
		if strings.Contains(entry.Message, pattern) {
			h.counts[key]++
		}
	}
	return nil
}

func (h *replayErrorHook) snapshot() (map[string]int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	counts := make(map[string]int, len(strictReplayErrorPatterns))
	total := 0
	for key := range strictReplayErrorPatterns {
		counts[key] = h.counts[key]
		total += h.counts[key]
	}
	return counts, total
}

func cloneLogrusHooks(in logrus.LevelHooks) logrus.LevelHooks {
	out := make(logrus.LevelHooks, len(in))
	for level, hooks := range in {
		out[level] = append([]logrus.Hook(nil), hooks...)
	}
	return out
}

func strictReplaySourceSession(
	s3Client *awss3.S3,
	bucket string,
	namespace, clusterName, sessionID string,
	expected int,
	expectedTaskLogMetadata TaskLogMetadataSummary,
) (HSReplayValidation, map[taskAttemptKey]taskLogMetadataRecord) {
	validation := HSReplayValidation{
		ExpectedAttempts: expected,
		ErrorCounts:      map[string]int{},
		Problems:         []string{},
	}
	reader := &storageS3.RayLogsHandler{
		S3Client:   s3Client,
		S3Bucket:   bucket,
		S3RootDir:  "log",
		HttpClient: &http.Client{Timeout: 5 * time.Second},
		LogFiles:   make(chan string, 100),
	}
	scheme := runtime.NewScheme()
	utilruntime.Must(rayv1.AddToScheme(scheme))
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	processor := historyserverpkg.NewSessionProcessor(reader, k8sClient)

	hook := newReplayErrorHook()
	logger := logrus.StandardLogger()
	oldHooks := cloneLogrusHooks(logger.Hooks)
	logger.AddHook(hook)
	defer logger.ReplaceHooks(oldHooks)

	status, snapshot, err := processor.ProcessSession(context.Background(), utils.ClusterInfo{
		Namespace:   namespace,
		Name:        clusterName,
		SessionName: sessionID,
	})
	validation.Status = sessionStatusName(status)
	validation.ErrorCounts, validation.TotalErrors = hook.snapshot()
	if err != nil {
		validation.Problems = append(validation.Problems, fmt.Sprintf("ProcessSession: %v", err))
	}
	if status != historyserverpkg.SessionStatusProcessed {
		validation.Problems = append(validation.Problems,
			fmt.Sprintf("status=%s, want processed", validation.Status))
	}
	if snapshot == nil {
		validation.Problems = append(validation.Problems, "ProcessSession returned a nil snapshot")
		return validation, nil
	}

	seenAttempts := map[string]struct{}{}
	seenTaskIDs := map[string]struct{}{}
	records := make([]taskLogMetadataRecord, 0, expected)
	projection := make(map[taskAttemptKey]taskLogMetadataRecord, expected)
	for _, task := range snapshot.Tasks {
		if !isBenchTaskName(task.GetTaskName()) {
			continue
		}
		attemptKey := fmt.Sprintf("%s/%d", task.TaskID, task.TaskAttempt)
		seenAttempts[attemptKey] = struct{}{}
		seenTaskIDs[task.TaskID] = struct{}{}
		if task.TaskAttempt == 0 {
			validation.AttemptZero++
		}
		if task.State == eventtypes.FINISHED {
			validation.Finished++
		}
		key := taskAttemptKey{TaskID: task.TaskID, Attempt: task.TaskAttempt}
		record := canonicalTaskLogMetadataRecord(
			task.TaskID, task.TaskAttempt, task.NodeID, task.WorkerID, task.TaskLogInfo,
		)
		if _, duplicate := projection[key]; duplicate {
			validation.Problems = append(validation.Problems,
				fmt.Sprintf("production replay contains duplicate task attempt %s/%d", key.TaskID, key.Attempt))
		} else {
			projection[key] = record
		}
		records = append(records, record)
	}
	validation.ObservedAttempts = len(seenAttempts)
	validation.DistinctTaskIDs = len(seenTaskIDs)
	for label, got := range map[string]int{
		"observed attempts": validation.ObservedAttempts,
		"distinct task IDs": validation.DistinctTaskIDs,
		"attempt zero":      validation.AttemptZero,
		"FINISHED":          validation.Finished,
	} {
		if got != expected {
			validation.Problems = append(validation.Problems,
				fmt.Sprintf("%s=%d, want %d", label, got, expected))
		}
	}
	validation.TaskLogMetadata = summarizeTaskLogMetadata(records)
	if err := validateTaskLogMetadataSummary(validation.TaskLogMetadata, expected); err != nil {
		validation.Problems = append(validation.Problems, fmt.Sprintf("task-log metadata: %v", err))
	}
	if !taskLogMetadataSummariesEqual(validation.TaskLogMetadata, expectedTaskLogMetadata) {
		validation.Problems = append(validation.Problems,
			"production replay task-log metadata differs from immutable raw source")
	}
	if validation.TotalErrors != 0 {
		validation.Problems = append(validation.Problems,
			fmt.Sprintf("production replay logged %d decode/read/store errors", validation.TotalErrors))
	}
	validation.Valid = len(validation.Problems) == 0
	return validation, projection
}

func sessionStatusName(status historyserverpkg.SessionStatus) string {
	switch status {
	case historyserverpkg.SessionStatusProcessed:
		return "processed"
	case historyserverpkg.SessionStatusLive:
		return "live"
	case historyserverpkg.SessionStatusClusterStateUnknown:
		return "cluster-state-unknown"
	case historyserverpkg.SessionStatusEventsErr:
		return "events-error"
	case historyserverpkg.SessionStatusCanceled:
		return "canceled"
	default:
		return "unknown"
	}
}

func validateHSLogs(raw []byte) HSLogValidation {
	validation := HSLogValidation{
		ErrorCounts: map[string]int{},
		Problems:    []string{},
	}
	text := string(raw)
	for key, pattern := range strictReplayErrorPatterns {
		count := strings.Count(text, pattern)
		validation.ErrorCounts[key] = count
		validation.TotalErrors += count
	}
	if validation.TotalErrors != 0 {
		validation.Problems = append(validation.Problems,
			fmt.Sprintf("History Server logged %d decode/read/store errors", validation.TotalErrors))
	}
	validation.Valid = len(validation.Problems) == 0
	return validation
}

func captureHSPodEvidence(test Test, namespace string) HSPodEvidence {
	evidence := HSPodEvidence{
		ExecutionNamespace: namespace,
		ContainerName:      "historyserver",
		Problems:           []string{},
	}
	pods, err := test.Client().Core().CoreV1().Pods(namespace).List(test.Ctx(), metav1.ListOptions{
		LabelSelector: "app=historyserver",
	})
	if err != nil {
		evidence.Problems = append(evidence.Problems, fmt.Sprintf("list pod: %v", err))
		return evidence
	}
	if len(pods.Items) != 1 {
		evidence.Problems = append(evidence.Problems,
			fmt.Sprintf("found %d History Server pods, want exactly 1", len(pods.Items)))
		return evidence
	}
	pod := &pods.Items[0]
	evidence.PodName = pod.Name
	evidence.PodUID = string(pod.UID)

	var container *corev1.Container
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == evidence.ContainerName {
			container = &pod.Spec.Containers[i]
			break
		}
	}
	if container == nil {
		evidence.Problems = append(evidence.Problems, "historyserver container spec is missing")
		return evidence
	}
	evidence.Image = container.Image
	evidence.CPURequest = container.Resources.Requests.Cpu().String()
	evidence.CPULimit = container.Resources.Limits.Cpu().String()
	evidence.MemoryRequest = container.Resources.Requests.Memory().String()
	evidence.MemoryLimit = container.Resources.Limits.Memory().String()

	status, err := GetContainerStatusByName(pod, evidence.ContainerName)
	if err != nil {
		evidence.Problems = append(evidence.Problems, err.Error())
		return evidence
	}
	evidence.ContainerID = bareContainerID(status.ContainerID)
	evidence.ImageID = status.ImageID
	evidence.Ready = status.Ready
	evidence.Running = status.State.Running != nil
	evidence.RestartCount = status.RestartCount
	for _, terminated := range []*corev1.ContainerStateTerminated{
		status.State.Terminated,
		status.LastTerminationState.Terminated,
	} {
		if terminated == nil {
			continue
		}
		evidence.TerminationReasons = append(evidence.TerminationReasons, terminated.Reason)
		if terminated.Reason == "OOMKilled" {
			evidence.OOMKilled = true
		}
	}
	return evidence
}

func quantityEqual(actual, expected string) bool {
	actualQuantity, actualErr := resource.ParseQuantity(actual)
	expectedQuantity, expectedErr := resource.ParseQuantity(expected)
	return actualErr == nil && expectedErr == nil && actualQuantity.Cmp(expectedQuantity) == 0
}

func finalizeHSPodEvidence(evidence *HSPodEvidence, cfg benchConfig, memory cgroupMemoryEvidence) {
	if evidence.Problems == nil {
		evidence.Problems = []string{}
	}
	evidence.CgroupObserved = memory.Observed
	evidence.CgroupMemoryMax = memory.MemoryMax
	evidence.CgroupMemoryMaxBytes = memory.MemoryMaxBytes
	evidence.MemoryEventsOOM = memory.OOM
	evidence.MemoryEventsOOMKill = memory.OOMKill
	evidence.CgroupReadErrors = memory.ReadErrors
	evidence.CgroupReadErrorFields = memory.ReadErrorFields

	for label, values := range map[string][2]string{
		"CPU request":    {evidence.CPURequest, cfg.HSCPURequest},
		"CPU limit":      {evidence.CPULimit, cfg.HSCPULimit},
		"memory request": {evidence.MemoryRequest, cfg.HSMemoryRequest},
		"memory limit":   {evidence.MemoryLimit, cfg.HSMemoryLimit},
	} {
		actual, expected := values[0], values[1]
		if expected == "" || !quantityEqual(actual, expected) {
			evidence.Problems = append(evidence.Problems,
				fmt.Sprintf("%s=%q, want %q", label, actual, expected))
		}
	}
	if evidence.PodUID == "" || evidence.ContainerID == "" || evidence.ImageID == "" {
		evidence.Problems = append(evidence.Problems, "pod UID, container ID, and image ID must be present")
	}
	if !evidence.Ready || !evidence.Running {
		evidence.Problems = append(evidence.Problems, "History Server container is not running and ready")
	}
	if evidence.RestartCount != 0 {
		evidence.Problems = append(evidence.Problems,
			fmt.Sprintf("restartCount=%d, want 0", evidence.RestartCount))
	}
	if evidence.OOMKilled {
		evidence.Problems = append(evidence.Problems, "container was OOMKilled")
	}
	if !evidence.CgroupObserved {
		evidence.Problems = append(evidence.Problems, "no cgroup memory.max/memory.events sample")
	}
	if evidence.CgroupReadErrors != 0 {
		evidence.Problems = append(evidence.Problems,
			fmt.Sprintf("cgroup read errors=%d", evidence.CgroupReadErrors))
	}
	if evidence.MemoryEventsOOM != 0 || evidence.MemoryEventsOOMKill != 0 {
		evidence.Problems = append(evidence.Problems,
			fmt.Sprintf("memory.events oom=%d oom_kill=%d", evidence.MemoryEventsOOM, evidence.MemoryEventsOOMKill))
	}
	limit, err := resource.ParseQuantity(cfg.HSMemoryLimit)
	if err != nil || evidence.CgroupMemoryMaxBytes != limit.Value() {
		evidence.Problems = append(evidence.Problems,
			fmt.Sprintf("cgroup memory.max=%q (%d), want %q", evidence.CgroupMemoryMax,
				evidence.CgroupMemoryMaxBytes, cfg.HSMemoryLimit))
	}
	evidence.Valid = len(evidence.Problems) == 0
}

func finalizeHSValidation(report *Report) {
	if !report.Config.HSStrictCold {
		return
	}
	validation := &report.HSValidation
	if validation.Problems == nil {
		validation.Problems = []string{}
	}
	if validation.Scope != (HSBenchmarkScope{Replay: true, TaskList: true, LogsFile: false}) {
		validation.Problems = append(validation.Problems,
			fmt.Sprintf("benchmark scope=%+v, want replay+task-list only and logsFile=false", validation.Scope))
	}
	if err := validateFormalHSPhaseTimestamps(report.HistoryServerPhases, report.Config.HSProtocol); err != nil {
		validation.Problems = append(validation.Problems,
			fmt.Sprintf("History Server phase timestamps are invalid: %v", err))
	}
	coldSucceeded := report.HistoryServer.EnterMeasured && report.HistoryServer.EnterStatus == http.StatusOK
	validation.MeetsColdSLO = coldSucceeded && report.HistoryServer.EnterColdLatency > 0 &&
		report.HistoryServer.EnterColdLatency <= report.Config.HSColdSLO
	if report.HistoryServer.EnterAttempts != 1 {
		validation.Problems = append(validation.Problems,
			fmt.Sprintf("enter attempts=%d, formal cold mode requires exactly 1", report.HistoryServer.EnterAttempts))
	}
	if report.HistoryServer.EnterColdLatency <= 0 {
		validation.Problems = append(validation.Problems, "cold latency is missing")
	}
	if !coldSucceeded {
		validation.Problems = append(validation.Problems,
			fmt.Sprintf("cold request did not complete with one measured HTTP 200: latency=%s status=%d",
				report.HistoryServer.EnterColdLatency, report.HistoryServer.EnterStatus))
	}
	if coldSucceeded {
		if !validation.TaskCountQuery.Valid {
			validation.Problems = append(validation.Problems, "running History Server task count query is invalid")
		}
		if !validation.WarmTaskQuery.Valid {
			validation.Problems = append(validation.Problems, "Q=1 detailed warm task query is invalid")
		}
	}
	if !validation.FullReplay.Valid {
		validation.Problems = append(validation.Problems, "production-path full replay is invalid")
	}
	if !validation.Logs.Valid {
		validation.Problems = append(validation.Problems, "History Server log error gate is invalid")
	}
	if !report.HSPodEvidence.Valid {
		validation.Problems = append(validation.Problems, "History Server pod/cgroup evidence is invalid")
	}
	if !report.CgroupSampler.StreamComplete {
		validation.Problems = append(validation.Problems, "cgroup sampler stream is incomplete")
	}
	if report.Config.HSProtocol == hsIsolatedRequestProtocol && !report.HSRequestIsolation.Valid {
		validation.Problems = append(validation.Problems,
			"isolated request CPU checkpoint gate is invalid")
	}
	if report.Config.HSProtocol == hsIsolatedRequestProtocol &&
		(report.HistoryServer.GC == nil || report.HistoryServer.GC.GOMAXPROCS != 2 || report.HistoryServer.GC.Cycles <= 0) {
		validation.Problems = append(validation.Problems, "isolated protocol runtime GOMAXPROCS/gctrace evidence is invalid")
	}
	if validation.LifetimeMemoryPeakBytes <= 0 {
		validation.Problems = append(validation.Problems, "lifetime memory.peak is missing")
	}
	if report.SourceSessionFingerprint.Algorithm != sourceFingerprintAlgorithm ||
		report.SourceSessionFingerprint.Bucket != report.Config.S3Bucket ||
		report.SourceSessionFingerprint.Start == "" ||
		report.SourceSessionFingerprint.Start != report.SourceSessionFingerprint.End {
		validation.Problems = append(validation.Problems, "source session fingerprint changed or is missing")
	}
	if report.SourceSessionFingerprint.ObjectCount != report.Config.HSSourceObjectCount {
		validation.Problems = append(validation.Problems,
			fmt.Sprintf("source object count=%d, want %d", report.SourceSessionFingerprint.ObjectCount,
				report.Config.HSSourceObjectCount))
	}
	if report.SourceSessionFingerprint.TotalBytes != report.Config.HSSourceTotalBytes {
		validation.Problems = append(validation.Problems,
			fmt.Sprintf("source bytes=%d, want %d", report.SourceSessionFingerprint.TotalBytes,
				report.Config.HSSourceTotalBytes))
	}
	validation.MeasurementValid = len(validation.Problems) == 0
	validation.Valid = validation.MeasurementValid
}

func validateFormalHSPhaseTimestamps(observed []HSPhaseTimestamp, protocol string) error {
	expectedSequence := formalHSPhaseSequence
	if protocol == hsIsolatedRequestProtocol {
		expectedSequence = isolatedHSPhaseSequence
	}
	if len(observed) != len(expectedSequence) {
		return fmt.Errorf("phase count=%d, want %d", len(observed), len(expectedSequence))
	}
	var previous int64
	for i, expected := range expectedSequence {
		mark := observed[i]
		if mark.Phase != expected {
			return fmt.Errorf("phase[%d]=%q, want %q", i, mark.Phase, expected)
		}
		if mark.TimeNano <= 0 {
			return fmt.Errorf("phase[%d] timeNano=%d, want positive", i, mark.TimeNano)
		}
		if i > 0 && mark.TimeNano <= previous {
			return fmt.Errorf("phase[%d] timeNano=%d is not after %d", i, mark.TimeNano, previous)
		}
		previous = mark.TimeNano
	}
	return nil
}

func validateHSFormalConfig(cfg benchConfig) error {
	if strings.Contains(cfg.HSOnly, ",") {
		return fmt.Errorf("BENCH_HS_ONLY=%q, want exactly one namespace/cluster/session source", cfg.HSOnly)
	}
	if _, err := parseHSSourceSpec(cfg.HSOnly); err != nil {
		return fmt.Errorf("BENCH_HS_ONLY=%q is unsafe: %w", cfg.HSOnly, err)
	}
	if cfg.TaskCount <= 0 {
		return fmt.Errorf("BENCH_TASK_COUNT=%d, want a positive expected attempt count", cfg.TaskCount)
	}
	if cfg.HSSourceObjectCount <= 0 {
		return fmt.Errorf("BENCH_HS_SOURCE_OBJECT_COUNT=%d, want a positive source inventory", cfg.HSSourceObjectCount)
	}
	if cfg.HSSourceTotalBytes <= 0 {
		return fmt.Errorf("BENCH_HS_SOURCE_TOTAL_BYTES=%d, want a positive source inventory", cfg.HSSourceTotalBytes)
	}
	if err := validateTaskLogMetadataSummary(expectedTaskLogMetadataFromConfig(cfg), cfg.TaskCount); err != nil {
		return fmt.Errorf("immutable source task-log metadata is invalid: %w", err)
	}
	for label, values := range map[string][2]string{
		"CPU":    {cfg.HSCPURequest, cfg.HSCPULimit},
		"memory": {cfg.HSMemoryRequest, cfg.HSMemoryLimit},
	} {
		request, limit := values[0], values[1]
		if request == "" || limit == "" || request == "none" || limit == "none" {
			return fmt.Errorf("%s request and limit must both be finite, got request=%q limit=%q",
				label, request, limit)
		}
		if cfg.HSProtocol != hsIsolatedRequestProtocol && !quantityEqual(request, limit) {
			return fmt.Errorf("%s request=%q must equal limit=%q", label, request, limit)
		}
		requestQuantity, err := resource.ParseQuantity(request)
		if err != nil || requestQuantity.Sign() <= 0 {
			return fmt.Errorf("%s request=%q must be a positive Kubernetes quantity", label, request)
		}
	}
	if cfg.HSProtocol == hsIsolatedRequestProtocol {
		expectedResources := map[string]string{
			"CPU request":    cfg.HSCPURequest,
			"CPU limit":      cfg.HSCPULimit,
			"memory request": cfg.HSMemoryRequest,
			"memory limit":   cfg.HSMemoryLimit,
		}
		wantedResources := map[string]string{
			"CPU request":    "1",
			"CPU limit":      "2",
			"memory request": "1Gi",
			"memory limit":   "12Gi",
		}
		for label, observed := range expectedResources {
			if !quantityEqual(observed, wantedResources[label]) {
				return fmt.Errorf("isolated protocol %s=%q, want %q", label, observed, wantedResources[label])
			}
		}
		if cfg.HSRequestQuietGap != hsIsolatedQuietGap {
			return fmt.Errorf("BENCH_HS_REQUEST_QUIET_GAP=%s, want %s",
				cfg.HSRequestQuietGap, hsIsolatedQuietGap)
		}
		if cfg.HSPreColdIdle != hsIsolatedPreColdIdle {
			return fmt.Errorf("BENCH_HS_PRE_COLD_IDLE=%s, want %s", cfg.HSPreColdIdle, hsIsolatedPreColdIdle)
		}
		if cfg.HSEnv != "GOMAXPROCS=2,GODEBUG=gctrace=1" {
			return fmt.Errorf("BENCH_HS_ENV=%q, want exact isolated protocol environment", cfg.HSEnv)
		}
	} else if cfg.HSProtocol != "" {
		return fmt.Errorf("BENCH_HS_PROTOCOL=%q is unsupported", cfg.HSProtocol)
	} else if cfg.HSRequestQuietGap != 0 || cfg.HSPreColdIdle != 0 {
		return fmt.Errorf("isolated request timing requires isolated request protocol")
	}
	if cfg.HSArgs != "--session-cache-size=1,--session-cache-max-bytes=2147483648,--session-cache-ttl=0s,--session-process-timeout=10m" {
		return fmt.Errorf("BENCH_HS_ARGS=%q, want cache size 1, 2 GiB byte budget, TTL 0, and server timeout 10m", cfg.HSArgs)
	}
	if cfg.HSColdSLO != 120*time.Second {
		return fmt.Errorf("BENCH_HS_COLD_SLO=%s, want 120s", cfg.HSColdSLO)
	}
	if cfg.HSEnterTimeout != 12*time.Minute {
		return fmt.Errorf("BENCH_HS_ENTER_TIMEOUT=%s, want 12m", cfg.HSEnterTimeout)
	}
	if cfg.S3LocalPort != 19003 {
		return fmt.Errorf("BENCH_S3_LOCAL_PORT=%d, want dedicated formal port 19003", cfg.S3LocalPort)
	}
	if !filepath.IsAbs(cfg.ExecutionIdentityFile) || filepath.Base(cfg.ExecutionIdentityFile) != "execution-namespace.json" {
		return fmt.Errorf("BENCH_EXECUTION_IDENTITY_FILE=%q, want an absolute execution-namespace.json path", cfg.ExecutionIdentityFile)
	}
	if !cfg.HSSourceRayJobOwned || !cfg.HSSourceShutdownAfterJob || cfg.HSSourceJobTTLSeconds != 30 {
		return fmt.Errorf("source RayJob lifecycle owned/shutdown/TTL=%v/%v/%d, want true/true/30",
			cfg.HSSourceRayJobOwned, cfg.HSSourceShutdownAfterJob, cfg.HSSourceJobTTLSeconds)
	}
	if cfg.HSSourceRayJobBackoffLimit == nil || *cfg.HSSourceRayJobBackoffLimit != 0 ||
		cfg.HSSourceSubmitterBackoffLimit == nil || *cfg.HSSourceSubmitterBackoffLimit != 0 {
		return fmt.Errorf("source RayJob backoff limits=%s/%s, want explicit 0/0",
			formatOptionalInt32(cfg.HSSourceRayJobBackoffLimit),
			formatOptionalInt32(cfg.HSSourceSubmitterBackoffLimit))
	}
	if cfg.S3Bucket != benchmarkS3BucketName {
		return fmt.Errorf("benchmark S3 bucket=%q, want %q", cfg.S3Bucket, benchmarkS3BucketName)
	}
	if cfg.HSWarmWait != 0 {
		return fmt.Errorf("BENCH_HS_WARM_WAIT=%s, formal cold mode prohibits retries", cfg.HSWarmWait)
	}
	if cfg.HSSessionSettle != 0 {
		return fmt.Errorf("BENCH_HS_SESSION_SETTLE=%s, formal mode prohibits hidden enter_cluster probes", cfg.HSSessionSettle)
	}
	if cfg.WarmIterations != 1 {
		return fmt.Errorf("BENCH_WARM_ITERATIONS=%d, want 1", cfg.WarmIterations)
	}
	if cfg.HSQueryConcurrency != 1 {
		return fmt.Errorf("BENCH_HS_QUERY_CONCURRENCY=%d, want 1", cfg.HSQueryConcurrency)
	}
	return nil
}

func TestFingerprintRecordsIsOrderIndependentAndFailClosed(t *testing.T) {
	records := []sourceFingerprintRecord{
		{Key: "b", Size: 2, ETag: "b-etag", ContentSHA256: "b-sha"},
		{Key: "a", Size: 1, ETag: "a-etag", ContentSHA256: "a-sha"},
	}
	one, err := fingerprintRecords(records)
	if err != nil {
		t.Fatal(err)
	}
	two, err := fingerprintRecords([]sourceFingerprintRecord{records[1], records[0]})
	if err != nil {
		t.Fatal(err)
	}
	if one != two {
		t.Fatalf("fingerprint depends on list order: %s != %s", one, two)
	}
	records[0].ContentSHA256 = "changed"
	changed, err := fingerprintRecords(records)
	if err != nil {
		t.Fatal(err)
	}
	if one == changed {
		t.Fatal("same-size content rewrite did not change the fingerprint")
	}
	if _, err := fingerprintRecords(nil); err == nil {
		t.Fatal("empty source snapshot must fail closed")
	}
}

func TestValidateHSLogsCatchesPartialReplayPaths(t *testing.T) {
	validation := validateHSLogs([]byte(strings.Join([]string{
		"Failed to decode events for file a",
		"Failed to store events for file b: failed to unmarshal task lifecycle event",
		"Attempting to create bucket ray-history...",
	}, "\n")))
	if validation.Valid || validation.TotalErrors != 4 || validation.ErrorCounts["decode"] != 1 ||
		validation.ErrorCounts["store"] != 1 || validation.ErrorCounts["taskLifecycleUnmarshal"] != 1 ||
		validation.ErrorCounts["bucketCreateAttempt"] != 1 {
		t.Fatalf("decode/store errors did not fail closed: %#v", validation)
	}
	clean := validateHSLogs(nil)
	if !clean.Valid || clean.TotalErrors != 0 {
		t.Fatalf("clean log rejected: %#v", clean)
	}
}

func TestFinalizeHSValidationRejectsFingerprintDrift(t *testing.T) {
	report := validHSFormalReportFixture()
	report.SourceSessionFingerprint.End = strings.Repeat("b", 64)
	finalizeHSValidation(&report)
	if report.HSValidation.Valid || !containsString(report.HSValidation.Problems, "source session fingerprint") {
		t.Fatalf("fingerprint drift did not fail closed: %#v", report.HSValidation)
	}
}

func TestFinalizeHSValidationRejectsFingerprintFromAnotherBucket(t *testing.T) {
	report := validHSFormalReportFixture()
	report.SourceSessionFingerprint.Bucket = S3BucketName
	finalizeHSValidation(&report)
	if report.HSValidation.Valid || !containsString(report.HSValidation.Problems, "source session fingerprint") {
		t.Fatalf("cross-bucket fingerprint did not fail closed: %#v", report.HSValidation)
	}
}

func containsString(values []string, substring string) bool {
	for _, value := range values {
		if strings.Contains(value, substring) {
			return true
		}
	}
	return false
}

func validHSFormalReportFixtureFor(taskCount, objectCount int, totalBytes int64) Report {
	zeroErrors := func() map[string]int {
		counts := make(map[string]int, len(strictReplayErrorPatterns))
		for key := range strictReplayErrorPatterns {
			counts[key] = 0
		}
		return counts
	}
	digest := strings.Repeat("a", 64)
	warmLimit := formalWarmTaskLimit(taskCount)
	sourceTaskLogMetadata := TaskLogMetadataSummary{
		Algorithm: taskLogMetadataAlgorithm,
		SHA256:    strings.Repeat("e", 64),
		Attempts:  taskCount,
		Counts: TaskLogMetadataCounts{
			Present:          taskCount,
			IncompleteNonNil: taskCount,
		},
		Valid:    true,
		Problems: []string{},
	}
	warmTaskLogMetadata := TaskLogMetadataSummary{
		Algorithm: taskLogMetadataAlgorithm,
		SHA256:    strings.Repeat("f", 64),
		Attempts:  warmLimit,
		Counts: TaskLogMetadataCounts{
			Present:          warmLimit,
			IncompleteNonNil: warmLimit,
		},
		Valid:    true,
		Problems: []string{},
	}
	report := Report{
		StartedAt: time.Unix(1, 0).UTC(),
		Config: benchConfig{
			TaskCount:                        taskCount,
			S3Bucket:                         benchmarkS3BucketName,
			KindNode:                         "kind-control-plane",
			WarmIterations:                   1,
			HSCPURequest:                     "1",
			HSCPULimit:                       "1",
			HSMemoryRequest:                  "8Gi",
			HSMemoryLimit:                    "8Gi",
			HSArgs:                           "--session-cache-size=1,--session-cache-max-bytes=2147483648,--session-cache-ttl=0s,--session-process-timeout=10m",
			HSOnly:                           "test-ns-vp8s9/rayjob-bench-rjff2/session_fixture",
			HSSourceObjectCount:              objectCount,
			HSSourceTotalBytes:               totalBytes,
			HSSourceTaskLogMetadataAlgorithm: sourceTaskLogMetadata.Algorithm,
			HSSourceTaskLogMetadataSHA256:    sourceTaskLogMetadata.SHA256,
			HSSourceTaskLogMetadataAttempts:  sourceTaskLogMetadata.Attempts,
			HSSourceTaskLogMetadataNil:       sourceTaskLogMetadata.Counts.Nil,
			HSSourceTaskLogMetadataPresent:   sourceTaskLogMetadata.Counts.Present,
			HSSourceTaskLogMetadataStructurallyInvalid:       sourceTaskLogMetadata.Counts.StructurallyInvalid,
			HSSourceTaskLogMetadataIncompleteNonNil:          sourceTaskLogMetadata.Counts.IncompleteNonNil,
			HSSourceTaskLogMetadataStdoutExactResolvable:     sourceTaskLogMetadata.Counts.StdoutExactResolvable,
			HSSourceTaskLogMetadataStderrExactResolvable:     sourceTaskLogMetadata.Counts.StderrExactResolvable,
			HSSourceTaskLogMetadataLegacyWholeWorkerFallback: sourceTaskLogMetadata.Counts.LegacyWholeWorkerFallback,
			HSStrictCold:                  true,
			HSColdSLO:                     120 * time.Second,
			HSQueryConcurrency:            1,
			HSEnterTimeout:                12 * time.Minute,
			S3LocalPort:                   19003,
			HSWarmWait:                    0,
			HSSessionSettle:               0,
			ExecutionIdentityFile:         "/tmp/execution-namespace.json",
			HSSourceRayJobOwned:           true,
			HSSourceShutdownAfterJob:      true,
			HSSourceJobTTLSeconds:         30,
			HSSourceRayJobBackoffLimit:    int32Pointer(0),
			HSSourceSubmitterBackoffLimit: int32Pointer(0),
		},
		Namespace:             "test-ns-vp8s9",
		ExecutionNamespace:    "test-ns-formal-arm",
		ExecutionNamespaceUID: "execution-namespace-uid",
		ClusterName:           "rayjob-bench-rjff2",
		SessionID:             "session_fixture",
		CollectorLogs:         []CollectorLogStat{},
		StorageDiffs:          []SnapshotDiff{},
		Resources:             []ResourceUsage{},
		Cgroups: []CgroupUsage{
			{
				Container:      "historyserver-pod/historyserver",
				Phase:          "historyserver",
				Samples:        2,
				PeakCurrentMiB: 1024,
			},
			{
				Container:         "historyserver-pod/historyserver",
				Phase:             "lifetime",
				LifetimePeakMiB:   1024,
				LifetimePeakBytes: 1 << 30,
			},
		},
		CollectorWindows:      []CollectorResourceWindow{},
		CollectorIngressGates: []CollectorIngressGate{},
		Timeline:              []TimelineEvent{},
		PodTerminations:       []PodTermination{},
		RayJobLifecycle: RayJobLifecycleEvidence{
			OwnedCluster:             true,
			ShutdownAfterJobFinishes: true,
			TTLSecondsAfterFinished:  30,
			RayJobBackoffLimit:       int32Pointer(0),
			SubmitterBackoffLimit:    int32Pointer(0),
		},
		HistoryServer: HSBenchResult{
			EnterColdLatency: 50 * time.Second,
			EnterMeasured:    true,
			EnterStatus:      http.StatusOK,
			EnterAttempts:    1,
			Notes:            []string{},
		},
		HistoryServerPhases: []HSPhaseTimestamp{
			{Phase: "startup", TimeNano: 1},
			{Phase: "cold-load", TimeNano: 2},
			{Phase: "endpoint-test", TimeNano: 3},
			{Phase: "after-endpoint-test", TimeNano: 4},
			{Phase: "arm-end", TimeNano: 5},
		},
		HSPodEvidence: HSPodEvidence{
			ExecutionNamespace:    "test-ns-formal-arm",
			PodName:               "historyserver-pod",
			PodUID:                "pod-uid",
			ContainerName:         "historyserver",
			ContainerID:           strings.Repeat("c", 64),
			Image:                 "kuberay/history-server:ray-2.56.0",
			ImageID:               "sha256:" + strings.Repeat("d", 64),
			Ready:                 true,
			Running:               true,
			CPURequest:            "1",
			CPULimit:              "1",
			MemoryRequest:         "8Gi",
			MemoryLimit:           "8Gi",
			TerminationReasons:    []string{},
			CgroupObserved:        true,
			CgroupMemoryMax:       "8589934592",
			CgroupMemoryMaxBytes:  8 << 30,
			CgroupReadErrorFields: []string{},
			Valid:                 true,
			Problems:              []string{},
		},
		HSValidation: HSValidation{
			ExpectedTaskAttempts: taskCount,
			Scope:                HSBenchmarkScope{Replay: true, TaskList: true, LogsFile: false},
			TaskCountQuery: HSTaskQueryValidation{
				Endpoint:       formalTaskEndpoint(0, false),
				Concurrency:    1,
				HTTPStatus:     http.StatusOK,
				Latency:        time.Millisecond,
				ResponseResult: true,
				NumFiltered:    taskCount,
				Valid:          true,
				Problems:       []string{},
			},
			WarmTaskQuery: HSTaskQueryValidation{
				Endpoint:                 formalTaskEndpoint(warmLimit, true),
				Concurrency:              1,
				Limit:                    warmLimit,
				HTTPStatus:               http.StatusOK,
				Latency:                  time.Millisecond,
				ResponseResult:           true,
				Rows:                     warmLimit,
				NumFiltered:              taskCount,
				DistinctTaskIDs:          warmLimit,
				AttemptZero:              warmLimit,
				Finished:                 warmLimit,
				TaskLogMetadata:          warmTaskLogMetadata,
				ExpectedProjectionSHA256: warmTaskLogMetadata.SHA256,
				ProjectionMatches:        true,
				Valid:                    true,
				Problems:                 []string{},
			},
			FullReplay: HSReplayValidation{
				Status:           "processed",
				ExpectedAttempts: taskCount,
				ObservedAttempts: taskCount,
				DistinctTaskIDs:  taskCount,
				AttemptZero:      taskCount,
				Finished:         taskCount,
				TaskLogMetadata:  sourceTaskLogMetadata,
				ErrorCounts:      zeroErrors(),
				Valid:            true,
				Problems:         []string{},
			},
			Logs: HSLogValidation{
				ErrorCounts: zeroErrors(),
				Valid:       true,
				Problems:    []string{},
			},
			LifetimeMemoryPeakBytes: 1 << 30,
			Problems:                []string{},
		},
		SourceSessionFingerprint: SourceSessionFingerprint{
			Algorithm:   sourceFingerprintAlgorithm,
			Bucket:      benchmarkS3BucketName,
			Start:       digest,
			End:         digest,
			ObjectCount: objectCount,
			TotalBytes:  totalBytes,
		},
		CgroupSampler: CgroupSamplerStatus{
			StartAttempted: true,
			Started:        true,
			StopRequested:  true,
			StreamEnded:    true,
			StreamComplete: true,
		},
		Completed: true,
	}
	finalizeHSValidation(&report)
	return report
}

func validHSFormalReportFixture() Report {
	return validHSFormalReportFixtureFor(50_000, 143, 19_582_280)
}

func TestValidHSFormalReportFixturePassesGoGate(t *testing.T) {
	tests := []struct {
		taskCount   int
		objectCount int
		totalBytes  int64
	}{
		{taskCount: 1_000, objectCount: 143, totalBytes: 741_488},
		{taskCount: 5_000, objectCount: 143, totalBytes: 2_500_000},
		{taskCount: 10_000, objectCount: 143, totalBytes: 4_268_828},
		{taskCount: 50_000, objectCount: 143, totalBytes: 19_582_280},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("n%d", test.taskCount), func(t *testing.T) {
			report := validHSFormalReportFixtureFor(test.taskCount, test.objectCount, test.totalBytes)
			if !report.HSValidation.MeasurementValid || !report.HSValidation.MeetsColdSLO || !report.HSValidation.Valid {
				t.Fatalf("valid fixture failed Go gate: %#v", report.HSValidation)
			}
			if report.HSValidation.WarmTaskQuery.Limit != formalWarmTaskLimit(test.taskCount) {
				t.Fatalf("warm limit=%d, want %d", report.HSValidation.WarmTaskQuery.Limit,
					formalWarmTaskLimit(test.taskCount))
			}
		})
	}
}

func TestFinalizeHSValidationRejectsInvalidPhaseTimestamps(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Report)
	}{
		{name: "missing", mutate: func(report *Report) {
			report.HistoryServerPhases = report.HistoryServerPhases[:4]
		}},
		{name: "reordered", mutate: func(report *Report) {
			report.HistoryServerPhases[1], report.HistoryServerPhases[2] =
				report.HistoryServerPhases[2], report.HistoryServerPhases[1]
		}},
		{name: "non-positive", mutate: func(report *Report) {
			report.HistoryServerPhases[0].TimeNano = 0
		}},
		{name: "not increasing", mutate: func(report *Report) {
			report.HistoryServerPhases[2].TimeNano = report.HistoryServerPhases[1].TimeNano
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			report := validHSFormalReportFixture()
			test.mutate(&report)
			report.HSValidation.Problems = []string{}
			finalizeHSValidation(&report)
			if report.HSValidation.MeasurementValid || report.HSValidation.Valid {
				t.Fatalf("invalid phase timeline passed: %#v", report.HSValidation)
			}
		})
	}
}

func TestFinalizeHSValidationSeparatesMeasurementFromColdSLO(t *testing.T) {
	report := validHSFormalReportFixture()
	report.HistoryServer.EnterColdLatency = report.Config.HSColdSLO + time.Second
	report.HSValidation.Problems = []string{}
	finalizeHSValidation(&report)
	if !report.HSValidation.MeasurementValid || !report.HSValidation.Valid || report.HSValidation.MeetsColdSLO {
		t.Fatalf("complete HTTP 200 above SLO was not preserved as a valid measurement: %#v", report.HSValidation)
	}

	report = validHSFormalReportFixture()
	report.HistoryServer.EnterMeasured = false
	report.HistoryServer.EnterStatus = http.StatusInternalServerError
	report.HistoryServer.EnterColdLatency = time.Second
	report.HSValidation.Problems = []string{}
	finalizeHSValidation(&report)
	if report.HSValidation.MeasurementValid || report.HSValidation.Valid {
		t.Fatalf("early cold failure was accepted: %#v", report.HSValidation)
	}
}

func TestWriteHSFormalReportFixture(t *testing.T) {
	path := os.Getenv("BENCH_HS_VALIDATOR_FIXTURE_OUT")
	if path == "" {
		t.Skip("set BENCH_HS_VALIDATOR_FIXTURE_OUT to write the serialized Go fixture")
	}
	raw, err := json.MarshalIndent(validHSFormalReportFixture(), "", "  ")
	if err != nil {
		t.Fatalf("marshal formal report fixture: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write formal report fixture: %v", err)
	}
}
