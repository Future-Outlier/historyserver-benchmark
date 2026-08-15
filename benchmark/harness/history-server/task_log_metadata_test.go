package benchmark

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"

	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

const taskLogMetadataAlgorithm = "task-log-metadata-sha256-v1"

type TaskLogMetadataCounts struct {
	Nil                       int `json:"nil"`
	Present                   int `json:"present"`
	StructurallyInvalid       int `json:"structurallyInvalid"`
	IncompleteNonNil          int `json:"incompleteNonNil"`
	StdoutExactResolvable     int `json:"stdoutExactResolvable"`
	StderrExactResolvable     int `json:"stderrExactResolvable"`
	LegacyWholeWorkerFallback int `json:"legacyWholeWorkerFallback"`
}

type TaskLogMetadataSummary struct {
	Algorithm string                `json:"algorithm"`
	SHA256    string                `json:"sha256"`
	Attempts  int                   `json:"attempts"`
	Counts    TaskLogMetadataCounts `json:"counts"`
	Valid     bool                  `json:"valid"`
	Problems  []string              `json:"problems"`
}

type taskLogMetadataRecord struct {
	TaskID                    string                  `json:"taskId"`
	Attempt                   int                     `json:"attempt"`
	NodeID                    string                  `json:"nodeId"`
	WorkerID                  string                  `json:"workerId"`
	TaskLogInfo               *eventtypes.TaskLogInfo `json:"taskLogInfo"`
	State                     string                  `json:"state"`
	StdoutExactResolvable     bool                    `json:"stdoutExactResolvable"`
	StderrExactResolvable     bool                    `json:"stderrExactResolvable"`
	LegacyWholeWorkerFallback bool                    `json:"legacyWholeWorkerFallback"`
	Conflicts                 []string                `json:"conflicts"`
}

type flexibleInt64 int64

func (value *flexibleInt64) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	var parsed int64
	if len(data) > 0 && data[0] == '"' {
		var encoded string
		if err := json.Unmarshal(data, &encoded); err != nil {
			return err
		}
		converted, err := strconv.ParseInt(encoded, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid int64 value %q: %w", encoded, err)
		}
		parsed = converted
	} else if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	*value = flexibleInt64(parsed)
	return nil
}

type taskLogInfoPatch struct {
	StdoutFile  *string        `json:"stdoutFile"`
	StderrFile  *string        `json:"stderrFile"`
	StdoutStart *flexibleInt64 `json:"stdoutStart"`
	StdoutEnd   *flexibleInt64 `json:"stdoutEnd"`
	StderrStart *flexibleInt64 `json:"stderrStart"`
	StderrEnd   *flexibleInt64 `json:"stderrEnd"`
}

type taskLogMetadataAggregate struct {
	present      bool
	nodeIDs      map[string]struct{}
	workerIDs    map[string]struct{}
	stdoutFiles  map[string]struct{}
	stderrFiles  map[string]struct{}
	stdoutStarts map[int64]struct{}
	stdoutEnds   map[int64]struct{}
	stderrStarts map[int64]struct{}
	stderrEnds   map[int64]struct{}
}

type taskLogMetadataAccumulator struct {
	attempts map[taskAttemptKey]*taskLogMetadataAggregate
}

func newTaskLogMetadataAccumulator() *taskLogMetadataAccumulator {
	return &taskLogMetadataAccumulator{attempts: map[taskAttemptKey]*taskLogMetadataAggregate{}}
}

func (a *taskLogMetadataAccumulator) get(key taskAttemptKey) *taskLogMetadataAggregate {
	result := a.attempts[key]
	if result != nil {
		return result
	}
	result = &taskLogMetadataAggregate{
		nodeIDs:      map[string]struct{}{},
		workerIDs:    map[string]struct{}{},
		stdoutFiles:  map[string]struct{}{},
		stderrFiles:  map[string]struct{}{},
		stdoutStarts: map[int64]struct{}{},
		stdoutEnds:   map[int64]struct{}{},
		stderrStarts: map[int64]struct{}{},
		stderrEnds:   map[int64]struct{}{},
	}
	a.attempts[key] = result
	return result
}

func (a *taskLogMetadataAccumulator) observe(probe eventProbe) {
	if probe.TaskLifecycle == nil || probe.TaskLifecycle.TaskID == "" || probe.TaskLifecycle.TaskAttempt == nil {
		return
	}
	key := taskAttemptKey{TaskID: probe.TaskLifecycle.TaskID, Attempt: *probe.TaskLifecycle.TaskAttempt}
	aggregate := a.get(key)
	addNonEmptyString(aggregate.nodeIDs, probe.TaskLifecycle.NodeID)
	addNonEmptyString(aggregate.workerIDs, probe.TaskLifecycle.WorkerID)
	patch := probe.TaskLifecycle.TaskLogInfo
	if patch == nil {
		return
	}
	aggregate.present = true
	addTaskLogStart(aggregate.stdoutFiles, aggregate.stdoutStarts, patch.StdoutFile, patch.StdoutStart)
	addTaskLogStart(aggregate.stderrFiles, aggregate.stderrStarts, patch.StderrFile, patch.StderrStart)
	addOptionalInt64(aggregate.stdoutEnds, patch.StdoutEnd, false)
	addOptionalInt64(aggregate.stderrEnds, patch.StderrEnd, false)
}

func addNonEmptyString(values map[string]struct{}, value string) {
	if value != "" {
		values[value] = struct{}{}
	}
}

func addTaskLogStart(
	files map[string]struct{},
	starts map[int64]struct{},
	file *string,
	start *flexibleInt64,
) {
	if file != nil && *file != "" {
		files[*file] = struct{}{}
		if start == nil {
			// Production decodes a missing scalar offset as zero, and a non-empty
			// filename makes that zero a meaningful start update.
			starts[0] = struct{}{}
		} else {
			addOptionalInt64(starts, start, true)
		}
		return
	}
	addOptionalInt64(starts, start, false)
}

func addOptionalInt64(values map[int64]struct{}, value *flexibleInt64, includeZero bool) {
	if value == nil {
		return
	}
	converted := int64(*value)
	if converted != 0 || includeZero {
		values[converted] = struct{}{}
	}
}

func (a *taskLogMetadataAccumulator) records(keys map[taskAttemptKey]struct{}) []taskLogMetadataRecord {
	records := make([]taskLogMetadataRecord, 0, len(keys))
	for key := range keys {
		aggregate := a.attempts[key]
		if aggregate == nil {
			aggregate = a.get(key)
		}
		records = append(records, aggregate.canonicalRecord(key))
	}
	return records
}

func (a *taskLogMetadataAggregate) canonicalRecord(key taskAttemptKey) taskLogMetadataRecord {
	record := taskLogMetadataRecord{
		TaskID:    canonicalRawRayID(key.TaskID),
		Attempt:   key.Attempt,
		NodeID:    canonicalRawRayID(uniqueString(a.nodeIDs)),
		WorkerID:  canonicalRawRayID(uniqueString(a.workerIDs)),
		Conflicts: []string{},
	}
	for _, field := range []struct {
		label string
		raw   string
	}{
		{label: "taskId", raw: key.TaskID},
		{label: "nodeId", raw: uniqueString(a.nodeIDs)},
		{label: "workerId", raw: uniqueString(a.workerIDs)},
	} {
		label, raw := field.label, field.raw
		if raw == "" {
			continue
		}
		if _, err := utils.ConvertBase64ToHex(raw); err != nil {
			record.Conflicts = append(record.Conflicts, fmt.Sprintf("%s is not a valid Ray ID", label))
		}
	}
	appendSetConflict(&record.Conflicts, "nodeId", sortedStrings(a.nodeIDs))
	appendSetConflict(&record.Conflicts, "workerId", sortedStrings(a.workerIDs))
	if !a.present {
		classifyTaskLogMetadata(&record)
		return record
	}

	info := &eventtypes.TaskLogInfo{
		StdoutFile:  uniqueString(a.stdoutFiles),
		StderrFile:  uniqueString(a.stderrFiles),
		StdoutStart: uniqueInt64(a.stdoutStarts),
		StdoutEnd:   uniqueInt64(a.stdoutEnds),
		StderrStart: uniqueInt64(a.stderrStarts),
		StderrEnd:   uniqueInt64(a.stderrEnds),
	}
	record.TaskLogInfo = info
	appendSetConflict(&record.Conflicts, "stdoutFile", sortedStrings(a.stdoutFiles))
	appendSetConflict(&record.Conflicts, "stderrFile", sortedStrings(a.stderrFiles))
	appendSetConflict(&record.Conflicts, "stdoutStart", sortedInt64Strings(a.stdoutStarts))
	appendSetConflict(&record.Conflicts, "stdoutEnd", sortedInt64Strings(a.stdoutEnds))
	appendSetConflict(&record.Conflicts, "stderrStart", sortedInt64Strings(a.stderrStarts))
	appendSetConflict(&record.Conflicts, "stderrEnd", sortedInt64Strings(a.stderrEnds))
	classifyTaskLogMetadata(&record)
	return record
}

func canonicalRawRayID(value string) string {
	if value == "" {
		return ""
	}
	converted, err := utils.ConvertBase64ToHex(value)
	if err != nil {
		return value
	}
	return converted
}

func canonicalTaskLogMetadataRecord(taskID string, attempt int, nodeID, workerID string, info *eventtypes.TaskLogInfo) taskLogMetadataRecord {
	record := taskLogMetadataRecord{
		TaskID:      taskID,
		Attempt:     attempt,
		NodeID:      nodeID,
		WorkerID:    workerID,
		TaskLogInfo: cloneTaskLogInfo(info),
		Conflicts:   []string{},
	}
	classifyTaskLogMetadata(&record)
	return record
}

func cloneTaskLogInfo(info *eventtypes.TaskLogInfo) *eventtypes.TaskLogInfo {
	if info == nil {
		return nil
	}
	cloned := *info
	return &cloned
}

func classifyTaskLogMetadata(record *taskLogMetadataRecord) {
	record.State = ""
	record.StdoutExactResolvable = false
	record.StderrExactResolvable = false
	record.LegacyWholeWorkerFallback = false
	if record.TaskLogInfo == nil {
		if len(record.Conflicts) != 0 {
			record.State = "structurallyInvalid"
			return
		}
		record.State = "nil"
		record.LegacyWholeWorkerFallback = record.NodeID != "" && record.WorkerID != ""
		return
	}
	info := record.TaskLogInfo
	stdoutInvalid := info.StdoutStart < 0 || info.StdoutEnd < 0 || info.StdoutStart > info.StdoutEnd
	stderrInvalid := info.StderrStart < 0 || info.StderrEnd < 0 || info.StderrStart > info.StderrEnd
	record.StdoutExactResolvable = len(record.Conflicts) == 0 && !stdoutInvalid && record.NodeID != "" &&
		taskLogBasenameForMetadata(info.StdoutFile) != "" && info.StdoutEnd > 0
	record.StderrExactResolvable = len(record.Conflicts) == 0 && !stderrInvalid && record.NodeID != "" &&
		taskLogBasenameForMetadata(info.StderrFile) != "" && info.StderrEnd > 0
	invalid := len(record.Conflicts) != 0 || stdoutInvalid || stderrInvalid
	if invalid {
		record.State = "structurallyInvalid"
		return
	}
	if record.StdoutExactResolvable && record.StderrExactResolvable {
		record.State = "exactResolvable"
		return
	}
	record.State = "incompleteNonNil"
}

func taskLogBasenameForMetadata(filename string) string {
	if filename == "" {
		return ""
	}
	filename = path.Base(filename)
	if filename == "." || filename == ".." || filename == "/" {
		return ""
	}
	return filename
}

func summarizeTaskLogMetadata(records []taskLogMetadataRecord) TaskLogMetadataSummary {
	summary := TaskLogMetadataSummary{
		Algorithm: taskLogMetadataAlgorithm,
		Attempts:  len(records),
		Problems:  []string{},
	}
	sorted := append([]taskLogMetadataRecord(nil), records...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].TaskID == sorted[j].TaskID {
			if sorted[i].Attempt == sorted[j].Attempt {
				left, _ := json.Marshal(sorted[i])
				right, _ := json.Marshal(sorted[j])
				return string(left) < string(right)
			}
			return sorted[i].Attempt < sorted[j].Attempt
		}
		return sorted[i].TaskID < sorted[j].TaskID
	})
	for i := range sorted {
		if sorted[i].TaskID == "" {
			summary.Problems = append(summary.Problems, "canonical record has an empty task ID")
		}
		if i > 0 && sorted[i-1].TaskID == sorted[i].TaskID && sorted[i-1].Attempt == sorted[i].Attempt {
			summary.Problems = append(summary.Problems,
				fmt.Sprintf("duplicate canonical task attempt %s/%d", sorted[i].TaskID, sorted[i].Attempt))
		}
		if sorted[i].TaskLogInfo == nil {
			summary.Counts.Nil++
		} else {
			summary.Counts.Present++
		}
		switch sorted[i].State {
		case "nil":
		case "structurallyInvalid":
			summary.Counts.StructurallyInvalid++
			summary.Problems = append(summary.Problems,
				fmt.Sprintf("task %s/%d has structurally invalid log metadata", sorted[i].TaskID, sorted[i].Attempt))
		case "incompleteNonNil":
			summary.Counts.IncompleteNonNil++
		case "exactResolvable":
		default:
			summary.Problems = append(summary.Problems,
				fmt.Sprintf("task %s/%d has unknown log metadata state %q", sorted[i].TaskID, sorted[i].Attempt, sorted[i].State))
		}
		if sorted[i].StdoutExactResolvable {
			summary.Counts.StdoutExactResolvable++
		}
		if sorted[i].StderrExactResolvable {
			summary.Counts.StderrExactResolvable++
		}
		if sorted[i].LegacyWholeWorkerFallback {
			summary.Counts.LegacyWholeWorkerFallback++
		}
	}
	if summary.Counts.Nil+summary.Counts.Present != summary.Attempts {
		summary.Problems = append(summary.Problems, "nil and present counts do not cover every attempt")
	}
	raw, err := json.Marshal(sorted)
	if err != nil {
		summary.Problems = append(summary.Problems, fmt.Sprintf("marshal canonical records: %v", err))
	} else {
		digest := sha256.Sum256(raw)
		summary.SHA256 = hex.EncodeToString(digest[:])
	}
	sort.Strings(summary.Problems)
	summary.Valid = len(summary.Problems) == 0 && summary.SHA256 != ""
	return summary
}

func taskLogMetadataSummariesEqual(left, right TaskLogMetadataSummary) bool {
	return left.Algorithm == right.Algorithm && left.SHA256 == right.SHA256 &&
		left.Attempts == right.Attempts && left.Counts == right.Counts &&
		left.Valid == right.Valid && strings.Join(left.Problems, "\n") == strings.Join(right.Problems, "\n")
}

func expectedTaskLogMetadataFromConfig(cfg benchConfig) TaskLogMetadataSummary {
	return TaskLogMetadataSummary{
		Algorithm: cfg.HSSourceTaskLogMetadataAlgorithm,
		SHA256:    cfg.HSSourceTaskLogMetadataSHA256,
		Attempts:  cfg.HSSourceTaskLogMetadataAttempts,
		Counts: TaskLogMetadataCounts{
			Nil:                       cfg.HSSourceTaskLogMetadataNil,
			Present:                   cfg.HSSourceTaskLogMetadataPresent,
			StructurallyInvalid:       cfg.HSSourceTaskLogMetadataStructurallyInvalid,
			IncompleteNonNil:          cfg.HSSourceTaskLogMetadataIncompleteNonNil,
			StdoutExactResolvable:     cfg.HSSourceTaskLogMetadataStdoutExactResolvable,
			StderrExactResolvable:     cfg.HSSourceTaskLogMetadataStderrExactResolvable,
			LegacyWholeWorkerFallback: cfg.HSSourceTaskLogMetadataLegacyWholeWorkerFallback,
		},
		Valid:    true,
		Problems: []string{},
	}
}

func validateTaskLogMetadataSummary(summary TaskLogMetadataSummary, expectedAttempts int) error {
	problems := make([]string, 0)
	if summary.Algorithm != taskLogMetadataAlgorithm {
		problems = append(problems, fmt.Sprintf("algorithm=%q, want %q", summary.Algorithm, taskLogMetadataAlgorithm))
	}
	decoded, err := hex.DecodeString(summary.SHA256)
	if err != nil || len(decoded) != sha256.Size || summary.SHA256 != strings.ToLower(summary.SHA256) {
		problems = append(problems, "sha256 is not exactly 64 lowercase hexadecimal characters")
	}
	if summary.Attempts != expectedAttempts {
		problems = append(problems, fmt.Sprintf("attempts=%d, want %d", summary.Attempts, expectedAttempts))
	}
	counts := summary.Counts
	if counts.Nil < 0 || counts.Present < 0 || counts.StructurallyInvalid < 0 ||
		counts.IncompleteNonNil < 0 || counts.StdoutExactResolvable < 0 ||
		counts.StderrExactResolvable < 0 || counts.LegacyWholeWorkerFallback < 0 {
		problems = append(problems, "metadata counts must be non-negative")
	}
	if counts.Nil+counts.Present != summary.Attempts {
		problems = append(problems, "nil and present counts do not cover every attempt")
	}
	if counts.StructurallyInvalid != 0 {
		problems = append(problems, fmt.Sprintf("structurallyInvalid=%d, want 0", counts.StructurallyInvalid))
	}
	if counts.IncompleteNonNil > counts.Present || counts.StdoutExactResolvable > counts.Present ||
		counts.StderrExactResolvable > counts.Present {
		problems = append(problems, "present metadata subset counts exceed present")
	}
	if counts.LegacyWholeWorkerFallback > counts.Nil {
		problems = append(problems, "legacy whole-worker fallback count exceeds nil")
	}
	if !summary.Valid {
		problems = append(problems, "validity verdict is false")
	}
	if len(summary.Problems) != 0 {
		problems = append(problems, fmt.Sprintf("summary problems=%v, want empty", summary.Problems))
	}
	if len(problems) != 0 {
		return fmt.Errorf("%s", strings.Join(problems, "; "))
	}
	return nil
}

func uniqueString(values map[string]struct{}) string {
	if len(values) != 1 {
		return ""
	}
	for value := range values {
		return value
	}
	return ""
}

func uniqueInt64(values map[int64]struct{}) int64 {
	if len(values) != 1 {
		return 0
	}
	for value := range values {
		return value
	}
	return 0
}

func sortedStrings(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func sortedInt64Strings(values map[int64]struct{}) []string {
	result := make([]int64, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	encoded := make([]string, 0, len(result))
	for _, value := range result {
		encoded = append(encoded, strconv.FormatInt(value, 10))
	}
	return encoded
}

func appendSetConflict(problems *[]string, field string, values []string) {
	if len(values) > 1 {
		*problems = append(*problems, fmt.Sprintf("%s has conflicting values [%s]", field, strings.Join(values, ",")))
	}
}

func metadataProbe(t *testing.T, raw string) eventProbe {
	t.Helper()
	var probe eventProbe
	if err := json.Unmarshal([]byte(raw), &probe); err != nil {
		t.Fatalf("decode metadata probe: %v", err)
	}
	return probe
}

func summarizeMetadataProbes(t *testing.T, probes ...eventProbe) TaskLogMetadataSummary {
	t.Helper()
	accumulator := newTaskLogMetadataAccumulator()
	keys := map[taskAttemptKey]struct{}{}
	for _, probe := range probes {
		accumulator.observe(probe)
		if probe.TaskLifecycle != nil && probe.TaskLifecycle.TaskAttempt != nil {
			keys[taskAttemptKey{
				TaskID:  probe.TaskLifecycle.TaskID,
				Attempt: *probe.TaskLifecycle.TaskAttempt,
			}] = struct{}{}
		}
	}
	return summarizeTaskLogMetadata(accumulator.records(keys))
}

func TestRawTaskLogMetadataMergesCrossEventFieldsOrderIndependently(t *testing.T) {
	start := metadataProbe(t, `{"taskLifecycleEvent":{"taskId":"AQAAAA==","taskAttempt":0,"nodeId":"AwAAAA==","workerId":"BAAAAA==","taskLogInfo":{"stdoutFile":"worker.out","stderrFile":"worker.err","stdoutStart":"11","stdoutEnd":"0","stderrStart":21,"stderrEnd":"0"}}}`)
	end := metadataProbe(t, `{"taskLifecycleEvent":{"taskId":"AQAAAA==","taskAttempt":0,"nodeId":"AwAAAA==","workerId":"BAAAAA==","taskLogInfo":{"stdoutFile":"","stderrFile":"","stdoutStart":"0","stdoutEnd":"31","stderrStart":"0","stderrEnd":41}}}`)

	forward := summarizeMetadataProbes(t, start, end)
	reverse := summarizeMetadataProbes(t, end, start)
	if !forward.Valid || !taskLogMetadataSummariesEqual(forward, reverse) {
		t.Fatalf("cross-event merge depends on scan order: forward=%#v reverse=%#v", forward, reverse)
	}
	if forward.Counts.Present != 1 || forward.Counts.IncompleteNonNil != 0 ||
		forward.Counts.StdoutExactResolvable != 1 || forward.Counts.StderrExactResolvable != 1 {
		t.Fatalf("cross-event fields were not merged exactly: %#v", forward)
	}
}

func TestRawTaskLogMetadataMissingStartConflictsWithNonzeroStartOrderIndependently(t *testing.T) {
	missingStart := metadataProbe(t, `{"taskLifecycleEvent":{"taskId":"AQAAAA==","taskAttempt":0,"nodeId":"AwAAAA==","workerId":"BAAAAA==","taskLogInfo":{"stdoutFile":"worker.out"}}}`)
	nonzeroStart := metadataProbe(t, `{"taskLifecycleEvent":{"taskId":"AQAAAA==","taskAttempt":0,"nodeId":"AwAAAA==","workerId":"BAAAAA==","taskLogInfo":{"stdoutStart":"11"}}}`)

	forward := summarizeMetadataProbes(t, missingStart, nonzeroStart)
	reverse := summarizeMetadataProbes(t, nonzeroStart, missingStart)
	if forward.Valid || reverse.Valid || forward.Counts.StructurallyInvalid != 1 ||
		forward.SHA256 != reverse.SHA256 ||
		!strings.Contains(strings.Join(forward.Problems, " "), "structurally invalid") {
		t.Fatalf("missing and nonzero start did not conflict deterministically: forward=%#v reverse=%#v", forward, reverse)
	}
}

func TestTaskLogMetadataExactResolutionMatchesReaderPrerequisites(t *testing.T) {
	tests := []struct {
		name      string
		nodeID    string
		info      *eventtypes.TaskLogInfo
		wantExact bool
	}{
		{
			name:   "complete range",
			nodeID: "node",
			info: &eventtypes.TaskLogInfo{
				StdoutFile: "/tmp/ray/session/logs/worker.out", StdoutStart: 1, StdoutEnd: 2,
			},
			wantExact: true,
		},
		{name: "missing node", info: &eventtypes.TaskLogInfo{StdoutFile: "worker.out", StdoutEnd: 2}},
		{name: "zero end", nodeID: "node", info: &eventtypes.TaskLogInfo{StdoutFile: "worker.out"}},
		{name: "invalid basename", nodeID: "node", info: &eventtypes.TaskLogInfo{StdoutFile: ".", StdoutEnd: 2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := canonicalTaskLogMetadataRecord("task", 0, test.nodeID, "worker", test.info)
			if record.StdoutExactResolvable != test.wantExact {
				t.Fatalf("stdout exact=%v, want %v: %#v", record.StdoutExactResolvable, test.wantExact, record)
			}
			if !test.wantExact && record.State != "incompleteNonNil" {
				t.Fatalf("unavailable metadata state=%q, want incompleteNonNil", record.State)
			}
		})
	}
}

func TestRawTaskLogMetadataCanonicalIDsMatchProductionSnapshot(t *testing.T) {
	raw := summarizeMetadataProbes(t, metadataProbe(t,
		`{"taskLifecycleEvent":{"taskId":"AQAAAA==","taskAttempt":0,"nodeId":"AwAAAA==","workerId":"BAAAAA==","taskLogInfo":{"stdoutFile":"worker.out","stderrFile":"worker.err","stdoutStart":"1","stdoutEnd":"2","stderrStart":"3","stderrEnd":"4"}}}`))
	taskID, _ := utils.ConvertBase64ToHex("AQAAAA==")
	nodeID, _ := utils.ConvertBase64ToHex("AwAAAA==")
	workerID, _ := utils.ConvertBase64ToHex("BAAAAA==")
	production := summarizeTaskLogMetadata([]taskLogMetadataRecord{
		canonicalTaskLogMetadataRecord(taskID, 0, nodeID, workerID, &eventtypes.TaskLogInfo{
			StdoutFile: "worker.out", StderrFile: "worker.err", StdoutStart: 1, StdoutEnd: 2,
			StderrStart: 3, StderrEnd: 4,
		}),
	})
	if !taskLogMetadataSummariesEqual(raw, production) {
		t.Fatalf("raw Base64 IDs and production hex IDs canonicalized differently: raw=%#v production=%#v", raw, production)
	}
}

func TestTaskLogMetadataClassifiesIncompleteAndLegacyWithoutClaimingTaskExactFallback(t *testing.T) {
	endOnly := metadataProbe(t, `{"taskLifecycleEvent":{"taskId":"AQAAAA==","taskAttempt":0,"nodeId":"AwAAAA==","workerId":"BAAAAA==","taskLogInfo":{"stdoutEnd":"31","stderrEnd":"41"}}}`)
	nilInfo := metadataProbe(t, `{"taskLifecycleEvent":{"taskId":"AgAAAA==","taskAttempt":0,"nodeId":"BQAAAA==","workerId":"BgAAAA=="}}`)
	summary := summarizeMetadataProbes(t, endOnly, nilInfo)
	if !summary.Valid || summary.Counts.Present != 1 || summary.Counts.Nil != 1 ||
		summary.Counts.IncompleteNonNil != 1 || summary.Counts.LegacyWholeWorkerFallback != 1 ||
		summary.Counts.StdoutExactResolvable != 0 || summary.Counts.StderrExactResolvable != 0 {
		t.Fatalf("resolver classifications are incorrect: %#v", summary)
	}
}

func TestTaskLogMetadataFailsMalformedOverflowNegativeAndConflicts(t *testing.T) {
	for _, raw := range []string{
		`{"taskLifecycleEvent":{"taskId":"AQAAAA==","taskAttempt":0,"taskLogInfo":{"stdoutStart":{}}}}`,
		`{"taskLifecycleEvent":{"taskId":"AQAAAA==","taskAttempt":0,"taskLogInfo":{"stdoutEnd":"9223372036854775808"}}}`,
	} {
		var probe eventProbe
		if err := json.Unmarshal([]byte(raw), &probe); err == nil {
			t.Fatalf("malformed or overflowing TaskLogInfo accepted: %s", raw)
		}
	}

	negative := summarizeMetadataProbes(t, metadataProbe(t,
		`{"taskLifecycleEvent":{"taskId":"AQAAAA==","taskAttempt":0,"taskLogInfo":{"stdoutFile":"worker.out","stdoutStart":-1,"stdoutEnd":3}}}`))
	if negative.Valid || negative.Counts.StructurallyInvalid != 1 {
		t.Fatalf("negative offset did not fail closed: %#v", negative)
	}

	first := metadataProbe(t, `{"taskLifecycleEvent":{"taskId":"AQAAAA==","taskAttempt":0,"nodeId":"AwAAAA==","taskLogInfo":{"stdoutFile":"one.out","stdoutStart":1}}}`)
	second := metadataProbe(t, `{"taskLifecycleEvent":{"taskId":"AQAAAA==","taskAttempt":0,"nodeId":"BAAAAA==","taskLogInfo":{"stdoutFile":"two.out","stdoutStart":2}}}`)
	forward := summarizeMetadataProbes(t, first, second)
	reverse := summarizeMetadataProbes(t, second, first)
	if forward.Valid || forward.Counts.StructurallyInvalid != 1 || forward.SHA256 != reverse.SHA256 {
		t.Fatalf("conflicting metadata was not deterministic and fail closed: forward=%#v reverse=%#v", forward, reverse)
	}
}

func TestTaskLogMetadataDigestCatchesDropChangeAndSwapBetweenTaskIDs(t *testing.T) {
	recordA := canonicalTaskLogMetadataRecord("task-a", 0, "node", "worker", &eventtypes.TaskLogInfo{
		StdoutFile: "worker.out", StderrFile: "worker.err", StdoutStart: 1, StdoutEnd: 2, StderrStart: 3, StderrEnd: 4,
	})
	recordB := canonicalTaskLogMetadataRecord("task-b", 0, "node", "worker", &eventtypes.TaskLogInfo{
		StdoutFile: "worker.out", StderrFile: "worker.err", StdoutStart: 5, StdoutEnd: 6, StderrStart: 7, StderrEnd: 8,
	})
	original := summarizeTaskLogMetadata([]taskLogMetadataRecord{recordA, recordB})
	dropped := summarizeTaskLogMetadata([]taskLogMetadataRecord{recordA})
	changedB := recordB
	changedB.TaskLogInfo = cloneTaskLogInfo(recordB.TaskLogInfo)
	changedB.TaskLogInfo.StdoutEnd++
	changed := summarizeTaskLogMetadata([]taskLogMetadataRecord{recordA, changedB})
	swappedA, swappedB := recordA, recordB
	swappedA.TaskLogInfo, swappedB.TaskLogInfo = cloneTaskLogInfo(recordB.TaskLogInfo), cloneTaskLogInfo(recordA.TaskLogInfo)
	classifyTaskLogMetadata(&swappedA)
	classifyTaskLogMetadata(&swappedB)
	swapped := summarizeTaskLogMetadata([]taskLogMetadataRecord{swappedA, swappedB})
	for label, candidate := range map[string]TaskLogMetadataSummary{
		"drop": dropped, "change": changed, "swap": swapped,
	} {
		if candidate.SHA256 == original.SHA256 {
			t.Fatalf("%s mutation did not change canonical digest", label)
		}
	}
}

func TestTaskLogMetadataCanonicalCollisionIsDeterministicAndFailsClosed(t *testing.T) {
	first := canonicalTaskLogMetadataRecord("same-task", 0, "node", "worker", &eventtypes.TaskLogInfo{
		StdoutFile: "worker.out", StdoutEnd: 10,
	})
	second := canonicalTaskLogMetadataRecord("same-task", 0, "node", "worker", &eventtypes.TaskLogInfo{
		StdoutFile: "worker.out", StdoutEnd: 20,
	})
	forward := summarizeTaskLogMetadata([]taskLogMetadataRecord{first, second})
	reverse := summarizeTaskLogMetadata([]taskLogMetadataRecord{second, first})
	if forward.Valid || reverse.Valid || forward.SHA256 != reverse.SHA256 ||
		!strings.Contains(strings.Join(forward.Problems, " "), "duplicate canonical task attempt") {
		t.Fatalf("canonical collision was not deterministic and fail closed: forward=%#v reverse=%#v", forward, reverse)
	}
}

func TestWarmTaskProjectionFailsOnFieldMismatch(t *testing.T) {
	actual := canonicalTaskLogMetadataRecord("task-a", 0, "node", "worker", &eventtypes.TaskLogInfo{
		StdoutFile: "worker.out", StderrFile: "worker.err", StdoutEnd: 10, StderrEnd: 20,
	})
	query := HSTaskQueryValidation{
		Rows:            1,
		TaskLogMetadata: summarizeTaskLogMetadata([]taskLogMetadataRecord{actual}),
		Problems:        []string{},
		records:         []taskLogMetadataRecord{actual},
		Valid:           true,
	}
	expected := actual
	expected.TaskLogInfo = cloneTaskLogInfo(actual.TaskLogInfo)
	expected.TaskLogInfo.StdoutEnd++
	classifyTaskLogMetadata(&expected)
	validateWarmTaskProjection(&query, map[taskAttemptKey]taskLogMetadataRecord{
		{TaskID: "task-a", Attempt: 0}: expected,
	})
	if query.Valid || query.ProjectionMatches || !strings.Contains(strings.Join(query.Problems, " "), "differs") {
		t.Fatalf("warm/full mismatch was accepted: %#v", query)
	}
}
