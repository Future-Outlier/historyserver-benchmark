package benchmark

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func observeValidityEvent(t *testing.T, accumulator *benchTaskValidityAccumulator, raw string) {
	t.Helper()
	var probe eventProbe
	if err := json.Unmarshal([]byte(raw), &probe); err != nil {
		t.Fatalf("unmarshal event fixture: %v", err)
	}
	accumulator.observe(probe)
}

func TestDriverDisablesBenchTaskRetries(t *testing.T) {
	const decorator = "@ray.remote(num_cpus=__TASK_NUM_CPUS__, max_retries=0)"
	if !strings.Contains(driverTemplate, decorator) {
		t.Fatalf("driver template must contain %q", decorator)
	}
}

func TestFormalRayJobDriverContainsNoSleepCode(t *testing.T) {
	script := renderDriverScript(benchConfig{
		TaskCount:      1000,
		WaveSize:       500,
		TaskNumCPUs:    "0.5",
		DrainSleepSec:  0,
		TargetTaskRate: 0,
		Drivers:        1,
	})
	if strings.Contains(script, "time.sleep(") {
		t.Fatalf("formal unpaced RayJob driver must not contain sleep code:\n%s", script)
	}
	if strings.Contains(script, "__") {
		t.Fatalf("rendered driver still contains a template placeholder:\n%s", script)
	}
}

func TestFormalOwnedRayJobUsesAPILifecycleWithoutDriverSleep(t *testing.T) {
	cfg := benchConfig{
		TaskCount:        1_000,
		WaveSize:         500,
		TaskNumCPUs:      "0.5",
		DrainSleepSec:    0,
		TargetTaskRate:   0,
		Drivers:          1,
		ShutdownAfterJob: true,
		JobTTLSeconds:    30,
	}
	ownedSpec := &rayv1.RayClusterSpec{}
	job := buildBenchRayJob("bench-ns", "", ownedSpec, cfg)

	if job.Spec.RayClusterSpec != ownedSpec || !job.Spec.ShutdownAfterJobFinishes || job.Spec.TTLSecondsAfterFinished != 30 {
		t.Fatalf("formal owned RayJob lifecycle fields are wrong: %#v", job.Spec)
	}
	if job.Spec.ClusterSelector != nil {
		t.Fatalf("owned RayJob must not select an external RayCluster: %#v", job.Spec.ClusterSelector)
	}
	if strings.Contains(job.Spec.Entrypoint, "time.sleep(") {
		t.Fatalf("formal RayJob entrypoint contains driver sleep: %s", job.Spec.Entrypoint)
	}
	if err := validateOwnedRayJobDriverPolicy(cfg, renderDriverScript(cfg)); err != nil {
		t.Fatalf("valid unpaced owned RayJob policy rejected: %v", err)
	}
}

func TestFormalOwnedRateRayJobAllowsOnlyTargetRatePacingSleep(t *testing.T) {
	cfg := benchConfig{
		TaskCount:        50_000,
		WaveSize:         2_000,
		TaskNumCPUs:      "0.5",
		DrainSleepSec:    0,
		TargetTaskRate:   1_000,
		Drivers:          1,
		ShutdownAfterJob: true,
		JobTTLSeconds:    30,
	}
	script := renderDriverScript(cfg)
	if err := validateOwnedRayJobDriverPolicy(cfg, script); err != nil {
		t.Fatalf("valid paced owned RayJob policy rejected: %v", err)
	}
	if strings.Count(script, "time.sleep(") != 1 || !strings.Contains(script, "time.sleep(behind)") {
		t.Fatalf("paced owned RayJob must contain exactly one pacing sleep:\n%s", script)
	}
}

func TestFormalOwnedRayJobRejectsPostJobDriverDrainSleep(t *testing.T) {
	cfg := benchConfig{
		TaskCount:        50_000,
		WaveSize:         2_000,
		TaskNumCPUs:      "0.5",
		DrainSleepSec:    25,
		TargetTaskRate:   1_000,
		Drivers:          1,
		ShutdownAfterJob: true,
		JobTTLSeconds:    30,
	}
	if err := validateOwnedRayJobDriverPolicy(cfg, renderDriverScript(cfg)); err == nil {
		t.Fatal("owned RayJob accepted a post-job driver drain sleep")
	}
}

func TestRayJobLifecycleEvidenceComesFromCreatedSpec(t *testing.T) {
	job := &rayv1.RayJob{Spec: rayv1.RayJobSpec{
		RayClusterSpec:           &rayv1.RayClusterSpec{},
		ShutdownAfterJobFinishes: true,
		TTLSecondsAfterFinished:  30,
		BackoffLimit:             int32Pointer(0),
		SubmitterConfig: &rayv1.SubmitterConfig{
			BackoffLimit: int32Pointer(0),
		},
	}}
	evidence := lifecycleEvidenceFromRayJob(job)
	if !evidence.OwnedCluster || !evidence.ShutdownAfterJobFinishes ||
		evidence.TTLSecondsAfterFinished != 30 ||
		evidence.RayJobBackoffLimit == nil || *evidence.RayJobBackoffLimit != 0 ||
		evidence.SubmitterBackoffLimit == nil || *evidence.SubmitterBackoffLimit != 0 {
		t.Fatalf("unexpected RayJob lifecycle evidence: %#v", evidence)
	}
	job.Spec.BackoffLimit = nil
	job.Spec.SubmitterConfig = nil
	evidence = lifecycleEvidenceFromRayJob(job)
	if evidence.RayJobBackoffLimit != nil || evidence.SubmitterBackoffLimit != nil {
		t.Fatalf("nil retry controls were normalized into values: %#v", evidence)
	}
}

func TestBenchRayJobExplicitlyDisablesBothRetryLayers(t *testing.T) {
	job := buildBenchRayJob("test-ns", "", &rayv1.RayClusterSpec{}, benchConfig{
		TaskCount:   1000,
		WaveSize:    100,
		TaskNumCPUs: "0.5",
		Drivers:     1,
	})
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Fatalf("RayJob outer backoffLimit is not explicit zero: %#v", job.Spec.BackoffLimit)
	}
	if job.Spec.SubmitterConfig == nil || job.Spec.SubmitterConfig.BackoffLimit == nil ||
		*job.Spec.SubmitterConfig.BackoffLimit != 0 {
		t.Fatalf("RayJob submitter backoffLimit is not explicit zero: %#v", job.Spec.SubmitterConfig)
	}
}

func TestExploratoryDriverCanStillInjectPacingAndDrain(t *testing.T) {
	script := renderDriverScript(benchConfig{
		TaskCount:      1000,
		WaveSize:       500,
		TaskNumCPUs:    "0.5",
		DrainSleepSec:  25,
		TargetTaskRate: 100,
		Drivers:        1,
	})
	for _, fragment := range []string{"time.sleep(behind)", "time.sleep(25)", "wall = time.time() - t0 - 25"} {
		if !strings.Contains(script, fragment) {
			t.Fatalf("exploratory driver is missing %q:\n%s", fragment, script)
		}
	}
}

func TestBenchTaskValidityAcceptsExactlyAttemptZeroFinished(t *testing.T) {
	accumulator := newBenchTaskValidityAccumulator()

	// Lifecycle files are not guaranteed to be scanned after definition files.
	observeValidityEvent(t, accumulator, `{
		"eventType":"TASK_LIFECYCLE_EVENT",
		"taskLifecycleEvent":{
			"taskId":"task-2",
			"taskAttempt":0,
			"stateTransitions":[
				{"state":"FINISHED","timestamp":"2026-08-08T00:00:03Z"},
				{"state":"SUBMITTED_TO_WORKER","timestamp":"2026-08-08T00:00:01Z"},
				{"state":"RUNNING","timestamp":"2026-08-08T00:00:02Z"}
			]
		}
	}`)
	observeValidityEvent(t, accumulator, `{
		"eventType":"TASK_DEFINITION_EVENT",
		"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}
	}`)
	observeValidityEvent(t, accumulator, `{
		"eventType":"TASK_DEFINITION_EVENT",
		"taskDefinitionEvent":{
			"taskId":"task-2",
			"taskAttempt":0,
			"taskFunc":{"functionName":"__main__.bench_task"}
		}
	}`)
	observeValidityEvent(t, accumulator, `{
		"eventType":"TASK_LIFECYCLE_EVENT",
		"taskLifecycleEvent":{
			"taskId":"task-1",
			"taskAttempt":0,
			"stateTransitions":[
				{"state":"SUBMITTED_TO_WORKER","timestamp":"2026-08-08T00:00:00Z"},
				{"state":"FINISHED","timestamp":"2026-08-08T00:00:01Z"}
			]
		}
	}`)
	// Duplicates must not inflate the attempt or FINISHED counts.
	observeValidityEvent(t, accumulator, `{
		"eventType":"TASK_LIFECYCLE_EVENT",
		"taskLifecycleEvent":{
			"taskId":"task-1",
			"taskAttempt":0,
			"stateTransitions":[
				{"state":"SUBMITTED_TO_WORKER","timestamp":"2026-08-08T00:00:00Z"},
				{"state":"FINISHED","timestamp":"2026-08-08T00:00:01Z"}
			]
		}
	}`)
	observeValidityEvent(t, accumulator, `{
		"eventType":"TASK_DEFINITION_EVENT",
		"taskDefinitionEvent":{"taskId":"other-task","taskAttempt":7,"taskName":"ray_internal"}
	}`)
	observeValidityEvent(t, accumulator, `{
		"eventType":"TASK_DEFINITION_EVENT",
		"taskDefinitionEvent":{"taskId":"lookalike-task","taskAttempt":0,"taskName":"not_bench_task_helper"}
	}`)

	got := accumulator.summarize(2)
	if err := validateBenchTaskValidity(got); err != nil {
		t.Fatalf("valid events rejected: %v; verdict=%+v", err, got)
	}
	if got.ObservedTaskIDs != 2 || got.ObservedAttempts != 2 || got.AttemptZero != 2 ||
		got.FinishedAttempts != 2 || got.SubmittedToWorkerAttempts != 2 || got.FinishedTransitionAttempts != 2 {
		t.Fatalf("unexpected validity counts: %+v", got)
	}
}

func TestBenchTaskValidityRejectsInvalidAttemptsAndStates(t *testing.T) {
	tests := []struct {
		name        string
		expected    int
		events      []string
		wantProblem string
	}{
		{
			name:     "missing expected task ID",
			expected: 2,
			events: []string{
				`{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`,
				`{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"FINISHED","timestamp":"2026-08-08T00:00:01Z"}]}}`,
			},
			wantProblem: "bench task IDs=1, expected=2",
		},
		{
			name:     "retry attempt from definition",
			expected: 1,
			events: []string{
				`{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`,
				`{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":1,"taskName":"bench_task"}}`,
				`{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"FAILED","timestamp":"2026-08-08T00:00:01Z"}]}}`,
				`{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":1,"stateTransitions":[{"state":"FINISHED","timestamp":"2026-08-08T00:00:02Z"}]}}`,
			},
			wantProblem: "bench task attempts=2, expected=1",
		},
		{
			name:     "retry visible only in lifecycle",
			expected: 1,
			events: []string{
				`{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`,
				`{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"FINISHED","timestamp":"2026-08-08T00:00:01Z"}]}}`,
				`{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":1,"stateTransitions":[{"state":"FINISHED","timestamp":"2026-08-08T00:00:02Z"}]}}`,
			},
			wantProblem: "bench task attempts=2, expected=1",
		},
		{
			name:     "definition missing task attempt",
			expected: 1,
			events: []string{
				`{"taskDefinitionEvent":{"taskId":"task-1","taskName":"bench_task"}}`,
				`{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"FINISHED","timestamp":"2026-08-08T00:00:01Z"}]}}`,
			},
			wantProblem: "bench definitions missing taskAttempt=1",
		},
		{
			name:     "lifecycle missing task attempt",
			expected: 1,
			events: []string{
				`{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`,
				`{"taskLifecycleEvent":{"taskId":"task-1","stateTransitions":[{"state":"FINISHED","timestamp":"2026-08-08T00:00:01Z"}]}}`,
			},
			wantProblem: "bench lifecycle events missing taskAttempt=1",
		},
		{
			name:     "latest state is failed",
			expected: 1,
			events: []string{
				`{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`,
				`{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"FINISHED","timestamp":"2026-08-08T00:00:01Z"},{"state":"FAILED","timestamp":"2026-08-08T00:00:02Z"}]}}`,
			},
			wantProblem: "latest state is not FINISHED=1",
		},
		{
			name:     "invalid lifecycle timestamp",
			expected: 1,
			events: []string{
				`{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`,
				`{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"FINISHED","timestamp":"not-a-time"}]}}`,
			},
			wantProblem: "invalid lifecycle transitions=1",
		},
		{
			name:     "ambiguous latest state",
			expected: 1,
			events: []string{
				`{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`,
				`{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"FINISHED","timestamp":"2026-08-08T00:00:01Z"},{"state":"RUNNING","timestamp":"2026-08-08T00:00:01Z"}]}}`,
			},
			wantProblem: "ambiguous latest lifecycle state=1",
		},
		{
			name:     "definition missing task ID",
			expected: 1,
			events: []string{
				`{"taskDefinitionEvent":{"taskAttempt":0,"taskName":"bench_task"}}`,
			},
			wantProblem: "bench definitions missing taskId=1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accumulator := newBenchTaskValidityAccumulator()
			for _, event := range test.events {
				observeValidityEvent(t, accumulator, event)
			}
			got := accumulator.summarize(test.expected)
			err := validateBenchTaskValidity(got)
			if err == nil {
				t.Fatalf("invalid events passed: %+v", got)
			}
			if !strings.Contains(err.Error(), test.wantProblem) {
				t.Fatalf("error %q does not contain %q; verdict=%+v", err, test.wantProblem, got)
			}
			if got.Valid {
				t.Fatalf("invalid events produced valid=true: %+v", got)
			}
		})
	}
}

func TestValidateBenchTaskValidityDoesNotTrustVerdictBit(t *testing.T) {
	validity := BenchTaskValidity{
		ExpectedTaskIDs:            1,
		ObservedTaskIDs:            1,
		ObservedAttempts:           1,
		AttemptZero:                1,
		FinishedAttempts:           1,
		SubmittedToWorkerAttempts:  1,
		FinishedTransitionAttempts: 1,
		Valid:                      false,
	}
	if err := validateBenchTaskValidity(validity); err == nil || !strings.Contains(err.Error(), "validity verdict is false") {
		t.Fatalf("false verdict bit must fail closed, got %v", err)
	}
}

func TestBenchTaskValidityJSONSchema(t *testing.T) {
	events := EventStats{BenchTaskValidity: BenchTaskValidity{
		ExpectedTaskIDs:            1,
		ObservedTaskIDs:            1,
		ObservedAttempts:           1,
		AttemptZero:                1,
		FinishedAttempts:           1,
		SubmittedToWorkerAttempts:  1,
		FinishedTransitionAttempts: 1,
		Valid:                      true,
		Problems:                   []string{},
	}}
	data, err := json.Marshal(events)
	if err != nil {
		t.Fatalf("marshal event stats: %v", err)
	}
	for _, field := range []string{
		`"benchTaskValidity"`,
		`"observedAttempts":1`,
		`"attemptZero":1`,
		`"finishedAttempts":1`,
		`"submittedToWorkerAttempts":1`,
		`"finishedTransitionAttempts":1`,
		`"valid":true`,
		`"problems":[]`,
	} {
		if !strings.Contains(string(data), field) {
			t.Errorf("JSON %s does not contain %s", data, field)
		}
	}
}

func TestBenchTaskLifecycleWindowsDeduplicateAndUseAttemptZero(t *testing.T) {
	accumulator := newBenchTaskValidityAccumulator()
	for _, raw := range []string{
		`{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`,
		`{"taskDefinitionEvent":{"taskId":"task-2","taskAttempt":0,"taskName":"bench_task"}}`,
		`{"taskLifecycleEvent":{"taskId":"task-2","taskAttempt":0,"stateTransitions":[{"state":"FINISHED","timestamp":"2026-08-08T00:00:12Z"},{"state":"SUBMITTED_TO_WORKER","timestamp":"2026-08-08T00:00:10Z"}]}}`,
		`{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"FINISHED","timestamp":"2026-08-08T00:00:09.900Z"},{"state":"SUBMITTED_TO_WORKER","timestamp":"2026-08-08T00:00:00.100Z"}]}}`,
		// Duplicate transitions from another uploaded lifecycle record must not inflate counts.
		`{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"SUBMITTED_TO_WORKER","timestamp":"2026-08-08T00:00:00.100Z"},{"state":"FINISHED","timestamp":"2026-08-08T00:00:09.900Z"}]}}`,
		// A retry is invalid for the formal run and must never enter attempt-0 workload windows.
		`{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":1,"stateTransitions":[{"state":"SUBMITTED_TO_WORKER","timestamp":"2026-08-08T00:00:01Z"},{"state":"FINISHED","timestamp":"2026-08-08T00:00:02Z"}]}}`,
	} {
		observeValidityEvent(t, accumulator, raw)
	}

	rows := accumulator.lifecycleWindows(10 * time.Second)
	if len(rows) != 2 {
		t.Fatalf("got %d lifecycle windows, want 2: %#v", len(rows), rows)
	}
	if rows[0].SubmittedToWorkerAttempts != 1 || rows[0].FinishedAttempts != 1 || rows[0].BacklogDelta != 0 {
		t.Fatalf("unexpected first lifecycle window: %#v", rows[0])
	}
	if rows[1].SubmittedToWorkerAttempts != 1 || rows[1].FinishedAttempts != 1 || rows[1].BacklogDelta != 0 {
		t.Fatalf("unexpected boundary lifecycle window: %#v", rows[1])
	}
	if got := time.Unix(0, rows[1].WindowStartUnixNano).UTC(); !got.Equal(time.Date(2026, 8, 8, 0, 0, 10, 0, time.UTC)) {
		t.Fatalf("exact 10s boundary went to %s", got)
	}

	path := t.TempDir() + "/task_lifecycle_10s.csv"
	if err := writeTaskLifecycleWindowsCSV(path, rows); err != nil {
		t.Fatalf("write lifecycle CSV: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read lifecycle CSV: %v", err)
	}
	if !strings.Contains(string(data), "submitted_to_worker_attempts,finished_attempts,backlog_delta") {
		t.Fatalf("lifecycle CSV missing task fields:\n%s", data)
	}
}

func TestDecodeEventLineFailsClosedOnMalformedJSON(t *testing.T) {
	stats := EventStats{CountByType: map[string]int64{}}
	globalDistinct := map[string]struct{}{}
	jobDistinct := map[string]struct{}{}
	validity := newBenchTaskValidityAccumulator()
	node := newNodeAccumulator()

	err := decodeEventLine(
		`{"eventType":"TASK_LIFECYCLE_EVENT","taskLifecycleEvent":`,
		&stats,
		globalDistinct,
		jobDistinct,
		validity,
		node,
	)
	if err == nil || !strings.Contains(err.Error(), "decode event JSON") {
		t.Fatalf("malformed JSON must return a decode error, got %v", err)
	}
	if stats.TotalEvents != 0 || node.events != 0 {
		t.Fatalf("malformed JSON partially mutated counters: stats=%+v node=%+v", stats, node)
	}
}

func TestBenchTaskValidityRejectsConflictingOrImpossibleTransitionTimes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		lifecycles []string
	}{
		{
			name: "same state has conflicting timestamps",
			lifecycles: []string{
				`{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"SUBMITTED_TO_WORKER","timestamp":"2026-08-08T00:00:01Z"},{"state":"FINISHED","timestamp":"2026-08-08T00:00:03Z"}]}}`,
				`{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"FINISHED","timestamp":"2026-08-08T00:00:04Z"}]}}`,
			},
		},
		{
			name: "finish precedes submission",
			lifecycles: []string{
				`{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"SUBMITTED_TO_WORKER","timestamp":"2026-08-08T00:00:03Z"},{"state":"FINISHED","timestamp":"2026-08-08T00:00:01Z"}]}}`,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			accumulator := newBenchTaskValidityAccumulator()
			observeValidityEvent(t, accumulator, `{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`)
			for _, raw := range tc.lifecycles {
				observeValidityEvent(t, accumulator, raw)
			}
			got := accumulator.summarize(1)
			if got.Valid || got.InvalidLifecycleTransitions != 1 {
				t.Fatalf("invalid transition chronology passed: %#v", got)
			}
		})
	}
}

func TestBenchTaskValidityRejectsTransitionsOutsideRayJobStatusInterval(t *testing.T) {
	accumulator := newBenchTaskValidityAccumulator()
	observeValidityEvent(t, accumulator, `{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`)
	observeValidityEvent(t, accumulator, `{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"SUBMITTED_TO_WORKER","timestamp":"2025-08-08T00:00:01Z"},{"state":"FINISHED","timestamp":"2025-08-08T00:00:02Z"}]}}`)

	jobStart := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	jobEnd := jobStart.Add(time.Minute)
	got := accumulator.summarizeWithin(1, jobStart, jobEnd)
	if got.Valid || got.OutOfRangeLifecycleTransitions != 1 {
		t.Fatalf("stale task lifecycle timestamps passed current RayJob bounds: %#v", got)
	}
}

func TestBenchTaskValidityAcceptsTransitionsInsideRayJobStatusInterval(t *testing.T) {
	accumulator := newBenchTaskValidityAccumulator()
	observeValidityEvent(t, accumulator, `{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`)
	observeValidityEvent(t, accumulator, `{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"SUBMITTED_TO_WORKER","timestamp":"2026-08-08T00:00:01Z"},{"state":"FINISHED","timestamp":"2026-08-08T00:00:02Z"}]}}`)

	jobStart := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	jobEnd := jobStart.Add(time.Minute)
	got := accumulator.summarizeWithin(1, jobStart, jobEnd)
	if !got.Valid || got.OutOfRangeLifecycleTransitions != 0 {
		t.Fatalf("valid task lifecycle timestamps failed Ray job bounds: %#v", got)
	}
}

func TestBenchTaskValidityAcceptsTransitionsInSerializedEndTimeBucket(t *testing.T) {
	accumulator := newBenchTaskValidityAccumulator()
	observeValidityEvent(t, accumulator, `{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`)
	observeValidityEvent(t, accumulator, `{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"SUBMITTED_TO_WORKER","timestamp":"2026-08-08T00:01:00.000166Z"},{"state":"FINISHED","timestamp":"2026-08-08T00:01:00.999999999Z"}]}}`)

	jobStart := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	jobEnd := jobStart.Add(time.Minute)
	got := accumulator.summarizeWithin(1, jobStart, jobEnd)
	if !got.Valid || got.OutOfRangeLifecycleTransitions != 0 {
		t.Fatalf("valid transitions in serialized RayJob end-time bucket failed: %#v", got)
	}
}

func TestRayJobStatusTimeRoundTripUsesWholeSecondPrecision(t *testing.T) {
	original := metav1.NewTime(time.Date(2026, 8, 8, 0, 1, 0, 267642755, time.UTC))
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal metav1.Time: %v", err)
	}
	var roundTrip metav1.Time
	if err := json.Unmarshal(raw, &roundTrip); err != nil {
		t.Fatalf("unmarshal metav1.Time: %v", err)
	}
	if got := roundTrip.Nanosecond(); got != 0 {
		t.Fatalf("metav1.Time retained subsecond precision: %d", got)
	}
}

func TestBenchTaskValidityKeepsSerializedStartAsInclusiveLowerBound(t *testing.T) {
	jobStart := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	jobEnd := jobStart.Add(time.Minute)
	for _, tc := range []struct {
		name        string
		submittedAt string
		wantValid   bool
	}{
		{name: "exact start", submittedAt: "2026-08-08T00:00:00Z", wantValid: true},
		{name: "one nanosecond before start", submittedAt: "2026-08-07T23:59:59.999999999Z", wantValid: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			accumulator := newBenchTaskValidityAccumulator()
			observeValidityEvent(t, accumulator, `{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`)
			observeValidityEvent(t, accumulator, `{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"SUBMITTED_TO_WORKER","timestamp":"`+tc.submittedAt+`"},{"state":"FINISHED","timestamp":"2026-08-08T00:00:01Z"}]}}`)
			got := accumulator.summarizeWithin(1, jobStart, jobEnd)
			if got.Valid != tc.wantValid {
				t.Fatalf("valid=%v, want %v: %#v", got.Valid, tc.wantValid, got)
			}
		})
	}
}

func TestBenchTaskValidityAcceptsSameSerializedStartAndEndSecond(t *testing.T) {
	accumulator := newBenchTaskValidityAccumulator()
	observeValidityEvent(t, accumulator, `{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`)
	observeValidityEvent(t, accumulator, `{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"SUBMITTED_TO_WORKER","timestamp":"2026-08-08T00:00:00.100Z"},{"state":"FINISHED","timestamp":"2026-08-08T00:00:00.900Z"}]}}`)

	jobTime := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	got := accumulator.summarizeWithin(1, jobTime, jobTime)
	if !got.Valid || got.OutOfRangeLifecycleTransitions != 0 {
		t.Fatalf("subsecond job with equal serialized bounds failed: %#v", got)
	}
}

func TestBenchTaskValidityRejectsTransitionInFollowingSecond(t *testing.T) {
	accumulator := newBenchTaskValidityAccumulator()
	observeValidityEvent(t, accumulator, `{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`)
	observeValidityEvent(t, accumulator, `{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"SUBMITTED_TO_WORKER","timestamp":"2026-08-08T00:00:59.999999999Z"},{"state":"FINISHED","timestamp":"2026-08-08T00:01:01Z"}]}}`)

	jobStart := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	jobEnd := jobStart.Add(time.Minute)
	got := accumulator.summarizeWithin(1, jobStart, jobEnd)
	if got.Valid || got.OutOfRangeLifecycleTransitions != 1 {
		t.Fatalf("transition after serialized RayJob end-time bucket passed: %#v", got)
	}
}

func TestBenchTaskValidityRejectsInvalidRayJobStatusInterval(t *testing.T) {
	for _, bounds := range []struct {
		name     string
		jobStart time.Time
		jobEnd   time.Time
	}{
		{name: "zero bounds"},
		{
			name:     "reversed bounds",
			jobStart: time.Date(2026, 8, 8, 0, 1, 0, 0, time.UTC),
			jobEnd:   time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC),
		},
	} {
		t.Run(bounds.name, func(t *testing.T) {
			accumulator := newBenchTaskValidityAccumulator()
			observeValidityEvent(t, accumulator, `{"taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`)
			observeValidityEvent(t, accumulator, `{"taskLifecycleEvent":{"taskId":"task-1","taskAttempt":0,"stateTransitions":[{"state":"SUBMITTED_TO_WORKER","timestamp":"2026-08-08T00:00:01Z"},{"state":"FINISHED","timestamp":"2026-08-08T00:00:02Z"}]}}`)
			got := accumulator.summarizeWithin(1, bounds.jobStart, bounds.jobEnd)
			if got.Valid || got.OutOfRangeLifecycleTransitions != 1 {
				t.Fatalf("invalid Ray job bounds passed: %#v", got)
			}
		})
	}
}

func TestDecodeEventLineRejectsPayloadEventTypeMismatchWithoutMutation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		raw       string
		wantError string
	}{
		{
			name:      "payload does not match event type",
			raw:       `{"eventType":"TASK_PROFILE_EVENT","taskDefinitionEvent":{"taskId":"task-1","taskAttempt":0,"taskName":"bench_task"}}`,
			wantError: "does not match eventType",
		},
		{
			name:      "event type is missing",
			raw:       `{"timestamp":"2026-08-08T00:00:00Z"}`,
			wantError: "eventType is missing",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stats := EventStats{CountByType: map[string]int64{}}
			globalDistinct := map[string]struct{}{}
			jobDistinct := map[string]struct{}{}
			validity := newBenchTaskValidityAccumulator()
			node := newNodeAccumulator()

			err := decodeEventLine(tc.raw, &stats, globalDistinct, jobDistinct, validity, node)
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("invalid event must fail closed with %q, got %v", tc.wantError, err)
			}
			if stats.TotalEvents != 0 || node.events != 0 || len(globalDistinct) != 0 || len(validity.taskIDs) != 0 {
				t.Fatalf("invalid event partially mutated counters: stats=%+v node=%+v", stats, node)
			}
		})
	}
}
