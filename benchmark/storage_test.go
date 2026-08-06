package benchmark

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	. "github.com/onsi/gomega"

	. "github.com/ray-project/kuberay/historyserver/test/support"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// bucketSnapshot maps every object key to its size at one point in time.
type bucketSnapshot map[string]int64

// takeBucketSnapshot inventories the whole bucket (paginated). Snapshots taken
// before/after each phase turn storage accounting into explicit diffs instead
// of trusting that all writes land under the session prefix.
func takeBucketSnapshot(s3Client *s3.S3, bucket string) (bucketSnapshot, error) {
	snap := bucketSnapshot{}
	err := s3Client.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}, func(page *s3.ListObjectsV2Output, _ bool) bool {
		for _, obj := range page.Contents {
			snap[aws.StringValue(obj.Key)] = aws.Int64Value(obj.Size)
		}
		return true
	})
	return snap, err
}

// SnapshotDiff is the delta between two bucket snapshots.
type SnapshotDiff struct {
	Label          string   `json:"label"`
	AddedObjects   int      `json:"addedObjects"`
	AddedBytes     int64    `json:"addedBytes"`
	ChangedObjects int      `json:"changedObjects"` // same key, size changed (e.g. fetched_endpoints overwrites)
	ChangedBytes   int64    `json:"changedBytes"`   // net byte delta of changed keys
	DeletedObjects int      `json:"deletedObjects"`
	UnexpectedKeys []string `json:"unexpectedKeys"` // added keys outside the expected prefixes (first 20)
}

// diffSnapshots compares two snapshots and flags additions that fall outside
// expectedPrefixes — catching writes the benchmark did not anticipate.
func diffSnapshots(label string, before, after bucketSnapshot, expectedPrefixes []string) SnapshotDiff {
	d := SnapshotDiff{Label: label}
	for key, size := range after {
		prev, existed := before[key]
		switch {
		case !existed:
			d.AddedObjects++
			d.AddedBytes += size
			expected := false
			for _, p := range expectedPrefixes {
				if strings.HasPrefix(key, p) {
					expected = true
					break
				}
			}
			if !expected && len(d.UnexpectedKeys) < 20 {
				d.UnexpectedKeys = append(d.UnexpectedKeys, key)
			}
		case prev != size:
			d.ChangedObjects++
			d.ChangedBytes += size - prev
		}
	}
	for key := range before {
		if _, ok := after[key]; !ok {
			d.DeletedObjects++
		}
	}
	return d
}

// ensureBenchS3Client is EnsureS3Client with a benchmark-owned local port:
// port 9000 is contended by e2e suites (their EnsureS3Client hardcodes it, and
// possibly against a DIFFERENT cluster — a silent cross-wiring hazard), so the
// benchmark forwards its own port and never touches 9000.
func ensureBenchS3Client(t *testing.T, localPort int) *s3.S3 {
	test := With(t)
	g := NewWithT(t)
	ApplyMinIO(test, g)

	stop := spawnMinioForward(t, localPort)
	t.Cleanup(stop)

	endpoint := fmt.Sprintf("http://localhost:%d", localPort)
	g.Eventually(func() error {
		c, err := NewS3Client(endpoint)
		if err != nil {
			return err
		}
		_, err = c.ListBuckets(&s3.ListBucketsInput{})
		return err
	}, TestTimeoutMedium).Should(Succeed(), "MinIO should be reachable on %s", endpoint)
	LogWithTimestamp(t, "Port-forwarded MinIO to localhost:%d (benchmark-owned)", localPort)

	client, err := NewS3Client(endpoint)
	g.Expect(err).NotTo(HaveOccurred())
	return client
}

// spawnMinioForward starts a kubectl port-forward owned by this process. It
// inherits KUBECONFIG from the environment, so it targets the same cluster as
// every other benchmark call.
func spawnMinioForward(t *testing.T, localPort int) (stop func()) {
	cmd := exec.Command("kubectl", "-n", MinioNamespace, "port-forward", "svc/minio-service",
		fmt.Sprintf("%d:%d", localPort, MinioAPIPort))
	if err := cmd.Start(); err != nil {
		t.Fatalf("start minio port-forward on %d: %v", localPort, err)
	}
	return func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}
}

// startS3Watchdog keeps the benchmark's MinIO tunnel reachable. The kubectl
// port-forward can die mid-run (observed once as A-n1000: every later snapshot,
// marker wait, and scan then fails with "send request failed"). The watchdog
// probes HeadBucket every 5s and respawns the forward — inheriting the
// process's KUBECONFIG, so it targets the same cluster — after two consecutive
// failures.
func startS3Watchdog(t *testing.T, s3Client *s3.S3, bucket string, localPort int) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		var child *exec.Cmd
		defer func() {
			if child != nil && child.Process != nil {
				_ = child.Process.Kill()
				_ = child.Wait()
			}
		}()
		failures := 0
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := s3Client.HeadBucket(&s3.HeadBucketInput{Bucket: aws.String(bucket)}); err == nil {
					failures = 0
					continue
				}
				failures++
				if failures < 2 {
					continue
				}
				t.Logf("s3 watchdog: localhost:%d unreachable twice, respawning port-forward", localPort)
				if child != nil && child.Process != nil {
					_ = child.Process.Kill()
					_ = child.Wait()
				}
				child = exec.Command("kubectl", "-n", MinioNamespace, "port-forward", "svc/minio-service",
					fmt.Sprintf("%d:%d", localPort, MinioAPIPort))
				if err := child.Start(); err != nil {
					t.Logf("s3 watchdog: respawn failed: %v", err)
					child = nil
					continue
				}
				failures = 0
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// waitForObject polls HeadObject until the key exists or the timeout expires.
func waitForObject(s3Client *s3.S3, bucket, key string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		_, err := s3Client.HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("object %s not visible within %s: %w", key, timeout, err)
		}
		time.Sleep(2 * time.Second)
	}
}

// StorageReport quantifies what one session left in the bucket.
type StorageReport struct {
	TotalBytes    int64            `json:"totalBytes"`
	ObjectCount   int              `json:"objectCount"`
	Categories    map[string]int64 `json:"categories"` // bytes by job_events/node_events/logs/fetched_endpoints/other
	MarkerPresent bool             `json:"markerPresent"`
	Events        EventStats       `json:"events"`
}

// EventStats is derived by decoding every uploaded event file line by line.
type EventStats struct {
	CountByType         map[string]int64 `json:"countByType"`
	TotalEvents         int64            `json:"totalEvents"`
	TaskScopedEvents    int64            `json:"taskScopedEvents"`   // TASK_* + ACTOR_TASK_*
	RawJSONLBytes       int64            `json:"rawJSONLBytes"`      // decompressed logical bytes
	StoredEventBytes    int64            `json:"storedEventBytes"`   // object sizes as stored
	DistinctTaskDefIDs  int              `json:"distinctTaskDefIDs"` // across ALL jobs in the session (system tasks included)
	BenchJobID          string           `json:"benchJobID"`         // job directory with the most distinct definition taskIds
	BenchJobTaskIDs     int              `json:"benchJobTaskIDs"`    // distinct definition taskIds within that job
	BenchTaskIDs        int              `json:"benchTaskIDs"`       // distinct definition taskIds named bench_task, across every job — the loss denominator, and the only one that works with several drivers
	ExpectedTasks       int              `json:"expectedTasks"`
	EventsPerTask       float64          `json:"eventsPerTask"`
	AvgRawBytesPerEvent float64          `json:"avgRawBytesPerEvent"`
	CompressionRatio    float64          `json:"compressionRatio"` // stored/raw; 1.0 when compression is off
	PerNode             []NodeEventStats `json:"perNode"`
}

// NodeEventStats attributes event traffic to the Ray node whose aggregator
// emitted it — the per-node view that collector sizing needs. Which node a
// task's events land on depends on WHO emits them: definition events come from
// the owner (the driver's node), execution-side events from the executing node.
type NodeEventStats struct {
	NodeID              string           `json:"nodeID"`
	Events              int64            `json:"events"`
	RawBytes            int64            `json:"rawBytes"`
	DistinctTaskIDs     int              `json:"distinctTaskIDs"` // any task-scoped payload with a taskId
	Peak1sEvents        int64            `json:"peak1sEvents"`
	Peak10sEventsPerSec float64          `json:"peak10sEventsPerSec"`
	CountByType         map[string]int64 `json:"countByType"`
}

// eventProbe decodes only the fields the report needs; everything else in the
// event JSON is skipped by encoding/json.
type eventProbe struct {
	EventType string `json:"eventType"`
	Timestamp string `json:"timestamp"`
	TaskDef   *struct {
		TaskID   string `json:"taskId"`
		TaskName string `json:"taskName"`
		TaskFunc *struct {
			FunctionName string `json:"functionName"`
		} `json:"taskFunc"`
	} `json:"taskDefinitionEvent"`
	TaskLifecycle *struct {
		TaskID string `json:"taskId"`
	} `json:"taskLifecycleEvent"`
	ActorTaskDef *struct {
		TaskID string `json:"taskId"`
	} `json:"actorTaskDefinitionEvent"`
}

func (p eventProbe) taskDefID() string {
	if p.TaskDef == nil {
		return ""
	}
	return p.TaskDef.TaskID
}

func (p eventProbe) lifecycleID() string {
	if p.TaskLifecycle == nil {
		return ""
	}
	return p.TaskLifecycle.TaskID
}

func (p eventProbe) actorTaskDefID() string {
	if p.ActorTaskDef == nil {
		return ""
	}
	return p.ActorTaskDef.TaskID
}

// nodeAccumulator gathers per-node statistics during the scan.
type nodeAccumulator struct {
	events      int64
	rawBytes    int64
	distinct    map[string]struct{}
	countByType map[string]int64
	perSecond   map[int64]int64 // unix second -> events
}

func newNodeAccumulator() *nodeAccumulator {
	return &nodeAccumulator{
		distinct:    map[string]struct{}{},
		countByType: map[string]int64{},
		perSecond:   map[int64]int64{},
	}
}

// buildStorageReport walks the session prefix (with proper pagination — the
// e2e helper caps at 1000 keys), decodes every event file, and attributes
// events to nodes. The per-node per-second series is written to node_rate.csv.
func buildStorageReport(t *testing.T, s3Client *s3.S3, bucket, sessionPrefix, markerKey string, cfg benchConfig, runDir string) StorageReport {
	rep := StorageReport{
		Categories: map[string]int64{},
		Events: EventStats{
			CountByType:   map[string]int64{},
			ExpectedTasks: cfg.TaskCount,
		},
	}

	var eventKeys []string
	err := s3Client.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(sessionPrefix),
	}, func(page *s3.ListObjectsV2Output, _ bool) bool {
		for _, obj := range page.Contents {
			key, size := aws.StringValue(obj.Key), aws.Int64Value(obj.Size)
			rep.TotalBytes += size
			rep.ObjectCount++
			switch {
			case strings.Contains(key, "/job_events/"):
				rep.Categories["job_events"] += size
				rep.Events.StoredEventBytes += size
				eventKeys = append(eventKeys, key)
			case strings.Contains(key, "/node_events/"):
				rep.Categories["node_events"] += size
				rep.Events.StoredEventBytes += size
				eventKeys = append(eventKeys, key)
			case strings.Contains(key, "/logs/"):
				rep.Categories["logs"] += size
			case strings.Contains(key, "/fetched_endpoints/"):
				rep.Categories["fetched_endpoints"] += size
			default:
				rep.Categories["other"] += size
			}
		}
		return true
	})
	if err != nil {
		t.Errorf("list session prefix %s: %v", sessionPrefix, err)
		return rep
	}

	if _, err := s3Client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(markerKey),
	}); err == nil {
		rep.MarkerPresent = true
	}

	globalDistinct := make(map[string]struct{}, cfg.TaskCount)
	benchDistinct := make(map[string]struct{}, cfg.TaskCount)
	jobDistinct := map[string]map[string]struct{}{}
	nodes := map[string]*nodeAccumulator{}
	for i, key := range eventKeys {
		nodeID := nodeIDFromKey(key, sessionPrefix)
		acc := nodes[nodeID]
		if acc == nil {
			acc = newNodeAccumulator()
			nodes[nodeID] = acc
		}
		var jobSet map[string]struct{}
		if jobID := jobIDFromKey(key); jobID != "" {
			jobSet = jobDistinct[jobID]
			if jobSet == nil {
				jobSet = map[string]struct{}{}
				jobDistinct[jobID] = jobSet
			}
		}
		if err := decodeEventObject(s3Client, bucket, key, &rep.Events, globalDistinct, jobSet, benchDistinct, acc); err != nil {
			t.Errorf("decode event object %s: %v", key, err)
		}
		if (i+1)%20 == 0 {
			t.Logf("storage scan: decoded %d/%d event files", i+1, len(eventKeys))
		}
	}

	rep.Events.DistinctTaskDefIDs = len(globalDistinct)
	// Attribute the loss denominator to the benchmark's own job: distinct IDs
	// across ALL jobs can mask benchmark losses with unrelated system tasks
	// (e.g. 100,005 total could be 99,995 benchmark + 10 system).
	for jobID, set := range jobDistinct {
		if len(set) > rep.Events.BenchJobTaskIDs {
			rep.Events.BenchJobTaskIDs = len(set)
			rep.Events.BenchJobID = jobID
		}
	}
	rep.Events.BenchTaskIDs = len(benchDistinct)
	if cfg.TaskCount > 0 {
		rep.Events.EventsPerTask = float64(rep.Events.TaskScopedEvents) / float64(cfg.TaskCount)
	}
	if rep.Events.TotalEvents > 0 {
		rep.Events.AvgRawBytesPerEvent = float64(rep.Events.RawJSONLBytes) / float64(rep.Events.TotalEvents)
	}
	if rep.Events.RawJSONLBytes > 0 {
		rep.Events.CompressionRatio = float64(rep.Events.StoredEventBytes) / float64(rep.Events.RawJSONLBytes)
	}
	rep.Events.PerNode = summarizeNodes(nodes)

	if err := writeNodeRateCSV(filepath.Join(runDir, "node_rate.csv"), nodes); err != nil {
		t.Logf("write node_rate.csv: %v", err)
	}
	return rep
}

// nodeIDFromKey extracts the nodeID path segment following the session prefix:
// {sessionPrefix}{nodeID}/{job_events|node_events}/...
func nodeIDFromKey(key, sessionPrefix string) string {
	rel := strings.TrimPrefix(key, sessionPrefix)
	if idx := strings.Index(rel, "/"); idx > 0 {
		return rel[:idx]
	}
	return "unknown"
}

// jobIDFromKey extracts the jobID directory from an event file key
// (…/job_events/{jobID}/{file}); node_events files return "".
func jobIDFromKey(key string) string {
	const marker = "/job_events/"
	idx := strings.Index(key, marker)
	if idx < 0 {
		return ""
	}
	rest := key[idx+len(marker):]
	if end := strings.Index(rest, "/"); end > 0 {
		return rest[:end]
	}
	return ""
}

// decodeEventObject streams one JSONL(.gz) object line by line so the test
// process never holds a whole event file in memory.
// benchTaskName is the remote function the driver submits; every other task in
// the session (the driver's own, Ray internals) is excluded by matching on it.
const benchTaskName = "bench_task"

func decodeEventObject(s3Client *s3.S3, bucket, key string, stats *EventStats, globalDistinct, jobDistinct, benchDistinct map[string]struct{}, acc *nodeAccumulator) error {
	obj, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	defer obj.Body.Close()

	var reader io.Reader = obj.Body
	if strings.HasSuffix(key, ".gz") {
		gz, err := gzip.NewReader(obj.Body)
		if err != nil {
			return err
		}
		defer gz.Close()
		reader = gz
	}

	br := bufio.NewReaderSize(reader, 1024*1024)
	for {
		line, err := br.ReadBytes('\n')
		trimmed := strings.TrimSpace(string(line))
		if trimmed != "" {
			stats.RawJSONLBytes += int64(len(line))
			acc.rawBytes += int64(len(line))
			var probe eventProbe
			if jerr := json.Unmarshal([]byte(trimmed), &probe); jerr == nil {
				stats.TotalEvents++
				stats.CountByType[probe.EventType]++
				acc.events++
				acc.countByType[probe.EventType]++
				if strings.HasPrefix(probe.EventType, "TASK_") || strings.HasPrefix(probe.EventType, "ACTOR_TASK_") {
					stats.TaskScopedEvents++
				}
				if probe.TaskDef != nil && probe.TaskDef.TaskID != "" {
					globalDistinct[probe.TaskDef.TaskID] = struct{}{}
					if jobDistinct != nil {
						jobDistinct[probe.TaskDef.TaskID] = struct{}{}
					}
					// Name-based counting is the only denominator that stays
					// correct with several drivers, and it excludes the driver's
					// own task, which otherwise hides one lost benchmark task.
					name := probe.TaskDef.TaskName
					if name == "" && probe.TaskDef.TaskFunc != nil {
						name = probe.TaskDef.TaskFunc.FunctionName
					}
					if strings.Contains(name, benchTaskName) && benchDistinct != nil {
						benchDistinct[probe.TaskDef.TaskID] = struct{}{}
					}
				}
				for _, id := range []string{probe.taskDefID(), probe.lifecycleID(), probe.actorTaskDefID()} {
					if id != "" {
						acc.distinct[id] = struct{}{}
					}
				}
				if ts, terr := time.Parse(time.RFC3339Nano, probe.Timestamp); terr == nil {
					acc.perSecond[ts.Unix()]++
				}
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// summarizeNodes reduces accumulators to reportable per-node rows, including
// the peak 1s and peak 10s-window event rates.
func summarizeNodes(nodes map[string]*nodeAccumulator) []NodeEventStats {
	var out []NodeEventStats
	for nodeID, acc := range nodes {
		row := NodeEventStats{
			NodeID:          nodeID,
			Events:          acc.events,
			RawBytes:        acc.rawBytes,
			DistinctTaskIDs: len(acc.distinct),
			CountByType:     acc.countByType,
		}
		secs := make([]int64, 0, len(acc.perSecond))
		for s := range acc.perSecond {
			secs = append(secs, s)
		}
		sort.Slice(secs, func(i, j int) bool { return secs[i] < secs[j] })
		for i, s := range secs {
			if acc.perSecond[s] > row.Peak1sEvents {
				row.Peak1sEvents = acc.perSecond[s]
			}
			var windowSum int64
			for j := i; j < len(secs) && secs[j] < s+10; j++ {
				windowSum += acc.perSecond[secs[j]]
			}
			if rate := float64(windowSum) / 10; rate > row.Peak10sEventsPerSec {
				row.Peak10sEventsPerSec = rate
			}
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Events > out[j].Events })
	return out
}

// writeNodeRateCSV dumps the per-node per-second event counts for plotting
// against the same-node collector resource series.
func writeNodeRateCSV(path string, nodes map[string]*nodeAccumulator) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintln(f, "node_id,unix_second,events"); err != nil {
		return err
	}
	for nodeID, acc := range nodes {
		secs := make([]int64, 0, len(acc.perSecond))
		for s := range acc.perSecond {
			secs = append(secs, s)
		}
		sort.Slice(secs, func(i, j int) bool { return secs[i] < secs[j] })
		for _, s := range secs {
			if _, err := fmt.Fprintf(f, "%s,%d,%d\n", nodeID, s, acc.perSecond[s]); err != nil {
				return err
			}
		}
	}
	return nil
}
