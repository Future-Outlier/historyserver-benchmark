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
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	. "github.com/onsi/gomega"

	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clustermetadata"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	. "github.com/ray-project/kuberay/historyserver/test/support"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// objectSnapshot records both size and ETag. Size alone cannot detect a
// same-size overwrite of an immutable object.
type objectSnapshot struct {
	Size int64
	ETag string
}

// bucketSnapshot maps every object key to its identity at one point in time.
type bucketSnapshot map[string]objectSnapshot

// ensureBenchmarkS3Bucket is intentionally fixed to the dedicated benchmark
// bucket. It makes a pre-Collector whole-bucket baseline possible without
// granting the harness authority over the shared e2e bucket.
func ensureBenchmarkS3Bucket(s3Client s3iface.S3API) error {
	if s3Client == nil {
		return fmt.Errorf("benchmark S3 client is nil")
	}
	input := &s3.HeadBucketInput{Bucket: aws.String(benchmarkS3BucketName)}
	if _, err := s3Client.HeadBucket(input); err == nil {
		return nil
	}
	if _, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(benchmarkS3BucketName),
	}); err != nil {
		// A concurrent creator is harmless only if the fixed bucket is now
		// observable. Any other failure remains fail-closed.
		if _, headErr := s3Client.HeadBucket(input); headErr != nil {
			return fmt.Errorf("create dedicated benchmark bucket: %w (head after failure: %v)", err, headErr)
		}
		return nil
	}
	if _, err := s3Client.HeadBucket(input); err != nil {
		return fmt.Errorf("dedicated benchmark bucket is not observable after create: %w", err)
	}
	return nil
}

type recordingBucketEnsurer struct {
	s3iface.S3API
	exists        bool
	headCalls     int
	createBuckets []string
}

func (e *recordingBucketEnsurer) HeadBucket(*s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	e.headCalls++
	if !e.exists {
		return nil, fmt.Errorf("not found")
	}
	return &s3.HeadBucketOutput{}, nil
}

func (e *recordingBucketEnsurer) CreateBucket(input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	e.createBuckets = append(e.createBuckets, aws.StringValue(input.Bucket))
	e.exists = true
	return &s3.CreateBucketOutput{}, nil
}

func TestEnsureBenchmarkS3BucketCanOnlyCreateDedicatedBucket(t *testing.T) {
	client := &recordingBucketEnsurer{}
	if err := ensureBenchmarkS3Bucket(client); err != nil {
		t.Fatal(err)
	}
	if !stringSlicesEqual(client.createBuckets, []string{benchmarkS3BucketName}) {
		t.Fatalf("created buckets=%v, want only %q", client.createBuckets, benchmarkS3BucketName)
	}
	if client.headCalls != 2 {
		t.Fatalf("HeadBucket calls=%d, want before and after create", client.headCalls)
	}
	if benchmarkS3BucketName == S3BucketName {
		t.Fatalf("benchmark bucket unexpectedly aliases shared e2e bucket %q", S3BucketName)
	}
}

// deleteBenchmarkRunObjects derives its deletion scope from a validated source
// identity. It cannot accept a caller-provided prefix or bucket, never deletes
// the bucket, and therefore cannot erase another immutable benchmark session.
func deleteBenchmarkRunObjects(s3Client s3iface.S3API, namespace, cluster, session string) error {
	if s3Client == nil {
		return fmt.Errorf("benchmark S3 client is nil")
	}
	if err := validateHSSourceIdentity(namespace, cluster, session); err != nil {
		return fmt.Errorf("refuse benchmark cleanup for invalid source identity: %w", err)
	}
	sessionPrefix := clusterlogs.SessionDir("log", "", "", namespace, cluster, session) + "/"
	markerKey := clustermetadata.EncodePath(
		utils.ClusterInfo{Namespace: namespace, Name: cluster}, "log", session)
	if sessionPrefix == "" || markerKey == "" || strings.HasSuffix(sessionPrefix, "//") {
		return fmt.Errorf("refuse benchmark cleanup with an empty or broad scope")
	}

	var objects []*s3.ObjectIdentifier
	var callbackErr error
	err := s3Client.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(benchmarkS3BucketName),
		Prefix: aws.String(sessionPrefix),
	}, func(page *s3.ListObjectsV2Output, _ bool) bool {
		for _, object := range page.Contents {
			key := aws.StringValue(object.Key)
			if key == "" || !strings.HasPrefix(key, sessionPrefix) {
				callbackErr = fmt.Errorf("session listing returned key %q outside exact prefix %q", key, sessionPrefix)
				return false
			}
			objects = append(objects, &s3.ObjectIdentifier{Key: aws.String(key)})
		}
		return true
	})
	if err != nil {
		return fmt.Errorf("list benchmark bucket %s: %w", benchmarkS3BucketName, err)
	}
	if callbackErr != nil {
		return callbackErr
	}
	// S3 DeleteObjects is idempotent, so delete the exact metadata marker even
	// when the run stopped before creating it. Never list/delete its parent
	// prefix: that prefix contains markers for every session in the cluster.
	objects = append(objects, &s3.ObjectIdentifier{Key: aws.String(markerKey)})
	sort.Slice(objects, func(i, j int) bool {
		return aws.StringValue(objects[i].Key) < aws.StringValue(objects[j].Key)
	})
	for start := 0; start < len(objects); start += 1000 {
		end := start + 1000
		if end > len(objects) {
			end = len(objects)
		}
		result, err := s3Client.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: aws.String(benchmarkS3BucketName),
			Delete: &s3.Delete{Objects: objects[start:end], Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("delete benchmark run objects: %w", err)
		}
		if result == nil {
			return fmt.Errorf("delete benchmark run objects returned a nil result")
		}
		if len(result.Errors) != 0 {
			return fmt.Errorf("delete benchmark run objects returned %d errors", len(result.Errors))
		}
	}
	return nil
}

type recordingBucketCleaner struct {
	s3iface.S3API
	objects            map[string]struct{}
	listBuckets        []string
	listPrefixes       []string
	deleteBuckets      []string
	deletedObjectKeys  []string
	deleteObjectsError error
	deleteBucketCalled bool
}

func (c *recordingBucketCleaner) ListObjectsV2Pages(
	input *s3.ListObjectsV2Input,
	callback func(*s3.ListObjectsV2Output, bool) bool,
) error {
	c.listBuckets = append(c.listBuckets, aws.StringValue(input.Bucket))
	prefix := aws.StringValue(input.Prefix)
	c.listPrefixes = append(c.listPrefixes, prefix)
	contents := make([]*s3.Object, 0)
	for key := range c.objects {
		if strings.HasPrefix(key, prefix) {
			contents = append(contents, &s3.Object{Key: aws.String(key)})
		}
	}
	sort.Slice(contents, func(i, j int) bool {
		return aws.StringValue(contents[i].Key) < aws.StringValue(contents[j].Key)
	})
	callback(&s3.ListObjectsV2Output{Contents: contents}, true)
	return nil
}

func (c *recordingBucketCleaner) DeleteObjects(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	c.deleteBuckets = append(c.deleteBuckets, aws.StringValue(input.Bucket))
	if c.deleteObjectsError != nil {
		return nil, c.deleteObjectsError
	}
	for _, object := range input.Delete.Objects {
		key := aws.StringValue(object.Key)
		c.deletedObjectKeys = append(c.deletedObjectKeys, key)
		delete(c.objects, key)
	}
	return &s3.DeleteObjectsOutput{}, nil
}

func (c *recordingBucketCleaner) DeleteBucket(input *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error) {
	c.deleteBucketCalled = true
	c.deleteBuckets = append(c.deleteBuckets, aws.StringValue(input.Bucket))
	return &s3.DeleteBucketOutput{}, nil
}

func TestDeleteBenchmarkRunObjectsPreservesOtherSessionsAndBucket(t *testing.T) {
	const (
		namespace = "test-ns-vp8s9"
		cluster   = "rayjob-bench-rjff2"
		session   = "session_2026-08-08_10-04-32_015260_1"
		other     = "session_2026-08-08_10-05-00_000000_2"
	)
	targetPrefix := clusterlogs.SessionDir("log", "", "", namespace, cluster, session) + "/"
	otherPrefix := clusterlogs.SessionDir("log", "", "", namespace, cluster, other) + "/"
	targetMarker := clustermetadata.EncodePath(utils.ClusterInfo{Namespace: namespace, Name: cluster}, "log", session)
	otherMarker := clustermetadata.EncodePath(utils.ClusterInfo{Namespace: namespace, Name: cluster}, "log", other)
	clusterMarkerDirectory := strings.TrimSuffix(targetMarker, session)
	immutable := []string{
		otherPrefix + "worker/logs/events.json.gz",
		otherMarker,
		clusterMarkerDirectory,
		"log/unrelated/immutable-object",
	}
	client := &recordingBucketCleaner{objects: map[string]struct{}{
		targetPrefix + "head/logs/events.json.gz":   {},
		targetPrefix + "worker/logs/events.json.gz": {},
		targetMarker: {},
	}}
	for _, key := range immutable {
		client.objects[key] = struct{}{}
	}
	if err := deleteBenchmarkRunObjects(client, namespace, cluster, session); err != nil {
		t.Fatal(err)
	}
	if !stringSlicesEqual(client.listBuckets, []string{benchmarkS3BucketName}) {
		t.Fatalf("listed buckets=%v, want only %q", client.listBuckets, benchmarkS3BucketName)
	}
	if !stringSlicesEqual(client.listPrefixes, []string{targetPrefix}) {
		t.Fatalf("listed prefixes=%v, want exact session prefix %q", client.listPrefixes, targetPrefix)
	}
	for _, bucket := range client.deleteBuckets {
		if bucket != benchmarkS3BucketName {
			t.Fatalf("cleanup targeted bucket %q, want only %q", bucket, benchmarkS3BucketName)
		}
		if bucket == S3BucketName {
			t.Fatalf("cleanup targeted shared e2e bucket %q", S3BucketName)
		}
	}
	wantDeleted := []string{
		targetMarker,
		targetPrefix + "head/logs/events.json.gz",
		targetPrefix + "worker/logs/events.json.gz",
	}
	sort.Strings(wantDeleted)
	if !stringSlicesEqual(client.deletedObjectKeys, wantDeleted) {
		t.Fatalf("deleted keys=%v, want only %v", client.deletedObjectKeys, wantDeleted)
	}
	if client.deleteBucketCalled {
		t.Fatal("benchmark cleanup must never delete the bucket")
	}
	for _, key := range immutable {
		if _, ok := client.objects[key]; !ok {
			t.Fatalf("immutable object %q was deleted", key)
		}
	}
}

func TestDeleteBenchmarkRunObjectsFailsClosed(t *testing.T) {
	client := &recordingBucketCleaner{
		objects:            map[string]struct{}{},
		deleteObjectsError: fmt.Errorf("synthetic delete failure"),
	}
	if err := deleteBenchmarkRunObjects(client, "test-ns", "ray-cluster", "session_1"); err == nil {
		t.Fatal("object deletion failure was ignored")
	}
	if client.deleteBucketCalled {
		t.Fatal("cleanup must never delete the bucket")
	}
	for _, invalid := range [][3]string{
		{"", "ray-cluster", "session_1"},
		{"..", "ray-cluster", "session_1"},
		{"test-ns", ".", "session_1"},
		{"test-ns", "ray-cluster", ".."},
		{"test-ns", "ray-cluster", "session/1"},
	} {
		if err := deleteBenchmarkRunObjects(client, invalid[0], invalid[1], invalid[2]); err == nil {
			t.Fatalf("unsafe cleanup identity %q was accepted", invalid)
		}
	}
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// takeBucketSnapshot inventories the whole bucket (paginated). Snapshots taken
// before/after each phase turn storage accounting into explicit diffs instead
// of trusting that all writes land under the session prefix.
func takeBucketSnapshot(s3Client s3iface.S3API, bucket string) (bucketSnapshot, error) {
	snap := bucketSnapshot{}
	var callbackErr error
	err := s3Client.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}, func(page *s3.ListObjectsV2Output, _ bool) bool {
		for _, obj := range page.Contents {
			if obj == nil {
				callbackErr = fmt.Errorf("bucket snapshot contains a nil object")
				return false
			}
			key := aws.StringValue(obj.Key)
			etag := aws.StringValue(obj.ETag)
			if key == "" || etag == "" {
				callbackErr = fmt.Errorf("bucket snapshot object has empty key or ETag: key=%q etag=%q", key, etag)
				return false
			}
			if _, exists := snap[key]; exists {
				callbackErr = fmt.Errorf("bucket snapshot contains duplicate key %q", key)
				return false
			}
			snap[key] = objectSnapshot{Size: aws.Int64Value(obj.Size), ETag: etag}
		}
		return true
	})
	if err == nil && callbackErr != nil {
		err = callbackErr
	}
	return snap, err
}

// SnapshotDiff is the delta between two bucket snapshots.
type SnapshotDiff struct {
	Label          string   `json:"label"`
	AddedObjects   int      `json:"addedObjects"`
	AddedBytes     int64    `json:"addedBytes"`
	ChangedObjects int      `json:"changedObjects"` // same key, size or ETag changed
	ChangedBytes   int64    `json:"changedBytes"`   // net byte delta of changed keys
	DeletedObjects int      `json:"deletedObjects"`
	UnexpectedKeys []string `json:"unexpectedKeys"` // added keys outside the expected prefixes (first 20)
	// UnexpectedChangedKeys contains overwritten keys outside this run's exact
	// session prefix or metadata marker, including same-size ETag changes.
	UnexpectedChangedKeys []string `json:"unexpectedChangedKeys"`
	DeletedKeys           []string `json:"deletedKeys"` // deleted keys from anywhere in the bucket (first 20)
}

func expectedSnapshotKey(key, sessionPrefix string, expectedExactKeys map[string]struct{}) bool {
	if sessionPrefix != "" && strings.HasSuffix(sessionPrefix, "/") && strings.HasPrefix(key, sessionPrefix) {
		return true
	}
	_, expected := expectedExactKeys[key]
	return expected
}

func firstSortedKeys(keys []string) []string {
	sort.Strings(keys)
	if len(keys) > 20 {
		return keys[:20]
	}
	return keys
}

// diffSnapshots compares two snapshots. Additions and overwrites are allowed
// only under this run's exact session prefix or at explicitly named exact keys;
// every deletion is evidence because benchmark runs never own pre-existing
// objects. Empty slices are initialized so JSON emits [] rather than null.
func diffSnapshots(
	label string,
	before, after bucketSnapshot,
	sessionPrefix string,
	expectedExactKeys map[string]struct{},
) SnapshotDiff {
	d := SnapshotDiff{
		Label:                 label,
		UnexpectedKeys:        []string{},
		UnexpectedChangedKeys: []string{},
		DeletedKeys:           []string{},
	}
	for key, object := range after {
		prev, existed := before[key]
		switch {
		case !existed:
			d.AddedObjects++
			d.AddedBytes += object.Size
			if !expectedSnapshotKey(key, sessionPrefix, expectedExactKeys) {
				d.UnexpectedKeys = append(d.UnexpectedKeys, key)
			}
		case prev.Size != object.Size || prev.ETag != object.ETag:
			d.ChangedObjects++
			d.ChangedBytes += object.Size - prev.Size
			if !expectedSnapshotKey(key, sessionPrefix, expectedExactKeys) {
				d.UnexpectedChangedKeys = append(d.UnexpectedChangedKeys, key)
			}
		}
	}
	for key := range before {
		if _, ok := after[key]; !ok {
			d.DeletedObjects++
			d.DeletedKeys = append(d.DeletedKeys, key)
		}
	}
	d.UnexpectedKeys = firstSortedKeys(d.UnexpectedKeys)
	d.UnexpectedChangedKeys = firstSortedKeys(d.UnexpectedChangedKeys)
	d.DeletedKeys = firstSortedKeys(d.DeletedKeys)
	return d
}

type snapshotLister struct {
	s3iface.S3API
	objects []*s3.Object
}

func (l *snapshotLister) ListObjectsV2Pages(
	_ *s3.ListObjectsV2Input,
	callback func(*s3.ListObjectsV2Output, bool) bool,
) error {
	callback(&s3.ListObjectsV2Output{Contents: l.objects}, true)
	return nil
}

func TestTakeBucketSnapshotRecordsSizeAndETag(t *testing.T) {
	client := &snapshotLister{objects: []*s3.Object{{
		Key:  aws.String("immutable"),
		Size: aws.Int64(42),
		ETag: aws.String(`"etag-a"`),
	}}}
	snapshot, err := takeBucketSnapshot(client, benchmarkS3BucketName)
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshot["immutable"]; got != (objectSnapshot{Size: 42, ETag: `"etag-a"`}) {
		t.Fatalf("snapshot=%+v, want size and ETag", got)
	}

	client.objects[0].ETag = nil
	if _, err := takeBucketSnapshot(client, benchmarkS3BucketName); err == nil {
		t.Fatal("snapshot accepted an object without an ETag")
	}
}

func TestDiffSnapshotsDetectsSameSizeOverwriteAndExactMarkerScope(t *testing.T) {
	const (
		sessionPrefix = "log/cluster-history/raycluster/test-ns/ray-cluster/session_1/"
		markerKey     = "log/cluster-metadata/raycluster/test-ns_ray-cluster/session_1"
	)
	markerDirectoryKey := "log/cluster-metadata/raycluster/test-ns_ray-cluster/"
	expectedExactKeys := map[string]struct{}{markerKey: {}, markerDirectoryKey: {}}
	before := bucketSnapshot{
		sessionPrefix + "existing": {Size: 10, ETag: `"session-old"`},
		"immutable-same":           {Size: 20, ETag: `"same"`},
		"immutable-overwritten":    {Size: 30, ETag: `"old"`},
		"immutable-deleted":        {Size: 40, ETag: `"deleted"`},
	}
	after := bucketSnapshot{
		sessionPrefix + "existing": {Size: 10, ETag: `"session-new"`},
		sessionPrefix + "new":      {Size: 11, ETag: `"new"`},
		markerKey:                  {Size: 12, ETag: `"marker"`},
		markerDirectoryKey:         {Size: 0, ETag: `"directory"`},
		markerKey + "-other":       {Size: 13, ETag: `"other-marker"`},
		"unexpected-new":           {Size: 14, ETag: `"unexpected"`},
		"immutable-same":           {Size: 20, ETag: `"same"`},
		"immutable-overwritten":    {Size: 30, ETag: `"new"`},
	}

	diff := diffSnapshots("attack", before, after, sessionPrefix, expectedExactKeys)
	if diff.AddedObjects != 5 || diff.ChangedObjects != 2 || diff.ChangedBytes != 0 || diff.DeletedObjects != 1 {
		t.Fatalf("unexpected diff counters: %+v", diff)
	}
	if !stringSlicesEqual(diff.UnexpectedKeys, []string{markerKey + "-other", "unexpected-new"}) {
		t.Fatalf("unexpected additions=%v", diff.UnexpectedKeys)
	}
	if !stringSlicesEqual(diff.UnexpectedChangedKeys, []string{"immutable-overwritten"}) {
		t.Fatalf("same-size outside overwrite was not isolated: %v", diff.UnexpectedChangedKeys)
	}
	if !stringSlicesEqual(diff.DeletedKeys, []string{"immutable-deleted"}) {
		t.Fatalf("deleted evidence=%v", diff.DeletedKeys)
	}
}

func TestFullLifecycleDiffCatchesPrePhaseForeignMutations(t *testing.T) {
	const sessionPrefix = "log/cluster-history/raycluster/test-ns/ray-cluster/session_1/"
	exactKeys := map[string]struct{}{
		"log/cluster-metadata/raycluster/test-ns_ray-cluster/session_1": {},
		"log/cluster-metadata/raycluster/test-ns_ray-cluster/":          {},
	}
	preStart := bucketSnapshot{
		"foreign-overwritten": {Size: 10, ETag: `"before"`},
		"foreign-deleted":     {Size: 20, ETag: `"deleted"`},
	}
	// This is the state an old T0 snapshot would see after Collector startup.
	// The two later phase snapshots are identical and therefore look clean.
	oldT0 := bucketSnapshot{
		"foreign-overwritten":   {Size: 10, ETag: `"after"`},
		"foreign-added":         {Size: 30, ETag: `"added"`},
		sessionPrefix + "event": {Size: 40, ETag: `"owned"`},
	}
	phase := diffSnapshots("phase", oldT0, oldT0, sessionPrefix, exactKeys)
	if phase.ChangedObjects != 0 || phase.AddedObjects != 0 || phase.DeletedObjects != 0 {
		t.Fatalf("phase control should look clean: %+v", phase)
	}

	full := diffSnapshots("full", preStart, oldT0, sessionPrefix, exactKeys)
	if !stringSlicesEqual(full.UnexpectedKeys, []string{"foreign-added"}) ||
		!stringSlicesEqual(full.UnexpectedChangedKeys, []string{"foreign-overwritten"}) ||
		!stringSlicesEqual(full.DeletedKeys, []string{"foreign-deleted"}) {
		t.Fatalf("full lifecycle missed pre-phase mutations: %+v", full)
	}
}

func TestDiffSnapshotsCapsAndSortsDeletedEvidence(t *testing.T) {
	before := bucketSnapshot{}
	for i := 24; i >= 0; i-- {
		before[fmt.Sprintf("immutable-%02d", i)] = objectSnapshot{Size: int64(i), ETag: fmt.Sprintf(`"%d"`, i)}
	}
	diff := diffSnapshots(
		"deletions", before, bucketSnapshot{}, "session/", map[string]struct{}{"marker": {}},
	)
	if diff.DeletedObjects != 25 || len(diff.DeletedKeys) != 20 {
		t.Fatalf("deleted evidence=%+v, want 25 total and first 20 keys", diff)
	}
	if diff.DeletedKeys[0] != "immutable-00" || diff.DeletedKeys[19] != "immutable-19" {
		t.Fatalf("deleted evidence is not deterministic first-20: %v", diff.DeletedKeys)
	}
}

func TestSnapshotDiffSerializesEmptyIsolationEvidenceAsArrays(t *testing.T) {
	diff := diffSnapshots(
		"safe", bucketSnapshot{}, bucketSnapshot{}, "session/", map[string]struct{}{"marker": {}},
	)
	raw, err := json.Marshal(diff)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		`"unexpectedKeys":[]`,
		`"unexpectedChangedKeys":[]`,
		`"deletedKeys":[]`,
	} {
		if !strings.Contains(string(raw), field) {
			t.Fatalf("SnapshotDiff JSON %s is missing fail-closed field %s", raw, field)
		}
	}
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
	CountByType          map[string]int64       `json:"countByType"`
	TotalEvents          int64                  `json:"totalEvents"`
	DistinctEventIDs     int64                  `json:"distinctEventIDs"`
	MissingEventIDs      int64                  `json:"missingEventIDs"`
	DuplicateEventIDs    int64                  `json:"duplicateEventIDs"`  // extra occurrences beyond the first non-empty eventId
	TaskScopedEvents     int64                  `json:"taskScopedEvents"`   // TASK_* + ACTOR_TASK_*
	RawJSONLBytes        int64                  `json:"rawJSONLBytes"`      // decompressed logical bytes
	StoredEventBytes     int64                  `json:"storedEventBytes"`   // object sizes as stored
	DistinctTaskDefIDs   int                    `json:"distinctTaskDefIDs"` // across ALL jobs in the session (system tasks included)
	BenchJobID           string                 `json:"benchJobID"`         // job directory with the most distinct definition taskIds
	BenchJobTaskIDs      int                    `json:"benchJobTaskIDs"`    // distinct definition taskIds within that job
	BenchTaskIDs         int                    `json:"benchTaskIDs"`       // distinct definition taskIds named bench_task, across every job — the loss denominator, and the only one that works with several drivers
	ExpectedTasks        int                    `json:"expectedTasks"`
	BenchTaskValidity    BenchTaskValidity      `json:"benchTaskValidity"`
	TaskLogMetadata      TaskLogMetadataSummary `json:"taskLogMetadata"`
	TaskLifecycleWindows []TaskLifecycleWindow  `json:"taskLifecycleWindows"`
	EventsPerTask        float64                `json:"eventsPerTask"`
	AvgRawBytesPerEvent  float64                `json:"avgRawBytesPerEvent"`
	CompressionRatio     float64                `json:"compressionRatio"` // stored/raw; 1.0 when compression is off
	PerNode              []NodeEventStats       `json:"perNode"`
	eventIDs             map[string]struct{}
}

// BenchTaskValidity is the fail-closed verdict for the benchmark workload.
// Ray calls the raw event field taskAttempt; the State API exposes the same
// value as attempt_number. A valid run has exactly one attempt (attempt 0) per
// expected bench_task ID, and every attempt's latest lifecycle state is
// FINISHED.
type BenchTaskValidity struct {
	ExpectedTaskIDs                int      `json:"expectedTaskIDs"`
	ObservedTaskIDs                int      `json:"observedTaskIDs"`
	ObservedAttempts               int      `json:"observedAttempts"`
	AttemptZero                    int      `json:"attemptZero"`
	FinishedAttempts               int      `json:"finishedAttempts"`
	SubmittedToWorkerAttempts      int      `json:"submittedToWorkerAttempts"`
	FinishedTransitionAttempts     int      `json:"finishedTransitionAttempts"`
	MalformedDefinitions           int      `json:"malformedDefinitions"`
	MissingDefinitionAttemptFields int      `json:"missingDefinitionAttemptFields"`
	MissingLifecycleAttemptFields  int      `json:"missingLifecycleAttemptFields"`
	InvalidLifecycleTransitions    int      `json:"invalidLifecycleTransitions"`
	OutOfRangeLifecycleTransitions int      `json:"outOfRangeLifecycleTransitions"`
	AmbiguousLifecycleTransitions  int      `json:"ambiguousLifecycleTransitions"`
	MissingLifecycleAttempts       int      `json:"missingLifecycleAttempts"`
	NonFinishedAttempts            int      `json:"nonFinishedAttempts"`
	Valid                          bool     `json:"valid"`
	Problems                       []string `json:"problems"`
}

// TaskLifecycleWindow is a job-wide workload counter bucket. "Submitted" here
// means Ray's SUBMITTED_TO_WORKER transition, not the driver's .remote() call.
// Unlike Collector
// ingress windows, its timestamps come from Ray task lifecycle transitions, not
// from Collector HTTP receive time. Repeating these counts on the head and
// worker Collector rows provides workload context without pretending either
// Collector owns the logical task count.
type TaskLifecycleWindow struct {
	WindowStartUnixNano       int64 `json:"windowStartUnixNano"`
	WindowEndUnixNano         int64 `json:"windowEndUnixNano"`
	SubmittedToWorkerAttempts int64 `json:"submittedToWorkerAttempts"`
	FinishedAttempts          int64 `json:"finishedAttempts"`
	BacklogDelta              int64 `json:"backlogDelta"`
}

const taskLifecycleWindowDuration = 10 * time.Second

// NodeEventStats attributes event traffic to the Ray node whose aggregator
// emitted it — the per-node view that collector sizing needs. Which node a
// task's events land on depends on WHO emits them: definition events come from
// the owner (the driver's node), execution-side events from the executing node.
type NodeEventStats struct {
	NodeID              string           `json:"nodeID"`
	Events              int64            `json:"events"`
	DistinctEventIDs    int64            `json:"distinctEventIDs"`
	MissingEventIDs     int64            `json:"missingEventIDs"`
	DuplicateEventIDs   int64            `json:"duplicateEventIDs"` // extra occurrences beyond the first non-empty eventId on this node
	RawBytes            int64            `json:"rawBytes"`
	DistinctTaskIDs     int              `json:"distinctTaskIDs"` // any task-scoped payload with a taskId
	Peak1sEvents        int64            `json:"peak1sEvents"`
	Peak10sEventsPerSec float64          `json:"peak10sEventsPerSec"`
	CountByType         map[string]int64 `json:"countByType"`
}

// eventProbe decodes only the fields the report needs; everything else in the
// event JSON is skipped by encoding/json.
type taskLifecycleProbe struct {
	TaskID           string            `json:"taskId"`
	TaskAttempt      *int              `json:"taskAttempt"`
	NodeID           string            `json:"nodeId"`
	WorkerID         string            `json:"workerId"`
	TaskLogInfo      *taskLogInfoPatch `json:"taskLogInfo"`
	StateTransitions []struct {
		State     string `json:"state"`
		Timestamp string `json:"timestamp"`
	} `json:"stateTransitions"`
}

type eventProbe struct {
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	Timestamp string `json:"timestamp"`
	TaskDef   *struct {
		TaskID      string `json:"taskId"`
		TaskAttempt *int   `json:"taskAttempt"`
		TaskName    string `json:"taskName"`
		TaskFunc    *struct {
			FunctionName string `json:"functionName"`
		} `json:"taskFunc"`
	} `json:"taskDefinitionEvent"`
	TaskLifecycle *taskLifecycleProbe `json:"taskLifecycleEvent"`
	ActorTaskDef  *struct {
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
	events            int64
	missingEventIDs   int64
	duplicateEventIDs int64
	rawBytes          int64
	eventIDs          map[string]struct{}
	distinct          map[string]struct{}
	countByType       map[string]int64
	perSecond         map[int64]int64 // unix second -> events
}

type taskAttemptKey struct {
	TaskID  string
	Attempt int
}

type latestTaskState struct {
	State     string
	Timestamp time.Time
	Ambiguous bool
}

// benchTaskValidityAccumulator collects definition and lifecycle events before
// deciding which lifecycle IDs belong to bench_task. Events may be uploaded in
// different files and scanned in either order, so validating line-by-line would
// incorrectly reject a lifecycle event that precedes its definition event.
type benchTaskValidityAccumulator struct {
	taskIDs                       map[string]struct{}
	definitionAttempts            map[taskAttemptKey]struct{}
	lifecycleAttempts             map[taskAttemptKey]struct{}
	lifecycleStates               map[taskAttemptKey]latestTaskState
	submittedAt                   map[taskAttemptKey]time.Time
	finishedAt                    map[taskAttemptKey]time.Time
	missingDefinitionAttemptIDs   map[string]struct{}
	missingLifecycleAttemptIDs    map[string]struct{}
	invalidLifecycleTransitionIDs map[string]struct{}
	outOfRangeTransitionIDs       map[string]struct{}
	malformedDefinitions          int
	taskLogMetadata               *taskLogMetadataAccumulator
}

func newBenchTaskValidityAccumulator() *benchTaskValidityAccumulator {
	return &benchTaskValidityAccumulator{
		taskIDs:                       map[string]struct{}{},
		definitionAttempts:            map[taskAttemptKey]struct{}{},
		lifecycleAttempts:             map[taskAttemptKey]struct{}{},
		lifecycleStates:               map[taskAttemptKey]latestTaskState{},
		submittedAt:                   map[taskAttemptKey]time.Time{},
		finishedAt:                    map[taskAttemptKey]time.Time{},
		missingDefinitionAttemptIDs:   map[string]struct{}{},
		missingLifecycleAttemptIDs:    map[string]struct{}{},
		invalidLifecycleTransitionIDs: map[string]struct{}{},
		outOfRangeTransitionIDs:       map[string]struct{}{},
		taskLogMetadata:               newTaskLogMetadataAccumulator(),
	}
}

func (a *benchTaskValidityAccumulator) observe(probe eventProbe) {
	a.taskLogMetadata.observe(probe)
	if probe.TaskDef != nil {
		name := probe.TaskDef.TaskName
		if name == "" && probe.TaskDef.TaskFunc != nil {
			name = probe.TaskDef.TaskFunc.FunctionName
		}
		if isBenchTaskName(name) {
			if probe.TaskDef.TaskID == "" {
				a.malformedDefinitions++
			} else {
				a.taskIDs[probe.TaskDef.TaskID] = struct{}{}
				if probe.TaskDef.TaskAttempt == nil {
					a.missingDefinitionAttemptIDs[probe.TaskDef.TaskID] = struct{}{}
				} else {
					a.definitionAttempts[taskAttemptKey{
						TaskID:  probe.TaskDef.TaskID,
						Attempt: *probe.TaskDef.TaskAttempt,
					}] = struct{}{}
				}
			}
		}
	}

	if probe.TaskLifecycle == nil || probe.TaskLifecycle.TaskID == "" {
		return
	}
	if probe.TaskLifecycle.TaskAttempt == nil {
		a.missingLifecycleAttemptIDs[probe.TaskLifecycle.TaskID] = struct{}{}
		return
	}

	key := taskAttemptKey{
		TaskID:  probe.TaskLifecycle.TaskID,
		Attempt: *probe.TaskLifecycle.TaskAttempt,
	}
	a.lifecycleAttempts[key] = struct{}{}
	for _, transition := range probe.TaskLifecycle.StateTransitions {
		at, err := time.Parse(time.RFC3339Nano, transition.Timestamp)
		if err != nil || transition.State == "" {
			a.invalidLifecycleTransitionIDs[probe.TaskLifecycle.TaskID] = struct{}{}
			continue
		}
		switch transition.State {
		case "SUBMITTED_TO_WORKER":
			a.keepUniqueTransition(a.submittedAt, key, at)
		case "FINISHED":
			a.keepUniqueTransition(a.finishedAt, key, at)
		}
		latest, exists := a.lifecycleStates[key]
		switch {
		case !exists || at.After(latest.Timestamp):
			a.lifecycleStates[key] = latestTaskState{State: transition.State, Timestamp: at}
		case at.Equal(latest.Timestamp) && transition.State != latest.State:
			latest.Ambiguous = true
			a.lifecycleStates[key] = latest
		}
	}
}

func (a *benchTaskValidityAccumulator) taskLogMetadataSummary() TaskLogMetadataSummary {
	attempts := map[taskAttemptKey]struct{}{}
	for key := range a.definitionAttempts {
		if _, ok := a.taskIDs[key.TaskID]; ok {
			attempts[key] = struct{}{}
		}
	}
	for key := range a.lifecycleAttempts {
		if _, ok := a.taskIDs[key.TaskID]; ok {
			attempts[key] = struct{}{}
		}
	}
	return summarizeTaskLogMetadata(a.taskLogMetadata.records(attempts))
}

// keepUniqueTransition accepts replayed copies only when they preserve the
// original Ray transition timestamp. Two different timestamps for the same
// task attempt/state are not interchangeable measurements, so the run must
// fail closed instead of silently choosing one.
func (a *benchTaskValidityAccumulator) keepUniqueTransition(
	dst map[taskAttemptKey]time.Time,
	key taskAttemptKey,
	at time.Time,
) {
	previous, exists := dst[key]
	if !exists {
		dst[key] = at
		return
	}
	if !at.Equal(previous) {
		a.invalidLifecycleTransitionIDs[key.TaskID] = struct{}{}
	}
}

func isBenchTaskName(name string) bool {
	return name == benchTaskName || strings.HasSuffix(name, "."+benchTaskName)
}

func (a *benchTaskValidityAccumulator) summarize(expected int) BenchTaskValidity {
	for key, submitted := range a.submittedAt {
		if finished, ok := a.finishedAt[key]; ok && submitted.After(finished) {
			a.invalidLifecycleTransitionIDs[key.TaskID] = struct{}{}
		}
	}
	validity := BenchTaskValidity{
		ExpectedTaskIDs:                expected,
		ObservedTaskIDs:                len(a.taskIDs),
		MalformedDefinitions:           a.malformedDefinitions,
		MissingDefinitionAttemptFields: a.countBenchIDs(a.missingDefinitionAttemptIDs),
		MissingLifecycleAttemptFields:  a.countBenchIDs(a.missingLifecycleAttemptIDs),
		InvalidLifecycleTransitions:    a.countBenchIDs(a.invalidLifecycleTransitionIDs),
		OutOfRangeLifecycleTransitions: a.countBenchIDs(a.outOfRangeTransitionIDs),
	}

	// Use the union of definition and lifecycle attempts. This catches a retry
	// attempt whose definition event was lost but whose lifecycle event arrived.
	attempts := map[taskAttemptKey]struct{}{}
	for key := range a.definitionAttempts {
		if _, ok := a.taskIDs[key.TaskID]; ok {
			attempts[key] = struct{}{}
		}
	}
	for key := range a.lifecycleAttempts {
		if _, ok := a.taskIDs[key.TaskID]; ok {
			attempts[key] = struct{}{}
		}
	}
	validity.ObservedAttempts = len(attempts)
	validity.SubmittedToWorkerAttempts = a.countBenchAttemptTimes(a.submittedAt, attempts)
	validity.FinishedTransitionAttempts = a.countBenchAttemptTimes(a.finishedAt, attempts)

	for key := range attempts {
		if key.Attempt == 0 {
			validity.AttemptZero++
		}
		latest, ok := a.lifecycleStates[key]
		if !ok {
			validity.MissingLifecycleAttempts++
			continue
		}
		if latest.Ambiguous {
			validity.AmbiguousLifecycleTransitions++
			continue
		}
		if latest.State == "FINISHED" {
			validity.FinishedAttempts++
		} else {
			validity.NonFinishedAttempts++
		}
	}

	validity.Problems = benchTaskValidityProblems(validity)
	validity.Valid = len(validity.Problems) == 0
	return validity
}

func (a *benchTaskValidityAccumulator) summarizeWithin(
	expected int,
	jobStart time.Time,
	jobEnd time.Time,
) BenchTaskValidity {
	if jobStart.IsZero() || jobEnd.IsZero() || jobEnd.Before(jobStart) {
		for taskID := range a.taskIDs {
			a.outOfRangeTransitionIDs[taskID] = struct{}{}
		}
		return a.summarize(expected)
	}
	// RayJobStatusInfo uses metav1.Time. Its JSON representation is RFC3339 at
	// whole-second precision even though the controller initially receives the
	// Ray Dashboard end time in milliseconds. Treat the serialized end as the
	// start of its final one-second bucket; otherwise valid nanosecond-precision
	// task transitions in that same bucket are rejected nondeterministically.
	// Keep the upper bound exclusive so events in the following second still
	// fail closed.
	jobEndExclusive := jobEnd.Add(time.Second)
	for key, submitted := range a.submittedAt {
		if submitted.Before(jobStart) || !submitted.Before(jobEndExclusive) {
			a.outOfRangeTransitionIDs[key.TaskID] = struct{}{}
		}
	}
	for key, finished := range a.finishedAt {
		if finished.Before(jobStart) || !finished.Before(jobEndExclusive) {
			a.outOfRangeTransitionIDs[key.TaskID] = struct{}{}
		}
	}
	return a.summarize(expected)
}

func (a *benchTaskValidityAccumulator) countBenchAttemptTimes(
	times map[taskAttemptKey]time.Time, attempts map[taskAttemptKey]struct{},
) int {
	count := 0
	for key := range times {
		if _, isBenchAttempt := attempts[key]; isBenchAttempt {
			count++
		}
	}
	return count
}

func (a *benchTaskValidityAccumulator) lifecycleWindows(window time.Duration) []TaskLifecycleWindow {
	if window <= 0 {
		return nil
	}
	type counters struct {
		submitted int64
		finished  int64
	}
	buckets := map[int64]*counters{}
	add := func(at time.Time, submitted bool) {
		start := at.Truncate(window).UnixNano()
		bucket := buckets[start]
		if bucket == nil {
			bucket = &counters{}
			buckets[start] = bucket
		}
		if submitted {
			bucket.submitted++
		} else {
			bucket.finished++
		}
	}
	for key, at := range a.submittedAt {
		if key.Attempt == 0 {
			if _, isBenchTask := a.taskIDs[key.TaskID]; isBenchTask {
				add(at, true)
			}
		}
	}
	for key, at := range a.finishedAt {
		if key.Attempt == 0 {
			if _, isBenchTask := a.taskIDs[key.TaskID]; isBenchTask {
				add(at, false)
			}
		}
	}

	rows := make([]TaskLifecycleWindow, 0, len(buckets))
	for start, counts := range buckets {
		rows = append(rows, TaskLifecycleWindow{
			WindowStartUnixNano:       start,
			WindowEndUnixNano:         start + window.Nanoseconds(),
			SubmittedToWorkerAttempts: counts.submitted,
			FinishedAttempts:          counts.finished,
			BacklogDelta:              counts.submitted - counts.finished,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].WindowStartUnixNano < rows[j].WindowStartUnixNano
	})
	return rows
}

func (a *benchTaskValidityAccumulator) countBenchIDs(ids map[string]struct{}) int {
	count := 0
	for id := range ids {
		if _, ok := a.taskIDs[id]; ok {
			count++
		}
	}
	return count
}

func benchTaskValidityProblems(v BenchTaskValidity) []string {
	problems := make([]string, 0)
	if v.ObservedTaskIDs != v.ExpectedTaskIDs {
		problems = append(problems, fmt.Sprintf("bench task IDs=%d, expected=%d", v.ObservedTaskIDs, v.ExpectedTaskIDs))
	}
	if v.ObservedAttempts != v.ExpectedTaskIDs {
		problems = append(problems, fmt.Sprintf("bench task attempts=%d, expected=%d", v.ObservedAttempts, v.ExpectedTaskIDs))
	}
	if v.AttemptZero != v.ExpectedTaskIDs {
		problems = append(problems, fmt.Sprintf("attempt 0=%d, expected=%d", v.AttemptZero, v.ExpectedTaskIDs))
	}
	if v.FinishedAttempts != v.ExpectedTaskIDs {
		problems = append(problems, fmt.Sprintf("FINISHED attempts=%d, expected=%d", v.FinishedAttempts, v.ExpectedTaskIDs))
	}
	if v.SubmittedToWorkerAttempts != v.ExpectedTaskIDs {
		problems = append(problems, fmt.Sprintf("SUBMITTED_TO_WORKER attempts=%d, expected=%d", v.SubmittedToWorkerAttempts, v.ExpectedTaskIDs))
	}
	if v.FinishedTransitionAttempts != v.ExpectedTaskIDs {
		problems = append(problems, fmt.Sprintf("FINISHED transition attempts=%d, expected=%d", v.FinishedTransitionAttempts, v.ExpectedTaskIDs))
	}
	anomalies := []struct {
		count int
		label string
	}{
		{v.MalformedDefinitions, "bench definitions missing taskId"},
		{v.MissingDefinitionAttemptFields, "bench definitions missing taskAttempt"},
		{v.MissingLifecycleAttemptFields, "bench lifecycle events missing taskAttempt"},
		{v.InvalidLifecycleTransitions, "bench tasks with invalid lifecycle transitions"},
		{v.OutOfRangeLifecycleTransitions, "bench tasks with lifecycle transitions outside the RayJob status interval"},
		{v.AmbiguousLifecycleTransitions, "bench attempts with ambiguous latest lifecycle state"},
		{v.MissingLifecycleAttempts, "bench attempts missing lifecycle state"},
		{v.NonFinishedAttempts, "bench attempts whose latest state is not FINISHED"},
	}
	for _, anomaly := range anomalies {
		if anomaly.count != 0 {
			problems = append(problems, fmt.Sprintf("%s=%d", anomaly.label, anomaly.count))
		}
	}
	sort.Strings(problems)
	return problems
}

func validateBenchTaskValidity(v BenchTaskValidity) error {
	// Recompute the invariant rather than trusting the serialized Valid bit. The
	// caller therefore fails closed even if a future producer forgets to set it.
	problems := benchTaskValidityProblems(v)
	if len(problems) == 0 && v.Valid {
		return nil
	}
	if len(problems) == 0 {
		problems = append(problems, "validity verdict is false")
	}
	return fmt.Errorf("%s", strings.Join(problems, "; "))
}

func newNodeAccumulator() *nodeAccumulator {
	return &nodeAccumulator{
		eventIDs:    map[string]struct{}{},
		distinct:    map[string]struct{}{},
		countByType: map[string]int64{},
		perSecond:   map[int64]int64{},
	}
}

func (s *EventStats) observeEventID(raw string) {
	id := strings.TrimSpace(raw)
	if id == "" {
		s.MissingEventIDs++
		return
	}
	if s.eventIDs == nil {
		s.eventIDs = map[string]struct{}{}
	}
	if _, exists := s.eventIDs[id]; exists {
		s.DuplicateEventIDs++
		return
	}
	s.eventIDs[id] = struct{}{}
	s.DistinctEventIDs++
}

func (a *nodeAccumulator) observeEventID(raw string) {
	id := strings.TrimSpace(raw)
	if id == "" {
		a.missingEventIDs++
		return
	}
	if a.eventIDs == nil {
		a.eventIDs = map[string]struct{}{}
	}
	if _, exists := a.eventIDs[id]; exists {
		a.duplicateEventIDs++
		return
	}
	a.eventIDs[id] = struct{}{}
}

// buildStorageReport walks the session prefix (with proper pagination — the
// e2e helper caps at 1000 keys), decodes every event file, and attributes
// events to nodes. The per-node per-second series is written to node_rate.csv.
func buildStorageReport(
	t *testing.T,
	s3Client *s3.S3,
	bucket, sessionPrefix, markerKey string,
	cfg benchConfig,
	runDir string,
	jobStart, jobEnd time.Time,
) StorageReport {
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
	benchValidity := newBenchTaskValidityAccumulator()
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
		if err := decodeEventObject(s3Client, bucket, key, &rep.Events, globalDistinct, jobSet, benchValidity, acc); err != nil {
			t.Fatalf("decode event object: %v", err)
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
	rep.Events.BenchTaskValidity = benchValidity.summarizeWithin(cfg.TaskCount, jobStart, jobEnd)
	rep.Events.BenchTaskIDs = rep.Events.BenchTaskValidity.ObservedTaskIDs
	rep.Events.TaskLogMetadata = benchValidity.taskLogMetadataSummary()
	rep.Events.TaskLifecycleWindows = benchValidity.lifecycleWindows(taskLifecycleWindowDuration)
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
		t.Errorf("write node_rate.csv: %v", err)
	}
	if err := writeTaskLifecycleWindowsCSV(
		filepath.Join(runDir, "task_lifecycle_10s.csv"),
		rep.Events.TaskLifecycleWindows,
	); err != nil {
		t.Errorf("write task_lifecycle_10s.csv: %v", err)
	}
	return rep
}

func writeTaskLifecycleWindowsCSV(path string, rows []TaskLifecycleWindow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintln(
		f,
		"window_start_unix_nano,window_end_unix_nano,submitted_to_worker_attempts,finished_attempts,backlog_delta",
	); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(
			f,
			"%d,%d,%d,%d,%d\n",
			row.WindowStartUnixNano,
			row.WindowEndUnixNano,
			row.SubmittedToWorkerAttempts,
			row.FinishedAttempts,
			row.BacklogDelta,
		); err != nil {
			return err
		}
	}
	return nil
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

func decodeEventObject(s3Client *s3.S3, bucket, key string, stats *EventStats, globalDistinct, jobDistinct map[string]struct{}, benchValidity *benchTaskValidityAccumulator, acc *nodeAccumulator) error {
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
	lineNumber := 0
	for {
		line, err := br.ReadBytes('\n')
		lineNumber++
		trimmed := strings.TrimSpace(string(line))
		if trimmed != "" {
			stats.RawJSONLBytes += int64(len(line))
			acc.rawBytes += int64(len(line))
			if decodeErr := decodeEventLine(trimmed, stats, globalDistinct, jobDistinct, benchValidity, acc); decodeErr != nil {
				return fmt.Errorf("%s line %d: %w", key, lineNumber, decodeErr)
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

func decodeEventLine(line string, stats *EventStats, globalDistinct, jobDistinct map[string]struct{}, benchValidity *benchTaskValidityAccumulator, acc *nodeAccumulator) error {
	var probe eventProbe
	if err := json.Unmarshal([]byte(line), &probe); err != nil {
		return fmt.Errorf("decode event JSON: %w", err)
	}
	if err := validateEventProbePayload(probe); err != nil {
		return err
	}

	stats.TotalEvents++
	stats.observeEventID(probe.EventID)
	stats.CountByType[probe.EventType]++
	acc.events++
	acc.observeEventID(probe.EventID)
	acc.countByType[probe.EventType]++
	if strings.HasPrefix(probe.EventType, "TASK_") || strings.HasPrefix(probe.EventType, "ACTOR_TASK_") {
		stats.TaskScopedEvents++
	}
	if probe.TaskDef != nil && probe.TaskDef.TaskID != "" {
		globalDistinct[probe.TaskDef.TaskID] = struct{}{}
		if jobDistinct != nil {
			jobDistinct[probe.TaskDef.TaskID] = struct{}{}
		}
	}
	// Name-based attribution excludes the driver's own task and works when
	// several Ray drivers spread work over several jobs.
	benchValidity.observe(probe)
	for _, id := range []string{probe.taskDefID(), probe.lifecycleID(), probe.actorTaskDefID()} {
		if id != "" {
			acc.distinct[id] = struct{}{}
		}
	}
	if ts, err := time.Parse(time.RFC3339Nano, probe.Timestamp); err == nil {
		acc.perSecond[ts.Unix()]++
	}
	return nil
}

// validateEventProbePayload keeps the benchmark validity scan consistent with
// History Server replay, which dispatches payloads by eventType. A syntactically
// valid payload under the wrong eventType must not be allowed to create tasks
// that the real History Server would ignore.
func validateEventProbePayload(probe eventProbe) error {
	if probe.EventType == "" {
		return fmt.Errorf("eventType is missing")
	}
	if probe.TaskDef != nil && probe.EventType != "TASK_DEFINITION_EVENT" {
		return fmt.Errorf("taskDefinitionEvent does not match eventType %q", probe.EventType)
	}
	if probe.TaskLifecycle != nil && probe.EventType != "TASK_LIFECYCLE_EVENT" {
		return fmt.Errorf("taskLifecycleEvent does not match eventType %q", probe.EventType)
	}
	if probe.ActorTaskDef != nil && probe.EventType != "ACTOR_TASK_DEFINITION_EVENT" {
		return fmt.Errorf("actorTaskDefinitionEvent does not match eventType %q", probe.EventType)
	}
	switch probe.EventType {
	case "TASK_DEFINITION_EVENT":
		if probe.TaskDef == nil {
			return fmt.Errorf("eventType TASK_DEFINITION_EVENT is missing taskDefinitionEvent")
		}
	case "TASK_LIFECYCLE_EVENT":
		if probe.TaskLifecycle == nil {
			return fmt.Errorf("eventType TASK_LIFECYCLE_EVENT is missing taskLifecycleEvent")
		}
	case "ACTOR_TASK_DEFINITION_EVENT":
		if probe.ActorTaskDef == nil {
			return fmt.Errorf("eventType ACTOR_TASK_DEFINITION_EVENT is missing actorTaskDefinitionEvent")
		}
	}
	return nil
}

// summarizeNodes reduces accumulators to reportable per-node rows, including
// the peak 1s and peak 10s-window event rates.
func summarizeNodes(nodes map[string]*nodeAccumulator) []NodeEventStats {
	var out []NodeEventStats
	for nodeID, acc := range nodes {
		row := NodeEventStats{
			NodeID:            nodeID,
			Events:            acc.events,
			DistinctEventIDs:  int64(len(acc.eventIDs)),
			MissingEventIDs:   acc.missingEventIDs,
			DuplicateEventIDs: acc.duplicateEventIDs,
			RawBytes:          acc.rawBytes,
			DistinctTaskIDs:   len(acc.distinct),
			CountByType:       acc.countByType,
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
