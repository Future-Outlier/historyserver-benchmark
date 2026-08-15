package benchmark

import "testing"

func decodeEventIDFixture(t *testing.T, stats *EventStats, node *nodeAccumulator, raw string) {
	t.Helper()
	if err := decodeEventLine(
		raw,
		stats,
		map[string]struct{}{},
		map[string]struct{}{},
		newBenchTaskValidityAccumulator(),
		node,
	); err != nil {
		t.Fatalf("decode event fixture: %v", err)
	}
}

func TestStoredEventIDsUnique(t *testing.T) {
	stats := EventStats{CountByType: map[string]int64{}}
	node := newNodeAccumulator()
	decodeEventIDFixture(t, &stats, node, `{"eventId":"event-a","eventType":"NODE_LIFECYCLE_EVENT"}`)
	decodeEventIDFixture(t, &stats, node, `{"eventId":"event-b","eventType":"NODE_LIFECYCLE_EVENT"}`)

	if stats.TotalEvents != 2 || stats.DistinctEventIDs != 2 || stats.MissingEventIDs != 0 || stats.DuplicateEventIDs != 0 {
		t.Fatalf("unexpected global event-ID counts: %+v", stats)
	}
	row := summarizeNodes(map[string]*nodeAccumulator{"node-a": node})[0]
	if row.Events != 2 || row.DistinctEventIDs != 2 || row.MissingEventIDs != 0 || row.DuplicateEventIDs != 0 {
		t.Fatalf("unexpected per-node event-ID counts: %+v", row)
	}
}

func TestStoredEventIDMissing(t *testing.T) {
	stats := EventStats{CountByType: map[string]int64{}}
	node := newNodeAccumulator()
	decodeEventIDFixture(t, &stats, node, `{"eventType":"NODE_LIFECYCLE_EVENT"}`)

	if stats.TotalEvents != 1 || stats.DistinctEventIDs != 0 || stats.MissingEventIDs != 1 || stats.DuplicateEventIDs != 0 {
		t.Fatalf("missing global eventId was not counted: %+v", stats)
	}
	row := summarizeNodes(map[string]*nodeAccumulator{"node-a": node})[0]
	if row.Events != 1 || row.DistinctEventIDs != 0 || row.MissingEventIDs != 1 || row.DuplicateEventIDs != 0 {
		t.Fatalf("missing per-node eventId was not counted: %+v", row)
	}
}

func TestStoredEventIDDuplicateOnSameNode(t *testing.T) {
	stats := EventStats{CountByType: map[string]int64{}}
	node := newNodeAccumulator()
	for i := 0; i < 3; i++ {
		decodeEventIDFixture(t, &stats, node, `{"eventId":"event-a","eventType":"NODE_LIFECYCLE_EVENT"}`)
	}

	if stats.TotalEvents != 3 || stats.DistinctEventIDs != 1 || stats.DuplicateEventIDs != 2 {
		t.Fatalf("same-node duplicate was not counted globally: %+v", stats)
	}
	row := summarizeNodes(map[string]*nodeAccumulator{"node-a": node})[0]
	if row.Events != 3 || row.DistinctEventIDs != 1 || row.DuplicateEventIDs != 2 {
		t.Fatalf("same-node duplicate was not counted per node: %+v", row)
	}
}

func TestStoredEventIDDuplicateAcrossNodes(t *testing.T) {
	stats := EventStats{CountByType: map[string]int64{}}
	head := newNodeAccumulator()
	worker := newNodeAccumulator()
	decodeEventIDFixture(t, &stats, head, `{"eventId":"shared-event","eventType":"NODE_LIFECYCLE_EVENT"}`)
	decodeEventIDFixture(t, &stats, worker, `{"eventId":"shared-event","eventType":"NODE_LIFECYCLE_EVENT"}`)

	if stats.TotalEvents != 2 || stats.DistinctEventIDs != 1 || stats.DuplicateEventIDs != 1 {
		t.Fatalf("cross-node duplicate was not counted globally: %+v", stats)
	}
	for _, row := range summarizeNodes(map[string]*nodeAccumulator{"node-head": head, "node-worker": worker}) {
		if row.Events != 1 || row.DistinctEventIDs != 1 || row.DuplicateEventIDs != 0 {
			t.Fatalf("cross-node duplicate incorrectly changed per-node counts: %+v", row)
		}
	}
}
