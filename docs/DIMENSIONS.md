# What was tested

## The data path under test

```
  ┌─ Ray head node ───────────────────┐   ┌─ Ray worker node ─────────────────┐
  │ CoreWorker (driver + owners)      │   │ CoreWorker (executors)            │
  │   task status events              │   │   task status events              │
  │        │                          │   │        │                          │
  │        v                          │   │        v                          │
  │ event aggregator agent            │   │ event aggregator agent            │
  │   batches <=10k events, retries   │   │   batches <=10k events, retries   │
  │        │ POST /v1/events          │   │        │ POST /v1/events          │
  │        v                          │   │        v                          │
  │ collector sidecar (1 per pod)     │   │ collector sidecar (1 per pod)     │
  │   write JSONL to local disk       │   │   write JSONL to local disk       │
  │   rotate: 5 min OR 100 MB         │   │   rotate: 5 min OR 100 MB         │
  │   503 backpressure at 160 MB      │   │   503 backpressure at 160 MB      │
  │   1 upload worker  ──────┐        │   │   1 upload worker  ──────┐        │
  └──────────────────────────┼────────┘   └──────────────────────────┼────────┘
                             v                                       v
                    ┌────────────────────────────────────────────────────┐
                    │ object storage (MinIO / S3 / GCS / AzureBlob)       │
                    │   log/cluster-history/...   <- event + log data     │
                    │   log/cluster-metadata/...  <- session markers,     │
                    │                                written ONLY on      │
                    │                                graceful shutdown    │
                    └────────────────────────┬───────────────────────────┘
                                             │ read on demand
                                             v
                    ┌────────────────────────────────────────────────────┐
                    │ History Server                                      │
                    │   GET /clusters       full uncached scan per call   │
                    │   GET /enter_cluster  cold load: serial whole-file  │
                    │                       decode -> []map[string]any    │
                    │                       -> snapshot in LRU (cap 100)  │
                    │   warm endpoints      served from the snapshot      │
                    └────────────────────────────────────────────────────┘
```

Two facts shape every measurement:

1. **The History Server does zero storage I/O at process start.** "Cold start"
   in any user-visible sense means the first `/enter_cluster` for a dead
   session, which is where all the load cost lives.
2. **The collector is disk-first.** Events hit local disk immediately and are
   uploaded by a single background worker, so collector memory stays flat while
   its disk queue absorbs bursts. Its 200 MB disk budget, not its heap, is what
   backpressure defends (503 to the aggregator at 160 MB).

## The load generator

One RayCluster (1 head + 1 worker, 2 CPU each) and one RayJob whose driver
submits `N` no-op tasks in waves of 2,000, then sleeps so the event queues drain
before the driver exits. The head is started with `num-cpus: "0"`, the usual
KubeRay practice, so **no task executes on it** — it only hosts the driver:

```python
@ray.remote(num_cpus=0.2)
def noop(i): return i
# submit N tasks in waves of WAVE_SIZE, ray.get() each wave, then sleep DRAIN_SLEEP
```

`num_cpus=0.2` on a 2-CPU worker gives 10 concurrent tasks per node — the knob
that sets *rate* independently of *total*.

## The three axes

```
axis A: total tasks N              1k -> 5k -> 10k -> 50k -> 100k   (gzip off)
        answers: storage size, HS load time, HS memory, collector flatness

axis B: per-node event rate        num_cpus 0.5 / 0.2 / 0.1 / 0.05  (N fixed at 20k)
        answers: collector CPU and memory vs events/s on one node

axis C: compression                axis A repeated with gzip on
        answers: storage savings at every N, and gzip's cost at load time
```

Axis A and axis C are the same runs with one flag flipped, which is what makes
the compression comparison apples-to-apples: same N, same driver script, same
cluster shape.

## What is measured, and from where

| Measurement | Source | Why this source |
|---|---|---|
| Container memory (`anon`, `memory.current`, lifetime `memory.peak`) | cgroup v2 files read directly on the kind node, 1 s | `anon` is heap without page-cache inflation; `memory.peak` is kernel-recorded and cannot be missed by polling |
| Container memory (`working_set`) | kubelet `/stats/summary`, 1 s poll | This is the metric Kubernetes eviction and limits act on — the one that matters for `resources:` |
| Container CPU | both sources, as deltas of cumulative counters | Deduplicated on the stat's own timestamp; deltas spanning a phase boundary are discarded |
| Events, bytes, event types, per-node attribution | Full decode of every event file in the bucket after the run | Ground truth, not sampling: `{nodeID}/` path segment attributes traffic to the node that produced it |
| Storage volume during job vs at shutdown | Bucket snapshots at T0 (before), T1 (after job), T2 (after graceful delete) | The T2−T1 diff is exactly the data that a SIGKILL would have lost |
| Task loss | Distinct `TASK_DEFINITION_EVENT` task IDs scoped to the benchmark job | Counting across all jobs would hide losses behind the submitter's own tasks |
| Collector backpressure | Followed container logs, including after termination | Uploads mostly happen during drain; a pre-deletion scrape sees none of them |
| HS cold load | Wall clock on `GET /enter_cluster`; on timeout, re-probe until the warm hit lands | The server keeps loading after the client gives up (singleflight), so the first warm hit upper-bounds the true load time |

The History Server is deployed from KubeRay's own sample manifest, which caps it
at `500m` of CPU. That cap is part of what the benchmark measures — see
[FINDINGS.md](FINDINGS.md) — and `BENCH_HS_CPU_LIMIT` varies it.
