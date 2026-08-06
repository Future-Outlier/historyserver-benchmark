# Results

All numbers come from [`../results/derived.csv`](../results/derived.csv), which is
generated from the per-run reports by `tools/derive.py`. Environment in
[ENVIRONMENT.md](ENVIRONMENT.md).

---

## 1. Events: how many, and of what kind

A task is not one event. At N = 50,000 the bucket contained 218,214 events:

| Event type | Count | Per task |
|---|---:|---:|
| `TASK_LIFECYCLE_EVENT` | 118,149 | 2.363 |
| `TASK_PROFILE_EVENT` | 50,045 | 1.001 |
| `TASK_DEFINITION_EVENT` | 50,005 | 1.000 |
| everything else (node, driver-job, actor) | 15 | ~0 |

**k = 4.36 events per task** (4.30–4.76 across all runs; the spread is lifecycle
events being emitted two or three times depending on how state transitions
batch). Average raw size is **725 bytes per event** (707–745), and it does not
drift with N — event payloads are fixed-shape.

Events split roughly evenly across the two Ray nodes, because a task's lifecycle
is recorded twice: once by its owner (driver, on the head) and once by its
executor (on the worker).

```
N = 50,000 tasks                    events    share
head node   ████████████████████    118,177    54%
worker node █████████████████       100,037    46%
```

That 1:1 split is why **the head's collector is not a special case** — size it
like a worker's.

---

## 2. Storage: 3.15 KiB per task raw, 91% smaller with gzip

```
bytes per task            0        1 KiB      2 KiB      3 KiB
raw (gzip off)   3.15 KiB  ███████████████████████████████
gzip on          0.29 KiB  ███
```

| N | raw | stored (gzip on) | ratio |
|---:|---:|---:|---:|
| 1,000 | 2.87 MiB | 0.26 MiB | 0.089 |
| 5,000 | 15.24 MiB | 1.38 MiB | 0.090 |
| 10,000 | 31.03 MiB | 2.83 MiB | 0.091 |
| 50,000 | 151.81 MiB | 13.85 MiB | 0.091 |
| 100,000 | 301.44 MiB | 27.47 MiB | 0.091 |

The ratio is **0.091 at every scale** — event JSONL is highly repetitive, and
compression does not get better or worse with volume. Practical form:

```
raw    S ≈ N × 3.15 KiB     100k tasks -> ~300 MiB per session
gzip   S ≈ N × 0.29 KiB     100k tasks -> ~28 MiB per session
```

Task **logs** are a separate, much smaller category for quiet tasks: about
0.3 MiB fixed plus ~45 B/task (4.6 MiB at N = 100,000). Real workloads that
print will grow this term independently — it is not covered by these runs.

**Gzip costs nothing measurable.** CPU per event in the collector was 118–125 µs
with compression on versus 115–124 µs with it off, and cold-load time changed by
under 10% (50k: 105.8 s gzip vs 97.9 s raw). It is off by default; there is no
measured reason to leave it off.

### Most of the data only lands at shutdown

Bucket snapshots before the job (T0), after the job (T1), and after graceful
deletion (T2) split the volume by when it was uploaded:

| N | uploaded during job | uploaded during shutdown flush |
|---:|---:|---:|
| 10,000 | 0 MiB | 30.8 MiB (100%) |
| 50,000 | 0 MiB | 153.3 MiB (100%) |
| 100,000 | 142.9 MiB | 159.9 MiB (53%) |

With the default 5-minute rotation interval, a job shorter than 5 minutes
uploads **nothing** until the pod terminates gracefully; only the 100 MB size
trigger rescues the 100k runs. Anything not uploaded when a pod is SIGKILLed —
node failure, `terminationGracePeriodSeconds` expiry, OOM kill — is lost, along
with the session marker that makes the session visible at all.

---

## 3. History Server: ~2 ms per task, until it isn't

Cold load = `GET /enter_cluster` for a dead session, the first user-visible
action. Bars are 10 s each:

```
    1k │▏                                          1.8 s
    5k │█                                          9.8 s
   10k │██                                        19.9 s
   20k │████                                      37.6 s
   50k │██████████                                97.9 s
  100k │████████████████████████████████████████→ 907.3 s   (linear prediction: 196 s)
       └────┬────┬────┬────┬────┬────┬────┬────┬────
            50  100  150  200  250  300  350  400 s
```

| N | cold load | ms per task | peak heap | heap per task |
|---:|---:|---:|---:|---:|
| 1,000 | 1.8 s | 1.82 | 95 MiB | — |
| 5,000 | 9.8 s | 1.97 | 184 MiB | 38 KiB |
| 10,000 | 19.9 s | 1.99 | 291 MiB | 31 KiB |
| 20,000 | 33.9–41.3 s | 1.69–2.06 | 467–494 MiB | 25 KiB |
| 50,000 | 97.9 s | 1.96 | 1,132 MiB | 24 KiB |
| 100,000 | **907.3 s** | **9.07** | 2,202 MiB | 23 KiB |

Up to 50,000 tasks the cost is **flatly linear at ~2 ms/task**. At 100,000 it is
4.6× the linear prediction, and no default timeout anywhere in the stack is long
enough to see the result — see [FINDINGS.md](FINDINGS.md) for the three separate
timeouts involved and why the client never gets an answer.

Memory converges to **~23 KiB of heap per task**, on top of a ~90 MiB floor. The
snapshot is a fully materialized `[]map[string]any`, so this is structural, not
transient: the whole session stays in the LRU cache (capacity 100 sessions, no
TTL by default) after loading.

```
peak History Server heap        0     500 MiB    1 GiB    1.5 GiB   2 GiB
     1k     95 MiB   █▉
     5k    184 MiB   ███▋
    10k    291 MiB   █████▊
    20k    480 MiB   █████████▌
    50k  1,132 MiB   ██████████████████████▋
   100k  2,202 MiB   ████████████████████████████████████████████
```

Warm reads from a loaded snapshot are fast and small: `/api/v0/tasks?limit=10000`
returned 4.5 MiB in ~1 s, `/api/v0/tasks/summarize` 0.15–2.5 s, `/api/jobs/` and
`/nodes` tens of milliseconds. Note the API caps `limit` at 10,000 (`400` above
that, matching Ray's dashboard API), so the task list is inherently paginated
regardless of session size.

`GET /clusters` is a full uncached scan of the metadata prefix on **every**
request — 5–140 ms with a handful of sessions here, but it grows with the
number of sessions ever stored, not with the session being opened.

---

## 4. Collector: constant cost per event, flat memory

The most portable result in this benchmark. Across every run — 1k to 100k tasks,
200 to 7,850 events/s per node, gzip on and off — the collector's CPU cost per
event stayed within a narrow band:

| Peak node rate (events/s) | CPU per event | Avg cores during job | Peak cores (1 s samples) |
|---:|---:|---:|---:|
| 202–246 | 143–150 µs | 0.01 | 0.24 |
| 1,209–1,378 | 124 µs | 0.05 | 0.60–0.67 |
| 2,342–2,537 | 119–122 µs | 0.08 | 0.79–0.95 |
| 4,277–4,776 | 117–124 µs | 0.13–0.17 | 0.92–1.11 |
| 6,962–7,850 | 115–131 µs | 0.29–0.46 | 1.05–2.05 |

**≈ 120 µs of CPU per event ≈ 0.52 ms of CPU per task.** The design is
streaming — receive, append to disk, upload in the background — so nothing about
the total job size changes the per-event price. Peak cores are 2–4× the sustained
average because event delivery is bursty (the aggregator batches up to 10,000
events per POST).

Memory is flat in N, which is the other half of the same story:

```
collector heap (anon) by session size       0      50 MiB   100 MiB
      1k tasks    62 MiB   ████████████
     10k tasks    88 MiB   █████████████████▋
     50k tasks   114 MiB   ██████████████████████▊
    100k tasks   117 MiB   ███████████████████████▍
```

A caveat that matters for `limits`: the collector writes its spool files, so the
cgroup's total (`memory.current`, which includes page cache and is what
`working_set` and eviction see) reaches **235–251 MiB** at 100k tasks while the
heap is only 117 MiB. Page cache is reclaimable — this is not an OOM risk — but a
256Mi limit would keep the container in constant reclaim.

Backpressure never engaged: **zero disk-pressure 503s and zero upload failures**
in every run where collector logs were captured (all three 100k runs plus
`rerun-A-n1000`). The 160 MB backpressure threshold was never approached because
the single upload worker kept up. Collector counters in the other matrix runs
read zero because their uploads all happened during termination, which the log
follower in that harness version did not capture — `collector_log_capture=no` in
`derived.csv` marks those; do not read them as measured zeros.

---

## 5. Rate is set by the workload, not by `num_cpus`

Axis B varied task concurrency (`num_cpus` 0.5 → 0.05, i.e. 4 → 40 concurrent
tasks per node) at a fixed N = 20,000, expecting event rate to rise:

| `num_cpus` | concurrency/node | driver rate | peak node event rate |
|---:|---:|---:|---:|
| 0.5 | 4 | 2,893 tasks/s | 4,646 events/s |
| 0.2 | 10 | 2,501 tasks/s | 4,776 events/s |
| 0.1 | 20 | 1,700 tasks/s | 4,277 events/s |
| 0.05 | 40 | 1,659 tasks/s | 4,538 events/s |

More concurrency made things **slower**, not faster. For no-op tasks the
bottleneck is the driver's submission loop and the scheduler, not worker
occupancy, and 40 Python worker processes per node cost more than they add. The
event rate is essentially constant at ~4,500 events/s regardless.

The practical consequence for sizing: **you cannot infer a collector's load from
`num_cpus` or replica count — only from tasks/s per node.**

---

## 6. Event loss at the tail

Task IDs seen in storage versus tasks submitted, at the highest rates tested:

| Run | Driver rate | Drain sleep | Head event buffer | Task IDs seen |
|---|---:|---:|---:|---:|
| `A-n100000` | 3,084 tasks/s | 10 s | default (100k) | **99,540 / 100,000** |
| `C-gzip-n100000` | 2,880 tasks/s | 10 s | default | 100,000 / 100,000 |
| `rerun-A-n100k-ring` | 2,692 tasks/s | 10 s | default | 100,000 / 100,000 |
| `armA-n100k` | 2,494 tasks/s | 30 s | 1,000,000 | 100,000 / 100,000 |

One run lost 460 task definitions (0.46%). The collector was not the cause —
zero 503s, zero upload failures, and the loss is of *definition* events that
never arrived. The consistent explanation is the **shutdown tail** in Ray's
CoreWorker: its status-event buffer drains at up to 10,000 events per flush
interval, so a backlog of ~76,000 events needs ~7.6 s to drain, and the driver
slept only 10 s before exiting. At 2,880 tasks/s the required drain fits inside
the sleep; at 3,084 it does not.

This is a benchmark-harness observation, not a KubeRay defect: real jobs are not
usually a tight submission loop that exits immediately. But it does mean **event
completeness is not guaranteed at extreme submission rates**, and the knob that
governs it is `RAY_task_events_max_num_status_events_buffer_on_worker` on the
process that owns the tasks (the driver, on the head) — not the GCS-side
`RAY_ray_event_recorder_max_queued_events`, which is a different buffer on a
different path.

---

## What to do with all this → [SIZING.md](SIZING.md)
