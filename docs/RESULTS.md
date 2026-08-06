# Results — the numbers

The narrative and charts are in the [README](../README.md). This is the
reference table behind them, plus the measurements that did not make it into a
chart. Everything derives from [`../results/derived.csv`](../results/derived.csv).

## Event mix

At N = 50,000 the bucket held 218,214 events:

| Event type | Count | Per task |
|---|---:|---:|
| `TASK_LIFECYCLE_EVENT` | 118,149 | 2.363 |
| `TASK_PROFILE_EVENT` | 50,045 | 1.001 |
| `TASK_DEFINITION_EVENT` | 50,005 | 1.000 |
| node / driver-job / actor events | 15 | ~0 |

**k = 4.36 events per task** (4.30–4.76 across runs — lifecycle events fire two
or three times depending on how state transitions batch), **725 B per event**
raw (707–745), independent of N.

Events split across nodes almost evenly, because each task's lifecycle is
recorded twice — once by its owner on the head, once by its executor:

| Node | Events | Raw bytes | Peak 10 s rate |
|---|---:|---:|---:|
| head | 118,177 (54%) | 87.8 MiB | 7,485/s |
| worker | 100,037 (46%) | 63.2 MiB | 6,433/s |

## Storage

| N | raw | gzip | ratio | raw/task | gzip/task | logs |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 2.87 MiB | 0.26 MiB | 0.089 | 3.01 KiB | 269 B | 0.30 MiB |
| 5,000 | 15.24 MiB | 1.38 MiB | 0.090 | 3.12 KiB | 288 B | 0.51 MiB |
| 10,000 | 31.03 MiB | 2.83 MiB | 0.091 | 3.18 KiB | 297 B | 0.73 MiB |
| 50,000 | 151.81 MiB | 13.85 MiB | 0.091 | 3.11 KiB | 291 B | 2.48 MiB |
| 100,000 | 301.44 MiB | 27.47 MiB | 0.091 | 3.09 KiB | 288 B | 4.61 MiB |

Logs are ~0.3 MiB fixed plus ~45 B/task for no-op tasks; a workload that prints
grows this term independently.

When the bytes actually reach the bucket, from snapshots at T0 (before), T1
(after the job) and T2 (after graceful deletion):

| N | during job | during shutdown flush |
|---:|---:|---:|
| 10,000 | 0 MiB | 30.8 MiB (100%) |
| 50,000 | 0 MiB | 153.3 MiB (100%) |
| 100,000 | 142.9 MiB | 159.9 MiB (53%) |

## History Server

Cold load = `GET /enter_cluster` on a dead session. **All of these ran under the
sample manifest's 500m CPU limit, which the load saturates** — see
[FINDINGS.md](FINDINGS.md#2-the-sample-manifest-gives-the-history-server-500m-of-cpu-and-the-cold-load-saturates-it).

| N | cold load | ms/task | peak heap | heap/task | avg cores |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 2.3 s | 2.28 | 95 MiB | — | 0.42 |
| 5,000 | 9.8 s | 1.97 | 184 MiB | 38 KiB | 0.46 |
| 10,000 | 19.9 s | 1.99 | 291 MiB | 31 KiB | 0.48 |
| 20,000 | 33.9–41.3 s | 1.69–2.06 | 467–494 MiB | 25 KiB | 0.48–0.49 |
| 50,000 | 97.9 s | 1.96 | 1,132 MiB | 24 KiB | 0.49 |
| 100,000 | 907.3 s | 9.07 | 2,202 MiB | 23 KiB | 0.50 |

The 100k figure is an upper bound with 10 s granularity: the client request
times out, the server keeps loading (singleflight), and the harness re-probes
until the warm hit lands. In the matrix runs the probe budget (15 min = 900 s)
was shorter than the load, which is why they report "never succeeded".

Warm reads from a loaded snapshot, 10 iterations each:

| Endpoint | p50 | Response |
|---|---:|---:|
| `/api/v0/tasks?limit=10000` | 0.99 s | 4.5 MiB |
| `/api/v0/tasks/summarize` | 0.15–2.5 s | 408 B |
| `/api/jobs/` | 9–46 ms | 1.2 KiB |
| `/nodes?view=summary` | 13–89 ms | 2.8 KiB |
| `/clusters` (uncached full scan) | 5–140 ms | 208 B |

`limit` above 10,000 returns `400` by design (`RayMaxLimitFromAPIServer`,
mirroring Ray's dashboard API), so the task list is always paginated.

### Interventions measured so far

| Change | N | Before | After |
|---|---:|---:|---:|
| per-event `Infof` → `Debugf` | 50,000 | 97.9 s | 85.3 s (−13%) |

## Collector

CPU cost per event, computed as (avg cores × job wall-clock) ÷ events on that node:

| Peak node rate | CPU/event | Avg cores | Peak cores |
|---:|---:|---:|---:|
| 202–246/s | 143–150 µs | 0.01 | 0.24 |
| 1,209–1,378/s | 124 µs | 0.05 | 0.60–0.67 |
| 2,342–2,537/s | 119–122 µs | 0.08 | 0.79–0.95 |
| 4,277–4,776/s | 117–124 µs | 0.13–0.17 | 0.92–1.11 |
| 6,962–7,850/s | 115–131 µs | 0.29–0.46 | 1.05–2.05 |

Memory, worker sidecar (head is 10–25% higher):

| N | heap (anon) | cgroup total incl. page cache | kernel lifetime peak |
|---:|---:|---:|---:|
| 1,000 | 62 MiB | 65 MiB | 69 MiB |
| 10,000 | 88 MiB | 103 MiB | 110 MiB |
| 50,000 | 114 MiB | 174 MiB | 180 MiB |
| 100,000 | 118 MiB | 236 MiB | 248 MiB |

Zero disk-pressure 503s and zero upload failures in every run where collector
logs were captured (the three 100k runs and `rerun-A-n1000`). Runs marked
`collector_log_capture=no` in `derived.csv` have structurally zero counters —
their uploads happened during termination, which that harness version did not
capture. Do not read those zeros as measurements.

## Concurrency (axis B, N = 20,000)

| `num_cpus` | concurrency/node | driver rate | peak node event rate |
|---:|---:|---:|---:|
| 0.5 | 4 | 2,893 tasks/s | 4,646/s |
| 0.2 | 10 | 2,501 tasks/s | 4,776/s |
| 0.1 | 20 | 1,700 tasks/s | 4,277/s |
| 0.05 | 40 | 1,659 tasks/s | 4,538/s |

## Event loss at the tail

| Run | Driver rate | Drain sleep | Head event buffer | Task IDs seen |
|---|---:|---:|---:|---:|
| `A-n100000` | 3,084 tasks/s | 10 s | default (100k) | **99,540 / 100,000** |
| `C-gzip-n100000` | 2,880 tasks/s | 10 s | default | 100,000 / 100,000 |
| `rerun-A-n100k-ring` | 2,692 tasks/s | 10 s | default | 100,000 / 100,000 |
| `armA-n100k` | 2,494 tasks/s | 30 s | 1,000,000 | 100,000 / 100,000 |

One run lost 460 task definitions (0.46%). Not the collector — zero 503s, zero
upload failures, and the missing events never arrived. The fit is Ray's
shutdown tail: the owner's status-event buffer drains at ≤10,000 events per
flush, so a ~76,000-event backlog needs ~7.6 s and the driver slept 10 s. At
2,880 tasks/s the drain fits; at 3,084 it does not. The governing knob is
`RAY_task_events_max_num_status_events_buffer_on_worker` on the *owning*
process (the driver, on the head) — not the GCS-side
`RAY_ray_event_recorder_max_queued_events`.
