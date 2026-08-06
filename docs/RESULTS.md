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

Events split across nodes almost evenly — **even though the head runs no tasks
at all.** It is started with `num-cpus: "0"`, so every task executes on the
worker, and the split comes from roles rather than placement: the driver on the
head *owns* every task, and the worker *executes* them.

| Event type | head (`num-cpus: 0`) | worker |
|---|---:|---:|
| `TASK_DEFINITION` (owner) | 50,005 | 0 |
| `TASK_LIFECYCLE` (both sides) | 68,117 | 50,032 |
| `TASK_PROFILE` (executor) | 40 | 50,005 |
| **total** | **118,177 (54%)** | **100,037 (46%)** |
| raw bytes | 87.8 MiB | 63.2 MiB |
| peak 10 s rate | 7,485/s | 6,433/s |

Counts are per *receiving collector*, keyed by the node segment of the object
path. The head file also holds 40 profile events — the driver's own — so "the
worker emits the profile events" is true of the benchmark tasks, not literally
of every record in the bucket.

The 40 profile events on the head are the driver's own bookkeeping. The
practical consequence: **a head collector's load scales with tasks *owned* by
drivers there, not with tasks executed there** — excluding the head from
scheduling does not make its collector cheap.

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

Cold load = `GET /enter_cluster` on a dead session. **The table below ran under
the sample manifest's 500m CPU limit, which the load saturates** — see
[FINDINGS.md](FINDINGS.md#2-the-sample-manifest-gives-the-history-server-500m-of-cpu-and-the-cold-load-saturates-it).

| N | cold load | ms/task | peak heap | heap/task | avg cores |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 2.3 s | 2.28 | 95 MiB | — | 0.42 |
| 5,000 | 9.8 s | 1.97 | 184 MiB | 38 KiB | 0.46 |
| 10,000 | 19.9 s | 1.99 | 291 MiB | 31 KiB | 0.48 |
| 20,000 | 33.9–41.3 s | 1.69–2.06 | 467–494 MiB | 25 KiB | 0.48–0.49 |
| 50,000 | 97.9 s | 1.96 | 1,132 MiB | 24 KiB | 0.49 |
| 100,000 | **never completed** under the default timeout; **151.3 s** with it raised | 1.51 | 2,202 MiB | — | 0.50 |

At 100k under `500m` no request ever returned 200 in any run, up to a 21-minute
probe budget — because the server's `--session-process-timeout` (2 min default)
aborts the load and every retry starts over. With that flag raised to 30 minutes
the same session loads in **151.3 s**, i.e. 1.51 ms/task, the same constant as
50k (76.3 s, 1.53 ms/task). There is no knee.

Earlier versions of this report quoted the elapsed probe time (907 s) as if it
were a load time and called the result superlinear. Both claims were wrong and
have been removed.

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

### The same axis at `limits.cpu: 4`

| N | cold load | ms/task | peak heap | avg cores | peak cores |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 0.69 s | 0.69 | 97 MiB | 0.6 | 1.4 |
| 5,000 | 3.17 s | 0.63 | 197 MiB | 0.9 | 1.9 |
| 10,000 | 6.25 s | 0.63 | 224 MiB | 1.0 | 2.0 |
| 20,000 | 14.45 s | 0.72 | 510 MiB | 1.1 | 2.1 |
| 50,000 | 30.52 s | 0.61 | 1,192 MiB | 1.18 | 2.17 |
| 100,000 | 62.67 s | 0.63 | 1,673 MiB | 1.16 | 2.22 |

**0.61–0.72 ms/task, flat across two orders of magnitude.** The 500m column above
is 1.88–2.28 ms/task up to 50k and then 9.07 ms/task at 100k; the difference is
the quota, not the data.

### Is the runtime's automatic tuning already right? Yes.

At a fixed `limits.cpu: 2` and 100k tasks, overriding what Go picks for itself
only makes things worse:

| Override | Cold load | GC share | Peak Go heap |
|---|---:|---:|---:|
| none (`GOMAXPROCS`=2, `GOGC`=100) | **64.4 s** | 5% | ~1.2 GB |
| `GOMAXPROCS=4` | 64.6 s | 3% | 1.3 GB |
| `GOMAXPROCS=8` | 72.2 s | 2% | 1.4 GB |
| `GOGC=200` | 65.9 s | 2% | 2.0 GB |
| `GOGC=400` | 70.8 s | 1% | 3.1 GB |

Forcing more `P`s than the quota allows costs 12%. Raising `GOGC` does exactly
what it promises — GC share falls from 5% to 1% — and the load still gets
*slower* while using 2.5× the memory, because GC was never the constraint.

Holding `GOMAXPROCS` fixed at 2 and varying only the quota isolates the real
effect: 50k tasks go from **84.9 s at `500m` to 34.1 s at 4 cores**, with
identical Go parallelism on both sides.

### Interventions measured

| Change | N | Before | After | |
|---|---:|---:|---:|---|
| CPU limit 500m → 2 | 50,000 | 84.9 s | 31.7 s | 2.7× |
| CPU limit 500m → 4 | 100,000 | never completed | 62.7 s | — |
| per-event `Infof` → `Debugf` | 50,000 | 97.9 s | 85.3 s | 1.15× |
| per-event `Infof` → `Debugf` (at 500m) | 100,000 | never completed | still never completed | none |
| forced `GOMAXPROCS=4` at 500m | 50,000 | 85.3 s | 84.7 s | none |
| forced `GOMAXPROCS=1` at 4 cores | 100,000 | 62.7 s | 76.3 s | 0.8× |

Measured GC share (`GODEBUG=gctrace=1`): 8% at `500m`/50k, 1% at 4 cores/50k,
2% at 4 cores/100k. GC gets more expensive under starvation but is nowhere near
large enough to be the main cost.

Total CPU consumed is roughly conserved at 50k — about 41–42 core-seconds either
way (0.8 ms of CPU per task) — so wall time is essentially that work divided by
the parallelism the quota allows.

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

**The 503 counter is broken and its zeros mean nothing.** The collector's
disk-pressure path returns HTTP 503 without logging
(`pkg/collector/eventcollector/eventcollector.go:313`), and the counter greps
the logs, so it reports zero unconditionally. Upload-failure and queue-full
counters do have log lines and were genuinely zero in the runs where logs were
captured; runs marked `collector_log_capture=no` in `derived.csv` captured no
lines at all.

Memory here is cgroup *anonymous* memory, not Go heap. Where both are available
the gap is material: at 50k the History Server's gctrace reported an 816 MB Go
heap against 1,015 MiB of anonymous memory.

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
| `hscpu4-n50000` | 2,965 tasks/s | 20 s | default | 50,000 / 50,000 |
| `hscpu4-n100000` | 3,138 tasks/s | 30 s | default | **98,322 / 100,000** |

Loss follows the submission rate: both runs above 3,000 tasks/s are short
(0.46% and 1.7%), every run at or below 2,965 is complete. Two caveats on these
counts. The denominator includes the driver's own task definition, so a run
showing exactly `N/N` could still be missing one benchmark task, and a shortfall
is one larger than it appears. And we cannot rule the collector out on
backpressure grounds: its 503 path emits no log line, so the counter that reads
zero could not have observed one.

Our first explanation was Ray's shutdown tail (the owner's status-event buffer
drains at ≤10,000 events per flush, so a backlog needs seconds the exiting
driver does not give it). **The `hscpu4-n100000` run refutes it**: 30 s of drain
and it still lost 1.7%. Overflow during submission fits the data better. The
governing knob is `RAY_task_events_max_num_status_events_buffer_on_worker` on
the *owning* process (the driver, on the head) — not the GCS-side
`RAY_ray_event_recorder_max_queued_events`, which is a different buffer.
