# KubeRay History Server Benchmark

End-to-end sizing measurements for the [KubeRay](https://github.com/ray-project/kuberay)
History Server data path — Ray event aggregator → `collector` sidecar → object
storage → History Server — running a real Ray job on a real cluster, from 1,000
to 100,000 tasks in a single session.

Every chart below is generated from [`results/derived.csv`](results/derived.csv),
which is generated from the raw per-run reports in [`results/`](results/), which
are produced by the harness in [`benchmark/`](benchmark/). Nothing here is
hand-entered.

```
events   E ≈ N × 4.36              1 definition + 2.36 lifecycle + 1 profile per task
storage  S ≈ N × 3.15 KiB          raw   →   N × 0.29 KiB with gzip (91% smaller)
HS load  T ≈ N × 0.62 ms           with enough CPU — but N × 2 ms, and superlinear
                                   past 50k, at the 500m limit the sample manifest ships
HS RSS   M ≈ 100 MiB + N × 23 KiB  per session held in the snapshot cache
```

> **The single most valuable thing in this repo:** KubeRay's sample History
> Server manifest caps the container at `500m` CPU, and the cold load saturates
> it for its entire duration against ~1.2 cores of demand. Same code, same
> cluster, 50,000 tasks: **85 s at `500m` versus 32 s at `2`**. At 100,000 tasks
> the `500m` configuration never returned a response at all, while `2`–`4`
> finish in 63–75 s. Nothing else measured here comes close to that return.

`N` = tasks in one Ray session. Ray actor calls emit task events too, so count them in `N`.

---

## Change these two things first

**1. Give the History Server real CPU.** `historyserver/config/historyserver.yaml`
sets only `limits.cpu: "500m"`, so Kubernetes pins `requests.cpu` there too, and
the cold load saturates it — 0.42–0.50 cores for the entire load, in every run.

```yaml
resources:
  requests:
    cpu: "500m"
  limits:
    cpu: "4"      # was "500m"
```

50k tasks: **85 s → 32 s**. At 100k the `500m` configuration never completed a
cold load inside a 21-minute budget, so there is no "before" number to divide —
that is the finding. With enough CPU the cost per task is flat at **0.61–0.72 ms**
from 1k to 100k. The load uses ~1.2 cores sustained and bursts to 2.2, so `2`
already captures most of the win and `8` or no limit at all buy nothing.

**2. Turn compression on.** `RAY_COLLECTOR_EVENT_COMPRESSION_ENABLED=true` on the
collector sidecars cuts stored bytes by **91%** (3.15 KiB → 0.29 KiB per task)
with no measurable CPU or load-time cost. It is off by default.

Everything else worth configuring is in [docs/SIZING.md](docs/SIZING.md); the two
above are the ones with no downside.

### And one that needs a code change

Every cold load writes one INFO line per event (`eventserver.go:136`, called from
the per-event loop at `:1164`) — 436,000 lines for a 100k-task session, with no
log-level flag to turn them off:

```go
logrus.Debugf("current eventType: %v", eventType)   // was Infof
```

Worth 13% at 50k (97.9 s → 85.3 s). Note it does **not** fix the 100k case — that
was the CPU limit, as the charts below show.

---

## Storage

<picture><source media="(prefers-color-scheme: dark)" srcset="docs/img/storage-per-task-dark.svg"><img alt="Storage cost per task: 3.15 KiB raw vs 0.29 KiB gzipped, constant from 1k to 100k tasks" src="docs/img/storage-per-task-light.svg"></picture>

Compression saves **91%**, and the ratio does not move between 1,000 and 100,000
tasks — event JSONL is highly repetitive at every scale. It is **off by default**,
and it cost nothing measurable: CPU per event was 118–125 µs with gzip against
115–124 µs without, and cold-load time changed by under 10%.

<picture><source media="(prefers-color-scheme: dark)" srcset="docs/img/storage-dark.svg"><img alt="Total storage per session: 300 MiB raw vs 27 MiB gzipped at 100k tasks" src="docs/img/storage-light.svg"></picture>

<picture><source media="(prefers-color-scheme: dark)" srcset="docs/img/flush-split-dark.svg"><img alt="When events reach storage: at 10k and 50k tasks, 100% only during the shutdown flush" src="docs/img/flush-split-light.svg"></picture>

With the default 5-minute rotation interval, **a job shorter than 5 minutes
uploads nothing until its pod terminates gracefully.** Everything in orange would
be lost to a SIGKILL — along with the session marker written during the same
drain, without which the session never appears in the UI at all.

## History Server

<picture><source media="(prefers-color-scheme: dark)" srcset="docs/img/hs-cpu-limit-dark.svg"><img alt="Cold load of a 50k-task session by CPU limit: 84.9s at 500m, 38.2s at 1 core, 31.7s at 2, 34.9s at 4" src="docs/img/hs-cpu-limit-light.svg"></picture>

Cold load is `GET /enter_cluster` — the first time anyone opens a dead session,
and where all of the History Server's cost lives (process start does zero
storage I/O). Same session, same build, same cluster; the only difference is
`resources.limits.cpu`.

<picture><source media="(prefers-color-scheme: dark)" srcset="docs/img/hs-load-knee-dark.svg"><img alt="Per-task cold load cost: flat at 2ms then 9.07ms at 100k under 500m, versus a flat 0.61-0.63ms at 4 cores" src="docs/img/hs-load-knee-light.svg"></picture>

The "superlinear blowup past 50k tasks" was not the data — it was the quota.
Under `500m` the container sat at 0.42–0.50 cores for the entire load in every
run, so as the live heap grew, Go's GC and the decoder fought over the same half
core. Given 4 cores (it used 1.18 average, 2.2 peak) the cost per task is flat
again: **0.61 ms/task at 50k, 0.63 ms/task at 100k**.

Two effects could be bundled here, and we separated them. The History Server is
a Go 1.26 binary, and since Go 1.25 the runtime takes `GOMAXPROCS` from the
cgroup CPU **limit**, never the request — rounded up, with a floor of 2. So
`500m` also means `GOMAXPROCS = 2`. Controls: forcing `GOMAXPROCS=4` while
holding the limit at `500m` changes nothing (85.3 s → 84.7 s), and raising the
limit to 4 with `GOMAXPROCS` pinned to 1 still loads 100k in 76.3 s. **The CFS
quota is the binding constraint; Go parallelism is worth about 1.2×.** Measured
GC share backs this up: 8% of CPU at `500m`, 1–2% at 4 cores.

<picture><source media="(prefers-color-scheme: dark)" srcset="docs/img/hs-load-dark.svg"><img alt="Log-log scaling curve: at 4 cores cold load is a straight line from 0.69s at 1k tasks to 62.7s at 100k; the 500m series stops at 50k because 100k never completed" src="docs/img/hs-load-light.svg"></picture>

On log-log axes a straight line means the cost scales linearly with task count.
The 4-core series is straight across two orders of magnitude. The 500m series
tracks it up to 50k and then stops: at 100k that configuration never returned a
response, so there is no point to plot. The server's own
`--session-process-timeout` (2 minutes by default) aborts a load that long and
the next attempt starts from scratch, so "how slow is it really" is a question
this configuration cannot answer without raising that flag.

Two other things sit on this path regardless of CPU: a hardcoded 35 s HTTP
`WriteTimeout` that is *shorter* than the 2-minute load timeout it should
outlive, and ~436,000 INFO log lines per 100k-task load (removing them bought
13%). See [docs/FINDINGS.md](docs/FINDINGS.md).

<picture><source media="(prefers-color-scheme: dark)" srcset="docs/img/hs-memory-dark.svg"><img alt="History Server peak heap: 184 MiB at 5k tasks up to 2.2 GiB at 100k tasks" src="docs/img/hs-memory-light.svg"></picture>

**~23 KiB of heap per task**, retained: the snapshot is a fully materialized
`[]map[string]any` kept in an LRU of up to 100 sessions with no TTL by default.
Budget for every session a user might open, not for one.

> **Every load time above was measured under a 500m CPU limit** — the value
> `historyserver/config/historyserver.yaml` ships. The container sat at
> 0.42–0.50 cores for the whole load in all thirteen runs, i.e. saturated.
> Raising that limit is the first thing to try, and costs nothing but YAML.

## Collector sidecar

<picture><source media="(prefers-color-scheme: dark)" srcset="docs/img/collector-cpu-dark.svg"><img alt="Collector CPU per event is a flat 115-131 microseconds across all runs and rates" src="docs/img/collector-cpu-light.svg"></picture>

The most portable result here: **≈120 µs of CPU per event ≈ 0.52 ms per task**,
unchanged across three orders of magnitude of session size and a 40× range of
event rate. Size the collector from the *rate* on its node, never from the job's
total size.

<picture><source media="(prefers-color-scheme: dark)" srcset="docs/img/collector-memory-dark.svg"><img alt="Collector heap stays between 62 and 147 MiB regardless of session size" src="docs/img/collector-memory-light.svg"></picture>

Memory does not grow with session size because the collector streams to disk
and uploads in the background — events never accumulate in it. Note this is
*anonymous memory* from cgroup accounting, not Go heap: where we have both
numbers, anonymous memory overstates the Go heap by roughly 20%.

We cannot say backpressure never engaged. The collector's 503 path writes an
HTTP response without logging anything, and our counter searched the logs, so it
reports zero whether or not any 503 happened. That instrumentation gap is a
finding, not a result.

**Size the head's collector like a worker's.** The head ran `num-cpus: "0"` — not
a single task executed there — and it still produced **54% of all events**, because
the driver that *owns* every task lives there. Owner-side events (all 50,005
`TASK_DEFINITION`s plus 68,117 of the 118,149 `TASK_LIFECYCLE`s) do not care where
the task ran.

<picture><source media="(prefers-color-scheme: dark)" srcset="docs/img/rate-vs-concurrency-dark.svg"><img alt="Raising task concurrency from 4 to 40 per node lowered throughput and left event rate flat" src="docs/img/rate-vs-concurrency-light.svg"></picture>

Raising task concurrency 10× did **not** raise the event rate — for small tasks
the driver's submission loop is the bottleneck, and 40 Python workers per node
cost more than they add. You cannot infer collector load from `num_cpus` or
replica count; only from tasks/s per node.

---

## Sizing everything else

| | Recommendation | Why |
|---|---|---|
| Collector CPU | `requests: R × 0.52 millicores`, `limits:` 3× that (R = sustained tasks/s **per node**) | 120 µs/event × 4.36 events/task; bursts run 2–4× the sustained average |
| Collector memory | `requests: 128Mi`, `limits: 512Mi` | heap is flat at ~120 MiB; cgroup total incl. page cache hits 251 MiB |
| History Server memory | `100 MiB + 23 KiB × N × sessions cached` | snapshots are retained in the LRU |
| `--session-process-timeout` | `N × 0.62 ms × 2` | with adequate CPU the 2-minute default already covers ~190k tasks |
| Session size | under ~56k tasks, until `WriteTimeout` is configurable | a load over 35 s cannot deliver any response to an HTTP/1 client |

Full derivation and worked examples: **[docs/SIZING.md](docs/SIZING.md)**.

## Documentation

| Doc | Contents |
|---|---|
| [docs/SIZING.md](docs/SIZING.md) | Decide your own resource requests — formulas, tables, checklist |
| [docs/RESULTS.md](docs/RESULTS.md) | Every measurement with the numbers behind these charts |
| [docs/FINDINGS.md](docs/FINDINGS.md) | Bugs and design limits found, with source references |
| [docs/DIMENSIONS.md](docs/DIMENSIONS.md) | The data path and what each axis varied |
| [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md) | Hardware, versions, image digests |
| [benchmark/README.md](benchmark/README.md) | Harness internals and every knob |

## Reproduce

The harness is a Go test that lives inside a KubeRay checkout (it reuses the
History Server e2e support package):

```bash
git clone https://github.com/ray-project/kuberay.git
./benchmark/install.sh /path/to/kuberay
cd /path/to/kuberay/historyserver/test/benchmark
./run_matrix.sh          # dedicated kind cluster, 14 runs, ~2.5h
```

One point instead of the matrix:

```bash
cd /path/to/kuberay/historyserver
BENCH_RUN=1 BENCH_TASK_COUNT=10000 go test ./test/benchmark -run TestHistoryServerBenchmark -v -timeout 90m
```

Regenerate the table and charts from whatever is in `results/`:

```bash
python3 tools/derive.py results > results/derived.csv
python3 tools/gen_charts.py results/derived.csv docs/img
```

## Scope and honesty notes

- Measured on **kind on macOS** (Docker Desktop VM). Byte counts, event counts,
  ratios and scaling shapes port anywhere; absolute CPU seconds do not.
- Tasks are no-ops. Real tasks emit the same event counts but write more logs;
  the `logs/` category here is only ~45 B/task.
- One head + one worker node. Per-node numbers generalize; cluster totals are a
  sum over nodes, not a measured scale-out.
- Several numbers in `results/` are not measurements and are labelled as such:
  `A-n1000` (flush race in an early harness version — the fixed rerun is
  `rerun-A-n1000`), every run marked `collector_log_capture=no`, every
  `enterMeasured: false` run (the client never got a 200, so the elapsed time is
  the probe budget), and all `collector_503s` values (the 503 path emits no log
  line for the counter to find).
- Earlier versions of this report quoted 907 s as a 100k cold-load time. That was
  elapsed probe time on a load that never returned. It has been removed
  everywhere; the honest statement is that the configuration did not complete.

## License

Apache 2.0, matching KubeRay. The harness derives from KubeRay's e2e test support code.
