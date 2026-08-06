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
> it. Raising that limit to 4 cores made a 100,000-task session load
> **14.5× faster — 907 s → 62.7 s** — and removed the superlinearity entirely.
> Nothing else measured here comes close to that return.

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

100k tasks: **907 s → 62.7 s**. 50k tasks: 97.9 s → 30.5 s. Measured at every
size from 1k to 100k, the cost per task is then flat at **0.61–0.72 ms** instead
of degrading. The load uses ~1.2 cores sustained and bursts to 2.2, so `2`
captures most of the win if 4 is too much to reserve.

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

<picture><source media="(prefers-color-scheme: dark)" srcset="docs/img/hs-cpu-limit-dark.svg"><img alt="Cold load at 50k and 100k tasks: 97.9s and 907s at a 500m CPU limit versus 30.5s and 62.7s at 4 cores" src="docs/img/hs-cpu-limit-light.svg"></picture>

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

Two effects are bundled in that number, and we did not separate them: the
History Server is a Go 1.26 binary, and since Go 1.25 the runtime takes
`GOMAXPROCS` from the cgroup CPU **limit** (never the request), rounded up. So
`500m` also meant `GOMAXPROCS = 1` — no parallel garbage collection at all,
against a 2.2 GiB live heap. That is probably the larger half of the 14.5×. It
also means removing the limit outright is not free here: `GOMAXPROCS` would then
follow the node's core count. See [docs/FINDINGS.md](docs/FINDINGS.md).

<picture><source media="(prefers-color-scheme: dark)" srcset="docs/img/hs-load-dark.svg"><img alt="Cold load from 1k to 50k tasks tracks the 2 ms/task line exactly under the 500m limit" src="docs/img/hs-load-light.svg"></picture>

Under the shipped limit the 1k–50k region is still perfectly linear, which is
why the blowup at 100k looked like an algorithmic cliff rather than a resource
one. Two other things sit on this path regardless of CPU: a hardcoded 35 s HTTP
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

Memory is flat because the collector streams to disk and uploads in the
background — events never accumulate in its heap. Backpressure (503 to the Ray
aggregator) never engaged in any run.

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
- Two runs in `results/` are kept deliberately even though they are not usable
  measurements: `A-n1000` (hit a flush race in an early harness version — the
  fixed rerun is `rerun-A-n1000`) and every run marked
  `collector_log_capture=no` in `derived.csv`, whose collector counters read
  zero because no upload lines were captured, not because nothing happened.

## License

Apache 2.0, matching KubeRay. The harness derives from KubeRay's e2e test support code.
