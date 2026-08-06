# History Server Benchmark

Opt-in, single-run benchmark answering three sizing questions with one real
workload — 1 RayCluster + 1 RayJob submitting N no-op tasks (default 50,000):

1. **Collector sizing** — CPU / RSS of every `collector` sidecar while the job
   runs, plus backpressure signals (disk-pressure 503s, rotation-queue-full,
   upload failures) scraped from collector logs.
2. **History server sizing** — cold `/enter_cluster` load latency and RSS for
   the N-task session, `GET /clusters` scan latency, warm snapshot endpoint
   p50/p95.
3. **Storage sizing** — bytes per category (`job_events` / `node_events` /
   `logs` / `fetched_endpoints`), events-per-task (k), raw bytes/event, and the
   gzip ratio when compression is enabled.

## Prerequisites

Same as the e2e suite (see `historyserver/DEVELOPMENT.md`): a kind cluster with
the KubeRay operator running and the `collector` / `historyserver` images
loaded. MinIO is applied automatically by the harness (`EnsureS3Client`).

> ⚠️ Use a **dedicated kind cluster + kubectl context** for benchmark runs.
> The bucket name, image tags, and localhost ports are shared globals — another
> e2e run on the same cluster can delete the bucket mid-benchmark.

## Run

Full matrix (dedicated `bench` kind cluster, images built from this checkout,
14 runs ≈ 2–2.5h, aggregated summary at the end):

```bash
cd historyserver/test/benchmark
./run_matrix.sh                # BENCH_ONLY=A|B|C for one axis; BENCH_TEARDOWN=1 to delete the cluster after
```

| axis | runs | answers |
|---|---|---|
| A | N = 1k/5k/10k/50k/100k @ 0.2 | storage & HS load-latency/memory vs total tasks; collector flatness |
| B | N=20k @ num_cpus 0.5/0.2/0.1/0.05 | collector CPU/mem vs per-node event rate |
| C | A repeated with gzip on | compression savings vs total tasks |

Single run against an existing cluster + operator:

```bash
cd historyserver
BENCH_RUN=1 go test ./test/benchmark -run TestHistoryServerBenchmark -v -timeout 90m
```

Without `BENCH_RUN=1` the test skips immediately, so `go test ./...` stays fast.

## Knobs (env vars)

| Variable | Default | Meaning |
|---|---|---|
| `BENCH_TASK_COUNT` | `50000` | Tasks the driver submits |
| `BENCH_WAVE_SIZE` | `2000` | Tasks per `ray.get` wave (bounds in-flight refs) |
| `BENCH_TASK_NUM_CPUS` | `0.2` | `num_cpus` per task; fractional raises concurrency (0.2 → 10 concurrent on the 2-CPU worker; going lower multiplies Ray worker processes and can OOM the 2G worker container) |
| `BENCH_COMPRESSION` | `false` | Sets `RAY_COLLECTOR_EVENT_COMPRESSION_ENABLED` on collectors |
| `BENCH_EVENT_ROTATION_INTERVAL` | (collector default, 5m) | Sets `RAY_COLLECTOR_EVENT_ROTATION_INTERVAL`, e.g. `1m` to observe steady-state rotation during short jobs |
| `BENCH_WORKER_MEMORY_LIMIT` | (manifest default, 2G) | Overrides the ray-worker container memory limit; needed below `num_cpus=0.2` |
| `BENCH_RAY_EVENT_RING` | (Ray default, 10000) | Sets `RAY_ray_event_recorder_max_queued_events` on Ray containers; the default ring drops definition events at ~13k events/s |
| `BENCH_HS_ENTER_TIMEOUT` | `5m` | Client budget for the first `/enter_cluster` attempt |
| `BENCH_HS_WARM_WAIT` | `15m` | After a timed-out first attempt, keep re-probing (the server-side load keeps running); the first warm hit upper-bounds the true load time |
| `BENCH_KIND_NODE` | `kind-control-plane` | kind node container name for the cgroup sampler |
| `BENCH_JOB_TIMEOUT` | `45m` | Budget for the RayJob to succeed |
| `BENCH_WARM_ITERATIONS` | `10` | Requests per warm history server endpoint |
| `BENCH_OUT_DIR` | `out` | Report directory (one timestamped subdir per run) |
| `BENCH_SKIP_CLEANUP` | `false` | Keep bucket contents after the run |

Start with a smoke run (`BENCH_TASK_COUNT=500`) before the full 50k run.

### Comparing history server configurations honestly

A full run regenerates the session, so two cells never read the same bytes:
object layout, event counts and even task loss differ between them. At 100k
tasks two runs of the *same* configuration differed by 10.7 s, which is larger
than most of the differences worth measuring. Generate the data once and reuse it:

```bash
# 1. produce one session and keep it
BENCH_RUN=1 BENCH_TASK_COUNT=50000 BENCH_SKIP_CLEANUP=1 go test ./test/benchmark -run TestHistoryServerBenchmark -v -timeout 90m
#    the log prints:  BENCH_HS_ONLY=<namespace>/<cluster>/<sessionID>

# 2. measure any number of configurations against those exact bytes
BENCH_RUN=1 BENCH_HS_ONLY=test-ns-abcde/raycluster-historyserver/session_... \
  BENCH_HS_CPU_LIMIT=2 BENCH_HS_ENV=GODEBUG=gctrace=1 \
  go test ./test/benchmark -run TestHistoryServerBenchmark -v -timeout 30m
```

Each invocation deploys a fresh history server, so the snapshot cache starts
empty every time. Randomize the order of the configurations across repeats —
an hour of runs drifts with host load, and running all of one setting first
aliases that drift into the result.

## What a run does

Resource metrics come from two complementary sources:

- **kubelet summary API** (1s polls, ~10s effective resolution): `working_set`
  — the metric k8s eviction/limits act on. → `samples.csv`
- **cgroup v2 direct reads** (1s, via one `docker exec` loop on the kind
  node): `anon` (pure heap, no page-cache inflation), `memory.current`, and
  the kernel-recorded lifetime `memory.peak` that polling can never miss.
  → `cgroup_samples.csv`

Event traffic is attributed per Ray node (whose aggregator emitted it) from
the event files' `{nodeID}/` path segment and per-event timestamps
→ `node_rate.csv` pairs with the same-node collector series for rate-based
collector sizing.

```
apply RayCluster (collector sidecars)          phase: baseline
  └─ bucket snapshot T0; kubelet sampler (1s) + cgroup sampler (1s) start
run RayJob: N no-op tasks in waves             phase: job
  └─ scrape collector logs (upload timeline, 503s) while pods still exist
  └─ bucket snapshot T1  → diff T1-T0 = uploaded while running
delete RayCluster                              phase: flush
  └─ graceful shutdown = final rotate/upload + session marker write
  └─ bucket snapshot T2  → diff T2-T1 = flush-only volume (= SIGKILL-at-risk)
walk bucket, decode every event JSONL(.gz)     phase: storage-scan
deploy history server, port-forward            phase: historyserver
  └─ GET /clusters ×5, /enter_cluster cold load, warm endpoints ×N
write out/<ts>/bench-report.{md,json} + samples.csv
```

Every diff also asserts additions stay under the expected prefixes, so a write
landing anywhere unexpected in the bucket is reported instead of missed.

The report is also printed to the `go test -v` log, and partial results are
written even when an assertion fails mid-run.

## Reading the numbers

- **Fact to keep in mind**: process start of the history server does zero
  storage I/O — the meaningful "cold start" is `/enter_cluster`, which reads
  and decodes the whole session serially.
- `GET /clusters` rescans the entire `cluster-metadata/` prefix on every
  request (no cache, `MaxKeys=100` pagination), so its latency scales with
  total sessions ever stored, not with this run.
- A `500` from `/enter_cluster` after ~2min means the session exceeded the
  server's cold-load timeout (`DefaultSessionProcessTimeout`) — that is a
  finding, not a harness bug.
- Missing `taskDefinitionEvent` IDs (report shows `distinct/expected`) can come
  from source-side drops (Ray's recorder ring, default 10k events) or from the
  30s pod termination grace period cutting the final flush short.
- kind on macOS runs inside a VM: treat CPU numbers as **relative** (scaling
  curves, ratios); confirm absolute sizing on Linux before publishing.
