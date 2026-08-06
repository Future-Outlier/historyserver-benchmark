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
| `BENCH_RAY_STATUS_BUFFER_HEAD` | (Ray default, 100000) | Sets `RAY_task_events_max_num_status_events_buffer_on_worker` on the **head** Ray container only — the buffer the task owner drains at exit. (`RAY_ray_event_recorder_max_queued_events` is a different, GCS-side buffer and does not affect this path.) |
| `BENCH_DRIVER_DRAIN_SLEEP` | `10` | Seconds the driver sleeps after the last wave so the owner's status-event buffer drains before the process exits |
| `BENCH_S3_LOCAL_PORT` | `9002` | Local port for the benchmark's own MinIO port-forward; deliberately not 9000, which e2e suites fight over |
| `BENCH_HS_CPU_LIMIT` | (manifest default, `500m`) | Rewrites the history server manifest's CPU limit; `none` removes it and keeps `requests: 500m`. The shipped 500m is saturated for the whole cold load |
| `BENCH_HS_ENV` | (none) | Extra env on the history server container, `K=V,K=V` — e.g. `GOMAXPROCS=1`, `GODEBUG=gctrace=1`, `GOGC=400`. Needed to separate CFS quota from Go parallelism, since Go ≥1.25 derives GOMAXPROCS from the CPU limit |
| `BENCH_HS_ARGS` | (none) | Extra history server CLI flags, comma separated — e.g. `--session-process-timeout=30m`. Without this a load longer than 2 minutes is aborted server-side and every retry starts over |
| `BENCH_HS_ENTER_TIMEOUT` | `5m` | Client budget for the first `/enter_cluster` attempt |
| `BENCH_HS_WARM_WAIT` | `15m` | After a timed-out first attempt, keep re-probing (the server-side load keeps running); the first warm hit upper-bounds the true load time |
| `BENCH_KIND_NODE` | `kind-control-plane` | kind node container name for the cgroup sampler |
| `BENCH_JOB_TIMEOUT` | `45m` | Budget for the RayJob to succeed |
| `BENCH_WARM_ITERATIONS` | `10` | Requests per warm history server endpoint |
| `BENCH_OUT_DIR` | `out` | Report directory (one timestamped subdir per run) |
| `BENCH_SKIP_CLEANUP` | `false` | Keep bucket contents after the run |

Start with a smoke run (`BENCH_TASK_COUNT=500`) before the full 50k run.

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
- **`enterColdLatency` is a load time only when `enterMeasured` is true.** A
  timeout does not stop the server-side load, so the harness re-probes; if a
  probe lands, the elapsed time upper-bounds the true load. If none does, the
  elapsed time is just the probe budget — it says nothing about the server, and
  must not be quoted as a latency.
- Raise `--session-process-timeout` (via `BENCH_HS_ARGS`) before concluding a
  configuration is slow: at the 2-minute default the server aborts long loads
  and discards the partial state, so retries never converge.
- Missing `taskDefinitionEvent` IDs (report shows `distinct/expected`) come from
  source-side drops in Ray's owner-side status-event buffer when the driver exits
  with a backlog (raise `BENCH_DRIVER_DRAIN_SLEEP`), or from the pod termination
  grace period cutting the final flush short.
- kind on macOS runs inside a VM: treat CPU numbers as **relative** (scaling
  curves, ratios); confirm absolute sizing on Linux before publishing.
