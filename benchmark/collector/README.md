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
> Image tags, localhost ports, and the macOS Kind VM are still shared globals.
> Object storage is isolated: this package always uses
> `ray-historyserver-benchmark`, while the shared e2e suite keeps using
> `ray-historyserver`. Benchmark cleanup deletes only the current run's exact
> session prefix and metadata marker; it never deletes a bucket or another
> immutable session.

## Run

Formal Ray 2.56 Collector rate matrix (dedicated `kind-bench` context, fixed
50,000 tasks, rates 250/500/1000/2000/3000/unpaced, three repeats each):

```bash
cd historyserver/test/benchmark
./sweeps/ray256_collector.sh
```

The runner refuses a different kubectl context or kind node, refuses to
overwrite an output directory, builds the operator from the current checkout,
and binds the expected matrix plus runtime image IDs/source labels into initial
and final provenance. It does not manually delete RayClusters: every arm uses
an owned RayJob with `shutdownAfterJobFinishes=true` and a 30-second TTL. This
rate campaign is Collector-only: it sets `BENCH_SKIP_HISTORY_SERVER=true`,
finishes after the shutdown upload and storage/Collector validity artifacts,
and never deploys or queries a History Server in any arm.

Validate the common Collector resource candidate against the prior uncapped
campaign's highest-load arm (3,000 target tasks/s, 50,000 tasks, three repeats):

```bash
BENCH_COLLECTOR_CAP_VALIDATION=true ./sweeps/ray256_collector.sh
```

This cap matrix fixes both head and worker Collectors to CPU request/limit
`150m/1200m` and memory request/limit `160Mi/192Mi`. In addition to the rate
campaign's conservation, upload, lifecycle, and cgroup gates, it requires the
actual Pod resources, `memory.max=192Mi`, zero `oom`/`oom_kill`, and at least
4,000 measured peak events/s for each Collector. The 4,000 floor is rounded
down from the lowest 4,102.1 events/s role/run observation across the prior
uncapped 3,000-target and unpaced high-load arms.

New expected matrices use schema v4, which binds the dedicated benchmark bucket
and makes `SkipHistoryServer` explicit (`true` for by-rate and `false` for
by-task). Collector-cap v4 matrices also bind all four Collector resource
values. Legacy v1-v3 artifacts are rejected by default; archived artifacts may
be inspected offline only with the explicit `--allow-legacy-schema` validator
flag, which formal runners never pass.

History Server source identities are validated before any storage path is
built: namespace and cluster follow Kubernetes DNS naming, and session IDs must
be one safe Ray `session_...` path segment. This rejects `.`/`..` and path
traversal before `path.Join` can normalize them into another source.

Formal source reports also prove bucket isolation with a whole-bucket baseline
taken after the dedicated bucket exists but before any benchmark namespace,
RayJob, RayCluster, or Collector exists. `storageIsolation` compares that
pre-start baseline with T2 after the final flush; T0/T1/T2 phase diffs remain for
analysis. Every object identity includes size and ETag, so a same-size overwrite
is still a change. Only the current session prefix, its exact metadata marker,
and the exact zero-byte marker-directory object written by `CreateDirectory`
may be added or changed; the marker directory is never treated as a prefix. Any
foreign addition/change or any deletion makes source and CPU-matrix generation
fail closed.

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
| `BENCH_RAY_EVENT_RING` | (Ray default, 10000) | Sets `RAY_ray_event_recorder_max_queued_events` on Ray containers. This is the separate RayEventRecorder path, not the CoreWorker TaskEventBuffer, and is not evidence for TaskDefinition loss. |
| `BENCH_HS_ENTER_TIMEOUT` | `5m` | Client budget for the first `/enter_cluster` attempt |
| `BENCH_HS_WARM_WAIT` | `15m` | After a timed-out first attempt, keep re-probing (the server-side load keeps running); the first warm hit upper-bounds the true load time |
| `BENCH_KIND_NODE` | `kind-control-plane` | kind node container name for the cgroup sampler |
| `BENCH_JOB_TIMEOUT` | `45m` | Budget for the RayJob to succeed |
| `BENCH_TARGET_TASK_RATE` | `0` | Target task submissions/s (`0` = unpaced). A positive value adds only the workload generator's in-loop pacing wait. |
| `BENCH_DRIVER_DRAIN_SLEEP` | `0` | Post-job driver drain delay. Formal owned-RayJob runs require `0`; use the RayJob TTL instead. |
| `BENCH_SHUTDOWN_AFTER_JOB` | `false` | When `true`, the RayJob owns the RayCluster and the operator performs shutdown after job completion. |
| `BENCH_JOB_TTL_SECONDS` | `0` | Delay between RayJob completion and owned RayCluster shutdown; formal matrices use `30`. |
| `BENCH_SKIP_HISTORY_SERVER` | `false` | Stop after Collector shutdown, storage decode, and Collector artifact capture. The formal Collector rate matrix sets `true`; History Server sizing runs keep `false`. |
| `BENCH_COLLECTOR_CPU_REQUEST` | (manifest default) | Collector CPU request applied to both head and worker sidecars. |
| `BENCH_COLLECTOR_CPU_LIMIT` | (manifest default) | Collector CPU limit applied to both head and worker sidecars. |
| `BENCH_COLLECTOR_MEMORY_REQUEST` | (manifest default) | Collector memory request applied to both head and worker sidecars. |
| `BENCH_COLLECTOR_MEMORY_LIMIT` | (manifest default) | Collector memory limit applied to both head and worker sidecars. |
| `BENCH_WARM_ITERATIONS` | `10` | Requests per warm history server endpoint |
| `BENCH_OUT_DIR` | `out` | Report directory (one timestamped subdir per run) |
| `BENCH_SKIP_CLEANUP` | `false` | Keep the dedicated `ray-historyserver-benchmark` bucket contents after the run |

Start with a smoke run (`BENCH_TASK_COUNT=500`) before the full 50k run.

Target-rate pacing and post-job drain are different controls. Pacing waits inside
the submission loop before all tasks have finished, making arrival rate an
experimental input. A drain sleep waits after the workload and changes the
shutdown window. Formal rate sweeps may use the former, but require
`BENCH_DRIVER_DRAIN_SLEEP=0`, `shutdownAfterJobFinishes=true`, and a 30-second
RayJob TTL. The TTL delays owned-RayCluster deletion; it is not a CoreWorker
task-event acknowledgement or drain guarantee.

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
- **collector HTTP ingress** (10s fixed wall-clock windows): each successfully
  persisted JSON batch contributes one counter update for batch count, event
  count, request bytes, and the top-level `RayEvent.nodeId` (normalized to hex;
  the collector sidecar's current Ray NodeID is the fallback). Rejections add
  one reason counter (`draining`, `disk_pressure`, `bad_request`, or `internal`),
  and rotation-queue pressure has a separate counter. Those counters are joined
  to the same pod's direct cgroup samples in the identical window.
  → `collector_ingress_cgroup_10s.csv`; per-collector validity verdicts are in
  `collector_ingress_gate.csv`.

`node_rate.csv` remains an event-creation-time view decoded after storage. Use
`collector_ingress_cgroup_10s.csv`, not `node_rate.csv`, for Collector sizing:
its timestamp is captured when the HTTP handler starts receiving the batch, so
Ray-side queueing delay and storage flush time are not assigned to the wrong
resource window.

The first and last 10-second buckets can be partial. They remain in the raw CSV
for audit, but a row is usable for sizing only when its Ray NodeID is known, it
has at least 80% cgroup CPU interval coverage, and it has zero rejected HTTP
requests and zero rotation-queue-full signals. The per-collector gate also
requires the peak event-rate window to meet that coverage; otherwise it fails
closed instead of silently choosing a lower, better-sampled window.

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
if BENCH_SKIP_HISTORY_SERVER=false:
  deploy history server, port-forward          phase: historyserver
    └─ GET /clusters ×5, /enter_cluster cold load, warm endpoints ×N
else: mark Collector-only run complete; do not apply History Server
write out/<ts>/bench-report.{md,json} + samples.csv
```

Every diff also asserts additions stay under the expected prefixes, so a write
landing anywhere unexpected in the bucket is reported instead of missed.

The report is also printed to the `go test -v` log, and partial results are
written even when an assertion fails mid-run.

## Formal History Server multi-N campaigns

Run one independent CPU campaign and one derived memory campaign for each
immutable source size: `N = 1k, 5k, 10k, 50k`.  The expected matrix reads the
namespace/cluster/session, task attempts, object count, byte count, and source
report SHA-256 from that source report.  It does not carry a hard-coded 50k
identity across campaigns.

Source generation has one fixed, task-count-selected pacing contract. It is not
an ambient tuning knob:

| Task attempts | Wave size | Pacing variant |
|---:|---:|---|
| 1k, 5k, 10k | 2000 | `baseline-wave2000-v1` |
| 50k | 100 | `single-driver-wave100-v1` |

The 50k variant is separately labelled because pacing is an experimental
control. Its lineage records the rejected wave-2000 source report SHA-256
`7873ec98d6e1d376a5994a86aaaa18ee41b6f5e1a124f67249589c152ad94264`.
That rejected source is preserved as evidence and is never accepted as CPU or
memory input. A wave barrier reduces each submission burst, but `ray.get()` does
not acknowledge delivery of task events to the aggregator. Therefore wave size
does not impose a structural upper bound on task-event rate or prove lossless
delivery. Only the post-run exact task/attempt/state and storage-event gates can
accept the single immutable run. If the predeclared 50k wave-100 run fails, stop;
do not try wave 500/250/100 in sequence or select the first stochastic pass.

CPU and memory results for 50k remain valid for the labelled wave-100 reference
source, but they are not a homogeneous `N`-only curve with the lower-N wave-2000
sources. Charts and takeaways must show the pacing variant and should compare
source object bytes/event counts alongside task count. Driver task rate is a
measured description of that run, not a correctness gate or promised rate cap.

The cold-load SLO remains 120 seconds, while the processing timeout is 10
minutes and the client measurement timeout is 12 minutes.  This separation lets
a complete HTTP 200 at 121 seconds remain a valid measurement that missed the
SLO.  A failed/incomplete request is never a valid measurement.  Warm task
query size is `min(N, 10000)`; the full replay and TaskLog gates require exactly
`N` attempts.

Formal source reports, expected matrices, fingerprints, History Server replay,
and provenance all bind the fixed `ray-historyserver-benchmark` bucket. A source
report from the shared e2e bucket is rejected. Formal source generation also
requires `SkipCleanup=true`, so CPU and memory campaigns can replay immutable
bytes without a later benchmark arm removing them.

A successful source publishes exactly one `accepted/` directory. The runner
first builds and verifies a same-filesystem `.accepted-staging/` bundle, then
atomically renames it only after the report, planned config, lineage, initial
and final provenance, source contract, CPU matrix, terminal status, and
completion manifest cross-bind by SHA-256. Both RayJob retry layers are observed
as explicit integer zero. CPU and memory runners accept only this terminal
bundle; an arbitrary report/provenance pair or interrupted staging directory is
not a valid input. Source and consumer bundle paths must be canonical absolute
paths whose existing components contain no symlink (on macOS, use
`/private/tmp/...`, not the `/tmp` alias). Publication pins the runner-owned
parent directory by file descriptor and performs an fd-relative rename after a
fresh identity check. The campaign lock and unique output root exclude a
hostile concurrent writer; the bundle protocol does not claim a general
cross-process no-replace primitive against an attacker mutating that directory.

All formal scripts fail closed unless the current kubectl context is
`kind-bench`, the Kind node is `bench-control-plane`, no active in-cluster
KubeRay operator exists, and localhost ports `19003` (S3) and `30080`
(History Server) are free. Before a host operator or campaign arm starts, a
context-pinned cluster-wide inventory also rejects every existing RayCluster or
RayService, including a Ready CR with zero current pods and a CR already being
deleted. A RayJob residue is allowed only when its exact status is
`SUCCEEDED`/`Complete`, its owned RayCluster is absent, its submitter Kubernetes
Job, if still present, is terminal, and every associated KubeRay pod is terminal. Missing,
malformed, nonterminal, failed, or partially cleaned inventory fails closed.
Each arm records the generated namespace name and UID, then waits until that
exact UID is deleted and no pods remain before the next arm starts. Generate
any immutable source in the formal
`1k/5k/10k/50k` matrix with the same compatible source script. The task-count
variable defaults to `5000`; the script selects the fixed wave mapping above,
and ignores ambient `BENCH_WAVE_SIZE` when choosing it:

```bash
BENCH_HS_SOURCE_TASK_COUNT=5000 \
BENCH_SWEEP_OUT=/absolute/path/to/hs-source-n5000 \
  ./test/benchmark/sweeps/ray256_hs_source_5k.sh
```

For each accepted source bundle, run CPU discovery, then memory confirmation:

```bash
BENCH_HS_SOURCE_ACCEPTED_DIR=/absolute/path/to/hs-source-n5000/accepted \
BENCH_SWEEP_OUT=/absolute/path/to/hs-cpu-n5000 \
  ./test/benchmark/sweeps/ray256_hs_cpu.sh

BENCH_HS_SOURCE_ACCEPTED_DIR=/absolute/path/to/hs-source-n5000/accepted \
BENCH_HS_CPU_CAMPAIGN=/absolute/path/to/hs-cpu-n5000 \
BENCH_SWEEP_OUT=/absolute/path/to/hs-memory-n5000 \
  ./test/benchmark/sweeps/ray256_hs_memory.sh
```

Do not run two benchmark campaigns beside each other: they share one macOS
Kind VM.  Every arm uses a fresh History Server pod/cache but fingerprints the
same read-only source before and after the arm.

The rejected r6 50k source attempt exposed why the cluster-wide inventory is a
correctness precondition: two pre-existing History Server RayClusters were
reconciled after the otherwise-clean host-operator/port preflight, and Ray's
memory monitor later observed node usage above its 95% kill threshold. Those
foreign workloads are a measured confound, but the attempt did not preregister
or measure node-wide free-memory headroom before starting, so they are not
proven to be the sole cause. This patch intentionally adds no guessed memory
threshold. A future node-headroom gate requires its own preregistered,
falsifiable experiment and must account for the macOS Kind VM boundary.

## Reading the numbers

- **Fact to keep in mind**: process start of the history server does zero
  storage I/O — the meaningful "cold start" is `/enter_cluster`, which reads
  and decodes the whole session serially.
- `GET /clusters` rescans the entire `cluster-metadata/` prefix on every
  request (no cache, `MaxKeys=100` pagination), so its latency scales with
  total sessions ever stored, not with this run.
- A `500` from formal `/enter_cluster` is an incomplete measurement and fails
  the arm.  The formal processing timeout is deliberately 10 minutes, separate
  from the 120-second SLO, so slower successful loads can still be measured.
- Ray 2.56 has three different controls that must not be conflated: the
  CoreWorker TaskEventBuffer status capacity is 100,000, its status send batch
  is 10,000, and the separate RayEventRecorder queue is 10,000. The rejected
  wave-2000 50k run lost one contiguous newest task-definition tail. Combined
  with Collector ingress conservation and Ray's capped shutdown flush path,
  a pre-aggregator CoreWorker shutdown residual is the leading explanation,
  but it is **derived**, not a directly measured queue/drop counter. That run
  did not persist Ray aggregator Prometheus counters.
- kind on macOS runs inside a VM: treat CPU numbers as **relative** (scaling
  curves, ratios); confirm absolute sizing on Linux before publishing.

Legacy schema support for already completed 1k/5k/10k campaigns is only a
task-count/schema compatibility boundary. It is not an artifact-identity
allowlist. A 50k source cannot use that legacy path and must carry the complete
source-generation and accepted-bundle contract.
