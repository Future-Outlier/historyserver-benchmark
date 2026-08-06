# How to size your own deployment

Three inputs decide everything. Get these from your workload, not from a
default:

```
N     tasks in one Ray session          (actor calls count as tasks)
R     peak tasks/s on the busiest node  (NOT cluster-wide, NOT num_cpus)
D     sessions you need to keep         (drives storage cost and cache pressure)
```

Everything below follows from the measured constants: **4.36 events/task**,
**725 B/event**, **120 µs of collector CPU per event**, **0.60 ms of History
Server load time per task** at 2+ cores (1.51 ms at the shipped `500m`), and
**~23 KiB of History Server memory per task**. Load time is linear in task count
at both CPU limits, measured from 1,000 to 200,000 tasks.

---

## 1. Collector sidecar (one per Ray pod)

```
cpu request  = R × 4.36 events × 120 µs  =  R × 0.52 millicores
cpu limit    = 3 × request, minimum 1 core     (bursts are 2-4× sustained)
memory       = request 128Mi, limit 512Mi      (flat in N; independent of R)
```

| Peak tasks/s per node | CPU request | CPU limit | Memory |
|---:|---|---|---|
| ≤ 200 | `100m` | `1` | `128Mi` / `512Mi` |
| 500 | `250m` | `1` | `128Mi` / `512Mi` |
| 1,000 | `500m` | `2` | `128Mi` / `512Mi` |
| 2,000 | `1` | `3` | `128Mi` / `512Mi` |

**A safe default for most clusters:**

```yaml
resources:
  requests: { cpu: 200m, memory: 128Mi }
  limits:   { cpu: "2",  memory: 512Mi }
```

Why memory does not track N: the collector streams events to disk and uploads
in the background, so it holds buffers, not history. Measured 62 MiB of
anonymous memory at 1k tasks and 117 MiB at 100k — bounded and sublinear rather
than literally flat, and anonymous memory is not the same thing as Go heap.

One caveat this benchmark cannot answer: active files are keyed by job, so a
node running many concurrent jobs may hold proportionally more. Every run here
was a single job.

Why the limit is 512Mi and not 256Mi: the container's cgroup total including page
cache from its own spool files reached 251 MiB at 100k tasks. Page cache is
reclaimable, so 256Mi would not OOM — it would just keep the container
permanently in reclaim.

**Do not skip the head's collector, and do not size it smaller.** In these runs
the head was started with `num-cpus: "0"` — no task executed on it — and it still
produced **54% of all events**, because the driver that owns every task lives
there. A head collector's load tracks tasks *owned* by drivers on that node, not
tasks executed on it, so excluding the head from scheduling does not reduce it.

### Disk

The collector spools to an `emptyDir` with a 200 MB budget and starts returning
503 to the Ray aggregator at 160 MB. That was never approached here (peak
sustained upload kept up at 7,850 events/s), but if your object store is slow or
far away, that headroom is what absorbs it.

---

## 2. History Server

```
memory   = 100 MiB + (N_largest × 23 KiB) × (sessions held in cache)
cpu      = limits 4, requests 1        NOT the 500m the sample manifest ships with
load time = N × 0.60 ms at that limit  (N × 1.5 ms at the shipped 500m)
```

**Raise the CPU limit first — it is the highest-return change available.**
`historyserver/config/historyserver.yaml` sets `limits.cpu: "500m"` and nothing
else, so Kubernetes pins `requests.cpu = 500m` too. Cold load is CPU-bound (JSON
decode, map building, GC), and the container sat at 0.42–0.50 cores for the
entire load in *every* run at that limit — saturated, not comfortable.

Measured, same build and cluster, only the limit changed:

| `limits.cpu` | 100k cold load | cores used | peak | GC share |
|---|---:|---:|---:|---:|
| `500m` (shipped) | **never completed** | 0.50 | — | 6% |
| `1` | 71.2 s | 0.98 | 1.05 | 8% |
| `1.5` | 67.2 s | 1.12 | 1.51 | 6% |
| **`2`** | **64.4 s** | 1.19 | 1.97 | 5% |
| `3` | 64.7 s | 1.21 | 2.58 | 2% |
| `4` | 62.7–64.8 s | 1.16–1.23 | 2.20 | 2% |
| `8` | 66.3 s | 1.25 | — | — |
| no limit at all | 66.8 s | 1.23 | — | — |

**Returns diminish around 1.5–2 cores.** Everything at or above 2 overlaps
within the spread we actually measured — the two 2-core runs differ by 10.7 s,
and a later 4-core run came in at 67.9 s, outside the 62.7–64.8 s of the earlier
two. Removing the limit entirely is no faster than 4. The load only wants ~1.2
cores, and even `1` avoids the cliff; only the shipped `500m` fails outright.

Most points here are single runs, so this is a shape, not a ranking: **anything
from 2 upwards is defensible, `500m` is not**. If you need to choose, `4` gives
the ~2.2-core peaks room without costing anything, since `requests` stays low.

### Should you set a CPU limit at all?

The usual Kubernetes advice for latency-sensitive services is to set requests and
**no** CPU limit, because limits are enforced by CFS quota: when the quota runs
out the container is paused for the rest of the 100 ms period even if the node is
idle ([Resource Management](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/),
[kubernetes#67577](https://github.com/kubernetes/kubernetes/issues/67577)). That
advice is community convention, not documented Kubernetes policy — the official
docs describe the mechanism and stop there.

For this particular workload there is a catch. The History Server is a Go 1.26
binary, and since Go 1.25 `GOMAXPROCS` is derived from the cgroup CPU **limit**,
never the request. Dropping the limit therefore sets `GOMAXPROCS` to the node's
core count, which on a 96-core node means dozens of Ps for a container that
actually uses ~1.2 cores.

Two defensible configurations:

```yaml
# A. bounded and simple - what we measured
resources:
  requests: { cpu: "500m" }
  limits:   { cpu: "4" }      # GOMAXPROCS=4; measured peak was 2.2, so throttling is rare

# B. no limit, but tell the runtime what to expect
resources:
  requests: { cpu: "1" }
env:
  - name: GOMAXPROCS
    valueFrom:
      resourceFieldRef: { resource: requests.cpu, divisor: "1" }
```

A is easier to reason about and is what the numbers above come from. B avoids
throttling entirely and is the better fit if you follow the no-CPU-limits
convention — but do not do it without pinning `GOMAXPROCS`.

The cache holds up to `--session-cache-size` sessions (default 100) with no TTL
unless you set `--session-cache-ttl`, so the memory you must budget is not one
session but every session a user might open before eviction. **We only ever
measured one session resident.** Whether N cached sessions cost N times the
single-session peak — the snapshots share no structure as far as we can tell,
but we did not verify it — is untested, so treat the multi-session numbers below
as arithmetic, not measurement.

```
one session resident  >=  400 MiB + N x 20 KiB     (cgroup total, what a limit sees)
```

| Largest session | One session loaded | Safe limit (a few sessions cached) |
|---:|---|---|
| 10,000 tasks | ~0.3 GiB | `1Gi` |
| 50,000 tasks | ~1.3 GiB | `4Gi` |
| 100,000 tasks | ~2.2 GiB | `8Gi` |
| 200,000 tasks | ~2.9 GiB | `12Gi` |

Per-task cost falls as sessions get bigger (a ~90 MiB floor is amortized): about
30 KiB/task at 10k, 25 at 50k, 21 at 100k, 15 at 200k. The formula above is the
upper bound that covers all of them.

```yaml
resources:
  requests: { cpu: "1", memory: 1Gi }
  limits:   { cpu: "4", memory: 4Gi }     # for sessions up to ~50k tasks
```

If your sessions are large, cap the blast radius instead of buying RAM:

```
--session-cache-size=5        # fewer concurrent snapshots resident
--session-cache-ttl=30m       # let idle snapshots go
```

### Do not tune the Go runtime

Both obvious knobs were tried at `limits.cpu: 2`, 100k tasks, and neither
helped: `GOMAXPROCS=8` measured 72.2 s and `GOGC=400` 70.8 s, against 64.4 s
with nothing set. Take the ordering as preliminary — those gaps are the same
size as the run-to-run spread at 2 cores — but the memory side is unambiguous:
`GOGC=400` tripled the Go heap to 3.1 GB while cutting GC share from 5% to 1%,
so it buys memory pressure for a cost that was never the bottleneck. Set the
CPU limit; there is no evidence that touching the Go runtime helps.

### Timeouts — and where the wall really is

This is the ceiling that actually stops people, and crossing it does not make a
session slow — it makes it **unopenable**, because the load is aborted at the
timeout and every retry starts over.

```
tasks that fit = --session-process-timeout / per-task cost
   500m, 1.51 ms/task   ->  ~79,000     (100k needs 151.3s: confirmed both ways)
   2-4 cores, 0.60 ms   ->  ~200,000    (200k needs 119.9s against a 120s limit)
```

Both edges were confirmed twice: once by runs that never returned under the
default, and once by the same sessions completing after the flag was raised. So
raise the flag together with the CPU limit — CPU alone just moves the wall.

The HTTP server's hardcoded 35 s `WriteTimeout` binds even earlier for the
*client* — about 56,000 tasks at 0.63 ms/task — but the server-side cache is
still populated, so a later request succeeds. That one needs a code change; see
[FINDINGS.md](FINDINGS.md).

```
--session-process-timeout = N_largest × 0.62 ms × 2     (safety factor)
   100k tasks -> 124s   200k tasks -> 250s
```

---

## 3. Storage

```
per session:  N × 3.15 KiB    raw
              N × 0.29 KiB    with gzip     (RAY_COLLECTOR_EVENT_COMPRESSION_ENABLED=true)
plus task logs, which depend entirely on how much your tasks print
total:        (above) × D sessions retained
```

| Workload | Raw | With gzip |
|---|---:|---:|
| 100 jobs × 10k tasks | 3.1 GiB | 283 MiB |
| 1,000 jobs × 10k tasks | 31 GiB | 2.8 GiB |
| 100 jobs × 100k tasks | 31 GiB | 2.8 GiB |

**Turn compression on, but know what it costs.** It saves 91% of storage and
costs nothing on the collector side, but a compressed session takes about **21%
longer to cold-load** (100k tasks: 78.6 s vs 64.8 s at 4 cores). Storage is
usually the scarcer resource, so this is still the right default — just not a
free one.

```yaml
env:
  - name: RAY_COLLECTOR_EVENT_COMPRESSION_ENABLED
    value: "true"
```

### Rotation interval vs. data safety

The default 5-minute rotation means a job shorter than 5 minutes uploads nothing
until its pod shuts down gracefully. Measured: at N = 50,000, **100% of the
event bytes** arrived only during the shutdown flush. If a Ray pod is SIGKILLed
(node failure, grace period too short, OOM), that data — and the session marker
that makes the session listable — is gone.

The obvious mitigation is a shorter rotation interval:

```yaml
env:
  - name: RAY_COLLECTOR_EVENT_ROTATION_INTERVAL
    value: "1m"
```

We tested it at 50k and it changed nothing — still 0 bytes uploaded during the
job — because that job runs ~40 s and no file ever gets old enough to rotate.
**It can only help jobs that outlive the interval**, which we did not test. What
definitely matters either way is a `terminationGracePeriodSeconds` long enough
for the final upload; the flush itself took 1–4 s in these runs, and the
constraint is the upload of whatever backlog is on disk.

---

## 4. Sanity checklist

```
[ ] Collector CPU sized from peak tasks/s per NODE, not from cluster totals
[ ] Head pod's collector sized the same as workers (54% of events even with num-cpus: 0)
[ ] Collector memory limit >= 512Mi (page cache from its own spool files)
[ ] Compression enabled
[ ] History Server memory >= 23 KiB x largest session x sessions cached
[ ] History Server CPU limit raised above the sample manifest's 500m
[ ] --session-process-timeout >= 2 ms x largest session, x2 safety
[ ] Sessions kept under ~50k tasks, or load latency accepted as multi-minute
[ ] terminationGracePeriodSeconds long enough for the final flush
```

Everything here was measured on kind on macOS — see the
[caveats](ENVIRONMENT.md#caveats). Byte counts, event counts and scaling shapes
port directly; CPU numbers should be re-measured on your own hardware before you
treat them as absolute.
