# How to size your own deployment

Three inputs decide everything. Get these from your workload, not from a
default:

```
N     tasks in one Ray session          (actor calls count as tasks)
R     peak tasks/s on the busiest node  (NOT cluster-wide, NOT num_cpus)
D     sessions you need to keep         (drives storage cost and cache pressure)
```

Everything below follows from the measured constants: **4.36 events/task**,
**725 B/event**, **120 µs of collector CPU per event**, **~2 ms of History Server
load time per task**, **~23 KiB of History Server heap per task**.

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

Why memory is a constant and not a function of N: the collector streams events
to disk and uploads in the background, so its heap holds buffers, not history.
Measured 62 MiB at 1k tasks and 117 MiB at 100k tasks.

Why the limit is 512Mi and not 256Mi: the container's cgroup total including page
cache from its own spool files reached 251 MiB at 100k tasks. Page cache is
reclaimable, so 256Mi would not OOM — it would just keep the container
permanently in reclaim.

**Do not skip the head's collector.** The head node produced 54% of all events,
because task lifecycle is recorded by the owner as well as the executor.

### Disk

The collector spools to an `emptyDir` with a 200 MB budget and starts returning
503 to the Ray aggregator at 160 MB. That was never approached here (peak
sustained upload kept up at 7,850 events/s), but if your object store is slow or
far away, that headroom is what absorbs it.

---

## 2. History Server

```
memory  = 100 MiB + (N_largest × 23 KiB) × (sessions held in cache)
cpu     = 2 cores minimum        NOT the 500m the sample manifest ships with
```

**Raise the CPU limit first.** `historyserver/config/historyserver.yaml` sets
`limits.cpu: "500m"` and nothing else, so Kubernetes also pins
`requests.cpu = 500m`. Cold load is CPU-bound — JSON decode, map building, and
GC — and the container sat at 0.42–0.50 cores for the entire load in *every*
run: it is saturated, not comfortable. Every latency in this report was measured
under that half-core quota.

The cache holds up to `--session-cache-size` sessions (default 100) with no TTL
unless you set `--session-cache-ttl`. **The memory you must budget is not one
session — it is every session a user might open before eviction.**

| Largest session | One session loaded | Safe limit (a few sessions cached) |
|---:|---|---|
| 10,000 tasks | ~290 MiB | `1Gi` |
| 50,000 tasks | ~1.1 GiB | `4Gi` |
| 100,000 tasks | ~2.2 GiB | `8Gi` |

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

### Timeouts — and where the wall really is

Load time is ~2 ms/task, so `--session-process-timeout` (default `2m`) is
implicitly a **60,000-task ceiling**. Raising it is necessary for larger
sessions but not sufficient: the HTTP server's own `WriteTimeout` is a
hardcoded 35 s, which is *shorter* than the default load timeout, so a slow load
cannot deliver any response — success or error — to an HTTP/1 client. See
[FINDINGS.md](FINDINGS.md).

```
--session-process-timeout = N_largest × 2 ms × 2   (safety factor)
   50k tasks -> 200s   100k tasks -> 400s (and expect worse: 100k measured 907s)
```

Beyond ~50k tasks per session the honest recommendation is **don't** — split
work into more, smaller Ray jobs (each job is a separate session), or accept
that opening those sessions takes many minutes.

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

**Turn compression on.** It saves 91% and cost nothing measurable in CPU or load
time in these runs.

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

If your jobs are short and you care about surviving abrupt termination:

```yaml
env:
  - name: RAY_COLLECTOR_EVENT_ROTATION_INTERVAL
    value: "1m"
```

and give Ray pods a `terminationGracePeriodSeconds` long enough for the final
upload (the flush itself took 1–4 s in these runs; the constraint is the upload,
which scales with the un-uploaded backlog).

---

## 4. Sanity checklist

```
[ ] Collector CPU sized from peak tasks/s per NODE, not from cluster totals
[ ] Head pod's collector sized the same as workers (it sees ~54% of events)
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
