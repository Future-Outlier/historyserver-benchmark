# How to size your own deployment

Three inputs decide everything. Get these from your workload, not from a
default:

```
N     tasks in one Ray session          (actor calls count as tasks)
R     peak tasks/s on the busiest node  (NOT cluster-wide, NOT num_cpus)
D     sessions you need to keep         (drives storage cost and cache pressure)
```

Everything below follows from the measured constants: **4.36 events/task**,
**725 B/event**, **120 µs of collector CPU per event**, **0.62 ms of History
Server load time per task** (2 ms if you leave its CPU at the shipped 500m), and
**~23 KiB of History Server heap per task**.

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
memory   = 100 MiB + (N_largest × 23 KiB) × (sessions held in cache)
cpu      = limits 4, requests 1        NOT the 500m the sample manifest ships with
load time = N × 0.62 ms at that limit  (N × 2 ms and superlinear at 500m)
```

**Raise the CPU limit first — it is the highest-return change available.**
`historyserver/config/historyserver.yaml` sets `limits.cpu: "500m"` and nothing
else, so Kubernetes pins `requests.cpu = 500m` too. Cold load is CPU-bound (JSON
decode, map building, GC), and the container sat at 0.42–0.50 cores for the
entire load in *every* run at that limit — saturated, not comfortable.

Measured, same build and cluster, only the limit changed:

| N | 500m | 4 cores | speedup |
|---:|---:|---:|---:|
| 50,000 | 97.9 s | 30.5 s | 3.2× |
| 100,000 | 907.3 s | 62.7 s | **14.5×** |

At 4 cores it used 1.18 cores on average and peaked at 2.2, so 4 is headroom
rather than a target; 2 would capture most of the win. The apparent
"superlinear blowup past 50k" disappears: per-task cost is 0.61 ms at 50k and
0.63 ms at 100k.

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

With adequate CPU the default `--session-process-timeout` of 2 minutes covers
roughly **190,000 tasks** (at 0.62 ms/task), so it stops being the binding
constraint. What binds instead is the HTTP server's hardcoded 35 s
`WriteTimeout`: a load longer than that cannot deliver *any* response — success
or error — to an HTTP/1 client, which puts the practical ceiling at about
**56,000 tasks** even on a fast server. That one needs a code change; see
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
