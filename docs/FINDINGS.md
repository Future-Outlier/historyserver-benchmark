# Findings

Issues found while benchmarking, each verified against KubeRay source at commit
`be7fca77` (History Server built from `historyserver/`). Line references are to
that tree.

---

## 1. A large cold load can never answer the client

Three independent limits sit on the `/enter_cluster` path, and they are ordered
wrong:

```
client timeout        (browser / curl)          typically 30-300 s
  http.Server.WriteTimeout = 35 s               historyserver/pkg/historyserver/server.go:88
  --session-process-timeout = 2 min (default)   historyserver/pkg/historyserver/session_loader.go:18
```

`WriteTimeout` (35 s) is **shorter** than the load timeout it is supposed to
outlive. A load that takes between 35 s and 2 min completes server-side, but the
response write hits an expired deadline; a load that exceeds 2 min is supposed to
return an error that, for the same reason, cannot be delivered either. The
comment on the field says it "must be >= httpClient.Timeout for proxy requests",
which is true for proxying to a live dashboard, but it also silently caps every
locally-served slow response.

Worse, the 2-minute budget is not enforced at any fine granularity. The load loop
checks `ctx.Err()` only between event files:

```go
// historyserver/pkg/eventserver/eventserver.go:1118-1121
for _, eventFile := range eventFileList {
    if err := ctx.Err(); err != nil {
        return err
    }
```

Event files can be 100 MB (the collector's rotation size), so cancellation
granularity is one whole file.

**Measured:** at the shipped `500m` CPU limit, a 100,000-task session never
returned a response — not within a 15-minute budget, and not within 21 minutes
on a retry with the per-event log removed.

Then we raised `--session-process-timeout` to 30 minutes and ran it again:
**151.3 seconds.** The load was never slow in the way the failures suggested. It
needs 151 s, the server aborts it at 120 s, discards the partial state, and every
retry repeats that. A load that exceeds the timeout does not degrade — it becomes
impossible, and the excess can be as little as 25%.

Earlier versions of this report quoted the failed probes' elapsed time (907 s)
as if it were a load time, and called the result superlinear. Both were wrong.

Suggested fixes, cheapest first:
- Make `WriteTimeout` configurable and require it to be ≥ `--session-process-timeout`
  (or drop it to `0` for the non-proxy routes and enforce deadlines per-handler).
- Check `ctx.Err()` inside the per-file decode loop, not only between files.
- Return `202 Accepted` + a progress endpoint for loads that exceed a threshold,
  instead of holding the connection.

## 2. The sample manifest gives the History Server 500m of CPU, and the cold load saturates it

```yaml
# historyserver/config/historyserver.yaml:66-68
resources:
  limits:
    cpu: "500m"
```

No memory limit, and half a core. Because only `limits` is set, Kubernetes also
sets `requests.cpu = 500m`. Cold load is CPU-bound (JSON decode, map building,
GC), so it sits at the quota for its entire duration — measured across all
thirteen matrix runs:

| run | avg cores during load | peak |
|---|---:|---:|
| every single one | 0.42 – 0.50 | 0.51 – 0.54 |

Saturated, not "using half a core because that's all it needed".

**Measured, same code and cluster, only the limit changed (50,000 tasks):**

| `limits.cpu` | cold load | cores used | GOMAXPROCS |
|---|---:|---:|---:|
| `500m` (shipped) | 84.9 s | 0.49 (saturated) | 2 |
| `1` | 38.2 s | 0.94 (saturated) | 2 |
| `2` | 31.7 s | 1.15 | 2 |
| `4` | 34.9 s | 1.18 | 4 |

At 100,000 tasks: `2` → 75.1 s, `4` → 62.7–64.8 s, `8` → 66.3 s, no limit at all
→ 66.8 s, and `500m` → never completed. The load wants ~1.2 cores sustained with
bursts to ~2.2, which is why `2` captures most of the benefit and nothing above
`4` helps.

The quota is the binding constraint, and three competing explanations were
tested and eliminated:

| Hypothesis | Test | Result |
|---|---|---|
| Superlinear algorithm past 50k | raise the server timeout and let 100k finish at `500m` | **151.3 s = 1.51 ms/task**, the same constant as 50k: no knee exists |
| Go parallelism is too low | force `GOMAXPROCS=4` at `500m` | 85.3 → 84.7 s: **no effect** |
| …and conversely | pin `GOMAXPROCS=1` at 4 cores | 100k still loads in 76.3 s |
| GC pressure | `GOGC=400` at `500m`, 100k | GC share 6% → **1%**, heap 1.2 → 3.5 GB, and the load **still never completed** |
| Per-event logging | `Infof` → `Debugf` | −13% at 50k; 100k at `500m` still never completed |

Measured GC share is 8% at `500m` and 1–2% at 4 cores. The `GOGC` experiment is
the decisive one: removing almost all GC work does not rescue the configuration,
so GC was never the mechanism.

This is a config-only change — no code, no rebuild — and it is the highest-return
finding in this benchmark.

### Two effects, not one: the limit also sets `GOMAXPROCS`

The History Server is built with Go 1.26, and since Go 1.25 the runtime derives
`GOMAXPROCS` from the cgroup CPU bandwidth limit — **the limit, never the
request** ([Go 1.25 release notes](https://go.dev/doc/go1.25),
[container-aware GOMAXPROCS](https://go.dev/blog/container-aware-gomaxprocs)),
rounding up:

```
limits.cpu: 500m   ->  GOMAXPROCS = 2    (rounds up, but never below 2)
limits.cpu: 4      ->  GOMAXPROCS = 4
no limit at all    ->  GOMAXPROCS = the node's logical CPU count
```

We separated the two effects with forced `GOMAXPROCS` values (above): bandwidth
dominates, parallelism is worth about 1.2×. Note the floor — the Go runtime
never derives a `GOMAXPROCS` below 2, so `500m` gives 2, not 1, confirmed by the
`2 P` in its own gctrace output.

This also means **"just remove the CPU limit" is not free for this binary**: with
no limit, `GOMAXPROCS` becomes the node's core count, so a container using ~1.2
cores would start dozens of Ps for GC on a large node.

## 3. Cold load materializes the entire session in memory, serially

```go
// historyserver/pkg/eventserver/eventserver.go:1141
eventbytes, err := io.ReadAll(eventioReader)     // whole file into []byte
...
eventList, err := DecodeEventFileBytes(eventFile, eventbytes)   // -> []map[string]any (line 67)
```

Each file is read whole, then decoded whole into `[]map[string]any`, so both
representations are live at once, and files are processed one at a time on one
goroutine. Downstream there are two more full copies: every event is
re-marshalled from its generic map into a typed struct
(`eventserver.go:657-678`), and the finished snapshot is deep-copied per request
(`session_loader.go:57-62`). Four representations of the same data.

**Measured:** ~23 KiB of retained heap per task, and 0.62 ms/task of load time
once CPU is not the constraint. The allocation volume is what makes finding 2
bite so hard: with a half core, GC has nowhere to run.

The snapshot then stays in an LRU of up to 100 sessions with no TTL by default
(`session_loader.go:18-22`), so the peak is per *cached session*, not per request.

Fixes worth considering: stream-decode line by line into typed structs instead of
`map[string]any`; decode files in parallel; store a compact columnar snapshot.

## 4. The cold-load path logs one INFO line per event

```go
// historyserver/pkg/eventserver/eventserver.go:136, inside storeEvent(),
// which eventserver.go:1164 calls once per event
logrus.Infof("current eventType: %v", eventType)
```

A 100,000-task session emits ~436,000 of these lines on every cold load. The
binary never calls `logrus.SetLevel`, so there is no way to turn them off
without a rebuild.

**Measured A/B** — same cluster, same images otherwise, only this line changed to
`Debugf`:

| N | stock | `Debugf` | change |
|---:|---:|---:|---:|
| 50,000 | 97.9 s | 85.3 s | **−13%** |

Peak heap was unchanged (1,132 → 1,149 MiB), which is the expected signature of
a pure CPU/IO cost rather than an allocation one. A one-line change buying 13% is
worth taking, but it is not the explanation for the 100k knee.

## 5. Defaults imply a ~60,000-task ceiling per session

This is the finding that outlives every CPU tuning number, because crossing it
does not make the session slow — it makes the session **impossible to open**.

The load is aborted at `--session-process-timeout` (2 minutes by default) and
the partial state is discarded, so the next attempt starts from scratch and the
retry loop never converges. The threshold is just `timeout ÷ per-task cost`:

| CPU limit | measured per-task cost | tasks that fit in the 2-minute default |
|---|---:|---:|
| `500m` (shipped) | 1.51 ms | **~79,000** |
| `2`–`4` | 0.60 ms | **~200,000** |

Both edges were confirmed by runs that never returned, and then by runs that did
once the timeout was raised:

| Session | Default 2-minute timeout | With `--session-process-timeout=30m` |
|---|---|---:|
| 100k at `500m` | never returned, in any run | **151.3 s** |
| 200k at 4 cores | never returned | **119.9 s** |

The 200k case is the sharpest illustration available: it needs 119.9 s against a
120 s limit, and that ~0.1% overshoot is the difference between a session that
opens in two minutes and one that can never be opened at all. Nothing in the
documentation mentions the limit, and nothing warns a user approaching it.

Note the 35 s `WriteTimeout` from finding 1 binds even earlier for the *client*:
a load past 35 s cannot deliver its response at all, which is ~56,000 tasks at
0.63 ms. The server-side cache still gets populated, so a later request
succeeds — which is why the failure looks like "hangs, then mysteriously works".

## 6. Listing clusters logs an ERROR for its own placeholder objects

```go
// historyserver/pkg/storage/s3/s3.go:171-176
for _, object := range page.Contents {
    c, err := clustermetadata.DecodePath(*object.Key, r.S3RootDir)
    if err != nil {
        logrus.Errorf("Failed to parse meta file path: %s, error: %v", *object.Key, err)
        continue
    }
```

`DecodePath` requires exactly three path segments
(`clustermetadata.go:52-54`), but the collector also creates a trailing-slash
"directory" placeholder object in the same prefix. Every `GET /clusters`
therefore logs one ERROR per session directory on the completely normal path.
The listing itself is correct — this is pure noise, and it trains operators to
ignore real errors. Same pattern in the GCS, AzureBlob, and AliyunOSS readers.

Fix: skip keys ending in `/` before decoding (or stop creating the placeholder).

## 7. `GET /clusters` is an uncached full scan on every request

`MaxKeys=100` pagination over the whole `cluster-metadata/` prefix, with no
caching (`s3.go:160-181`). Cost grows with the number of sessions **ever**
stored, independent of what the user is opening. It was 5–140 ms with a handful
of sessions here; at thousands of retained sessions it becomes the landing page's
latency.

## 8. Short jobs upload nothing until graceful shutdown

With the default 5-minute rotation interval, a job that finishes sooner has all
its events sitting on the collector's local disk. Measured, by diffing bucket
snapshots before and after RayCluster deletion:

| N | bytes uploaded during the job | bytes uploaded only during the shutdown flush |
|---:|---:|---:|
| 10,000 | 0 | 30.8 MiB (100%) |
| 50,000 | 0 | 153.3 MiB (100%) |
| 100,000 | 142.9 MiB | 159.9 MiB (53%) |

If the pod is SIGKILLed instead — node failure, `terminationGracePeriodSeconds`
too short, OOM kill — all of that is lost, including the `cluster-metadata`
session marker, which is only written during the same graceful drain. Without
the marker the session does not appear in `/clusters` at all, so the loss is
total rather than partial.

This is a deliberate design trade (disk-first, batch upload), but it deserves
documentation: **history durability currently depends on graceful pod
termination.**

The obvious mitigation — shorten `RAY_COLLECTOR_EVENT_ROTATION_INTERVAL` — was
tested and **did not help here**: at 50k with a 1-minute interval, still 0 MiB
during the job and 152.4 MiB at the flush, against 153.3 MiB with the 5-minute
default. The reason is mundane: that job runs ~40 s, so no file is ever old
enough to rotate, and the age check itself only runs every 30 s. The mitigation
is untested for jobs that outlive their rotation interval, which is the case it
was meant for.

A related known gap is already flagged in the code: on SIGTERM the History
Server cancels in-flight loads immediately and returns 500
(`session_loader.go:79-82`, `TODO(jiangjiawei1103)`).

## 9. Compression is off by default and costs nothing

Not a bug, but the default looks wrong given the data: gzip cut stored bytes by
**91%** at every scale, with CPU per event unchanged (118–125 µs vs 115–124 µs)
and cold-load time within noise. Consider defaulting
`RAY_COLLECTOR_EVENT_COMPRESSION_ENABLED` to `true`.

---

## Not a bug, but easy to get wrong

- **`limit` above 10,000 returns 400.** `RayMaxLimitFromAPIServer = 10000` in
  `historyserver/pkg/utils/filter.go:20` intentionally mirrors Ray's dashboard
  API. Our first harness asked for `limit=N` and got errors at N > 10k — the
  cap is by design, but it means the task list is paginated no matter how large
  the session is.
- **Ray-side event loss has two different buffers with confusable names.** The
  one that governs task status events is
  `RAY_task_events_max_num_status_events_buffer_on_worker`, on the process that
  *owns* the tasks (the driver, on the head node).
  `RAY_ray_event_recorder_max_queued_events` is a different, GCS-side buffer and
  does not affect this path. Loss tracked the submission rate, not the drain: every
  run at ≥3,084 tasks/s lost tasks (0.46% and 1.7%) and every run at
  ≤2,965 tasks/s lost none, **including one run that lost 1.7% despite a 30 s
  drain**. That refutes the "shutdown tail" explanation we started with — the
  overflow happens during submission, not only at exit.
