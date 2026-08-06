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

**Measured:** a 100,000-task session kept loading for **907 seconds**. Every
client request timed out; the server kept working and eventually cached the
snapshot, which the next request got instantly. From the user's side this is
indistinguishable from a hang, then a mysterious success.

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

Saturated, not "using half a core because that's all it needed". Every load
latency in this report was therefore measured under a half-core quota, and
anyone deploying from the sample manifest is running the same way.

That makes CPU the first thing to raise before concluding the History Server is
slow — a config-only change requiring no code and no rebuild.

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

**Measured:** ~2 ms and ~23 KiB of retained heap per task; 50k tasks = 98 s and
1.1 GiB; 100k tasks = 907 s (4.6× the linear prediction) and 2.2 GiB. The
superlinear knee is consistent with GC pressure from tens of millions of live
map entries.

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

At the measured ~2 ms/task, the default 2-minute `--session-process-timeout`
is exhausted at roughly 60,000 tasks — and in practice the 35 s `WriteTimeout`
(finding 1) bites at ~17,000. Neither number appears in the documentation, and
nothing warns the user as a session approaches them. A session that exceeds them
is not degraded, it is simply unopenable.

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
termination.** Users who care should shorten `RAY_COLLECTOR_EVENT_ROTATION_INTERVAL`
and lengthen `terminationGracePeriodSeconds`.

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
  does not affect this path. We measured 0.46% task loss at 3,084 tasks/s with a
  10 s post-submission drain, and zero loss at ≤2,880 tasks/s — consistent with
  the owner's buffer draining at ≤10,000 events per flush at exit.
