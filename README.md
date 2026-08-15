# Ray History Server — Benchmark Evidence

Measurements behind **"Ray History Server: Bridging the Impossible Gaps in Observability"** (Ray Summit 2026, slides 16–22).

Two components, two sizing rules. Charts, the data behind them, and the code to run it again — all in [`benchmark/`](benchmark).

## The short version

| | Collector (per Ray node) | History Server |
|---|---|---|
| **CPU driven by** | events/s arriving | bursts: opening a session, answering a query |
| **CPU cost** | ~20 mCPU per 1,000 events/s | 4.98 CPU-seconds to open a 50,000-task session |
| **Memory driven by** | bytes still on local disk | tasks in the session opened |
| **Memory cost** | 21–26 MiB program + disk cache the kernel can drop | ~27 KiB per task + a ~37 MiB floor |
| **Question to ask** | "how much piles up between uploads?" | "how big is the biggest session?" |

**The Collector's memory number is not the Collector.** At 5,000 events/s the container showed ~160 MiB — only ~25 MiB was the program; the rest was Linux caching the on-disk file.

**Every memory number counts the whole container**, cache included — that is what Kubernetes OOM-kills you for.

## How it was measured

| | Collector | History Server |
|---|---|---|
| Workload | fixed event rate, 90 s | one pre-built session, opened and queried |
| Sizes | 1,000 / 2,000 / 3,000 / 5,000 events/s | 1,000 / 5,000 / 10,000 / 50,000 tasks |
| Repeats | 3 fresh Pods | 5 fresh Pods |
| Pod | CPU 100m / 2000m; memory limit varies | CPU 1 / 2; memory 1Gi / 12Gi, `GOMAXPROCS=2` |
| Counters | cgroup v2, memory split program / cache / kernel | same |

Dedicated cluster, one fresh Pod per test. Between History Server requests the container must go quiet (three intervals under 50 mCPU) or the test fails rather than sending the next one.

Across 27 Collector runs: 7,650,000 events planned = sent = accepted = found in storage, at exactly 895.0 bytes each, compressing **10.85 : 1**. Zero duplicates, corrupt lines, failed requests, retries, restarts, or OOM kills.

---

## Collector

A sidecar that receives events, appends them to a local file, then periodically compresses, uploads, and deletes it. **CPU is paid per event; memory per byte still on disk.**

### CPU

[![Collector CPU vs event ingress](benchmark/charts/collector/slide17-collector-cpu-scaling.png)](benchmark/charts/collector/slide17-collector-cpu-scaling.png)

| Events/s | Ingest mean, median | Range (3 runs) | Interval p95 | Peak interval, median (range) |
|---:|---:|---:|---:|---:|
| 1,000 | 18.1 mCPU | 17.4 – 20.3 | 65.2 mCPU | 748 mCPU (695 – 890) |
| 5,000 | 105.9 mCPU | 104.2 – 107.3 | 134.1 mCPU | 1,110 mCPU (1,109 – 1,116) |

**~20 mCPU per 1,000 events/s** — all seven points across both campaigns fall between 18 and 24.

**Do not size on the average.** Within a run, peak interval ÷ that run's mean was 37–49× at 1,000 events/s and ~10× at 5,000. Those peaks are compress-and-upload, not ingest.

> Detailed per-run CPU is retained for 1,000 and 5,000 events/s; the chart's 2k and 3k points are chart-only.

### Memory

[![Collector memory vs event ingress](benchmark/charts/collector/slide17-collector-memory-scaling.png)](benchmark/charts/collector/slide17-collector-memory-scaling.png)

At exactly 30 s in — before any upload at any rate.

| Events/s | Container | Range | Program | Disk cache | Bytes on disk |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 58.3 MiB | 50.3 – 61.2 | 25.0 MiB | 25.6 MiB | 25.6 MiB |
| 2,000 | 79.4 MiB | 79.3 – 82.7 | 21.3 MiB | 51.2 MiB | 51.2 MiB |
| 3,000 | 105.7 MiB | 105.1 – 108.1 | 22.9 MiB | 76.7 MiB | 76.8 MiB |
| 5,000 | 160.0 MiB | 159.7 – 164.0 | 24.5 MiB | 127.8 MiB | 127.8 MiB |

Read across, not down. **The program does not grow with rate** — 21 to 25 MiB throughout. Every extra MiB is cache, tracking the disk file within 0.14 MiB.

| Events/s | Written in 90 s | Uploads | Peak, median (range) | At the end |
|---:|---:|---|---:|---:|
| 1,000 | 76.8 MiB | shutdown 105.7 s | 119.3 MiB (113.5 – 122.5) | 28.9 MiB |
| 2,000 | 153.6 MiB | 72.9 s, shutdown 105.4 s | 171.9 MiB (171.2 – 175.3) | 27.0 MiB |
| 3,000 | 230.5 MiB | 43.8 s, 103.8 s | 167.3 MiB (162.7 – 167.8) | 26.3 MiB |
| 5,000 | 384.1 MiB | 44.1 s, 73.9 s, shutdown 105.7 s | 245.0 MiB (236.4 – 247.2) | 21.4 MiB |

**Peak was not monotonic with rate.** 3,000 events/s peaked below 2,000 — it uploaded earlier, so less was resident. Size for the largest pile of un-uploaded data.

At 1,000 events/s the 90-second run stayed under both rotation triggers (100 MiB, 5 minutes), so all 76.8 MiB was still local at shutdown. Every rate then settled back to 21–29 MiB. It was only ever cache.

### Lifecycle

Pointwise median across the three 5,000 events/s repeats: 10 s idle, 90 s traffic, ~15 s idle, shutdown.

[![Collector CPU lifecycle at 5k events/s](benchmark/charts/collector/slide18-collector-cpu-lifecycle-5k.png)](benchmark/charts/collector/slide18-collector-cpu-lifecycle-5k.png)

[![Collector memory lifecycle at 5k events/s](benchmark/charts/collector/slide18-collector-memory-lifecycle-5k.png)](benchmark/charts/collector/slide18-collector-memory-lifecycle-5k.png)

CPU holds a flat band near 100 mCPU. Memory is a sawtooth — climb, then drop the moment an upload completes and the local file is deleted. Typical and peak differ by more than 2×, **and only the peak can kill you.**

### How small can the memory limit go?

5,000 events/s, 3 fresh Pods per limit. Per-run medians.

| Limit | Peak, median | ÷ limit | Reclaims | PSI stall | OOM | Verdict |
|---:|---:|---:|---:|---:|---:|---|
| 192 Mi | 192.9 MiB | **1.005** | 1,084 | 16.5 ms | 0 | failed — pressure |
| 256 Mi | 245.4 MiB | 0.959 | 0 | 0 | 0 | failed — headroom rule only |
| 512 Mi | 239.8 MiB | **0.468** | 0 | 0 | 0 | **passed 3/3** |

| Limit | Program | Disk cache | Kernel | Events/s | p99 |
|---:|---:|---:|---:|---:|---:|
| 192 Mi | 22.1 MiB | 162.5 MiB | 2.9 MiB | 4,999.9 | 44.1 ms |
| 256 Mi | 27.3 MiB | 199.7 MiB | 6.5 MiB | 5,000.0 | 43.9 ms |
| 512 Mi | 20.7 MiB | 199.4 MiB | 6.5 MiB | 4,999.9 | 46.1 ms |

**At 192 Mi it did not die — it struggled quietly.** Peak pinned at the limit, memory reclaimed 1,084 times, 16.5 ms stalled — but zero OOM kills, still 4,999.9 events/s at 44.1 ms p99. *If your only alert is OOM kills, this looks healthy.*

**256 Mi showed no pressure at all** and failed only the campaign's "peak ≤ 80% of limit" rule (245.4 MiB is 95.9%). Policy, not an observed failure.

**2.7× more memory gave the program nothing** — 22.1 → 20.7 MiB. The extra room went to cache. Selected: **512Mi**.

### Head vs Worker

50,000 no-op tasks, both Collectors watched at once.

[![Collector CPU lifecycle, head vs worker](benchmark/charts/collector/slide19-collector-head-worker-cpu-lifecycle.png)](benchmark/charts/collector/slide19-collector-head-worker-cpu-lifecycle.png)

[![Collector memory lifecycle, head vs worker](benchmark/charts/collector/slide19-collector-head-worker-memory-lifecycle.png)](benchmark/charts/collector/slide19-collector-head-worker-memory-lifecycle.png)

| Target tasks/s | Head ev/s | Worker ev/s | Head CPU | Worker CPU | mCPU/1k ev/s | Head mem p95 | Worker mem p95 | ev/s ÷ target |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 250 | 510.2 | 525.7 | 12.3 m | 12.1 m | 23.6 | 109.0 MiB | 103.9 MiB | 4.14 |
| 500 | 1,020.5 | 1,027.2 | 23.4 m | 22.3 m | 22.3 | 115.9 MiB | 108.0 MiB | 4.10 |
| 1,000 | 2,088.7 | 2,012.7 | 46.4 m | 44.5 m | 22.2 | 112.4 MiB | 108.1 MiB | 4.10 |
| 2,000 | 4,666.6 | 4,304.7 | 98.6 m | 94.0 m | 21.5 | 113.8 MiB | 112.2 MiB | 4.49 |
| 3,000 | 4,895.9 | 4,750.9 | 103.8 m | 102.0 m | 21.3 | 118.1 MiB | 108.4 MiB | 3.22 ⚠︎ |

⚠︎ the driver could not submit 3,000 tasks/s; its measured ceiling was 2,270.

**Close enough to size the same** — CPU within 5.0%, memory within 9.0%. Memory stays flat near 110 MiB across a 12× range of rates.

**In the last half-second before shutdown both jumped to ~970 mCPU** to flush everything left on disk. A tight CPU limit makes that slower, not cheaper.

**Event ingress ÷ target task rate is ~4.1–4.5** on Ray 2.56 no-op tasks — the bridge between tasks/s, which you can estimate, and events/s, which is what costs money. Also the least portable number here.

---

## History Server

Loads a finished session from object storage into memory, then answers queries. Both costs rise with tasks in the session.

> The 12 GiB limit was measurement headroom, not a recommendation. Nothing here says what happens when it is squeezed.

### Load time

[![History Server cold-load wall time vs tasks](benchmark/charts/history-server/slide20-history-server-cold-load-scaling.png)](benchmark/charts/history-server/slide20-history-server-cold-load-scaling.png)

| Tasks | Median | Range (5 runs) | Per task |
|---:|---:|---:|---:|
| 1,000 | 0.112 s | 0.109 – 0.122 | 112 µs |
| 5,000 | 0.412 s | 0.402 – 0.441 | 82 µs |
| 10,000 | 0.871 s | 0.837 – 0.893 | 87 µs |
| 50,000 | 4.212 s | 4.157 – 4.329 | 84 µs |

**Fit: `17 ms + 84 µs × tasks`, R² = 0.9999.** No bend at the four sizes tested. Spread narrows with size: 11% at 1,000 tasks, 4% at 50,000.

**A 50,000-task session takes about four seconds to open.** Someone is waiting — that is a page-load budget, not a capacity plan.

### Memory

[![History Server memory peak vs tasks](benchmark/charts/history-server/slide20-history-server-memory-scaling.png)](benchmark/charts/history-server/slide20-history-server-memory-scaling.png)

| Tasks | Peak, median | Range (5 runs) | Per task |
|---:|---:|---:|---:|
| 1,000 | 48.8 MiB | 46.2 – 59.0 | 50.0 KiB |
| 5,000 | 171.4 MiB | 162.8 – 193.0 | 35.1 KiB |
| 10,000 | 319.8 MiB | 293.5 – 402.6 | 32.7 KiB |
| 50,000 | 1,367.6 MiB | 1,279.4 – 1,554.3 | 28.0 KiB |

**Fit: `37 MiB + 27 KiB × tasks`, R² = 0.9996.** Pulled by the big sessions — it overshoots 1,000 tasks by ~15 MiB, so treat it as a rule for 5,000 and up.

Peaks vary far more than load times: at 50,000 tasks the five runs spanned 1,279–1,554 MiB, a 20% spread. **Size against the top of the range.** Measurements stop at 50,000 tasks.

### Lifecycle

[![History Server CPU lifecycle, 50k tasks](benchmark/charts/history-server/slide21-history-server-cpu-lifecycle.png)](benchmark/charts/history-server/slide21-history-server-cpu-lifecycle.png)

| Phase | When | Duration | CPU-seconds |
|---|---:|---:|---:|
| Idle | 0.00 – 10.07 s | 10.07 s | — |
| **Open the session** | 10.07 – 14.29 s | 4.23 s | **4.98** |
| Idle | 14.29 – 22.37 s | 8.07 s | — |
| **Count the tasks** | 22.37 – 23.33 s | 0.96 s | **0.96** |
| Idle | 23.33 – 31.40 s | 8.08 s | — |
| **Return 10,000 tasks** | 31.40 – 32.60 s | 1.20 s | **1.09** |
| Idle | 32.60 – 40.61 s | 8.00 s | — |

Three spikes separated by flat zero. **Idle by default, hard work briefly** — the opposite of the Collector's constant hum, and why the two want different limits. Opening the session used **1.18 cores on average** and will use a second core if given one.

[![History Server memory lifecycle, 50k tasks](benchmark/charts/history-server/slide21-history-server-memory-lifecycle.png)](benchmark/charts/history-server/slide21-history-server-memory-lifecycle.png)

No sawtooth. Memory rises once while the session opens and **stays there** — peak 1,554 MiB, still 1,385 MiB 26 seconds after loading finished.

**The Collector's memory drains. The History Server's is a working set that stays.** Against the 1 GiB request that is 35% above at the final sample and 52% at peak. Requests schedule; they do not cap.

---

## Requests and limits

A starting point derived from the above, not universal defaults.

**Collector** — CPU request `25m per 1,000 events/s` on your busiest sidecar (measured 18–24). Leave the CPU limit generous or off: upload and shutdown bursts reached 695–1,116 mCPU, so a small multiple of the request would throttle them. Memory limit **512Mi** at 5,000 events/s under the 80%-headroom rule; 256Mi showed no pressure and failed only that rule. Most of the footprint is droppable cache.

Scale memory by what can accumulate before rotation (100 MiB or 5 minutes) or shutdown.

**History Server** — memory `37 MiB + 27 KiB × tasks` plus margin, for one loaded session. CPU: the tested profile averaged 1.18 cores opening a 50k session. Load-time budget `17 ms + 84 µs × tasks`.

**Caveat.** These tests fixed CPU request 1, limit 2, and `GOMAXPROCS=2` together, so they do not separate quota from Go parallelism. A different CPU limit is a different experiment — re-measure.

## What this does not prove

- Head Collector endpoint polling was not in the memory-limit sweep.
- The History Server was never run under a tight memory limit, and only one cached session was measured — multi-session accumulation and eviction were not.
- The 50k source used a different generation contract (paced 500 tasks/s) from the 1k/5k/10k sources, so the fits describe these four corpora rather than isolating task count.
- "~4.1–4.5" divides observed events/s by the *configured* target task rate.
- The two campaigns ran on different days; head/worker charts come from the earlier 50k-task campaign.
- Measurements stop at 5,000 events/s and 50,000 tasks. Past that is extrapolation.
- Raw campaign output is not published — logs, binaries, credentials, and stored events stay out.

Validation: the Collector campaign passed its validator (`SWEEP-VALID`), all hashes match, discrepancy count **0** — [`validation.json`](benchmark/data/collector/validation.json). Chart fingerprints: [`chart-output-manifest.json`](benchmark/data/history-server/chart-output-manifest.json).

## Reproducing

Prerequisites and full detail: **[`benchmark/harness/README.md`](benchmark/harness/README.md)**.

Use a dedicated `kind-bench` cluster with the `collector` / `historyserver` images loaded and **no** KubeRay operator already running — each runner builds and starts its own. The two harness snapshots are campaign-specific; install each as `historyserver/test/benchmark` in its own clean KubeRay checkout. Never run two campaigns side by side.

```bash
# from benchmark/harness/collector/ — head/worker task-rate campaign
./sweeps/ray256_collector.sh

# direct one-Collector event-rate matrix, lifecycle, and 192/256/512 Mi sweep
BENCH_SWEEP_OUT=/abs/path/out ./sweeps/ray256_collector_memory.sh
python3 ./sweeps/validate_collector_memory.py /abs/path/out --full   # expect SWEEP-VALID

# from benchmark/harness/history-server/ — isolated request protocol
BENCH_HS_SOURCE_ACCEPTED_DIR=/abs/path/accepted \
BENCH_SWEEP_OUT=/abs/path/hs-out \
  bash ./sweeps/ray256_hs_isolated.sh
python3 ./sweeps/validate_hs_sweep.py /abs/path/hs-out --expected-kind hs-isolated-request
```

[`benchmark/renderers/`](benchmark/renderers) shows which metric each chart plots. Some still point at their original working directories and will not run without the raw campaign output.

## Layout

```
benchmark/
├── charts/       the 10 images used by slides 17–21
├── data/         retained summaries behind the charts
├── renderers/    chart-drawing scripts (provenance; not standalone)
└── harness/      reproduce code — one snapshot per campaign
```

**charts** for the picture, **data** to check a number, **harness** to run it yourself.

[Apache 2.0](LICENSE)
