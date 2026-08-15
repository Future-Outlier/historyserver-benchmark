# Ray History Server — Benchmark Evidence

Measurements behind **"Ray History Server: Bridging the Impossible Gaps in Observability"** (Ray Summit 2026, slides 16–22).

Two components, two different sizing rules. This page is the evidence: every chart from the slides, the numbers behind each one, and the code to run it again. [`benchmark/`](benchmark) holds the charts, the data, and the reproduce code.

**For you if** you are deciding how much CPU and memory these two components need in Kubernetes.

---

## The short version

| | Collector (one per Ray node) | History Server (one service) |
|---|---|---|
| **What it does** | catches events from a Ray node, writes them to local disk, ships them to object storage | loads a finished session back from storage so people can look at it |
| **CPU driven by** | events per second arriving | one-off bursts: opening a session, answering a query |
| **CPU cost** | ~20 mCPU per 1,000 events/s | 4.98 CPU-seconds to open a 50,000-task session |
| **Memory driven by** | bytes still waiting on local disk | tasks in the session you opened |
| **Memory cost** | mostly disk cache the kernel can drop; the program itself stays at 21–26 MiB | ~27 KiB per task, on top of a ~37 MiB floor |
| **Question to ask** | "how much piles up between uploads?" | "how big is the biggest session anyone opens?" |

**1. The Collector's memory number is not the Collector.** At 5,000 events/s the container showed ~160 MiB — only ~25 MiB was the program. The rest was Linux caching the on-disk file. Shrink the limit and the kernel drops that cache; the program does not crash.

**2. The History Server is boringly predictable.** Load time = `84 µs × tasks`, R² = 0.9999 across a 50× range. No cliff.

**3. Every memory number counts the whole container**, cache included — because that is what Kubernetes OOM-kills you for.

<details>
<summary><b>Terms used on this page</b></summary>

| Term | Meaning |
|---|---|
| **mCPU** | thousandths of a core. `100m` = 10% of one core. |
| **request vs limit** | *request* = what you reserve (decides scheduling). *limit* = hard ceiling. Over the CPU limit you get slowed; over the memory limit you get killed. |
| **heap / "anon"** | memory the program allocated for itself. |
| **page cache / "file"** | a copy of a disk file Linux keeps in RAM. Counts against your limit, but the kernel can drop it anytime. |
| **peak** | the highest the container ever reached. Not an average. This is what kills you. |
| **p95 / p99** | the value 95% (or 99%) of samples came in under. |
| **R²** | how straight a line is. Above 0.99 means the formula is trustworthy. |
| **spool** | the file on the Collector's local disk where events pile up before upload. |
| **cold load** | opening a session with nothing cached. The slow path. |

</details>

---

## How things were measured

| | Collector campaign | History Server campaign |
|---|---|---|
| Workload | event generator at a fixed rate, 90 s of traffic | one pre-built session, opened and queried |
| Sizes tested | 1,000 / 2,000 / 3,000 / 5,000 events/s | 1,000 / 5,000 / 10,000 / 50,000 tasks |
| Repeats | 3 fresh Pods per size | 5 fresh Pods per size |
| Event size | 895 bytes (exact, campaign-wide) | — |
| Pod settings | CPU request 100m / limit 2000m; memory limit varies | CPU request 1 / limit 2; memory request 1Gi / limit 12Gi |
| Go runtime | — | `GOMAXPROCS=2`, pinned on purpose |
| Counters | Linux cgroup v2, memory split into program / cache / kernel | same |

Dedicated test cluster, one fresh Pod per test, nothing else running beside it.

**Why per-request CPU is attributable.** Between requests the test waits for the container to go quiet — three consecutive intervals under 50 mCPU — or fails rather than sending the next request anyway.

**The Collector campaign's books balance exactly.** Across 27 runs: 7,650,000 events planned = sent = accepted = found in storage. 6,846,750,000 bytes written and confirmed, exactly 895.0 bytes/event, compressing **10.85 : 1**. Zero duplicates, corrupt lines, failed requests, retries, restarts, OOM kills, or dropped events.

---

## Part 1 — Collector

A sidecar next to each Ray node. It receives events over HTTP, appends them to a local file, then periodically compresses that file, uploads it, and deletes the local copy.

That shape is the whole story: **CPU is paid per event; memory is paid per byte still on local disk.**

### CPU tracks events per second

[![Collector CPU vs event ingress](benchmark/charts/collector/slide17-collector-cpu-scaling.png)](benchmark/charts/collector/slide17-collector-cpu-scaling.png)

| Events/s | Average CPU while ingesting | Range (3 runs) | p95 | Highest single interval |
|---:|---:|---:|---:|---:|
| 1,000 | 18.1 mCPU | 17.4 – 20.3 | 65.2 mCPU | 748 mCPU |
| 5,000 | 105.9 mCPU | 104.2 – 107.3 | 134.1 mCPU | 1,110 mCPU |

**The rule: ~20 mCPU per 1,000 events/s.** Across this campaign and the separate [head/worker campaign](#head-collector-vs-worker-collector), all seven measured points land between **18 and 24**.

**But do not size on the average.** The busiest interval hit **41× the average** at 1,000 events/s and 10× at 5,000. Those spikes are compress-and-upload, not ingest. Nothing throttled here because the limit was 2,000m — a limit set to "average plus a bit" would throttle the Collector exactly when it is trying to flush.

> The chart plots four rates; the published data carries detailed per-run CPU for 1,000 and 5,000 events/s.

### Memory is disk cache, not the program

[![Collector memory vs event ingress](benchmark/charts/collector/slide17-collector-memory-scaling.png)](benchmark/charts/collector/slide17-collector-memory-scaling.png)

Container memory at exactly 30 s in — before any upload at any rate, so all four are compared at the same point in the cycle.

| Events/s | Container memory | Range (3 runs) | The program | Disk cache | Bytes on disk |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 58.3 MiB | 50.3 – 61.2 | 25.0 MiB | 25.6 MiB | 25.6 MiB |
| 2,000 | 79.4 MiB | 79.3 – 82.7 | 21.3 MiB | 51.2 MiB | 51.2 MiB |
| 3,000 | 105.7 MiB | 105.1 – 108.1 | 22.9 MiB | 76.7 MiB | 76.8 MiB |
| 5,000 | 160.0 MiB | 159.7 – 164.0 | 24.5 MiB | 127.8 MiB | 127.8 MiB |

Read the last three columns across, not down. **The program's own memory never grows** — 21 to 25 MiB whether it handles 1,000 or 5,000 events/s. Every extra MiB is cache, and cache tracks the disk file to within 0.1 MiB.

The container's memory number is answering a *storage* question, not a *software* one.

| Events/s | Written in 90 s | Uploads (when) | Peak, median (range) | Memory at the end |
|---:|---:|---|---:|---:|
| 1,000 | 76.8 MiB | shutdown only, 105.7 s | 119.3 MiB (113.5 – 122.5) | 28.9 MiB |
| 2,000 | 153.6 MiB | 72.9 s, shutdown 105.4 s | 171.9 MiB (171.2 – 175.3) | 27.0 MiB |
| 3,000 | 230.5 MiB | 43.8 s, 103.8 s | 167.3 MiB (162.7 – 167.8) | 26.3 MiB |
| 5,000 | 384.1 MiB | 44.1 s, 73.9 s, shutdown 105.7 s | 245.0 MiB (236.4 – 247.2) | 21.4 MiB |

**Peak does not follow the rate.** 3,000 events/s peaked *lower* than 2,000 — it uploaded earlier, so less was sitting around. Size for the largest pile of un-uploaded data, not the rate.

**Watch the 1,000 events/s row.** 76.8 MiB was never enough to trigger an upload, so nothing left the Pod until shutdown. **A quiet node keeps the entire session on local disk until it stops.**

And the last column: every rate settled back to 21–29 MiB after the final upload. It was only ever cache.

### One 90-second run, second by second

Same 5,000 events/s run in both charts: 10 s idle, 90 s of traffic, ~15 s idle, shutdown.

[![Collector CPU lifecycle at 5k events/s](benchmark/charts/collector/slide18-collector-cpu-lifecycle-5k.png)](benchmark/charts/collector/slide18-collector-cpu-lifecycle-5k.png)

Flat band around 100 mCPU, far below the 2,000m limit. The jumps above it are the `Upload complete` markers.

[![Collector memory lifecycle at 5k events/s](benchmark/charts/collector/slide18-collector-memory-lifecycle-5k.png)](benchmark/charts/collector/slide18-collector-memory-lifecycle-5k.png)

A sawtooth: climb as data piles up, drop straight down when an upload finishes and the local file is deleted, climb again. Typical value and peak differ by more than 2×, **and only the peak can get you killed.**

### How small can the memory limit go?

Traffic held at 5,000 events/s; memory limit varied. 3 fresh Pods per limit.

| Limit | Peak, median | Peak ÷ limit | Kernel reclaims | Time stalled | OOM kills | Result |
|---:|---:|---:|---:|---:|---:|---|
| 192 Mi | 192.9 MiB | **1.005** | 3,255 | 51.3 ms | 0 | failed |
| 256 Mi | 245.4 MiB | 0.959 | 0 | 0 | 0 | failed — headroom rule |
| 512 Mi | 239.8 MiB | **0.468** | 0 | 0 | 0 | **passed 3/3** |

| Limit | The program | Disk cache | Kernel | Events/s achieved | Latency p99 |
|---:|---:|---:|---:|---:|---:|
| 192 Mi | 22.1 MiB | 164.9 MiB | 2.1 MiB | 4,999.9 | 44.3 ms |
| 256 Mi | 26.4 MiB | 199.7 MiB | 6.5 MiB | 5,000.0 | 43.9 ms |
| 512 Mi | 20.7 MiB | 197.2 MiB | 6.4 MiB | 4,999.9 | 44.4 ms |

**At 192 Mi the container did not die — it struggled quietly.** Peak pinned at the limit, 3,255 reclaims, 51.3 ms fully stalled — but **zero OOM kills**, still 4,999.9 events/s, still a 44.3 ms p99. *If your only alert is "did it get OOM-killed", this looks perfectly healthy.*

**256 Mi showed no stress at all and still failed** — only because the test required peak ≤ 80% of the limit and 245.4 MiB is 95.9% of 256 MiB. That is a safety-margin *policy*, not an observed problem. Apply your own.

**2.7× more memory did not give the program more memory.** From 192 Mi to 512 Mi the program went 22.1 → 20.7 MiB. All the extra room went to cache.

Selected limit: **512Mi**.

### Head Collector vs Worker Collector

A separate campaign: 50,000 no-op tasks, both Collectors watched at once, five submission rates, 3 runs each.

[![Collector CPU lifecycle, head vs worker](benchmark/charts/collector/slide19-collector-head-worker-cpu-lifecycle.png)](benchmark/charts/collector/slide19-collector-head-worker-cpu-lifecycle.png)

[![Collector memory lifecycle, head vs worker](benchmark/charts/collector/slide19-collector-head-worker-memory-lifecycle.png)](benchmark/charts/collector/slide19-collector-head-worker-memory-lifecycle.png)

| Target tasks/s | Head ev/s | Worker ev/s | Head CPU | Worker CPU | mCPU per 1k ev/s | Head mem p95 | Worker mem p95 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 250 | 510.2 | 525.7 | 12.3 m | 12.1 m | 23.6 | 109.0 MiB | 103.9 MiB |
| 500 | 1,020.5 | 1,027.2 | 23.4 m | 22.3 m | 22.3 | 115.9 MiB | 108.0 MiB |
| 1,000 | 2,088.7 | 2,012.7 | 46.4 m | 44.5 m | 22.2 | 112.4 MiB | 108.1 MiB |
| 2,000 | 4,666.6 | 4,304.7 | 98.6 m | 94.0 m | 21.5 | 113.8 MiB | 112.2 MiB |
| 3,000 | 4,895.9 | 4,750.9 | 103.8 m | 102.0 m | 21.3 | 118.1 MiB | 108.4 MiB |

**The two roles cost the same** — within ~5% on both CPU and memory at every rate. **Size them the same.**

**Memory stays flat at ~110 MiB across a 12× range of rates.** Same story: cache, capped by the upload cycle.

**The 3,000 row is not really 3,000** — the driver could not submit that fast. The same campaign measured 2,270 tasks/s as its real ceiling.

**In the final half-second before shutdown, both Collectors jumped to ~970 mCPU** (Head 969m, Worker 974m) to compress and upload everything left on disk. A tight CPU limit does not make that cheaper — just slower.

### How many events does one task produce?

| Target tasks/s | Total events/s | Events per task |
|---:|---:|---:|
| 250 | 1,035.9 | 4.14 |
| 500 | 2,047.7 | 4.10 |
| 1,000 | 4,101.4 | 4.10 |
| 2,000 | 8,971.3 | 4.49 |
| 3,000 | 9,646.8 | 3.22 ⚠︎ driver-limited |

**Roughly 4.1 – 4.5 events per task** on Ray 2.56 with do-nothing tasks. This bridges "tasks/s", which you can estimate, and "events/s", which is what costs money.

It is also the least portable number here. Real tasks with logs and retries emit more. Use it for intuition, then size from your own busiest Collector's measured events/s.

---

## Part 2 — History Server

Loads a finished session out of object storage into memory, then answers questions about it. Both costs scale with one input: **tasks in the session someone opens.**

CPU request 1 / limit 2, memory request 1Gi / limit 12Gi, 5 fresh Pods per size.

> **The 12 GiB limit is measurement headroom, not a recommendation.** It was set high so the curve could be measured without the kernel interfering. Nothing here says what happens when the History Server is squeezed.

### Load time is a straight line

[![History Server cold-load wall time vs tasks](benchmark/charts/history-server/slide20-history-server-cold-load-scaling.png)](benchmark/charts/history-server/slide20-history-server-cold-load-scaling.png)

| Tasks | Load time, median | Range (5 runs) | Per task |
|---:|---:|---:|---:|
| 1,000 | 0.112 s | 0.109 – 0.122 | 112 µs |
| 5,000 | 0.412 s | 0.402 – 0.441 | 82 µs |
| 10,000 | 0.871 s | 0.837 – 0.893 | 87 µs |
| 50,000 | 4.212 s | 4.157 – 4.329 | 84 µs |

**`load time ≈ 84 µs × tasks`, R² = 0.9999.** No bend across a 50× range. Run-to-run variation tightens with size: 11% at 1,000 tasks, 4% at 50,000.

In practice: **a 50,000-task session takes about four seconds to open.** Someone is waiting. That is a page-load budget, not a capacity plan.

### Memory is a straight line too

[![History Server memory peak vs tasks](benchmark/charts/history-server/slide20-history-server-memory-scaling.png)](benchmark/charts/history-server/slide20-history-server-memory-scaling.png)

| Tasks | Peak, median | Range (5 runs) | Per task |
|---:|---:|---:|---:|
| 1,000 | 48.8 MiB | 46.2 – 59.0 | 50.0 KiB |
| 5,000 | 171.4 MiB | 162.8 – 193.0 | 35.1 KiB |
| 10,000 | 319.8 MiB | 293.5 – 402.6 | 32.7 KiB |
| 50,000 | 1,367.6 MiB | 1,279.4 – 1,554.3 | 28.0 KiB |

**`peak ≈ 27 KiB × tasks + 37 MiB`, R² = 0.9996.**

Two warnings. The fit is pulled by the big sessions and overshoots 1,000 tasks by ~15 MiB — treat it as a rule for 5,000 and up. And peaks vary far more than load times: at 50,000 tasks the five runs spanned 1,279 – 1,554 MiB (20%), because a peak depends on when GC happens to run. **Size against the top of that range.**

Measurements stop at 50,000 tasks. Past that is arithmetic, not evidence.

### One 50k session, second by second

[![History Server CPU lifecycle, 50k tasks](benchmark/charts/history-server/slide21-history-server-cpu-lifecycle.png)](benchmark/charts/history-server/slide21-history-server-cpu-lifecycle.png)

| Phase | When | How long | CPU-seconds |
|---|---:|---:|---:|
| Idle | 0.00 – 10.07 s | 10.07 s | — |
| **Open the session** | 10.07 – 14.29 s | 4.23 s | **4.98** |
| Idle | 14.29 – 22.37 s | 8.07 s | — |
| **Count the tasks** | 22.37 – 23.33 s | 0.96 s | **0.96** |
| Idle | 23.33 – 31.40 s | 8.08 s | — |
| **Return 10,000 tasks** | 31.40 – 32.60 s | 1.20 s | **1.09** |
| Idle | 32.60 – 40.61 s | 8.00 s | — |

Three spikes separated by flat zero. **Idle by default, hard work briefly** — the opposite of the Collector's constant hum, and why the two want different limits.

Opening the session used 4.98 CPU-seconds over 4.23 s: **about 1.2 cores**. It will use a second core if you give it one.

[![History Server memory lifecycle, 50k tasks](benchmark/charts/history-server/slide21-history-server-memory-lifecycle.png)](benchmark/charts/history-server/slide21-history-server-memory-lifecycle.png)

No sawtooth here. Memory rises once while the session opens and then **stays there** — peak 1,554 MiB, and still 1,385 MiB 26 seconds after loading finished.

**The Collector's memory is a buffer that drains. The History Server's is a working set that stays.** Once cached, a session occupies memory whether anyone looks at it or not.

Note the 1 GiB request line: the container sat 35–50% above its own request. Requests schedule; they do not cap.

---

## Turning this into requests and limits

Derived from the measurements above as a **starting point**, not universal defaults. Policy choices are marked.

**Worker / Head Collector**

| | Suggested | Why |
|---|---|---|
| CPU request | `25m per 1,000 events/s` on your busiest Collector | all seven points fell in 18 – 24; rounded up |
| CPU limit | ≥ 10× the request, or none | spikes hit 748 – 1,110 mCPU on upload, ~970 mCPU at shutdown |
| Memory limit | **512Mi** at 5,000 events/s | first limit that passed 3/3 — **measured** |
| Memory request | comfortably below the limit | **policy** — most of the footprint is droppable cache |

Scale the memory limit by how much can pile up between uploads — at low traffic that is *the whole session*, since the 1,000 events/s test never uploaded until shutdown.

**History Server**

| | Suggested | Why |
|---|---|---|
| Memory | `37 MiB + 27 KiB × tasks` per cached session, plus margin | R² = 0.9996; use the top of the range, ~1.55 GiB at 50k |
| CPU | ≥ 1 core, 2 if load time matters to users | opening a session averaged 1.2 cores |
| Load-time budget | `84 µs × tasks` | R² = 0.9999 |

**Caveat.** These tests pinned `GOMAXPROCS=2` alongside a 2-CPU limit. In a container that derives its thread count from the CPU limit, changing the limit changes parallelism too — so a different CPU limit is a *different experiment*, not a point on these curves. **Re-measure load time if you deploy at a different CPU limit.**

---

## What this does not prove

- **Head Collector endpoint polling was never in the memory-limit test.** 512Mi comes from the Worker-style workload.
- **The History Server was never run under a tight memory limit.** Its 12 GiB was headroom.
- **"4.1 – 4.5 events per task" is specific to Ray 2.56 and do-nothing tasks.** Intuition, not an input.
- **The two campaigns are separate experiments**, on different days — not a joint measurement under one load.
- **Head/worker lifecycle charts come from the earlier 50k-task campaign**, not the isolated one.
- **Measurements stop at 5,000 events/s and 50,000 tasks.** Past that, the formulas are extrapolation.
- **Raw output is not published.** ~58 MiB (Collector) and ~8.9 GiB (History Server) of logs, binaries, credentials and stored events stay out; the summarized evidence is in [`benchmark/data/`](benchmark/data).

**Validation.** The Collector campaign passed its official validator (`SWEEP-VALID`), every file hash matches, all 206 data columns are documented, discrepancy count **0** — [`validation.json`](benchmark/data/collector/validation.json). Chart fingerprints are in [`chart-output-manifest.json`](benchmark/data/history-server/chart-output-manifest.json); all four match the published images.

<details>
<summary><b>Slide-to-chart mapping</b> (16 and 22 are native slide shapes, no chart file)</summary>

| Slide | Chart, under `benchmark/charts/` |
|---|---|
| 17 | `collector/slide17-collector-cpu-scaling.png` |
| 17 | `collector/slide17-collector-memory-scaling.png` |
| 18 | `collector/slide18-collector-cpu-lifecycle-5k.png` |
| 18 | `collector/slide18-collector-memory-lifecycle-5k.png` |
| 19 | `collector/slide19-collector-head-worker-cpu-lifecycle.png` |
| 19 | `collector/slide19-collector-head-worker-memory-lifecycle.png` |
| 20 | `history-server/slide20-history-server-cold-load-scaling.png` |
| 20 | `history-server/slide20-history-server-memory-scaling.png` |
| 21 | `history-server/slide21-history-server-cpu-lifecycle.png` |
| 21 | `history-server/slide21-history-server-memory-lifecycle.png` |

</details>

---

## Reproducing the campaigns

Full prerequisites and every environment variable: **[`benchmark/harness/README.md`](benchmark/harness/README.md)**.

You need a **dedicated** Kind cluster and kubectl context with the KubeRay operator running and the `collector` / `historyserver` images loaded. The runners refuse the wrong context, refuse to overwrite an output directory, and record the expected results and image IDs before any test runs. Never run two campaigns side by side.

Copy a harness snapshot into `historyserver/test/benchmark` of a compatible KubeRay checkout, then:

```bash
# Collector: event-rate matrix
./sweeps/ray256_collector.sh

# Collector: memory-limit sweep (192 / 256 / 512 Mi)
BENCH_SWEEP_OUT=/absolute/path/to/out ./sweeps/ray256_collector_memory.sh

# History Server: one fixed source session per task count
BENCH_HS_SOURCE_ACCEPTED_DIR=/absolute/path/to/accepted \
BENCH_SWEEP_OUT=/absolute/path/to/out \
  bash ./sweeps/ray256_hs_isolated.sh

# Check the results
python3 ./sweeps/validate_collector_memory.py <campaign-root> --full   # expect SWEEP-VALID
python3 ./sweeps/validate_hs_sweep.py        <campaign-root>
```

The scripts in [`benchmark/renderers/`](benchmark/renderers) show which number each chart plots and how it was summarized. Personal paths were removed, but some still point at their original working directories, so they will **not** run without the raw campaign output.

---

## Repository layout

```
benchmark/
├── charts/       the 10 images used by slides 17–21
├── data/         the numbers behind every chart
├── renderers/    chart-drawing scripts (provenance; not standalone)
└── harness/      the reproduce code
    ├── README.md         how to run both campaigns
    ├── collector/        test package as it was for the Collector campaign
    └── history-server/   test package as it was for the History Server campaign
```

The two `harness/` folders are snapshots of the *same* test package, each from the checkout that produced that campaign. They overlap heavily but are kept separate, because merging them would blur which code produced which result.

**charts** for the picture, **data** to check a number, **harness** to run it yourself.

---

[Apache 2.0](LICENSE)
