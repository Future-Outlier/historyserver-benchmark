# The experiment log

Eleven campaigns, 106 runs, in the order they happened — which is also the order
things went wrong. Each stage restates what was believed going in, so you can
read one at a time without holding the previous ten in your head.

The short version: **five hypotheses were killed by the data, and three claims
survived publication before turning out to be wrong.** The numbers matter less
than that chain.

---

## Stage 1 — The original matrix (14 runs)

**Believed going in:** nothing measured yet. The plan was three axes: total tasks
(A), per-node event rate via task concurrency (B), and compression (C).

**Method:** one RayCluster (head with `num-cpus: 0`, one worker) plus one RayJob
submitting N no-op tasks. Full pipeline each time: collector sidecars →
MinIO → deploy History Server → measure the cold load. A: N = 1k/5k/10k/50k/100k.
B: N fixed at 20k, `num_cpus` 0.5/0.2/0.1/0.05. C: A repeated with gzip on.

**Result:**
- 4.36 events per task (1 definition + 2.36 lifecycle + 1 profile), 725 B each
- 3.15 KiB/task of storage raw, 0.29 KiB gzipped — ratio constant at every size
- History Server load looked linear to 50k, and **100k "took 907 seconds"**
- **Axis B failed**: a 10× change in concurrency moved the event rate only from
  4,277 to 4,776/s. Concurrency is not the knob for rate.

**Believed coming out:** the event and storage model. Plus a wrong belief — that
100k load time was measured, and superlinear.

---

## Stage 2 — Reruns (3 runs)

**Believed going in:** an early run reported zero events, which made no sense.

**Method:** the RayCluster CR disappears before its pods finish terminating, and
the collector's final flush happens inside the termination grace period. The
harness was scanning the bucket before that finished. Fixed by waiting for every
Ray pod to be gone *and* the session marker object to appear.

**Result:** the numbers came back. More importantly it established that the flush
and the session marker both happen **only during graceful shutdown**.

**Believed coming out:** same model, now with trustworthy storage numbers.

---

## Stage 3 — The logging A/B (2 runs)

**Believed going in:** the 100k load is slow for an algorithmic reason. A code
read found `storeEvent` logging once per event — 436,000 INFO lines per 100k
load, with no way to disable them (the binary never calls `logrus.SetLevel`).

**Method:** change that one line to `Debugf`, rebuild the image, rerun 50k and
100k unchanged otherwise.

**Result:** 50k went 97.9 s → 85.3 s, a real 13%. **100k still never finished.**

**Believed coming out:** **hypothesis 1 dead.** Logging is real but not the cause.
Something else explains 100k.

---

## Stage 4 — The CPU limit appears (6 runs)

**Believed going in:** the cause is inside the History Server's own code.

**Method:** a code review pointed at `config/historyserver.yaml`: it sets only
`limits.cpu: "500m"`, which also pins requests there. Added `BENCH_HS_CPU_LIMIT`
to rewrite the manifest, then reran 1k through 100k at 4 cores.

**Result:** at 4 cores the per-task cost is 0.61–0.72 ms and flat from 1k to
100k. And the cgroup data showed the container sitting at **0.42–0.50 cores for
the entire load in every earlier run** — saturated, not idle.

**Believed coming out:** the CPU limit is the cause. (Still wrong about *why* —
see stage 5 — and still quoting the 907 s figure.)

---

## Stage 5 — Eighteen controls (18 runs)

**Believed going in:** CPU is the cause, but three competing explanations remain:
Go parallelism, GC pressure, and the per-event logging.

**Method:** one sweep, seven groups, each isolating one variable.

| Group | What it did | What it showed |
|---|---|---|
| A | Force `GOMAXPROCS`: 4 at `500m`, 1 at 4 cores | **The quota is the constraint.** Extra `P`s at `500m` change nothing; one `P` with 4 cores is still 2.5× faster |
| B | Inject `GODEBUG=gctrace=1` | GC is 8% of CPU at `500m`, 1–2% at 4 cores |
| C | CPU = 1 / 2 / 8 / none at 100k | Everything from 2 up looks the same; **no limit at all is not faster** |
| D | `GOGC=400` at `500m`/100k | GC share 6% → 1%, heap 1.2 → 3.5 GB, and **the load still never finished** |
| E | 200k at 4 cores; gzip at 4 cores | **200k also never finished**; gzip costs 21% on the read path |
| F | Bigger submission waves to raise the rate | Failed — the rate did not rise, no loss provoked |
| G | Rotation interval 1 min at 50k | **Zero bytes moved out of the shutdown flush** |

**Result:** three hypotheses died at once. D is the cleanest: removing almost all
GC work does not rescue the configuration.

**Believed coming out:** **hypotheses 2, 3, 4 dead** (parallelism, GC, logging).
The quota is the cause. Also: gzip is not free, and my own rotation
recommendation does not work when the job is shorter than the interval.

---

## Stage 6 — The CPU curve (6 runs)

**Believed going in:** more CPU is better; where does it stop mattering?

**Method:** at 100k tasks, sweep the limit at 1 / 1.5 / 2 / 3.

**Result:** 71.2 → 67.2 → 64.4 → 64.7 s. A plateau starting around 2.

**Believed coming out:** "the plateau starts at 2." Each point was a single run,
which stage 9 later corrects.

---

## Stage 7 — Crossing the runtime knobs (7 runs)

**Believed going in:** the quota dominates, but by how much, and can the Go
runtime be tuned to help?

**Method:** hold `GOMAXPROCS` fixed at 2 and vary only the quota; hold the quota
at 2 and vary `GOMAXPROCS`; and try `GOGC` at 200 and 400.

**Result:**
- With parallelism identical on both sides, 50k went **84.9 s at `500m` to
  34.1 s at 4 cores** — the clean isolation of the quota effect
- `GOMAXPROCS=8` cost 12%; `GOGC=400` cost 10% and 2.5× the memory

**Believed coming out:** set the CPU limit, leave the Go runtime alone.

---

## Stage 8 — The decisive run (4 runs)

**Believed going in:** 100k at `500m` takes about 907 seconds.

**Method:** a review pointed out that every such run recorded `status: 0` — no
request ever returned 200 — and the harness had stored the elapsed *probe budget*
in the latency field. Added `BENCH_HS_ARGS` so `--session-process-timeout` could
be raised past the default 2 minutes, then reran.

**Result:**

| | Default 2-minute timeout | With the flag raised |
|---|---|---:|
| 100k at `500m` | never returned, in any run | **151.3 s** (1.51 ms/task) |
| 50k at `500m` | 85 s | 76.3 s (1.53 ms/task) |
| 200k at 4 cores | never returned | 119.9 s (0.60 ms/task) |

**Believed coming out:** **there was never a cliff.** Cost per task is constant at
each CPU limit, linear from 1k to 200k. The "superlinear blowup" was the server
aborting the load at its timeout, discarding the partial state, and every retry
starting over. The real finding is sharper than the wrong one: **a load that
exceeds the timeout does not degrade, it becomes impossible.**

---

## Stage 9 — One session, replicated (36 runs)

**Believed going in:** the plateau starts at 2 cores.

**Method:** every comparison so far regenerated its own session, so each cell read
different bytes. Added `BENCH_HS_ONLY`: generate one 50k session, keep it, then
point a fresh History Server at those exact bytes. Seven CPU settings, five runs
each, randomized order.

**Result:**

| `limits.cpu` | median | min–max |
|---|---:|---:|
| `500m` | **84.0 s** | 75.3–85.8 |
| `1` | 40.2 s | 37.8–41.2 |
| `1.5` | 37.4 s | 34.2–39.6 |
| `2` | 36.8 s | 31.9–38.5 |
| `4` | 33.3 s | 31.5–36.4 |
| none | 34.3 s | 31.1–36.4 |

**Only `500m` → `1` exceeds the noise.** From 1.5 cores upward nothing is
distinguishable, including removing the limit (which sets `GOMAXPROCS` to the
node's 14 cores and buys nothing).

**Believed coming out:** **hypothesis 5 dead** ("no CPU limit is faster"). And
"the plateau starts at 2" was itself over-specified — the shipped `500m` is
simply the one bad value.

---

## Stage 10 — Rate as an independent variable (6 runs)

**Believed going in:** collector CPU tracks the event rate and its memory is flat.
Axis B in stage 1 could not test this because the rate never moved.

**Method:** added `BENCH_TARGET_TASK_RATE`, which paces the driver, and a sampler
that reads the collector's spool directory size directly — the 503 backpressure
path writes no log, so the existing counter could never observe it. Swept
250 / 500 / 1000 / 2000 / 3000 tasks/s at a fixed 50k.

**Result:**

| events/s at one node | CPU | anon | spool backlog |
|---:|---:|---:|---:|
| 504 | 0.07 cores | 68 MiB | — |
| 2,214 | 0.18 cores | 84 MiB | 83 MiB |
| 6,794 | 0.28 cores | 144 MiB | 89 MiB |

- CPU is linear, converging to ~136 µs per event
- **Memory grows 2× with rate** — it is flat across *session sizes*, not across
  *rates*, because it is one request's transient working set
- Spool backlog settles at 83–103 MiB against a 160 MiB backpressure threshold,
  with MinIO on the same node

**Believed coming out:** the collector model, with memory corrected.

---

## Stage 11 — More drivers (4 runs)

**Believed going in:** one driver tops out near 2,600 tasks/s, so the collector
was never actually stressed. Ray allows many drivers, each with its own event
buffer and its own drain budget.

**Method:** added `BENCH_DRIVERS`, which forks N processes that each call
`ray.init()` — N real Ray drivers sharing the task count, all on the head, all
feeding the same head collector.

**Result:**
- 6 drivers reached 8,896 events/s, only **32% above one driver** — every driver
  process runs on the head, which has 2 CPUs
- Object count went **173 → 450**: each driver is its own Ray job, and the
  collector keeps one active file per job
- Spool backlog rose with driver count, 89 → 103 MiB

**Believed coming out:** the collector's ceiling was never reached, and the thing
that scales with concurrent jobs is file count, not CPU.

---

## What the chain adds up to

Five hypotheses killed by their own experiment:

1. ~~Superlinear algorithm past 50k~~ → stage 8
2. ~~Per-event logging~~ → stage 3
3. ~~Insufficient Go parallelism~~ → stage 5A
4. ~~GC pressure~~ → stage 5D
5. ~~"No CPU limit is faster"~~ → stage 9

Three claims that survived publication before turning out to be wrong:

- **"907 seconds"** was a probe budget, not a measurement
- **"gzip costs nothing"** was measured while the server was CPU-starved
- **"shorten the rotation interval"** does nothing when the job is shorter than
  the interval

And the one finding that outlives all the tuning numbers: **a session load that
exceeds `--session-process-timeout` never completes, because nothing is cached
and every retry starts from zero.**
