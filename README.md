# KubeRay History Server Benchmark

End-to-end sizing measurements for the [KubeRay](https://github.com/ray-project/kuberay)
History Server data path — Ray event aggregator → `collector` sidecar → object
storage → History Server — on a real cluster running a real Ray job.

Every number here was produced by the harness in [`benchmark/`](benchmark/) and is
reproducible with one script. Raw per-run reports are in [`results/`](results/).

## The three questions this answers

| Question | Answer |
|---|---|
| How much CPU/memory does the `collector` sidecar need? | `requests: 200m / 256Mi`, `limits: 2 / 512Mi` covers up to ~1,600 tasks/s **per node**. Memory is flat in job size; CPU tracks the node's event *rate*, not the total. |
| How much does gzip save? | **91%** — 3.15 KiB/task raw → 0.29 KiB/task compressed, constant from 1k to 100k tasks. It is off by default. |
| How long does the History Server take to load N tasks? | **~2 ms per task** up to 50k (50k = 98 s), then it degrades: 100k took **907 s** and never returned inside any default timeout. |

## Headline model

```
events   E ≈ N × 4.36              (1 definition + 2.36 lifecycle + 1 profile per task)
storage  S ≈ N × 3.15 KiB          raw          → 91% smaller with gzip: N × 0.29 KiB
HS load  T ≈ N × 2 ms              up to N=50k  → superlinear beyond (100k measured 4.6× the linear prediction)
HS RSS   M ≈ 100 MiB + N × 23 KiB  (50k ≈ 1.1 GiB, 100k ≈ 2.2 GiB)
```

`N` = tasks in the session. Ray actor calls are task events too, so count them in `N`.

## Read next

| Doc | Contents |
|---|---|
| [docs/DIMENSIONS.md](docs/DIMENSIONS.md) | What was varied and why — the data path and the three test axes |
| [docs/RESULTS.md](docs/RESULTS.md) | All measurements, charts, and what each one means |
| [docs/SIZING.md](docs/SIZING.md) | **Decide your own resource requests** — formulas + worked examples |
| [docs/FINDINGS.md](docs/FINDINGS.md) | Bugs and design limits found while benchmarking, with source references |
| [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md) | Hardware, versions, image digests, cluster topology |
| [benchmark/README.md](benchmark/README.md) | Harness internals and every knob |

## Reproduce

The harness is a Go test that lives inside a KubeRay checkout (it reuses the
History Server e2e support package). `install.sh` copies it into place.

```bash
git clone https://github.com/ray-project/kuberay.git
./benchmark/install.sh /path/to/kuberay        # copies harness into historyserver/test/benchmark
cd /path/to/kuberay/historyserver/test/benchmark
./run_matrix.sh                                # builds images, creates a dedicated kind cluster, runs the matrix
```

The full matrix is 14 runs and takes roughly 2.5 hours. A single point:

```bash
cd /path/to/kuberay/historyserver
BENCH_RUN=1 BENCH_TASK_COUNT=10000 go test ./test/benchmark -run TestHistoryServerBenchmark -v -timeout 90m
```

Then regenerate the derived table:

```bash
python3 tools/derive.py results > results/derived.csv
```

## Scope and honesty notes

- Measured on **kind on macOS** (Docker Desktop VM). Byte counts, event counts,
  ratios, and scaling *shapes* port anywhere. Absolute CPU seconds do not —
  treat CPU as relative until re-measured on bare-metal Linux.
- Tasks are no-ops (`num_cpus=0.2`, no payload). Real tasks emit the same event
  count but larger logs; the `logs/` category here is only ~46 B/task.
- One RayCluster, one head + one worker node. Per-node numbers are what
  generalize; cluster totals are the sum over nodes.
- Two runs in `results/matrix-20260805/` are known-bad and kept on purpose:
  `A-n1000` (flush race in an early harness version, replaced by
  `rerun-A-n1000`) and the `collector_log_capture=no` runs, where collector log
  counters read zero because nothing was captured, not because nothing happened.
  [docs/RESULTS.md](docs/RESULTS.md) says which numbers are trustworthy.

## License

Apache 2.0, matching KubeRay. The harness derives from KubeRay's e2e test support code.
