# Ray Summit 2026 History Server benchmarks

This repository is the publication bundle for benchmark material used by slides 16–22 of **Ray History Server: Bridging the Impossible Gaps in Observability**.

## What is included

- `docs/charts/`: the ten PNG charts currently used by slides 17–21.
- `tools/`: the chart renderer sources used to produce the published assets, with workstation-specific paths removed.
- `results/`: compact derived CSV/JSON evidence, validation records, and provenance manifests.
- `benchmark/collector/`: the selected formal Collector memory benchmark harness snapshot.
- `benchmark/history-server/`: the selected formal isolated History Server benchmark harness snapshot.
- `MANIFEST.sha256`: SHA-256 for every published file except the manifest itself.

Slides 16 and 22 use native Google Slides shapes and text, so they do not have benchmark chart files in this bundle. Deck backgrounds and the Anyscale logo are intentionally excluded.

## What this shows (60-second version)

Two components need different sizing rules:

1. **Collector:** CPU grows with per-Collector event traffic. Whole-container memory grows with the event bytes retained locally until rotation, compression, upload, and deletion. The isolated Worker-mode soak used 5,000 events/s, 895 bytes/event, 90 seconds, and three fresh Pods. A 192 MiB limit caused memory pressure; 256 MiB ran without OOM but exceeded the predefined 80% usage guardrail; 512 MiB passed 3/3. Head Collector endpoint polling was not covered by that limit sweep.
2. **History Server:** cold-load time and memory grow with session task count. The isolated campaign tested 1k, 5k, 10k, and 50k tasks with five fresh Pods per size. At 50k tasks, whole-container lifetime peaks ranged from 1,279 to 1,554 MiB, with a 1,368 MiB median, under a roomy 12 GiB test limit. That 12 GiB value was measurement headroom, not a constrained-limit result.
3. **Task-to-event context:** a separate Ray 2.56 no-op workload observed about 4.4 task-scoped events per task. This is workload- and version-specific, so Collector sizing should still use the busiest Collector's measured event/s and bytes/s.
4. **Slide 22:** the YAML is a benchmark-derived starting policy, not a set of universal defaults. Inline comments identify the Worker Collector's first tested passing limit separately from policy-chosen requests and ceilings.

The presentation sequence is intentional: Slide 16 defines the workloads; Slides 17–19 explain Collector scaling and lifecycle; Slides 20–21 explain History Server scaling and lifecycle; Slide 22 converts the evidence into a deployable starting point.

## Slide-to-chart mapping

| Slide | Chart |
|---|---|
| 17 | `docs/charts/collector/slide17-collector-cpu-scaling.png` |
| 17 | `docs/charts/collector/slide17-collector-memory-scaling.png` |
| 18 | `docs/charts/collector/slide18-collector-cpu-lifecycle-5k.png` |
| 18 | `docs/charts/collector/slide18-collector-memory-lifecycle-5k.png` |
| 19 | `docs/charts/collector/slide19-collector-head-worker-cpu-lifecycle.png` |
| 19 | `docs/charts/collector/slide19-collector-head-worker-memory-lifecycle.png` |
| 20 | `docs/charts/history-server/slide20-history-server-cold-load-scaling.png` |
| 20 | `docs/charts/history-server/slide20-history-server-memory-scaling.png` |
| 21 | `docs/charts/history-server/slide21-history-server-cpu-lifecycle.png` |
| 21 | `docs/charts/history-server/slide21-history-server-memory-lifecycle.png` |

## Evidence boundary

- Collector one-sidecar charts use the formal r7 isolated Collector campaign.
- Collector head/worker lifecycle charts use the earlier 50k no-op task campaign.
- History Server charts use the fixed-profile isolated campaign: 1 CPU / 1 GiB request, 2 CPU / 12 GiB limit, five fresh arms per task count.
- Raw campaign directories are intentionally excluded. The Collector raw formal output was about 58 MiB; the History Server raw campaign was about 8.9 GiB. This repository bundle keeps compact derived evidence instead of runtime logs, binaries, kubeconfigs, or object-store payloads.

## Reproduction note

The renderer logic is preserved for provenance, with personal workstation paths removed. Some renderers still refer to the original `/private/tmp/...` evidence roots and therefore are not standalone without the corresponding raw campaigns. Use the bundled derived data for audit and the benchmark snapshots to reproduce the campaigns in a compatible KubeRay checkout.

## Excluded files

The publication deliberately excludes `out/`, `__pycache__/`, `*.pyc`, logs, binaries, kubeconfigs, PDFs, contact sheets, pre-font-enlarge backups, and other QA-only images not used by slides 16–22.
