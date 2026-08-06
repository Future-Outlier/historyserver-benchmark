# History Server Benchmark Report

- Date: 2026-08-05T17:56:09-05:00
- Tasks: 1000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_15-56-35_176478_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 25s |
| driver-measured wall | 1.1s |
| driver-measured rate | 920.9 tasks/s |
| flush (cluster deletion incl. final upload) | 2m3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-flxxs | 0 | 0 B | 0 | 0 | 0 |
| raycluster-historyserver-head-wtnjg | 0 | 0 B | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 25 | 73.3 | 0.031 | 0.031 |
| collector (worker) | job | 25 | 63.3 | 0.018 | 0.018 |
| ray-head | job | 25 | 2009.2 | 0.172 | 0.241 |
| ray-worker | job | 25 | 421.7 | 0.165 | 0.165 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-flxxs/collector | flush | 1 | 65.5 | 66.6 | 0.065 | 0.065 |  |
| raycluster-historyserver-cpu-worker-flxxs/collector | job | 24 | 62.5 | 64.7 | 0.012 | 0.268 |  |
| raycluster-historyserver-cpu-worker-flxxs/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 68.8 |
| raycluster-historyserver-cpu-worker-flxxs/ray-worker | flush | 1 | 102.6 | 114.4 | 0.101 | 0.101 |  |
| raycluster-historyserver-cpu-worker-flxxs/ray-worker | job | 24 | 559.8 | 588.3 | 0.140 | 2.036 |  |
| raycluster-historyserver-cpu-worker-flxxs/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 624.9 |
| raycluster-historyserver-head-wtnjg/collector | flush | 1 | 73.5 | 76.7 | 0.074 | 0.074 |  |
| raycluster-historyserver-head-wtnjg/collector | job | 24 | 72.4 | 75.2 | 0.017 | 0.357 |  |
| raycluster-historyserver-head-wtnjg/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 77.1 |
| raycluster-historyserver-head-wtnjg/ray-head | flush | 1 | 1547.8 | 1594.9 | 0.208 | 0.208 |  |
| raycluster-historyserver-head-wtnjg/ray-head | job | 24 | 2097.0 | 2155.6 | 0.165 | 1.021 |  |
| raycluster-historyserver-head-wtnjg/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2159.3 |

## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| **total** | **0 B** | (0 objects) |

- Session marker present: false

## Event statistics

| metric | value |
|---|---|
| total events | 0 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 0 |
| events per task (k) | 0.00 |
| raw JSONL bytes | 0 B |
| stored event bytes | 0 B |
| avg raw bytes/event | 0 |
| compression ratio (stored/raw) | 0.000 |
| distinct taskDefinitionEvent taskIds | 0 / 0 expected |

| event type | count |
|---|---|

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 0s / 0s / 0s (errors: 0) |
| /enter_cluster cold load | 0s (HTTP 0) |

