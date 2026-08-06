# History Server Benchmark Report

- Date: 2026-08-06T15:19:02-05:00
- Tasks: 200000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_13-19-04_047897_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 2m10s |
| driver-measured wall | 69.3s |
| driver-measured rate | 2885.3 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-sxjgp | 2 | 252.78 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-m74h4 | 7 | 318.92 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 130 | 146.4 | 0.516 | 0.877 |
| collector (worker) | job | 130 | 120.1 | 0.398 | 0.711 |
| historyserver | historyserver | 149 | 2723.2 | 1.227 | 1.978 |
| ray-head | job | 130 | 4585.7 | 0.808 | 1.199 |
| ray-worker | job | 130 | 1132.1 | 1.202 | 2.061 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7b56559c6b-mbl9k/historyserver | historyserver | 143 | 2263.4 | 2875.2 | 1.277 | 3.283 |  |
| historyserver-demo-7b56559c6b-mbl9k/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2907.6 |
| raycluster-historyserver-cpu-worker-sxjgp/collector | flush | 1 | 105.9 | 107.2 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-sxjgp/collector | job | 124 | 118.5 | 260.0 | 0.394 | 1.125 |  |
| raycluster-historyserver-cpu-worker-sxjgp/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 261.8 |
| raycluster-historyserver-cpu-worker-sxjgp/ray-worker | job | 124 | 1108.4 | 1155.0 | 1.161 | 2.106 |  |
| raycluster-historyserver-cpu-worker-sxjgp/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1157.0 |
| raycluster-historyserver-head-m74h4/collector | flush | 1 | 132.6 | 133.8 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-m74h4/collector | job | 124 | 145.5 | 305.9 | 0.506 | 1.634 |  |
| raycluster-historyserver-head-m74h4/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 307.0 |
| raycluster-historyserver-head-m74h4/ray-head | flush | 1 | 2344.0 | 2388.7 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-m74h4/ray-head | job | 124 | 4516.4 | 4592.9 | 0.783 | 1.813 |  |
| raycluster-historyserver-head-m74h4/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 4597.2 |
| rayjob-bench-hpkvh/ray-job-submitter | job | 116 | 98.0 | 108.7 | 0.024 | 0.988 |  |
| rayjob-bench-hpkvh/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 109.6 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 6 | 549.00 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 169 | 31.77 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 571.70 MiB | 98.4% |
| logs | 9.07 MiB | 1.6% |
| node_events | 6.80 KiB | 0.0% |
| **total** | **580.78 MiB** | (176 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 828080 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 828066 |
| events per task (k) | 4.14 |
| raw JSONL bytes | 571.71 MiB |
| stored event bytes | 571.71 MiB |
| avg raw bytes/event | 724 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 187747 |
| distinct taskIds in benchmark job `03000000` | **187743 / 200000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| d92c755dcc4e124835f9ede3793154a7d2a55e851ee628e9ce765e09 | 427947 | 318.92 MiB | 187751 | 9245 | 7412.3 |
| 2bf4004122f20bbc03e6144646ddd30a691f34aea59711837646a092 | 400133 | 252.78 MiB | 200000 | 7323 | 6244.6 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 4 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 187747 |
| TASK_LIFECYCLE_EVENT | 440137 |
| TASK_PROFILE_EVENT | 200178 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 4ms / 91ms / 91ms (errors: 0) |
| /enter_cluster cold load | 1m59.929s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=200000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 1.942s | 2.807s | 2.807s | 550 B | 0 |
| /api/jobs/ | 45ms | 491ms | 491ms | 1.31 KiB | 0 |
| /nodes?view=summary | 49ms | 499ms | 499ms | 2.77 KiB | 0 |
| /events | 50ms | 521ms | 521ms | 5.72 KiB | 0 |

