# History Server Benchmark Report

- Date: 2026-08-06T12:50:39-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_10-50-41_862290_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m20s |
| driver-measured wall | 34.7s |
| driver-measured rate | 2880.4 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-wqpg2 | 2 | 126.39 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-phv49 | 6 | 174.48 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 87.1 | 0.000 | 0.000 |
| collector (head) | job | 80 | 132.4 | 0.444 | 0.774 |
| collector (worker) | job | 80 | 111.8 | 0.340 | 0.582 |
| historyserver | historyserver | 80 | 2021.2 | 1.243 | 1.997 |
| ray-head | flush | 1 | 1728.7 | 0.000 | 0.000 |
| ray-head | job | 80 | 3626.2 | 0.705 | 1.247 |
| ray-worker | job | 80 | 1034.2 | 1.085 | 2.074 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-5f6c7748f6-r9htr/historyserver | historyserver | 76 | 1640.0 | 2128.4 | 1.247 | 3.069 |  |
| historyserver-demo-5f6c7748f6-r9htr/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2132.9 |
| raycluster-historyserver-cpu-worker-wqpg2/collector | job | 76 | 116.0 | 245.2 | 0.320 | 1.397 |  |
| raycluster-historyserver-cpu-worker-wqpg2/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 246.6 |
| raycluster-historyserver-cpu-worker-wqpg2/ray-worker | job | 76 | 1017.5 | 1060.2 | 0.970 | 2.494 |  |
| raycluster-historyserver-cpu-worker-wqpg2/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1064.3 |
| raycluster-historyserver-head-phv49/collector | flush | 1 | 104.0 | 150.9 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-phv49/collector | job | 76 | 132.8 | 274.7 | 0.440 | 1.454 |  |
| raycluster-historyserver-head-phv49/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 296.3 |
| raycluster-historyserver-head-phv49/ray-head | flush | 1 | 1695.6 | 1736.2 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-phv49/ray-head | job | 76 | 3581.6 | 3646.4 | 0.659 | 1.834 |  |
| raycluster-historyserver-head-phv49/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3651.4 |
| rayjob-bench-4zc4x/ray-job-submitter | job | 69 | 97.6 | 108.1 | 0.038 | 0.984 |  |
| rayjob-bench-4zc4x/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.0 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 254.81 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 170 | 50.75 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 300.86 MiB | 98.5% |
| logs | 4.68 MiB | 1.5% |
| node_events | 6.91 KiB | 0.0% |
| **total** | **305.55 MiB** | (175 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 434275 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 434261 |
| events per task (k) | 4.34 |
| raw JSONL bytes | 300.87 MiB |
| stored event bytes | 300.87 MiB |
| avg raw bytes/event | 726 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| a56f6b35d5d1f86c6832db9a9686e9a4f1d8a2bf69f74140dc1e0fd3 | 234209 | 174.48 MiB | 100008 | 9864 | 7538.4 |
| dc04639bf6f16bb9821dd5be2de25d1915cdc2698e3a720be2c10c5a | 200066 | 126.39 MiB | 100000 | 7277 | 6466.5 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 3 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 100005 |
| TASK_LIFECYCLE_EVENT | 234163 |
| TASK_PROFILE_EVENT | 100090 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 8ms / 99ms / 99ms (errors: 0) |
| /enter_cluster cold load | 1m6.319s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 808ms | 1.177s | 1.177s | 536 B | 0 |
| /api/jobs/ | 16ms | 235ms | 235ms | 1.31 KiB | 0 |
| /nodes?view=summary | 20ms | 226ms | 226ms | 2.77 KiB | 0 |
| /events | 27ms | 368ms | 368ms | 3.20 KiB | 0 |

