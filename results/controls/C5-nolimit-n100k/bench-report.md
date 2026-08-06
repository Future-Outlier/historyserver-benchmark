# History Server Benchmark Report

- Date: 2026-08-06T12:47:29-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_10-47-31_092669_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m20s |
| driver-measured wall | 34.8s |
| driver-measured rate | 2872.5 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-m4smw | 1 | 126.40 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-8f76s | 6 | 174.68 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 80 | 146.7 | 0.460 | 0.764 |
| collector (worker) | job | 80 | 116.6 | 0.344 | 0.632 |
| historyserver | historyserver | 79 | 1965.5 | 1.137 | 1.469 |
| ray-head | flush | 1 | 3214.7 | 0.000 | 0.000 |
| ray-head | job | 80 | 3531.2 | 0.630 | 1.251 |
| ray-worker | job | 80 | 1049.0 | 1.014 | 1.994 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-74d9d85d4d-qbj5k/historyserver | historyserver | 75 | 1674.7 | 1993.5 | 1.226 | 2.958 |  |
| historyserver-demo-74d9d85d4d-qbj5k/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1996.2 |
| raycluster-historyserver-cpu-worker-m4smw/collector | flush | 1 | 110.3 | 111.4 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-m4smw/collector | job | 76 | 116.5 | 240.4 | 0.326 | 1.095 |  |
| raycluster-historyserver-cpu-worker-m4smw/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 245.9 |
| raycluster-historyserver-cpu-worker-m4smw/ray-worker | flush | 1 | 102.7 | 119.9 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-m4smw/ray-worker | job | 76 | 1024.0 | 1066.0 | 0.974 | 2.092 |  |
| raycluster-historyserver-cpu-worker-m4smw/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1076.8 |
| raycluster-historyserver-head-8f76s/collector | flush | 1 | 106.2 | 143.2 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-8f76s/collector | job | 76 | 145.9 | 290.1 | 0.447 | 2.034 |  |
| raycluster-historyserver-head-8f76s/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 301.4 |
| raycluster-historyserver-head-8f76s/ray-head | flush | 1 | 2827.3 | 2889.9 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-8f76s/ray-head | job | 76 | 3523.2 | 3590.1 | 0.658 | 2.098 |  |
| raycluster-historyserver-head-8f76s/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3595.3 |
| rayjob-bench-cpnbk/ray-job-submitter | job | 69 | 97.6 | 108.1 | 0.038 | 0.985 |  |
| rayjob-bench-cpnbk/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.4 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 266.14 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 164 | 39.61 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 301.07 MiB | 98.5% |
| logs | 4.67 MiB | 1.5% |
| node_events | 6.91 KiB | 0.0% |
| **total** | **305.75 MiB** | (169 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 434712 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 434698 |
| events per task (k) | 4.35 |
| raw JSONL bytes | 301.08 MiB |
| stored event bytes | 301.08 MiB |
| avg raw bytes/event | 726 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| f5ae2770e39f8a33cd5c3eb048b2032e4fe4b2cc19573330bb1cc76d | 234633 | 174.68 MiB | 100008 | 9398 | 7397.3 |
| 195488bf01a6ad1542889aaea39dd2465ed9dc85b291e1d7cb8fb104 | 200079 | 126.40 MiB | 100000 | 7161 | 6321.9 |

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
| TASK_LIFECYCLE_EVENT | 234596 |
| TASK_PROFILE_EVENT | 100094 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 100ms / 100ms (errors: 0) |
| /enter_cluster cold load | 1m6.84s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 854ms | 897ms | 897ms | 536 B | 0 |
| /api/jobs/ | 18ms | 250ms | 250ms | 1.31 KiB | 0 |
| /nodes?view=summary | 18ms | 250ms | 250ms | 2.77 KiB | 0 |
| /events | 17ms | 230ms | 230ms | 3.21 KiB | 0 |

