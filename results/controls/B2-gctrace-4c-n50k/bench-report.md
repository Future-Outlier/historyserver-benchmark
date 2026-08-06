# History Server Benchmark Report

- Date: 2026-08-06T12:33:21-05:00
- Tasks: 50000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_10-33-23_047625_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 50s |
| driver-measured wall | 18.3s |
| driver-measured rate | 2733.1 tasks/s |
| flush (cluster deletion incl. final upload) | 3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-l5mm6 | 1 | 63.20 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-2r25k | 5 | 88.35 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 117.0 | 0.000 | 0.000 |
| collector (head) | job | 50 | 130.0 | 0.285 | 0.661 |
| collector (worker) | flush | 1 | 110.4 | 0.000 | 0.000 |
| collector (worker) | job | 50 | 110.4 | 0.294 | 0.405 |
| historyserver | historyserver | 43 | 991.2 | 1.074 | 1.884 |
| ray-head | job | 50 | 3074.7 | 0.543 | 1.098 |
| ray-worker | job | 50 | 979.6 | 0.871 | 1.446 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7fb85b6799-vvlnt/historyserver | historyserver | 42 | 1111.1 | 1113.9 | 1.179 | 2.146 |  |
| historyserver-demo-7fb85b6799-vvlnt/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1115.6 |
| raycluster-historyserver-cpu-worker-l5mm6/collector | flush | 1 | 109.9 | 176.0 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-l5mm6/collector | job | 48 | 121.1 | 173.6 | 0.237 | 0.993 |  |
| raycluster-historyserver-cpu-worker-l5mm6/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 177.9 |
| raycluster-historyserver-cpu-worker-l5mm6/ray-worker | job | 48 | 975.1 | 1014.9 | 0.826 | 2.093 |  |
| raycluster-historyserver-cpu-worker-l5mm6/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1019.2 |
| raycluster-historyserver-head-2r25k/collector | flush | 1 | 117.7 | 209.8 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-2r25k/collector | job | 48 | 141.3 | 227.4 | 0.334 | 1.128 |  |
| raycluster-historyserver-head-2r25k/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 228.3 |
| raycluster-historyserver-head-2r25k/ray-head | job | 48 | 3030.1 | 3095.8 | 0.568 | 2.064 |  |
| raycluster-historyserver-head-2r25k/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3102.5 |
| rayjob-bench-xsv85/ray-job-submitter | job | 44 | 97.6 | 108.1 | 0.072 | 0.993 |  |
| rayjob-bench-xsv85/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.2 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 154.05 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 151.54 MiB | 98.4% |
| logs | 2.50 MiB | 1.6% |
| node_events | 6.55 KiB | 0.0% |
| **total** | **154.05 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 219467 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 219454 |
| events per task (k) | 4.39 |
| raw JSONL bytes | 151.55 MiB |
| stored event bytes | 151.55 MiB |
| avg raw bytes/event | 724 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 50005 |
| distinct taskIds in benchmark job `03000000` | **50001 / 50000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 3096bd46c9babd3cd98ceca3a9a09333c33fcc95939a4933e654ace4 | 119431 | 88.35 MiB | 50007 | 9908 | 7087.4 |
| 4d09d158b6fca4c3c7c2c706be0a1187807be7b76a2c828759abc3f5 | 100036 | 63.20 MiB | 50000 | 7317 | 6036.9 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 4 |
| ACTOR_TASK_DEFINITION_EVENT | 2 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 50005 |
| TASK_LIFECYCLE_EVENT | 119395 |
| TASK_PROFILE_EVENT | 50052 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 5ms / 96ms / 96ms (errors: 0) |
| /enter_cluster cold load | 34.945s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 410ms | 590ms | 590ms | 408 B | 0 |
| /api/jobs/ | 13ms | 104ms | 104ms | 1.31 KiB | 0 |
| /nodes?view=summary | 14ms | 114ms | 114ms | 2.77 KiB | 0 |
| /events | 13ms | 113ms | 113ms | 2.12 KiB | 0 |

