# History Server Benchmark Report

- Date: 2026-08-06T16:35:49-05:00
- Tasks: 50000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_14-35-51_963761_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m0s |
| driver-measured wall | 20.9s |
| driver-measured rate | 2395.8 tasks/s |
| flush (cluster deletion incl. final upload) | 3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-tdj7c | 1 | 63.20 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-nbgjc | 5 | 87.19 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 144.2 | 0.000 | 0.000 |
| collector (head) | job | 60 | 144.2 | 0.356 | 0.648 |
| collector (worker) | flush | 1 | 113.8 | 0.000 | 0.000 |
| collector (worker) | job | 60 | 113.8 | 0.249 | 0.517 |
| historyserver | historyserver | 39 | 1109.0 | 1.027 | 1.705 |
| ray-head | job | 60 | 4056.7 | 0.527 | 1.114 |
| ray-worker | job | 60 | 961.6 | 1.084 | 1.876 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7768d9678d-hkzr2/historyserver | historyserver | 37 | 1189.6 | 1192.7 | 1.108 | 2.033 |  |
| historyserver-demo-7768d9678d-hkzr2/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1192.9 |
| raycluster-historyserver-cpu-worker-tdj7c/collector | flush | 1 | 115.4 | 182.0 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-tdj7c/collector | job | 57 | 114.5 | 177.0 | 0.206 | 0.967 |  |
| raycluster-historyserver-cpu-worker-tdj7c/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 182.2 |
| raycluster-historyserver-cpu-worker-tdj7c/ray-worker | job | 57 | 967.4 | 1007.3 | 0.774 | 2.112 |  |
| raycluster-historyserver-cpu-worker-tdj7c/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1011.4 |
| raycluster-historyserver-head-nbgjc/collector | flush | 2 | 122.2 | 218.7 | 0.826 | 0.826 |  |
| raycluster-historyserver-head-nbgjc/collector | job | 57 | 144.0 | 231.5 | 0.300 | 1.053 |  |
| raycluster-historyserver-head-nbgjc/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 232.6 |
| raycluster-historyserver-head-nbgjc/ray-head | flush | 1 | 2347.8 | 2389.5 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-nbgjc/ray-head | job | 57 | 4023.8 | 4090.2 | 0.509 | 1.926 |  |
| raycluster-historyserver-head-nbgjc/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 4092.9 |
| rayjob-bench-89x7h/ray-job-submitter | job | 52 | 97.8 | 108.3 | 0.054 | 0.989 |  |
| rayjob-bench-89x7h/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 109.8 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 152.91 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 150.39 MiB | 98.4% |
| logs | 2.51 MiB | 1.6% |
| node_events | 6.55 KiB | 0.0% |
| **total** | **152.91 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 217046 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 217033 |
| events per task (k) | 4.34 |
| raw JSONL bytes | 150.40 MiB |
| stored event bytes | 150.40 MiB |
| avg raw bytes/event | 727 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 50005 |
| distinct taskIds in benchmark job `03000000` | **50001 / 50000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 233e17c28342cf36fd1d41ccb4c405d03155629dd63bee783cf1378e | 116998 | 87.19 MiB | 50007 | 9544 | 6575.3 |
| 9431d235d70e6a97efd532473fb6b5e7fab758f46861051c96d90352 | 100048 | 63.20 MiB | 50000 | 6280 | 5483.8 |

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
| TASK_LIFECYCLE_EVENT | 116968 |
| TASK_PROFILE_EVENT | 50058 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 6ms / 84ms / 84ms (errors: 0) |
| /enter_cluster cold load | 33.995s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 3 |
| /api/v0/tasks/summarize | 417ms | 555ms | 555ms | 408 B | 0 |
| /api/jobs/ | 9ms | 15ms | 15ms | 1.31 KiB | 0 |
| /nodes?view=summary | 13ms | 14ms | 14ms | 2.77 KiB | 0 |
| /events | 123ms | 161ms | 161ms | 2.48 KiB | 0 |

