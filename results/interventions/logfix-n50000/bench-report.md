# History Server Benchmark Report

- Date: 2026-08-05T19:46:47-05:00
- Tasks: 50000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_17-46-49_631432_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 55s |
| driver-measured wall | 20.4s |
| driver-measured rate | 2457.2 tasks/s |
| flush (cluster deletion incl. final upload) | 3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-dbzjh | 1 | 63.15 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-7jvss | 5 | 88.14 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 130.9 | 0.000 | 0.000 |
| collector (head) | job | 55 | 131.2 | 0.350 | 0.674 |
| collector (worker) | flush | 1 | 110.2 | 0.000 | 0.000 |
| collector (worker) | job | 55 | 110.2 | 0.270 | 0.386 |
| historyserver | historyserver | 112 | 1148.9 | 0.492 | 0.529 |
| ray-head | job | 55 | 3592.8 | 0.594 | 1.290 |
| ray-worker | job | 55 | 982.4 | 0.920 | 1.813 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-l2x5z/historyserver | historyserver | 107 | 1148.6 | 1151.5 | 0.490 | 0.527 |  |
| historyserver-demo-7f67bfd478-l2x5z/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1152.2 |
| raycluster-historyserver-cpu-worker-dbzjh/collector | flush | 1 | 109.7 | 175.7 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-dbzjh/collector | job | 53 | 115.0 | 173.4 | 0.222 | 1.113 |  |
| raycluster-historyserver-cpu-worker-dbzjh/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 179.4 |
| raycluster-historyserver-cpu-worker-dbzjh/ray-worker | job | 53 | 976.1 | 1013.7 | 0.826 | 2.098 |  |
| raycluster-historyserver-cpu-worker-dbzjh/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1016.3 |
| raycluster-historyserver-head-7jvss/collector | flush | 1 | 127.9 | 219.8 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-7jvss/collector | job | 53 | 127.7 | 219.1 | 0.313 | 1.107 |  |
| raycluster-historyserver-head-7jvss/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 221.1 |
| raycluster-historyserver-head-7jvss/ray-head | job | 53 | 3541.9 | 3606.1 | 0.560 | 2.012 |  |
| raycluster-historyserver-head-7jvss/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3610.6 |
| rayjob-bench-5zfsq/ray-job-submitter | job | 46 | 98.0 | 108.7 | 0.061 | 0.998 |  |
| rayjob-bench-5zfsq/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.1 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 153.80 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 151.28 MiB | 98.4% |
| logs | 2.51 MiB | 1.6% |
| node_events | 6.06 KiB | 0.0% |
| **total** | **153.80 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 219020 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 219008 |
| events per task (k) | 4.38 |
| raw JSONL bytes | 151.29 MiB |
| stored event bytes | 151.29 MiB |
| avg raw bytes/event | 724 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 50005 |
| distinct taskIds in benchmark job `03000000` | **50001 / 50000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 5cf6ae3c5b50ec275a35c0b2ac2ce29fd117cc706beec4ed7251bda7 | 118973 | 88.14 MiB | 50007 | 9559 | 6708.5 |
| 78f2be3796b2d2ace2d8162bdfe513935578a3826dfb2a3535de149c | 100047 | 63.15 MiB | 50000 | 6956 | 5778.9 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 3 |
| ACTOR_TASK_DEFINITION_EVENT | 2 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 50005 |
| TASK_LIFECYCLE_EVENT | 118942 |
| TASK_PROFILE_EVENT | 50059 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 189ms / 189ms (errors: 0) |
| /enter_cluster cold load | 1m25.301s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 2.004s | 3.26s | 3.26s | 408 B | 0 |
| /api/jobs/ | 43ms | 432ms | 432ms | 1.31 KiB | 0 |
| /nodes?view=summary | 82ms | 479ms | 479ms | 2.76 KiB | 0 |
| /events | 74ms | 485ms | 485ms | 2.12 KiB | 0 |

