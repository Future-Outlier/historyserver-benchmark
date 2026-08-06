# History Server Benchmark Report

- Date: 2026-08-06T15:07:24-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_13-07-26_544553_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m20s |
| driver-measured wall | 38.7s |
| driver-measured rate | 2584.7 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-4cmcn | 2 | 126.40 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-bb29z | 6 | 173.09 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 125.6 | 0.000 | 0.000 |
| collector (head) | job | 80 | 149.2 | 0.440 | 0.782 |
| collector (worker) | job | 80 | 119.8 | 0.396 | 0.495 |
| historyserver | historyserver | 210 | 2070.5 | 0.497 | 0.514 |
| ray-head | job | 80 | 4316.9 | 0.634 | 1.166 |
| ray-worker | job | 80 | 1039.4 | 1.244 | 2.025 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-99dd5848f-h55w2/historyserver | historyserver | 202 | 1761.9 | 2120.9 | 0.497 | 0.529 |  |
| historyserver-demo-99dd5848f-h55w2/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2121.4 |
| raycluster-historyserver-cpu-worker-4cmcn/collector | job | 76 | 118.4 | 228.3 | 0.324 | 1.657 |  |
| raycluster-historyserver-cpu-worker-4cmcn/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 242.1 |
| raycluster-historyserver-cpu-worker-4cmcn/ray-worker | job | 76 | 1013.6 | 1055.7 | 1.052 | 2.200 |  |
| raycluster-historyserver-cpu-worker-4cmcn/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1059.9 |
| raycluster-historyserver-head-bb29z/collector | flush | 1 | 127.5 | 181.9 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-bb29z/collector | job | 76 | 146.3 | 270.9 | 0.454 | 2.098 |  |
| raycluster-historyserver-head-bb29z/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 272.4 |
| raycluster-historyserver-head-bb29z/ray-head | flush | 1 | 2069.7 | 2115.5 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-bb29z/ray-head | job | 76 | 4265.9 | 4336.6 | 0.675 | 2.114 |  |
| raycluster-historyserver-head-bb29z/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 4341.6 |
| rayjob-bench-52rpf/ray-job-submitter | job | 72 | 98.0 | 108.7 | 0.039 | 0.984 |  |
| rayjob-bench-52rpf/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.2 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 234.46 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 170 | 69.71 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 299.49 MiB | 98.5% |
| logs | 4.68 MiB | 1.5% |
| node_events | 6.44 KiB | 0.0% |
| **total** | **304.18 MiB** | (175 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 431384 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 431371 |
| events per task (k) | 4.31 |
| raw JSONL bytes | 299.49 MiB |
| stored event bytes | 299.49 MiB |
| avg raw bytes/event | 728 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 0b1564f77bbe97bffbb3ac18f0b2f6b9e138218ec251b0e300ce1609 | 231298 | 173.09 MiB | 100008 | 9555 | 6797.3 |
| 7049311ba5d13aeacfcc1e05f2f7d72ead810ba9db711549b2ada9bd | 200086 | 126.40 MiB | 100000 | 6658 | 5860.3 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 4 |
| ACTOR_TASK_DEFINITION_EVENT | 3 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 100005 |
| TASK_LIFECYCLE_EVENT | 231260 |
| TASK_PROFILE_EVENT | 100103 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 4ms / 132ms / 132ms (errors: 0) |
| /enter_cluster cold load | 2m31.331s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 4.693s | 5.188s | 5.188s | 536 B | 0 |
| /api/jobs/ | 73ms | 1.022s | 1.022s | 1.31 KiB | 0 |
| /nodes?view=summary | 89ms | 1.101s | 1.101s | 2.77 KiB | 0 |
| /events | 91ms | 923ms | 923ms | 3.56 KiB | 0 |

