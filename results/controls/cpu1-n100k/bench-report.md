# History Server Benchmark Report

- Date: 2026-08-06T14:27:20-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_12-27-22_830815_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m20s |
| driver-measured wall | 36.7s |
| driver-measured rate | 2724.2 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-g5sqs | 2 | 126.40 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-w2n88 | 6 | 173.48 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 80 | 136.5 | 0.440 | 0.826 |
| collector (worker) | job | 80 | 117.5 | 0.356 | 0.706 |
| historyserver | historyserver | 98 | 2120.5 | 0.980 | 1.044 |
| ray-head | job | 80 | 3889.0 | 0.734 | 1.246 |
| ray-worker | job | 80 | 1015.2 | 1.102 | 2.049 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-6fddb7cf6c-9cbdx/historyserver | historyserver | 93 | 1738.8 | 2123.0 | 0.981 | 1.049 |  |
| historyserver-demo-6fddb7cf6c-9cbdx/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2123.3 |
| raycluster-historyserver-cpu-worker-g5sqs/collector | flush | 1 | 88.3 | 89.5 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-g5sqs/collector | job | 76 | 121.1 | 242.2 | 0.319 | 2.001 |  |
| raycluster-historyserver-cpu-worker-g5sqs/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 243.7 |
| raycluster-historyserver-cpu-worker-g5sqs/ray-worker | job | 76 | 1017.6 | 1059.9 | 1.016 | 2.097 |  |
| raycluster-historyserver-cpu-worker-g5sqs/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1064.6 |
| raycluster-historyserver-head-w2n88/collector | flush | 1 | 133.8 | 173.5 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-w2n88/collector | job | 76 | 145.8 | 272.8 | 0.439 | 1.529 |  |
| raycluster-historyserver-head-w2n88/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 288.3 |
| raycluster-historyserver-head-w2n88/ray-head | flush | 1 | 1985.7 | 2025.1 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-w2n88/ray-head | job | 76 | 3832.3 | 3899.1 | 0.662 | 1.856 |  |
| raycluster-historyserver-head-w2n88/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3903.3 |
| rayjob-bench-77x8t/ray-job-submitter | job | 70 | 97.8 | 108.5 | 0.040 | 0.997 |  |
| rayjob-bench-77x8t/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 109.9 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 252.56 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 170 | 52.00 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 299.87 MiB | 98.5% |
| logs | 4.68 MiB | 1.5% |
| node_events | 6.55 KiB | 0.0% |
| **total** | **304.56 MiB** | (175 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 432193 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 432180 |
| events per task (k) | 4.32 |
| raw JSONL bytes | 299.88 MiB |
| stored event bytes | 299.88 MiB |
| avg raw bytes/event | 728 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 960c93142de71a3bcdf2996e68ec9645d00906eb875fc4fcec37555c | 232116 | 173.48 MiB | 100008 | 9717 | 7113.7 |
| 06030ca0795ceefa901c333b75292634a210fe463c9195d4358df910 | 200077 | 126.40 MiB | 100000 | 7241 | 6175.1 |

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
| TASK_LIFECYCLE_EVENT | 232077 |
| TASK_PROFILE_EVENT | 100095 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 5ms / 89ms / 89ms (errors: 0) |
| /enter_cluster cold load | 1m11.215s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 2.017s | 2.18s | 2.18s | 536 B | 0 |
| /api/jobs/ | 23ms | 569ms | 569ms | 1.31 KiB | 0 |
| /nodes?view=summary | 69ms | 409ms | 409ms | 2.77 KiB | 0 |
| /events | 62ms | 446ms | 446ms | 3.56 KiB | 0 |

