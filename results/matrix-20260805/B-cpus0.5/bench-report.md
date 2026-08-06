# History Server Benchmark Report

- Date: 2026-08-05T18:12:50-05:00
- Tasks: 20000 (wave 2000, num_cpus=0.5), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_16-12-52_600292_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 30s |
| driver-measured wall | 6.9s |
| driver-measured rate | 2893.3 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-s825z | 0 | 0 B | 0 | 0 | 0 |
| raycluster-historyserver-head-dnd8p | 0 | 0 B | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 30 | 125.0 | 0.189 | 0.273 |
| collector (worker) | job | 30 | 96.3 | 0.193 | 0.438 |
| historyserver | historyserver | 44 | 497.9 | 0.506 | 0.520 |
| ray-head | job | 30 | 2671.8 | 0.510 | 0.698 |
| ray-worker | job | 30 | 565.7 | 0.643 | 2.004 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-gphxc/historyserver | historyserver | 43 | 467.0 | 543.4 | 0.494 | 0.534 |  |
| historyserver-demo-7f67bfd478-gphxc/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 545.2 |
| raycluster-historyserver-cpu-worker-s825z/collector | flush | 1 | 94.9 | 123.9 | 0.442 | 0.442 |  |
| raycluster-historyserver-cpu-worker-s825z/collector | job | 29 | 108.8 | 132.5 | 0.165 | 0.947 |  |
| raycluster-historyserver-cpu-worker-s825z/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 145.8 |
| raycluster-historyserver-cpu-worker-s825z/ray-worker | job | 29 | 610.4 | 637.6 | 0.529 | 2.102 |  |
| raycluster-historyserver-cpu-worker-s825z/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 641.9 |
| raycluster-historyserver-head-dnd8p/collector | flush | 1 | 126.4 | 163.8 | 0.483 | 0.483 |  |
| raycluster-historyserver-head-dnd8p/collector | job | 29 | 145.2 | 175.8 | 0.231 | 1.152 |  |
| raycluster-historyserver-head-dnd8p/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 177.1 |
| raycluster-historyserver-head-dnd8p/ray-head | job | 29 | 2733.6 | 2795.5 | 0.510 | 1.319 |  |
| raycluster-historyserver-head-dnd8p/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2797.7 |
| rayjob-bench-v2g6r/ray-job-submitter | job | 23 | 97.6 | 108.1 | 0.104 | 0.894 |  |
| rayjob-bench-v2g6r/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.2 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 142 | 61.09 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 59.98 MiB | 98.2% |
| logs | 1.10 MiB | 1.8% |
| node_events | 6.17 KiB | 0.0% |
| **total** | **61.09 MiB** | (143 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 86493 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 86481 |
| events per task (k) | 4.32 |
| raw JSONL bytes | 59.99 MiB |
| stored event bytes | 59.99 MiB |
| avg raw bytes/event | 727 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds | 20005 / 20000 expected |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| f5115a8728c699f29cf5b617d523d584c29c2f35bf50652e37663f9f | 46485 | 34.73 MiB | 20006 | 9291 | 4645.7 |
| ac4572e0c8bb96e26696feeedcc411f79580cef1d31d8e8b9868d0c1 | 40008 | 25.26 MiB | 20000 | 7289 | 4000.8 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 3 |
| ACTOR_TASK_DEFINITION_EVENT | 1 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 20005 |
| TASK_LIFECYCLE_EVENT | 46450 |
| TASK_PROFILE_EVENT | 20025 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 6ms / 124ms / 124ms (errors: 0) |
| /enter_cluster cold load | 33.883s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=20000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 701ms | 882ms | 882ms | 408 B | 0 |
| /api/jobs/ | 9ms | 174ms | 174ms | 1.22 KiB | 0 |
| /nodes?view=summary | 13ms | 268ms | 268ms | 2.76 KiB | 0 |
| /events | 86ms | 211ms | 211ms | 1.04 KiB | 0 |

