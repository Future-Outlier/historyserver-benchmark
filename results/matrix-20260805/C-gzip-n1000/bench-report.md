# History Server Benchmark Report

- Date: 2026-08-05T18:20:21-05:00
- Tasks: 1000 (wave 2000, num_cpus=0.2), compression=true
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_16-20-22_800106_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 25s |
| driver-measured wall | 1.0s |
| driver-measured rate | 960.2 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-596ht | 0 | 0 B | 0 | 0 | 0 |
| raycluster-historyserver-head-9gh4q | 0 | 0 B | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 25 | 74.9 | 0.014 | 0.023 |
| collector (worker) | job | 25 | 63.9 | 0.024 | 0.024 |
| historyserver | historyserver | 5 | 99.8 | 0.442 | 0.502 |
| ray-head | job | 25 | 2350.9 | 0.219 | 0.223 |
| ray-worker | job | 25 | 296.8 | 0.155 | 0.171 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-7zc2r/historyserver | historyserver | 4 | 98.1 | 102.4 | 0.422 | 0.512 |  |
| historyserver-demo-7f67bfd478-7zc2r/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 102.6 |
| raycluster-historyserver-cpu-worker-596ht/collector | job | 24 | 62.8 | 65.2 | 0.011 | 0.237 |  |
| raycluster-historyserver-cpu-worker-596ht/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 66.1 |
| raycluster-historyserver-cpu-worker-596ht/ray-worker | job | 24 | 576.2 | 605.3 | 0.131 | 1.306 |  |
| raycluster-historyserver-cpu-worker-596ht/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 609.7 |
| raycluster-historyserver-head-9gh4q/collector | job | 24 | 73.8 | 76.5 | 0.015 | 0.275 |  |
| raycluster-historyserver-head-9gh4q/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 77.3 |
| raycluster-historyserver-head-9gh4q/ray-head | flush | 1 | 0.0 | 21.3 | 1.858 | 1.858 |  |
| raycluster-historyserver-head-9gh4q/ray-head | job | 24 | 2382.8 | 2441.9 | 0.248 | 1.301 |  |
| raycluster-historyserver-head-9gh4q/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2496.8 |
| rayjob-bench-wl6tm/ray-job-submitter | job | 17 | 97.6 | 108.0 | 0.139 | 1.041 |  |
| rayjob-bench-wl6tm/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.4 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 147 | 572.67 KiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.1% |
| job_events | 261.48 KiB | 45.6% |
| logs | 309.54 KiB | 54.0% |
| node_events | 1.65 KiB | 0.3% |
| **total** | **573.14 KiB** | (148 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 4044 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 4030 |
| events per task (k) | 4.03 |
| raw JSONL bytes | 2.87 MiB |
| stored event bytes | 263.13 KiB |
| avg raw bytes/event | 745 |
| compression ratio (stored/raw) | 0.089 |
| distinct taskDefinitionEvent taskIds | 1005 / 1000 expected |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| c5174da1b7bf7ecaef291dae23dfcfba4363fda96d6c44678b1f17c3 | 2044 | 1.61 MiB | 1006 | 1007 | 201.7 |
| b29548ab0120bf8c940deb06f8e478f558a7968b8f4a0f57d117e1b2 | 2000 | 1.26 MiB | 1000 | 2000 | 200.0 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 1 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 1005 |
| TASK_LIFECYCLE_EVENT | 2011 |
| TASK_PROFILE_EVENT | 1013 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 5ms / 96ms / 96ms (errors: 0) |
| /enter_cluster cold load | 1.821s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=1000 | 94ms | 110ms | 110ms | 442.60 KiB | 0 |
| /api/v0/tasks/summarize | 7ms | 72ms | 72ms | 658 B | 0 |
| /api/jobs/ | 1ms | 3ms | 3ms | 1.22 KiB | 0 |
| /nodes?view=summary | 3ms | 57ms | 57ms | 2.76 KiB | 0 |
| /events | 2ms | 38ms | 38ms | 698 B | 0 |

