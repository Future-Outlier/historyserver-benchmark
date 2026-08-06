# History Server Benchmark Report

- Date: 2026-08-05T18:56:06-05:00
- Tasks: 1000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_16-56-09_001690_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 25s |
| driver-measured wall | 1.3s |
| driver-measured rate | 767.8 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-x85f9 | 1 | 1.26 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-lrwtp | 5 | 1.82 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 25 | 75.8 | 0.023 | 0.023 |
| collector (worker) | job | 25 | 65.5 | 0.016 | 0.016 |
| historyserver | historyserver | 7 | 106.2 | 0.384 | 0.501 |
| ray-head | job | 25 | 1608.3 | 0.230 | 0.411 |
| ray-worker | job | 25 | 482.8 | 0.217 | 0.217 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-zg8mh/historyserver | historyserver | 6 | 95.3 | 103.6 | 0.373 | 0.521 |  |
| historyserver-demo-7f67bfd478-zg8mh/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 106.0 |
| raycluster-historyserver-cpu-worker-x85f9/collector | flush | 1 | 67.4 | 68.8 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-x85f9/collector | job | 24 | 64.4 | 66.8 | 0.012 | 0.247 |  |
| raycluster-historyserver-cpu-worker-x85f9/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 70.6 |
| raycluster-historyserver-cpu-worker-x85f9/ray-worker | flush | 1 | 102.5 | 115.5 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-x85f9/ray-worker | job | 24 | 640.1 | 671.9 | 0.151 | 1.999 |  |
| raycluster-historyserver-cpu-worker-x85f9/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 676.9 |
| raycluster-historyserver-head-lrwtp/collector | flush | 1 | 67.7 | 72.9 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-lrwtp/collector | job | 24 | 74.8 | 77.8 | 0.017 | 0.219 |  |
| raycluster-historyserver-head-lrwtp/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 80.5 |
| raycluster-historyserver-head-lrwtp/ray-head | flush | 1 | 947.1 | 985.7 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-lrwtp/ray-head | job | 24 | 1677.8 | 1751.2 | 0.258 | 1.149 |  |
| raycluster-historyserver-head-lrwtp/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1756.3 |
| rayjob-bench-25qj2/ray-job-submitter | job | 17 | 97.6 | 108.0 | 0.175 | 0.992 |  |
| rayjob-bench-25qj2/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 109.8 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 147 | 3.39 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 3.08 MiB | 90.7% |
| logs | 315.39 KiB | 9.1% |
| node_events | 6.90 KiB | 0.2% |
| **total** | **3.39 MiB** | (148 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 4488 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 4474 |
| events per task (k) | 4.47 |
| raw JSONL bytes | 3.08 MiB |
| stored event bytes | 3.08 MiB |
| avg raw bytes/event | 721 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds | 1005 / 1000 expected |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| e12387aa9340987fb29f553f440d5a4d7958c3dea0eb1bac4ed9ddd5 | 2488 | 1.82 MiB | 1006 | 2006 | 246.2 |
| 949f4515b1ab506ee098964b517fcb32b4100441c43fb7b949842623 | 2000 | 1.26 MiB | 1000 | 1110 | 200.0 |

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
| TASK_LIFECYCLE_EVENT | 2455 |
| TASK_PROFILE_EVENT | 1013 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 6ms / 185ms / 185ms (errors: 0) |
| /enter_cluster cold load | 2.279s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=1000 | 92ms | 164ms | 164ms | 442.60 KiB | 0 |
| /api/v0/tasks/summarize | 8ms | 67ms | 67ms | 658 B | 0 |
| /api/jobs/ | 2ms | 58ms | 58ms | 1.22 KiB | 0 |
| /nodes?view=summary | 2ms | 5ms | 5ms | 2.76 KiB | 0 |
| /events | 3ms | 61ms | 61ms | 1.04 KiB | 0 |

