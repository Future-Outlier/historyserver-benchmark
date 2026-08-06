# History Server Benchmark Report

- Date: 2026-08-06T14:40:26-05:00
- Tasks: 50000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_12-40-28_100896_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 55s |
| driver-measured wall | 18.1s |
| driver-measured rate | 2770.2 tasks/s |
| flush (cluster deletion incl. final upload) | 3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-t8sb2 | 1 | 63.20 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-mh767 | 5 | 88.31 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 141.1 | 0.000 | 0.000 |
| collector (head) | job | 55 | 147.2 | 0.355 | 0.676 |
| collector (worker) | flush | 1 | 112.5 | 0.000 | 0.000 |
| collector (worker) | job | 55 | 112.5 | 0.233 | 0.378 |
| historyserver | historyserver | 46 | 963.4 | 1.036 | 1.457 |
| ray-head | job | 55 | 3753.2 | 0.461 | 1.176 |
| ray-worker | job | 55 | 993.9 | 0.853 | 1.974 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7d676b97c8-gv8c8/historyserver | historyserver | 44 | 1135.9 | 1140.2 | 1.089 | 1.465 |  |
| historyserver-demo-7d676b97c8-gv8c8/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1141.6 |
| raycluster-historyserver-cpu-worker-t8sb2/collector | flush | 1 | 111.8 | 178.0 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-t8sb2/collector | job | 52 | 120.2 | 175.7 | 0.215 | 1.149 |  |
| raycluster-historyserver-cpu-worker-t8sb2/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 182.0 |
| raycluster-historyserver-cpu-worker-t8sb2/ray-worker | flush | 1 | 102.6 | 117.6 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-t8sb2/ray-worker | job | 52 | 966.0 | 1006.0 | 0.745 | 2.091 |  |
| raycluster-historyserver-cpu-worker-t8sb2/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1026.5 |
| raycluster-historyserver-head-mh767/collector | flush | 2 | 137.5 | 229.6 | 0.947 | 0.947 |  |
| raycluster-historyserver-head-mh767/collector | job | 52 | 144.2 | 229.6 | 0.309 | 1.106 |  |
| raycluster-historyserver-head-mh767/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 230.9 |
| raycluster-historyserver-head-mh767/ray-head | flush | 1 | 3453.4 | 3514.9 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-mh767/ray-head | job | 52 | 3720.2 | 3785.8 | 0.513 | 2.511 |  |
| raycluster-historyserver-head-mh767/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3792.0 |
| rayjob-bench-hcvp8/ray-job-submitter | job | 44 | 97.6 | 108.1 | 0.055 | 0.996 |  |
| rayjob-bench-hcvp8/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.2 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 154.01 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 151.50 MiB | 98.4% |
| logs | 2.51 MiB | 1.6% |
| node_events | 7.47 KiB | 0.0% |
| **total** | **154.01 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 219378 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 219363 |
| events per task (k) | 4.39 |
| raw JSONL bytes | 151.51 MiB |
| stored event bytes | 151.51 MiB |
| avg raw bytes/event | 724 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 50005 |
| distinct taskIds in benchmark job `03000000` | **50001 / 50000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 42c1fa033771c33a1dc5f9d760fc972cdd6cf4abe947c746be024c79 | 119330 | 88.31 MiB | 50007 | 9767 | 7147.1 |
| fd9d5888e3c0389d5d5cb55ca954b79f75ea4e4b335c6d0ddb4a70b7 | 100048 | 63.20 MiB | 50000 | 7155 | 6091.8 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 2 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 3 |
| TASK_DEFINITION_EVENT | 50005 |
| TASK_LIFECYCLE_EVENT | 119299 |
| TASK_PROFILE_EVENT | 50057 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 4ms / 88ms / 88ms (errors: 0) |
| /enter_cluster cold load | 34.805s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 677ms | 716ms | 716ms | 408 B | 0 |
| /api/jobs/ | 13ms | 134ms | 134ms | 1.31 KiB | 0 |
| /nodes?view=summary | 14ms | 154ms | 154ms | 2.73 KiB | 0 |
| /events | 13ms | 138ms | 138ms | 2.13 KiB | 0 |

