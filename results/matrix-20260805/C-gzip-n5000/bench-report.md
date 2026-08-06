# History Server Benchmark Report

- Date: 2026-08-05T18:21:20-05:00
- Tasks: 5000 (wave 2000, num_cpus=0.2), compression=true
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_16-21-22_023099_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 25s |
| driver-measured wall | 3.0s |
| driver-measured rate | 1657.3 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-ntvtd | 0 | 0 B | 0 | 0 | 0 |
| raycluster-historyserver-head-q8nhn | 0 | 0 B | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 25 | 95.5 | 0.064 | 0.122 |
| collector (worker) | job | 25 | 64.0 | 0.074 | 0.074 |
| historyserver | historyserver | 18 | 191.1 | 0.476 | 0.502 |
| ray-head | job | 25 | 2504.6 | 0.345 | 0.345 |
| ray-worker | job | 25 | 345.0 | 0.383 | 0.430 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-vd8mn/historyserver | historyserver | 17 | 165.3 | 191.2 | 0.484 | 0.522 |  |
| historyserver-demo-7f67bfd478-vd8mn/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 199.0 |
| raycluster-historyserver-cpu-worker-ntvtd/collector | flush | 1 | 65.1 | 73.0 | 0.103 | 0.103 |  |
| raycluster-historyserver-cpu-worker-ntvtd/collector | job | 24 | 72.7 | 78.7 | 0.050 | 0.601 |  |
| raycluster-historyserver-cpu-worker-ntvtd/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 81.0 |
| raycluster-historyserver-cpu-worker-ntvtd/ray-worker | flush | 1 | 102.7 | 115.7 | 0.105 | 0.105 |  |
| raycluster-historyserver-cpu-worker-ntvtd/ray-worker | job | 24 | 852.5 | 889.2 | 0.322 | 2.012 |  |
| raycluster-historyserver-cpu-worker-ntvtd/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 890.7 |
| raycluster-historyserver-head-q8nhn/collector | flush | 1 | 94.1 | 105.4 | 0.099 | 0.099 |  |
| raycluster-historyserver-head-q8nhn/collector | job | 24 | 94.1 | 104.5 | 0.072 | 0.701 |  |
| raycluster-historyserver-head-q8nhn/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 106.8 |
| raycluster-historyserver-head-q8nhn/ray-head | flush | 1 | 1441.1 | 1486.8 | 0.309 | 0.309 |  |
| raycluster-historyserver-head-q8nhn/ray-head | job | 24 | 2493.3 | 2557.1 | 0.303 | 1.291 |  |
| raycluster-historyserver-head-q8nhn/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2560.2 |
| rayjob-bench-99j78/ray-job-submitter | job | 20 | 97.7 | 108.3 | 0.119 | 0.944 |  |
| rayjob-bench-99j78/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.0 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 167 | 1.88 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 1.37 MiB | 72.9% |
| logs | 521.76 KiB | 27.0% |
| node_events | 1.57 KiB | 0.1% |
| **total** | **1.89 MiB** | (168 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 22122 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 22110 |
| events per task (k) | 4.42 |
| raw JSONL bytes | 15.24 MiB |
| stored event bytes | 1.38 MiB |
| avg raw bytes/event | 722 |
| compression ratio (stored/raw) | 0.090 |
| distinct taskDefinitionEvent taskIds | 5005 / 5000 expected |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| dc373b9449bbc55fe52325ae3bae431b23f0700d95e34458da1f8c0d | 12119 | 8.93 MiB | 5006 | 4890 | 1209.3 |
| 7add2f8372ce1113c5d8a250f4d85776e2cf80410310754c99d413c6 | 10003 | 6.31 MiB | 5000 | 3963 | 1000.3 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 3 |
| ACTOR_TASK_DEFINITION_EVENT | 1 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 5005 |
| TASK_LIFECYCLE_EVENT | 12086 |
| TASK_PROFILE_EVENT | 5018 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 6ms / 176ms / 176ms (errors: 0) |
| /enter_cluster cold load | 9.5s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=5000 | 498ms | 694ms | 694ms | 2.16 MiB | 0 |
| /api/v0/tasks/summarize | 153ms | 195ms | 195ms | 658 B | 0 |
| /api/jobs/ | 3ms | 78ms | 78ms | 1.22 KiB | 0 |
| /nodes?view=summary | 6ms | 89ms | 89ms | 2.76 KiB | 0 |
| /events | 3ms | 138ms | 138ms | 1.04 KiB | 0 |

