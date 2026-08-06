# History Server Benchmark Report

- Date: 2026-08-06T15:00:28-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_13-00-30_075667_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m20s |
| driver-measured wall | 33.5s |
| driver-measured rate | 2986.0 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-gd4zb | 1 | 126.40 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-wvzft | 6 | 171.84 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 80 | 143.0 | 0.518 | 0.891 |
| collector (worker) | job | 80 | 117.4 | 0.366 | 0.692 |
| historyserver | historyserver | 80 | 2167.4 | 1.059 | 1.800 |
| ray-head | job | 80 | 4331.6 | 0.631 | 1.186 |
| ray-worker | job | 80 | 989.0 | 1.070 | 1.854 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7b4c5b8db8-vb6nq/historyserver | historyserver | 76 | 2400.8 | 2409.7 | 1.075 | 1.897 |  |
| historyserver-demo-7b4c5b8db8-vb6nq/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2415.7 |
| raycluster-historyserver-cpu-worker-gd4zb/collector | flush | 1 | 89.9 | 91.0 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-gd4zb/collector | job | 76 | 122.4 | 238.5 | 0.320 | 1.047 |  |
| raycluster-historyserver-cpu-worker-gd4zb/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 242.2 |
| raycluster-historyserver-cpu-worker-gd4zb/ray-worker | flush | 1 | 186.3 | 228.7 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-gd4zb/ray-worker | job | 76 | 1020.1 | 1062.8 | 0.938 | 2.106 |  |
| raycluster-historyserver-cpu-worker-gd4zb/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1065.0 |
| raycluster-historyserver-head-wvzft/collector | flush | 1 | 97.1 | 133.6 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-wvzft/collector | job | 76 | 145.4 | 270.5 | 0.433 | 2.122 |  |
| raycluster-historyserver-head-wvzft/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 288.1 |
| raycluster-historyserver-head-wvzft/ray-head | flush | 1 | 3572.5 | 3716.6 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-wvzft/ray-head | job | 76 | 4282.0 | 4349.3 | 0.633 | 2.073 |  |
| raycluster-historyserver-head-wvzft/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 4354.2 |
| rayjob-bench-d82bw/ray-job-submitter | job | 68 | 97.6 | 108.1 | 0.038 | 0.993 |  |
| rayjob-bench-d82bw/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.3 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 264.22 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 169 | 38.70 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 298.23 MiB | 98.5% |
| logs | 4.68 MiB | 1.5% |
| node_events | 6.91 KiB | 0.0% |
| **total** | **302.92 MiB** | (174 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 430373 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 430359 |
| events per task (k) | 4.30 |
| raw JSONL bytes | 298.24 MiB |
| stored event bytes | 298.24 MiB |
| avg raw bytes/event | 727 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 98779 |
| distinct taskIds in benchmark job `03000000` | **98775 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 8d04d9897094b18e7d1751f726bcfbd38bb8436c3c35faa853701853 | 230298 | 171.84 MiB | 98782 | 9896 | 7486.7 |
| a1bd5f50e01aa6e324386b5272ae19dbbb3f08554ecb5d7dc03d7290 | 200075 | 126.40 MiB | 100000 | 7461 | 6463.9 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 3 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 98779 |
| TASK_LIFECYCLE_EVENT | 231482 |
| TASK_PROFILE_EVENT | 100095 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 82ms / 82ms (errors: 0) |
| /enter_cluster cold load | 1m5.927s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 668ms | 1.291s | 1.291s | 536 B | 0 |
| /api/jobs/ | 26ms | 390ms | 390ms | 1.31 KiB | 0 |
| /nodes?view=summary | 27ms | 265ms | 265ms | 2.77 KiB | 0 |
| /events | 26ms | 287ms | 287ms | 3.20 KiB | 0 |

