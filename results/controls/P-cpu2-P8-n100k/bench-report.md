# History Server Benchmark Report

- Date: 2026-08-06T14:57:13-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_12-57-15_081359_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m15s |
| driver-measured wall | 34.4s |
| driver-measured rate | 2906.8 tasks/s |
| flush (cluster deletion incl. final upload) | 1s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-qkgz8 | 1 | 126.40 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-prqcs | 6 | 173.39 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 75 | 142.1 | 0.467 | 0.824 |
| collector (worker) | job | 75 | 113.7 | 0.384 | 0.659 |
| historyserver | historyserver | 90 | 1761.8 | 1.080 | 1.270 |
| ray-head | job | 75 | 4308.9 | 0.707 | 1.196 |
| ray-worker | job | 75 | 1035.2 | 1.211 | 1.979 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-56dbc5ddf5-94s4v/historyserver | historyserver | 86 | 1623.0 | 1928.6 | 1.122 | 1.775 |  |
| historyserver-demo-56dbc5ddf5-94s4v/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1933.7 |
| raycluster-historyserver-cpu-worker-qkgz8/collector | job | 72 | 122.6 | 240.0 | 0.339 | 1.109 |  |
| raycluster-historyserver-cpu-worker-qkgz8/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 245.6 |
| raycluster-historyserver-cpu-worker-qkgz8/ray-worker | job | 72 | 1022.9 | 1061.9 | 1.010 | 2.105 |  |
| raycluster-historyserver-cpu-worker-qkgz8/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1063.3 |
| raycluster-historyserver-head-prqcs/collector | job | 72 | 146.5 | 284.4 | 0.463 | 1.804 |  |
| raycluster-historyserver-head-prqcs/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 305.9 |
| raycluster-historyserver-head-prqcs/ray-head | job | 72 | 4243.9 | 4310.3 | 0.665 | 2.464 |  |
| raycluster-historyserver-head-prqcs/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 4313.1 |
| rayjob-bench-hms58/ray-job-submitter | job | 68 | 97.6 | 108.1 | 0.036 | 0.895 |  |
| rayjob-bench-hms58/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.4 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 267.76 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 169 | 36.68 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 299.78 MiB | 98.5% |
| logs | 4.66 MiB | 1.5% |
| node_events | 6.18 KiB | 0.0% |
| **total** | **304.44 MiB** | (174 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 432007 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 431995 |
| events per task (k) | 4.32 |
| raw JSONL bytes | 299.79 MiB |
| stored event bytes | 299.79 MiB |
| avg raw bytes/event | 728 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| c0abe52c3adcb7a22c8400a49f462188f889973a6a5ef952fa1810b5 | 231935 | 173.39 MiB | 100007 | 9482 | 7408.2 |
| 35bbac511255ee56b987727659bcc0347578753b2ec6fc531f9d805b | 200072 | 126.40 MiB | 100000 | 7321 | 6419.0 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 3 |
| ACTOR_TASK_DEFINITION_EVENT | 2 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 100005 |
| TASK_LIFECYCLE_EVENT | 231898 |
| TASK_PROFILE_EVENT | 100090 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 85ms / 85ms (errors: 0) |
| /enter_cluster cold load | 1m12.156s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 1.194s | 1.597s | 1.597s | 411 B | 0 |
| /api/jobs/ | 26ms | 419ms | 419ms | 1.31 KiB | 0 |
| /nodes?view=summary | 27ms | 393ms | 393ms | 2.77 KiB | 0 |
| /events | 26ms | 408ms | 408ms | 2.12 KiB | 0 |

