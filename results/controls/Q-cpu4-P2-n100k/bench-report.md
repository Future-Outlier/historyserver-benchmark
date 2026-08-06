# History Server Benchmark Report

- Date: 2026-08-06T14:45:21-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_12-45-23_187495_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m20s |
| driver-measured wall | 37.8s |
| driver-measured rate | 2647.5 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-zfmb5 | 2 | 126.40 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-4l4mv | 6 | 174.09 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 130.2 | 0.000 | 0.000 |
| collector (head) | job | 80 | 146.3 | 0.433 | 0.798 |
| collector (worker) | job | 80 | 116.8 | 0.388 | 0.599 |
| historyserver | historyserver | 89 | 1814.2 | 1.099 | 1.714 |
| ray-head | job | 80 | 4086.5 | 0.695 | 1.240 |
| ray-worker | job | 80 | 1032.5 | 1.206 | 2.071 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-68b567dcd9-xsbmv/historyserver | historyserver | 86 | 1570.7 | 1826.6 | 1.183 | 1.999 |  |
| historyserver-demo-68b567dcd9-xsbmv/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1828.6 |
| raycluster-historyserver-cpu-worker-zfmb5/collector | baseline | 1 | 52.6 | 53.4 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-zfmb5/collector | flush | 1 | 112.4 | 118.1 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-zfmb5/collector | job | 75 | 115.0 | 243.5 | 0.328 | 1.613 |  |
| raycluster-historyserver-cpu-worker-zfmb5/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 247.8 |
| raycluster-historyserver-cpu-worker-zfmb5/ray-worker | baseline | 1 | 267.9 | 286.8 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-zfmb5/ray-worker | flush | 1 | 102.6 | 119.8 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-zfmb5/ray-worker | job | 75 | 1019.3 | 1061.7 | 1.055 | 2.108 |  |
| raycluster-historyserver-cpu-worker-zfmb5/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1065.6 |
| raycluster-historyserver-head-4l4mv/collector | baseline | 1 | 59.6 | 60.5 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-4l4mv/collector | flush | 1 | 128.8 | 184.0 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-4l4mv/collector | job | 75 | 145.4 | 261.6 | 0.499 | 1.727 |  |
| raycluster-historyserver-head-4l4mv/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 282.9 |
| raycluster-historyserver-head-4l4mv/ray-head | baseline | 1 | 3100.2 | 3150.0 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-4l4mv/ray-head | flush | 1 | 3361.8 | 3421.3 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-4l4mv/ray-head | job | 75 | 4051.0 | 4117.5 | 0.679 | 1.953 |  |
| raycluster-historyserver-head-4l4mv/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 4122.4 |
| rayjob-bench-2xjqc/ray-job-submitter | job | 71 | 97.6 | 108.1 | 0.036 | 0.985 |  |
| rayjob-bench-2xjqc/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.1 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 243.60 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 170 | 61.57 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 300.48 MiB | 98.5% |
| logs | 4.68 MiB | 1.5% |
| node_events | 6.44 KiB | 0.0% |
| **total** | **305.17 MiB** | (175 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 433461 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 433448 |
| events per task (k) | 4.33 |
| raw JSONL bytes | 300.48 MiB |
| stored event bytes | 300.48 MiB |
| avg raw bytes/event | 727 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| dc2f43dde543bca26369918407866050390f4c0eb82a45d213efe7d0 | 233386 | 174.09 MiB | 100008 | 9862 | 7242.3 |
| 800d8a32ab3a9c73c5e8d125c45373f804c755721d34584a82ba74b4 | 200075 | 126.40 MiB | 100000 | 7008 | 6115.5 |

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
| TASK_LIFECYCLE_EVENT | 233348 |
| TASK_PROFILE_EVENT | 100092 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 6ms / 94ms / 94ms (errors: 0) |
| /enter_cluster cold load | 1m10.275s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 1.388s | 1.427s | 1.427s | 536 B | 0 |
| /api/jobs/ | 24ms | 225ms | 225ms | 1.31 KiB | 0 |
| /nodes?view=summary | 28ms | 233ms | 233ms | 2.77 KiB | 0 |
| /events | 26ms | 257ms | 257ms | 3.56 KiB | 0 |

