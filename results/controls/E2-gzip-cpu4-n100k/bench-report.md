# History Server Benchmark Report

- Date: 2026-08-06T13:19:30-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=true
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_11-19-32_273436_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m25s |
| driver-measured wall | 37.9s |
| driver-measured rate | 2641.9 tasks/s |
| flush (cluster deletion incl. final upload) | 3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-z8x54 | 2 | 126.40 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-tgs8m | 6 | 174.25 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 105.9 | 0.000 | 0.000 |
| collector (head) | job | 85 | 145.9 | 0.476 | 1.086 |
| collector (worker) | job | 85 | 118.0 | 0.400 | 0.809 |
| historyserver | historyserver | 92 | 1661.3 | 1.109 | 1.309 |
| ray-head | job | 85 | 3705.6 | 0.633 | 1.232 |
| ray-worker | job | 85 | 1040.5 | 1.046 | 2.049 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7768d9678d-tjhkp/historyserver | historyserver | 88 | 1542.5 | 1846.4 | 1.200 | 2.605 |  |
| historyserver-demo-7768d9678d-tjhkp/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1847.6 |
| raycluster-historyserver-cpu-worker-z8x54/collector | job | 82 | 117.1 | 252.4 | 0.325 | 1.943 |  |
| raycluster-historyserver-cpu-worker-z8x54/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 259.2 |
| raycluster-historyserver-cpu-worker-z8x54/ray-worker | job | 82 | 1015.5 | 1057.5 | 0.973 | 2.090 |  |
| raycluster-historyserver-cpu-worker-z8x54/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1063.6 |
| raycluster-historyserver-head-tgs8m/collector | flush | 1 | 93.7 | 139.3 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-tgs8m/collector | job | 82 | 143.7 | 295.2 | 0.454 | 1.816 |  |
| raycluster-historyserver-head-tgs8m/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 299.7 |
| raycluster-historyserver-head-tgs8m/ray-head | job | 82 | 3650.7 | 3713.8 | 0.636 | 1.944 |  |
| raycluster-historyserver-head-tgs8m/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3718.5 |
| rayjob-bench-8n5n2/ray-job-submitter | job | 74 | 97.8 | 108.5 | 0.039 | 0.980 |  |
| rayjob-bench-8n5n2/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.7 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 22.74 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 170 | 9.35 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 27.40 MiB | 85.4% |
| logs | 4.68 MiB | 14.6% |
| node_events | 1.61 KiB | 0.0% |
| **total** | **32.08 MiB** | (175 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 433806 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 433793 |
| events per task (k) | 4.34 |
| raw JSONL bytes | 300.65 MiB |
| stored event bytes | 27.40 MiB |
| avg raw bytes/event | 727 |
| compression ratio (stored/raw) | 0.091 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| f7edab8d9b62b4b77fc95f4e1619dfb08c23bf9127dc5bf4539d4a28 | 233729 | 174.25 MiB | 100008 | 9346 | 6568.4 |
| 50a49b97f73f6ec17042ee7534a5a6822e51de6580d734a3d3e137b0 | 200077 | 126.40 MiB | 100000 | 6765 | 5629.8 |

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
| TASK_LIFECYCLE_EVENT | 233688 |
| TASK_PROFILE_EVENT | 100097 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 94ms / 94ms (errors: 0) |
| /enter_cluster cold load | 1m18.612s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 916ms | 1.215s | 1.215s | 536 B | 0 |
| /api/jobs/ | 26ms | 234ms | 234ms | 1.31 KiB | 0 |
| /nodes?view=summary | 25ms | 245ms | 245ms | 2.77 KiB | 0 |
| /events | 27ms | 278ms | 278ms | 3.56 KiB | 0 |

