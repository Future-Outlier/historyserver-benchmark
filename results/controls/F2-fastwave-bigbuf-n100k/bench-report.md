# History Server Benchmark Report

- Date: 2026-08-06T13:26:24-05:00
- Tasks: 100000 (wave 10000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_11-26-27_122540_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m20s |
| driver-measured wall | 38.6s |
| driver-measured rate | 2587.9 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-cch7g | 2 | 126.40 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-hbvjb | 6 | 203.55 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 107.0 | 0.000 | 0.000 |
| collector (head) | job | 80 | 126.7 | 0.511 | 0.827 |
| collector (worker) | job | 80 | 116.6 | 0.344 | 0.666 |
| historyserver | historyserver | 90 | 1972.3 | 1.108 | 1.496 |
| ray-head | job | 80 | 3930.5 | 0.715 | 1.204 |
| ray-worker | job | 80 | 1029.2 | 1.283 | 2.029 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7768d9678d-9q9qq/historyserver | historyserver | 87 | 1564.1 | 2012.5 | 1.214 | 2.592 |  |
| historyserver-demo-7768d9678d-9q9qq/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2026.9 |
| raycluster-historyserver-cpu-worker-cch7g/collector | job | 77 | 119.2 | 233.1 | 0.318 | 1.924 |  |
| raycluster-historyserver-cpu-worker-cch7g/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 247.5 |
| raycluster-historyserver-cpu-worker-cch7g/ray-worker | job | 77 | 1013.8 | 1055.8 | 1.048 | 2.104 |  |
| raycluster-historyserver-cpu-worker-cch7g/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1076.7 |
| raycluster-historyserver-head-hbvjb/collector | flush | 1 | 108.1 | 186.0 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-hbvjb/collector | job | 77 | 138.4 | 265.5 | 0.531 | 1.769 |  |
| raycluster-historyserver-head-hbvjb/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 281.5 |
| raycluster-historyserver-head-hbvjb/ray-head | job | 77 | 3870.7 | 3938.0 | 0.718 | 2.569 |  |
| raycluster-historyserver-head-hbvjb/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3942.5 |
| rayjob-bench-2tkwr/ray-job-submitter | job | 73 | 97.8 | 108.3 | 0.041 | 0.988 |  |
| rayjob-bench-2tkwr/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.1 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 243.23 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 170 | 91.40 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 329.94 MiB | 98.6% |
| logs | 4.68 MiB | 1.4% |
| node_events | 6.91 KiB | 0.0% |
| **total** | **334.63 MiB** | (175 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 495137 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 495123 |
| events per task (k) | 4.95 |
| raw JSONL bytes | 329.95 MiB |
| stored event bytes | 329.95 MiB |
| avg raw bytes/event | 699 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 1d915fed9afd7921aa72e06cbfa85c37895b0ea143985d9991a69853 | 295054 | 203.55 MiB | 100008 | 22603 | 8602.4 |
| e2fc589c7984503f1766ea2ab957ac53dd6da0a21efea1139903b383 | 200083 | 126.40 MiB | 100000 | 6425 | 5553.7 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 3 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 100005 |
| TASK_LIFECYCLE_EVENT | 295034 |
| TASK_PROFILE_EVENT | 100081 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 8ms / 219ms / 219ms (errors: 0) |
| /enter_cluster cold load | 1m16.413s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 871ms | 909ms | 909ms | 536 B | 0 |
| /api/jobs/ | 16ms | 260ms | 260ms | 1.31 KiB | 0 |
| /nodes?view=summary | 17ms | 243ms | 243ms | 2.77 KiB | 0 |
| /events | 17ms | 282ms | 282ms | 3.57 KiB | 0 |

