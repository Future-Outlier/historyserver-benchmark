# History Server Benchmark Report

- Date: 2026-08-06T12:44:01-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_10-44-03_354664_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m20s |
| driver-measured wall | 37.1s |
| driver-measured rate | 2698.5 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-gxbc2 | 2 | 126.39 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-tkbj9 | 6 | 174.09 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 141.3 | 0.000 | 0.000 |
| collector (head) | job | 80 | 141.3 | 0.451 | 0.771 |
| collector (worker) | job | 80 | 113.2 | 0.356 | 0.582 |
| historyserver | historyserver | 94 | 1951.2 | 1.147 | 1.559 |
| ray-head | job | 80 | 3515.5 | 0.728 | 1.251 |
| ray-worker | job | 80 | 1006.0 | 1.068 | 2.005 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-6f9645fbb8-kgjsb/historyserver | historyserver | 90 | 1570.2 | 1984.8 | 1.164 | 1.998 |  |
| historyserver-demo-6f9645fbb8-kgjsb/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1999.0 |
| raycluster-historyserver-cpu-worker-gxbc2/collector | job | 77 | 115.5 | 237.7 | 0.320 | 1.669 |  |
| raycluster-historyserver-cpu-worker-gxbc2/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 244.1 |
| raycluster-historyserver-cpu-worker-gxbc2/ray-worker | job | 77 | 1017.2 | 1058.5 | 1.016 | 2.117 |  |
| raycluster-historyserver-cpu-worker-gxbc2/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1064.2 |
| raycluster-historyserver-head-tkbj9/collector | job | 77 | 144.8 | 267.8 | 0.451 | 1.510 |  |
| raycluster-historyserver-head-tkbj9/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 281.9 |
| raycluster-historyserver-head-tkbj9/ray-head | job | 77 | 3457.1 | 3522.7 | 0.686 | 1.971 |  |
| raycluster-historyserver-head-tkbj9/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3526.4 |
| rayjob-bench-8w4tz/ray-job-submitter | job | 72 | 97.6 | 108.1 | 0.034 | 0.936 |  |
| rayjob-bench-8w4tz/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.4 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 233.21 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 170 | 71.95 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 300.47 MiB | 98.5% |
| logs | 4.68 MiB | 1.5% |
| node_events | 6.91 KiB | 0.0% |
| **total** | **305.17 MiB** | (175 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 433453 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 433439 |
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
| 88c35b475bf84eb8ec68a18d43366ec5a54288adbce1a8830f3046ba | 233396 | 174.09 MiB | 100008 | 9346 | 7222.9 |
| 90f3a9899a611ff9be8000b345994dd1f647839359f4c94039e3eb82 | 200057 | 126.39 MiB | 100000 | 7022 | 6238.4 |

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
| TASK_LIFECYCLE_EVENT | 233333 |
| TASK_PROFILE_EVENT | 100098 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 97ms / 97ms (errors: 0) |
| /enter_cluster cold load | 1m15.07s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 1.353s | 1.423s | 1.423s | 536 B | 0 |
| /api/jobs/ | 26ms | 281ms | 281ms | 1.31 KiB | 0 |
| /nodes?view=summary | 228ms | 301ms | 301ms | 2.77 KiB | 0 |
| /events | 24ms | 227ms | 227ms | 3.57 KiB | 0 |

