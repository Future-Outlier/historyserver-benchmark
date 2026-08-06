# History Server Benchmark Report

- Date: 2026-08-06T15:16:00-05:00
- Tasks: 50000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_13-16-01_930282_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 50s |
| driver-measured wall | 18.6s |
| driver-measured rate | 2692.6 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-4n7v2 | 1 | 63.20 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-2v7kd | 5 | 86.75 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 127.9 | 0.000 | 0.000 |
| collector (head) | job | 50 | 130.0 | 0.346 | 0.653 |
| collector (worker) | flush | 1 | 120.5 | 0.000 | 0.000 |
| collector (worker) | job | 50 | 120.5 | 0.299 | 0.356 |
| historyserver | historyserver | 102 | 1241.7 | 0.494 | 0.510 |
| ray-head | job | 50 | 4150.9 | 0.525 | 1.142 |
| ray-worker | job | 50 | 915.9 | 0.824 | 1.776 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-99dd5848f-f5gmp/historyserver | historyserver | 98 | 1213.9 | 1313.9 | 0.493 | 0.531 |  |
| historyserver-demo-99dd5848f-f5gmp/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1321.0 |
| raycluster-historyserver-cpu-worker-4n7v2/collector | flush | 1 | 68.2 | 134.7 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-4n7v2/collector | job | 48 | 117.6 | 183.7 | 0.234 | 1.137 |  |
| raycluster-historyserver-cpu-worker-4n7v2/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 187.0 |
| raycluster-historyserver-cpu-worker-4n7v2/ray-worker | job | 48 | 979.9 | 1020.4 | 0.829 | 2.094 |  |
| raycluster-historyserver-cpu-worker-4n7v2/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1024.6 |
| raycluster-historyserver-head-2v7kd/collector | flush | 1 | 127.9 | 218.9 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-2v7kd/collector | job | 48 | 127.1 | 215.0 | 0.334 | 0.909 |  |
| raycluster-historyserver-head-2v7kd/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 227.3 |
| raycluster-historyserver-head-2v7kd/ray-head | job | 48 | 4100.7 | 4171.5 | 0.576 | 2.159 |  |
| raycluster-historyserver-head-2v7kd/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 4176.0 |
| rayjob-bench-wwmxj/ray-job-submitter | job | 44 | 98.0 | 108.5 | 0.065 | 0.993 |  |
| rayjob-bench-wwmxj/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 109.4 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 152.45 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 149.95 MiB | 98.4% |
| logs | 2.50 MiB | 1.6% |
| node_events | 6.44 KiB | 0.0% |
| **total** | **152.45 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 216121 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 216108 |
| events per task (k) | 4.32 |
| raw JSONL bytes | 149.95 MiB |
| stored event bytes | 149.95 MiB |
| avg raw bytes/event | 728 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 50005 |
| distinct taskIds in benchmark job `03000000` | **50001 / 50000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| f4b0b39a5a38d3c0ddf6a4a4e5fc7a047b743e4642b8540aa77c0e9a | 116080 | 86.75 MiB | 50007 | 9471 | 7034.1 |
| f4194732d1cb74919a202efbe4397062e4273f011677ea8b803123d6 | 100041 | 63.20 MiB | 50000 | 6919 | 6147.6 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 4 |
| ACTOR_TASK_DEFINITION_EVENT | 2 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 50005 |
| TASK_LIFECYCLE_EVENT | 116040 |
| TASK_PROFILE_EVENT | 50061 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 6ms / 83ms / 83ms (errors: 0) |
| /enter_cluster cold load | 1m16.313s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 2.09s | 2.271s | 2.271s | 408 B | 0 |
| /api/jobs/ | 56ms | 486ms | 486ms | 1.31 KiB | 0 |
| /nodes?view=summary | 81ms | 684ms | 684ms | 2.77 KiB | 0 |
| /events | 85ms | 497ms | 497ms | 2.12 KiB | 0 |

