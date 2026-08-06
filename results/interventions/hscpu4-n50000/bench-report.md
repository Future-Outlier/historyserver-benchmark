# History Server Benchmark Report

- Date: 2026-08-05T20:13:14-05:00
- Tasks: 50000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_18-13-15_934762_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 50s |
| driver-measured wall | 16.9s |
| driver-measured rate | 2965.2 tasks/s |
| flush (cluster deletion incl. final upload) | 3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-zhs74 | 1 | 63.15 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-kvb8g | 5 | 87.49 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 133.7 | 0.000 | 0.000 |
| collector (head) | job | 50 | 133.7 | 0.321 | 0.689 |
| collector (worker) | flush | 1 | 97.5 | 0.000 | 0.000 |
| collector (worker) | job | 50 | 115.4 | 0.253 | 0.636 |
| historyserver | historyserver | 38 | 1147.4 | 1.062 | 1.823 |
| ray-head | job | 50 | 3620.6 | 0.566 | 0.848 |
| ray-worker | job | 50 | 943.8 | 0.992 | 1.643 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-85f7c7ff99-nc2qd/historyserver | historyserver | 36 | 1192.2 | 1195.4 | 1.180 | 2.173 |  |
| historyserver-demo-85f7c7ff99-nc2qd/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1195.6 |
| raycluster-historyserver-cpu-worker-zhs74/collector | flush | 1 | 99.2 | 165.5 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-zhs74/collector | job | 48 | 114.5 | 170.5 | 0.232 | 1.039 |  |
| raycluster-historyserver-cpu-worker-zhs74/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 177.7 |
| raycluster-historyserver-cpu-worker-zhs74/ray-worker | flush | 1 | 102.5 | 117.0 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-zhs74/ray-worker | job | 48 | 969.3 | 1009.3 | 0.774 | 2.119 |  |
| raycluster-historyserver-cpu-worker-zhs74/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1034.7 |
| raycluster-historyserver-head-kvb8g/collector | flush | 2 | 133.3 | 227.2 | 0.965 | 0.965 |  |
| raycluster-historyserver-head-kvb8g/collector | job | 48 | 142.1 | 221.2 | 0.333 | 1.152 |  |
| raycluster-historyserver-head-kvb8g/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 228.9 |
| raycluster-historyserver-head-kvb8g/ray-head | flush | 1 | 1955.9 | 1997.8 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-kvb8g/ray-head | job | 48 | 3570.2 | 3638.5 | 0.549 | 2.256 |  |
| raycluster-historyserver-head-kvb8g/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3643.5 |
| rayjob-bench-xnhkr/ray-job-submitter | job | 44 | 97.6 | 108.1 | 0.057 | 0.964 |  |
| rayjob-bench-xnhkr/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.6 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 153.14 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 150.63 MiB | 98.4% |
| logs | 2.50 MiB | 1.6% |
| node_events | 6.90 KiB | 0.0% |
| **total** | **153.14 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 217646 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 217632 |
| events per task (k) | 4.35 |
| raw JSONL bytes | 150.64 MiB |
| stored event bytes | 150.64 MiB |
| avg raw bytes/event | 726 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 50005 |
| distinct taskIds in benchmark job `03000000` | **50001 / 50000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 536b4f9359d462424f5c4fc91b758d11d5cc32539ecf9b53908f8614 | 117607 | 87.49 MiB | 50007 | 8606 | 7736.8 |
| 8e60bf584bce3596631857a349dc7f4855fb0eb81536197d0392aedc | 100039 | 63.15 MiB | 50000 | 7167 | 6559.2 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 2 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 50005 |
| TASK_LIFECYCLE_EVENT | 117574 |
| TASK_PROFILE_EVENT | 50051 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 4ms / 87ms / 87ms (errors: 0) |
| /enter_cluster cold load | 30.519s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 379ms | 493ms | 493ms | 408 B | 0 |
| /api/jobs/ | 13ms | 107ms | 107ms | 1.31 KiB | 0 |
| /nodes?view=summary | 13ms | 97ms | 97ms | 2.76 KiB | 0 |
| /events | 12ms | 102ms | 102ms | 1.76 KiB | 0 |

