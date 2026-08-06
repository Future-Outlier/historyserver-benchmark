# History Server Benchmark Report

- Date: 2026-08-06T14:48:41-05:00
- Tasks: 50000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_12-48-43_459381_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 50s |
| driver-measured wall | 17.7s |
| driver-measured rate | 2831.4 tasks/s |
| flush (cluster deletion incl. final upload) | 3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-c4plj | 1 | 63.19 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-2rgq7 | 5 | 87.35 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 109.3 | 0.000 | 0.000 |
| collector (head) | job | 50 | 121.4 | 0.343 | 0.664 |
| collector (worker) | flush | 1 | 80.4 | 0.000 | 0.000 |
| collector (worker) | job | 50 | 108.6 | 0.286 | 0.540 |
| historyserver | historyserver | 43 | 1030.2 | 1.171 | 1.726 |
| ray-head | job | 50 | 3889.5 | 0.595 | 0.933 |
| ray-worker | job | 50 | 997.3 | 0.875 | 1.991 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-68b567dcd9-624t6/historyserver | historyserver | 42 | 948.9 | 1183.3 | 1.176 | 1.713 |  |
| historyserver-demo-68b567dcd9-624t6/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1207.7 |
| raycluster-historyserver-cpu-worker-c4plj/collector | flush | 1 | 79.9 | 146.1 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-c4plj/collector | job | 48 | 112.6 | 164.4 | 0.237 | 0.967 |  |
| raycluster-historyserver-cpu-worker-c4plj/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 177.6 |
| raycluster-historyserver-cpu-worker-c4plj/ray-worker | job | 48 | 974.4 | 1014.2 | 0.797 | 2.092 |  |
| raycluster-historyserver-cpu-worker-c4plj/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1018.7 |
| raycluster-historyserver-head-2rgq7/collector | flush | 2 | 110.0 | 201.1 | 0.811 | 0.811 |  |
| raycluster-historyserver-head-2rgq7/collector | job | 48 | 146.5 | 221.1 | 0.333 | 1.102 |  |
| raycluster-historyserver-head-2rgq7/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 230.0 |
| raycluster-historyserver-head-2rgq7/ray-head | flush | 1 | 2227.0 | 2266.1 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-2rgq7/ray-head | job | 48 | 3832.2 | 3895.6 | 0.550 | 2.575 |  |
| raycluster-historyserver-head-2rgq7/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3899.9 |
| rayjob-bench-qm6wh/ray-job-submitter | job | 43 | 97.6 | 108.1 | 0.059 | 0.992 |  |
| rayjob-bench-qm6wh/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.4 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 153.05 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 150.54 MiB | 98.4% |
| logs | 2.50 MiB | 1.6% |
| node_events | 6.18 KiB | 0.0% |
| **total** | **153.05 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 217354 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 217342 |
| events per task (k) | 4.35 |
| raw JSONL bytes | 150.55 MiB |
| stored event bytes | 150.55 MiB |
| avg raw bytes/event | 726 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 50005 |
| distinct taskIds in benchmark job `03000000` | **50001 / 50000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 8f55beddd70788da163e9c4d666a9f5eae3fa90dd9303ccf1286602d | 117325 | 87.35 MiB | 50007 | 9938 | 7428.4 |
| d758c55f4ec6740c58865899de5ede51744c507b2c55be7e7bbb08f1 | 100029 | 63.19 MiB | 50000 | 7152 | 6431.4 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 3 |
| ACTOR_TASK_DEFINITION_EVENT | 2 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 50005 |
| TASK_LIFECYCLE_EVENT | 117281 |
| TASK_PROFILE_EVENT | 50054 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 5ms / 93ms / 93ms (errors: 0) |
| /enter_cluster cold load | 34.107s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 577ms | 630ms | 630ms | 408 B | 0 |
| /api/jobs/ | 13ms | 105ms | 105ms | 1.31 KiB | 0 |
| /nodes?view=summary | 102ms | 147ms | 147ms | 2.77 KiB | 0 |
| /events | 13ms | 108ms | 108ms | 2.12 KiB | 0 |

