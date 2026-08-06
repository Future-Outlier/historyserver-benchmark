# History Server Benchmark Report

- Date: 2026-08-05T20:57:03-05:00
- Tasks: 10000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_18-57-05_179405_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 35s |
| driver-measured wall | 4.8s |
| driver-measured rate | 2078.0 tasks/s |
| flush (cluster deletion incl. final upload) | 1s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-rn2pv | 1 | 12.63 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-z6tjf | 5 | 17.45 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 35 | 90.7 | 0.100 | 0.171 |
| collector (worker) | job | 35 | 96.6 | 0.074 | 0.135 |
| historyserver | historyserver | 14 | 265.1 | 0.951 | 1.277 |
| ray-head | job | 35 | 3389.3 | 0.274 | 0.365 |
| ray-worker | job | 35 | 464.6 | 0.440 | 1.095 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-d978c6465-jbrwm/historyserver | historyserver | 13 | 224.1 | 274.1 | 1.051 | 1.444 |  |
| historyserver-demo-d978c6465-jbrwm/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 291.5 |
| raycluster-historyserver-cpu-worker-rn2pv/collector | job | 34 | 95.2 | 109.2 | 0.067 | 0.668 |  |
| raycluster-historyserver-cpu-worker-rn2pv/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 109.7 |
| raycluster-historyserver-cpu-worker-rn2pv/ray-worker | job | 34 | 908.5 | 948.8 | 0.336 | 2.080 |  |
| raycluster-historyserver-cpu-worker-rn2pv/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 956.8 |
| raycluster-historyserver-head-z6tjf/collector | job | 34 | 97.2 | 112.9 | 0.095 | 0.647 |  |
| raycluster-historyserver-head-z6tjf/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 119.1 |
| raycluster-historyserver-head-z6tjf/ray-head | job | 34 | 3329.5 | 3396.4 | 0.295 | 1.148 |  |
| raycluster-historyserver-head-z6tjf/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3400.0 |
| rayjob-bench-956sp/ray-job-submitter | job | 26 | 97.6 | 108.1 | 0.100 | 0.988 |  |
| rayjob-bench-956sp/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 111.2 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 30.81 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 30.08 MiB | 97.6% |
| logs | 748.04 KiB | 2.4% |
| node_events | 6.54 KiB | 0.0% |
| **total** | **30.81 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 43420 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 43407 |
| events per task (k) | 4.34 |
| raw JSONL bytes | 30.08 MiB |
| stored event bytes | 30.08 MiB |
| avg raw bytes/event | 726 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 10005 |
| distinct taskIds in benchmark job `03000000` | **10001 / 10000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 7e7cc65f4c44890445e2880265f0c79077d40153d102de6484dc5c06 | 23417 | 17.45 MiB | 10006 | 7673 | 2338.6 |
| f0f08fede024f91e9fa4f0e7ed290e17c36ac1f94a4c08279cea4bad | 20003 | 12.63 MiB | 10000 | 5807 | 2000.3 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 4 |
| ACTOR_TASK_DEFINITION_EVENT | 1 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 10005 |
| TASK_LIFECYCLE_EVENT | 23376 |
| TASK_PROFILE_EVENT | 10025 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 5ms / 98ms / 98ms (errors: 0) |
| /enter_cluster cold load | 6.254s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=10000 | 412ms | 472ms | 472ms | 4.32 MiB | 0 |
| /api/v0/tasks/summarize | 72ms | 84ms | 84ms | 533 B | 0 |
| /api/jobs/ | 3ms | 20ms | 20ms | 1.31 KiB | 0 |
| /nodes?view=summary | 3ms | 21ms | 21ms | 2.76 KiB | 0 |
| /events | 3ms | 25ms | 25ms | 1.04 KiB | 0 |

