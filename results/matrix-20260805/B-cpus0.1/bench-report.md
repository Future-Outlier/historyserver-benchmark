# History Server Benchmark Report

- Date: 2026-08-05T18:16:23-05:00
- Tasks: 20000 (wave 2000, num_cpus=0.1), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_16-16-24_799584_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 35s |
| driver-measured wall | 11.8s |
| driver-measured rate | 1699.7 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-qrqz2 | 0 | 0 B | 0 | 0 | 0 |
| raycluster-historyserver-head-t2rwc | 0 | 0 B | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 35 | 90.2 | 0.234 | 0.292 |
| collector (worker) | job | 35 | 114.1 | 0.238 | 0.265 |
| historyserver | historyserver | 54 | 452.9 | 0.476 | 0.527 |
| ray-head | job | 35 | 2739.4 | 0.427 | 0.619 |
| ray-worker | job | 35 | 1426.0 | 1.091 | 1.295 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-mvgwl/historyserver | historyserver | 52 | 492.7 | 519.5 | 0.476 | 0.528 |  |
| historyserver-demo-7f67bfd478-mvgwl/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 537.2 |
| raycluster-historyserver-cpu-worker-qrqz2/collector | job | 34 | 111.9 | 139.4 | 0.137 | 1.113 |  |
| raycluster-historyserver-cpu-worker-qrqz2/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 143.5 |
| raycluster-historyserver-cpu-worker-qrqz2/ray-worker | job | 34 | 1483.3 | 1533.6 | 0.795 | 2.117 |  |
| raycluster-historyserver-cpu-worker-qrqz2/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1534.6 |
| raycluster-historyserver-head-t2rwc/collector | job | 34 | 107.5 | 143.8 | 0.202 | 0.811 |  |
| raycluster-historyserver-head-t2rwc/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 149.1 |
| raycluster-historyserver-head-t2rwc/ray-head | job | 34 | 2694.0 | 2757.6 | 0.438 | 1.705 |  |
| raycluster-historyserver-head-t2rwc/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2765.1 |
| rayjob-bench-gntmh/ray-job-submitter | job | 28 | 97.6 | 108.0 | 0.098 | 0.987 |  |
| rayjob-bench-gntmh/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.4 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 222 | 62.73 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 61.48 MiB | 98.0% |
| logs | 1.24 MiB | 2.0% |
| node_events | 6.17 KiB | 0.0% |
| **total** | **62.73 MiB** | (223 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 89634 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 89622 |
| events per task (k) | 4.48 |
| raw JSONL bytes | 61.49 MiB |
| stored event bytes | 61.49 MiB |
| avg raw bytes/event | 719 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds | 20005 / 20000 expected |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 9c34fd3dacb606592d1005d658a577390664ac3470fe87e3a1d27410 | 49597 | 36.22 MiB | 20006 | 6844 | 4277.2 |
| 54e53d9f96f61ed529f22605a562e7963a37297548e10f28a7b1ee45 | 40037 | 25.27 MiB | 20000 | 5334 | 3603.0 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 3 |
| ACTOR_TASK_DEFINITION_EVENT | 1 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 20005 |
| TASK_LIFECYCLE_EVENT | 49575 |
| TASK_PROFILE_EVENT | 20041 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 213ms / 213ms (errors: 0) |
| /enter_cluster cold load | 41.288s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=20000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 862ms | 1.061s | 1.061s | 408 B | 0 |
| /api/jobs/ | 16ms | 196ms | 196ms | 1.22 KiB | 0 |
| /nodes?view=summary | 8ms | 338ms | 338ms | 2.76 KiB | 0 |
| /events | 97ms | 240ms | 240ms | 1.76 KiB | 0 |

