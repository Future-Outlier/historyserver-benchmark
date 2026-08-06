# History Server Benchmark Report

- Date: 2026-08-05T20:55:58-05:00
- Tasks: 5000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_18-56-00_264476_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 30s |
| driver-measured wall | 3.2s |
| driver-measured rate | 1552.7 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-bpgr6 | 1 | 6.31 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-7d9j8 | 5 | 9.32 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 30 | 72.5 | 0.070 | 0.149 |
| collector (worker) | job | 30 | 75.8 | 0.046 | 0.109 |
| historyserver | historyserver | 8 | 199.4 | 0.802 | 1.204 |
| ray-head | job | 30 | 3353.9 | 0.321 | 0.430 |
| ray-worker | job | 30 | 427.3 | 0.427 | 0.449 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-d978c6465-gpd78/historyserver | historyserver | 7 | 197.1 | 199.1 | 0.831 | 1.223 |  |
| historyserver-demo-d978c6465-gpd78/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 200.8 |
| raycluster-historyserver-cpu-worker-bpgr6/collector | job | 29 | 74.7 | 82.2 | 0.041 | 0.429 |  |
| raycluster-historyserver-cpu-worker-bpgr6/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 82.7 |
| raycluster-historyserver-cpu-worker-bpgr6/ray-worker | job | 29 | 896.0 | 934.3 | 0.300 | 2.116 |  |
| raycluster-historyserver-cpu-worker-bpgr6/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 938.4 |
| raycluster-historyserver-head-7d9j8/collector | flush | 1 | 64.0 | 65.0 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-7d9j8/collector | job | 29 | 93.4 | 103.0 | 0.062 | 0.714 |  |
| raycluster-historyserver-head-7d9j8/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 106.3 |
| raycluster-historyserver-head-7d9j8/ray-head | flush | 1 | 388.5 | 594.2 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-7d9j8/ray-head | job | 29 | 3294.7 | 3360.6 | 0.289 | 1.368 |  |
| raycluster-historyserver-head-7d9j8/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3362.7 |
| rayjob-bench-znchv/ray-job-submitter | job | 24 | 97.6 | 108.1 | 0.099 | 0.921 |  |
| rayjob-bench-znchv/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.4 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 16.16 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 15.63 MiB | 96.7% |
| logs | 532.82 KiB | 3.2% |
| node_events | 6.17 KiB | 0.0% |
| **total** | **16.16 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 22949 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 22937 |
| events per task (k) | 4.59 |
| raw JSONL bytes | 15.64 MiB |
| stored event bytes | 15.64 MiB |
| avg raw bytes/event | 714 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 5005 |
| distinct taskIds in benchmark job `03000000` | **5001 / 5000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 26085db6a81143b18ffda5745975e514f8023cb63918b528806ae9bf | 12946 | 9.32 MiB | 5006 | 4957 | 1291.8 |
| 7c3fa43b39f200bd16a54924869b0afa6b6415bb08f72c2c2f06affd | 10003 | 6.31 MiB | 5000 | 3423 | 1000.3 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 3 |
| ACTOR_TASK_DEFINITION_EVENT | 1 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 5005 |
| TASK_LIFECYCLE_EVENT | 12912 |
| TASK_PROFILE_EVENT | 5019 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 5ms / 87ms / 87ms (errors: 0) |
| /enter_cluster cold load | 3.175s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=5000 | 168ms | 176ms | 176ms | 2.16 MiB | 0 |
| /api/v0/tasks/summarize | 37ms | 42ms | 42ms | 658 B | 0 |
| /api/jobs/ | 2ms | 15ms | 15ms | 1.31 KiB | 0 |
| /nodes?view=summary | 2ms | 12ms | 12ms | 2.76 KiB | 0 |
| /events | 2ms | 11ms | 11ms | 1.04 KiB | 0 |

