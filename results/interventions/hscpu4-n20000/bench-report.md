# History Server Benchmark Report

- Date: 2026-08-05T20:58:18-05:00
- Tasks: 20000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_18-58-19_916801_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 35s |
| driver-measured wall | 7.7s |
| driver-measured rate | 2582.8 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-269fr | 1 | 25.28 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-nznss | 5 | 35.12 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 35 | 103.7 | 0.194 | 0.347 |
| collector (worker) | job | 35 | 107.2 | 0.247 | 0.247 |
| historyserver | historyserver | 17 | 513.0 | 1.039 | 1.431 |
| ray-head | job | 35 | 3502.6 | 0.511 | 0.537 |
| ray-worker | job | 35 | 651.9 | 0.628 | 1.816 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-d978c6465-hnbj9/historyserver | historyserver | 17 | 510.2 | 512.8 | 1.133 | 1.824 |  |
| historyserver-demo-d978c6465-hnbj9/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 514.7 |
| raycluster-historyserver-cpu-worker-269fr/collector | job | 34 | 111.4 | 133.6 | 0.136 | 0.833 |  |
| raycluster-historyserver-cpu-worker-269fr/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 139.2 |
| raycluster-historyserver-cpu-worker-269fr/ray-worker | job | 34 | 936.0 | 974.3 | 0.529 | 2.091 |  |
| raycluster-historyserver-cpu-worker-269fr/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 977.6 |
| raycluster-historyserver-head-nznss/collector | job | 34 | 115.4 | 145.1 | 0.201 | 1.112 |  |
| raycluster-historyserver-head-nznss/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 173.0 |
| raycluster-historyserver-head-nznss/ray-head | job | 34 | 3448.6 | 3509.3 | 0.407 | 1.149 |  |
| raycluster-historyserver-head-nznss/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3513.1 |
| rayjob-bench-k7tld/ray-job-submitter | job | 29 | 97.6 | 108.1 | 0.084 | 0.961 |  |
| rayjob-bench-k7tld/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.0 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 61.55 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 60.39 MiB | 98.1% |
| logs | 1.15 MiB | 1.9% |
| node_events | 6.18 KiB | 0.0% |
| **total** | **61.55 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 87301 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 87289 |
| events per task (k) | 4.36 |
| raw JSONL bytes | 60.40 MiB |
| stored event bytes | 60.40 MiB |
| avg raw bytes/event | 725 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 20005 |
| distinct taskIds in benchmark job `03000000` | **20001 / 20000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 5778df9cc89adaf71da152c6abeb44b0580aea35a10b70c9a38e0dcd | 47289 | 35.12 MiB | 20006 | 9986 | 4725.7 |
| ed5810c7a138b31ea8a44f50dcc224bd4d7b205b0d9d7a68b13f8856 | 40012 | 25.28 MiB | 20000 | 6799 | 4001.2 |

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
| TASK_LIFECYCLE_EVENT | 47252 |
| TASK_PROFILE_EVENT | 20031 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 6ms / 87ms / 87ms (errors: 0) |
| /enter_cluster cold load | 14.449s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=20000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 128ms | 171ms | 171ms | 408 B | 0 |
| /api/jobs/ | 6ms | 41ms | 41ms | 1.31 KiB | 0 |
| /nodes?view=summary | 5ms | 44ms | 44ms | 2.77 KiB | 0 |
| /events | 4ms | 43ms | 43ms | 1.40 KiB | 0 |

