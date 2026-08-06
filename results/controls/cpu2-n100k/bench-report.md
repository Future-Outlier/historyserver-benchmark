# History Server Benchmark Report

- Date: 2026-08-06T14:34:10-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_12-34-12_451820_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m15s |
| driver-measured wall | 34.6s |
| driver-measured rate | 2890.6 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-bjbrl | 1 | 126.39 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-cc4st | 6 | 175.36 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 75 | 134.9 | 0.571 | 0.699 |
| collector (worker) | job | 75 | 110.3 | 0.354 | 0.649 |
| historyserver | historyserver | 81 | 1996.6 | 1.168 | 1.567 |
| ray-head | job | 75 | 3964.9 | 0.626 | 1.165 |
| ray-worker | job | 75 | 1032.5 | 1.113 | 1.959 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-6f87d5d7c9-zfptt/historyserver | historyserver | 78 | 1639.3 | 2118.4 | 1.188 | 1.968 |  |
| historyserver-demo-6f87d5d7c9-zfptt/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2120.7 |
| raycluster-historyserver-cpu-worker-bjbrl/collector | job | 72 | 116.6 | 230.0 | 0.337 | 1.106 |  |
| raycluster-historyserver-cpu-worker-bjbrl/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 236.6 |
| raycluster-historyserver-cpu-worker-bjbrl/ray-worker | job | 72 | 1018.1 | 1060.0 | 1.017 | 2.108 |  |
| raycluster-historyserver-cpu-worker-bjbrl/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1064.3 |
| raycluster-historyserver-head-cc4st/collector | flush | 1 | 97.0 | 136.1 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-cc4st/collector | job | 72 | 145.5 | 293.8 | 0.474 | 1.841 |  |
| raycluster-historyserver-head-cc4st/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 294.2 |
| raycluster-historyserver-head-cc4st/ray-head | job | 72 | 3924.7 | 3989.3 | 0.675 | 1.942 |  |
| raycluster-historyserver-head-cc4st/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3993.6 |
| rayjob-bench-pg7gg/ray-job-submitter | job | 68 | 97.7 | 108.3 | 0.037 | 0.936 |  |
| rayjob-bench-pg7gg/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.4 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 268.97 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 169 | 37.46 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 301.75 MiB | 98.5% |
| logs | 4.67 MiB | 1.5% |
| node_events | 6.18 KiB | 0.0% |
| **total** | **306.42 MiB** | (174 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 436126 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 436114 |
| events per task (k) | 4.36 |
| raw JSONL bytes | 301.75 MiB |
| stored event bytes | 301.75 MiB |
| avg raw bytes/event | 726 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 4950ed230287db4dcc8d30314a954121e25da25c9500e67508d45957 | 236056 | 175.36 MiB | 100007 | 9689 | 7391.0 |
| c214e27c151477dfb50e5e22a616c51d498ff91d8a429add70f1b791 | 200070 | 126.39 MiB | 100000 | 6848 | 6242.6 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 3 |
| ACTOR_TASK_DEFINITION_EVENT | 2 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 100005 |
| TASK_LIFECYCLE_EVENT | 236017 |
| TASK_PROFILE_EVENT | 100090 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 87ms / 87ms (errors: 0) |
| /enter_cluster cold load | 1m4.382s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 1.199s | 1.581s | 1.581s | 411 B | 0 |
| /api/jobs/ | 24ms | 325ms | 325ms | 1.31 KiB | 0 |
| /nodes?view=summary | 216ms | 265ms | 265ms | 2.77 KiB | 0 |
| /events | 24ms | 210ms | 210ms | 3.20 KiB | 0 |

