# History Server Benchmark Report

- Date: 2026-08-06T15:03:38-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_13-03-40_447142_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m20s |
| driver-measured wall | 33.4s |
| driver-measured rate | 2990.9 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-x6p6d | 1 | 126.39 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-vclk6 | 6 | 173.53 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 80 | 128.6 | 0.409 | 0.670 |
| collector (worker) | job | 80 | 114.3 | 0.389 | 0.691 |
| historyserver | historyserver | 82 | 2779.4 | 1.033 | 1.709 |
| ray-head | job | 80 | 4359.0 | 0.618 | 1.210 |
| ray-worker | job | 80 | 1022.6 | 0.943 | 2.031 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-8556d857d6-hkzd6/historyserver | historyserver | 79 | 3214.8 | 3224.3 | 1.040 | 1.998 |  |
| historyserver-demo-8556d857d6-hkzd6/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3226.9 |
| raycluster-historyserver-cpu-worker-x6p6d/collector | job | 77 | 116.3 | 223.9 | 0.312 | 1.078 |  |
| raycluster-historyserver-cpu-worker-x6p6d/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 242.4 |
| raycluster-historyserver-cpu-worker-x6p6d/ray-worker | job | 77 | 1013.7 | 1055.8 | 0.922 | 2.102 |  |
| raycluster-historyserver-cpu-worker-x6p6d/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1058.5 |
| raycluster-historyserver-head-vclk6/collector | job | 77 | 144.4 | 281.0 | 0.425 | 1.862 |  |
| raycluster-historyserver-head-vclk6/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 301.1 |
| raycluster-historyserver-head-vclk6/ray-head | job | 77 | 4316.9 | 4387.4 | 0.619 | 1.999 |  |
| raycluster-historyserver-head-vclk6/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 4388.6 |
| rayjob-bench-nhzst/ray-job-submitter | job | 69 | 97.9 | 108.3 | 0.039 | 0.989 |  |
| rayjob-bench-nhzst/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 109.5 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 264.57 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 169 | 40.02 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 299.91 MiB | 98.5% |
| logs | 4.68 MiB | 1.5% |
| node_events | 6.91 KiB | 0.0% |
| **total** | **304.59 MiB** | (174 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 432957 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 432943 |
| events per task (k) | 4.33 |
| raw JSONL bytes | 299.91 MiB |
| stored event bytes | 299.91 MiB |
| avg raw bytes/event | 726 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 99528 |
| distinct taskIds in benchmark job `03000000` | **99524 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| e8425e1954a1f21d5fa87a9ce11f4acb5728537b3a42b76a8a91c425 | 232901 | 173.53 MiB | 99531 | 9954 | 7457.0 |
| 88969cfa435f760fd2668af2ea20a6595fd813e66e0f8b7b1d681d2e | 200056 | 126.39 MiB | 100000 | 7469 | 6494.7 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 3 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 99528 |
| TASK_LIFECYCLE_EVENT | 233323 |
| TASK_PROFILE_EVENT | 100089 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 5ms / 80ms / 80ms (errors: 0) |
| /enter_cluster cold load | 1m10.832s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 692ms | 1.261s | 1.261s | 549 B | 0 |
| /api/jobs/ | 26ms | 29ms | 29ms | 1.31 KiB | 0 |
| /nodes?view=summary | 22ms | 359ms | 359ms | 2.77 KiB | 0 |
| /events | 26ms | 266ms | 266ms | 3.20 KiB | 0 |

