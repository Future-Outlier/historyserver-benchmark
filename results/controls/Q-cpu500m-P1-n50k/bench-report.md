# History Server Benchmark Report

- Date: 2026-08-06T14:50:45-05:00
- Tasks: 50000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_12-50-46_745753_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 50s |
| driver-measured wall | 17.2s |
| driver-measured rate | 2912.5 tasks/s |
| flush (cluster deletion incl. final upload) | 3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-fzjdw | 1 | 63.20 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-scg8b | 5 | 87.53 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 114.1 | 0.000 | 0.000 |
| collector (head) | job | 50 | 146.6 | 0.358 | 0.632 |
| collector (worker) | flush | 1 | 85.3 | 0.000 | 0.000 |
| collector (worker) | job | 50 | 85.3 | 0.282 | 0.576 |
| historyserver | historyserver | 121 | 1247.8 | 0.488 | 0.531 |
| ray-head | job | 50 | 3930.5 | 0.544 | 1.136 |
| ray-worker | job | 50 | 920.7 | 1.065 | 1.717 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7d5ff8867f-lt4mh/historyserver | historyserver | 116 | 1245.7 | 1248.7 | 0.490 | 0.520 |  |
| historyserver-demo-7d5ff8867f-lt4mh/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1249.2 |
| raycluster-historyserver-cpu-worker-fzjdw/collector | flush | 1 | 85.2 | 151.4 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-fzjdw/collector | job | 48 | 116.1 | 175.5 | 0.235 | 0.957 |  |
| raycluster-historyserver-cpu-worker-fzjdw/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 180.4 |
| raycluster-historyserver-cpu-worker-fzjdw/ray-worker | job | 48 | 969.9 | 1009.7 | 0.784 | 2.091 |  |
| raycluster-historyserver-cpu-worker-fzjdw/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1020.0 |
| raycluster-historyserver-head-scg8b/collector | flush | 2 | 115.0 | 208.8 | 0.920 | 0.920 |  |
| raycluster-historyserver-head-scg8b/collector | job | 48 | 128.7 | 211.0 | 0.342 | 1.039 |  |
| raycluster-historyserver-head-scg8b/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 225.9 |
| raycluster-historyserver-head-scg8b/ray-head | flush | 1 | 2239.5 | 2279.4 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-scg8b/ray-head | job | 48 | 3876.3 | 3940.6 | 0.552 | 2.120 |  |
| raycluster-historyserver-head-scg8b/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3945.9 |
| rayjob-bench-j85rp/ray-job-submitter | job | 42 | 97.6 | 108.1 | 0.056 | 0.893 |  |
| rayjob-bench-j85rp/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.6 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 153.23 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 150.72 MiB | 98.4% |
| logs | 2.50 MiB | 1.6% |
| node_events | 6.91 KiB | 0.0% |
| **total** | **153.23 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 217723 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 217709 |
| events per task (k) | 4.35 |
| raw JSONL bytes | 150.72 MiB |
| stored event bytes | 150.72 MiB |
| avg raw bytes/event | 726 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 50005 |
| distinct taskIds in benchmark job `03000000` | **50001 / 50000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 12e88598328ea8a16a1bd457dcfdc8a4bbfdddc4bc61bbdeb9c58d44 | 117691 | 87.53 MiB | 50007 | 9888 | 7445.2 |
| f2bfeb13f06f149b041d9257d4a9759af23578db54d5536a503e6a0b | 100032 | 63.20 MiB | 50000 | 7194 | 6472.0 |

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
| TASK_LIFECYCLE_EVENT | 117647 |
| TASK_PROFILE_EVENT | 50055 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 5ms / 148ms / 148ms (errors: 0) |
| /enter_cluster cold load | 1m22.084s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 3.179s | 4.204s | 4.204s | 408 B | 0 |
| /api/jobs/ | 60ms | 765ms | 765ms | 1.31 KiB | 0 |
| /nodes?view=summary | 58ms | 500ms | 500ms | 2.77 KiB | 0 |
| /events | 376ms | 522ms | 522ms | 2.12 KiB | 0 |

