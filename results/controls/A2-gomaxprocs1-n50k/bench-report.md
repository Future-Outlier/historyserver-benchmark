# History Server Benchmark Report

- Date: 2026-08-06T12:27:35-05:00
- Tasks: 50000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_10-27-37_114092_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 55s |
| driver-measured wall | 17.8s |
| driver-measured rate | 2812.3 tasks/s |
| flush (cluster deletion incl. final upload) | 3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-pvrxv | 1 | 63.20 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-2bm9z | 5 | 88.01 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 138.6 | 0.000 | 0.000 |
| collector (head) | job | 55 | 138.6 | 0.323 | 0.642 |
| collector (worker) | flush | 1 | 116.4 | 0.000 | 0.000 |
| collector (worker) | job | 55 | 110.0 | 0.283 | 0.624 |
| historyserver | historyserver | 57 | 961.8 | 0.938 | 1.023 |
| ray-head | job | 55 | 3008.0 | 0.491 | 1.128 |
| ray-worker | job | 55 | 979.2 | 0.754 | 2.054 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-57cf774765-czrmq/historyserver | historyserver | 54 | 1064.1 | 1066.7 | 0.949 | 1.046 |  |
| historyserver-demo-57cf774765-czrmq/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1067.2 |
| raycluster-historyserver-cpu-worker-pvrxv/collector | flush | 2 | 113.2 | 179.3 | 1.016 | 1.016 |  |
| raycluster-historyserver-cpu-worker-pvrxv/collector | job | 52 | 111.7 | 173.2 | 0.215 | 0.980 |  |
| raycluster-historyserver-cpu-worker-pvrxv/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 183.7 |
| raycluster-historyserver-cpu-worker-pvrxv/ray-worker | flush | 1 | 102.6 | 117.6 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-pvrxv/ray-worker | job | 52 | 978.2 | 1020.8 | 0.740 | 2.074 |  |
| raycluster-historyserver-cpu-worker-pvrxv/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1022.7 |
| raycluster-historyserver-head-2bm9z/collector | flush | 2 | 137.6 | 229.6 | 1.134 | 1.134 |  |
| raycluster-historyserver-head-2bm9z/collector | job | 52 | 146.2 | 226.7 | 0.305 | 1.133 |  |
| raycluster-historyserver-head-2bm9z/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 230.6 |
| raycluster-historyserver-head-2bm9z/ray-head | flush | 1 | 2379.7 | 2434.3 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-2bm9z/ray-head | job | 52 | 2971.9 | 3033.9 | 0.523 | 1.862 |  |
| raycluster-historyserver-head-2bm9z/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3038.7 |
| rayjob-bench-vnhlb/ray-job-submitter | job | 44 | 97.6 | 108.0 | 0.061 | 0.990 |  |
| rayjob-bench-vnhlb/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.2 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 153.71 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 151.20 MiB | 98.4% |
| logs | 2.50 MiB | 1.6% |
| node_events | 6.91 KiB | 0.0% |
| **total** | **153.71 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 218754 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 218740 |
| events per task (k) | 4.37 |
| raw JSONL bytes | 151.21 MiB |
| stored event bytes | 151.21 MiB |
| avg raw bytes/event | 725 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 50005 |
| distinct taskIds in benchmark job `03000000` | **50001 / 50000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 56adb31adc97f332f59deae9cebc909a0424bfc8e38d12ee57401ca6 | 118715 | 88.01 MiB | 50007 | 9666 | 7481.8 |
| d6f23e132ddc385de5acda0f65027c1b4ba7d5a91416503fabdc93e3 | 100039 | 63.20 MiB | 50000 | 7027 | 6353.8 |

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
| TASK_LIFECYCLE_EVENT | 118684 |
| TASK_PROFILE_EVENT | 50049 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 88ms / 88ms (errors: 0) |
| /enter_cluster cold load | 38.927s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 1.228s | 1.576s | 1.576s | 408 B | 0 |
| /api/jobs/ | 18ms | 287ms | 287ms | 1.31 KiB | 0 |
| /nodes?view=summary | 21ms | 236ms | 236ms | 2.77 KiB | 0 |
| /events | 199ms | 236ms | 236ms | 2.12 KiB | 0 |

