# History Server Benchmark Report

- Date: 2026-08-06T14:54:06-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_12-54-08_819368_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m15s |
| driver-measured wall | 33.6s |
| driver-measured rate | 2978.4 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-n6lzs | 1 | 126.39 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-jnls9 | 6 | 174.51 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 75 | 129.9 | 0.500 | 0.766 |
| collector (worker) | job | 75 | 115.1 | 0.418 | 0.694 |
| historyserver | historyserver | 79 | 1991.0 | 1.079 | 1.448 |
| ray-head | job | 75 | 4234.2 | 0.706 | 1.127 |
| ray-worker | job | 75 | 1017.2 | 1.187 | 1.960 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7b5f88f779-6k2gt/historyserver | historyserver | 77 | 1657.6 | 2024.9 | 1.128 | 1.950 |  |
| historyserver-demo-7b5f88f779-6k2gt/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2072.6 |
| raycluster-historyserver-cpu-worker-n6lzs/collector | flush | 1 | 86.2 | 87.2 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-n6lzs/collector | job | 72 | 117.2 | 235.6 | 0.337 | 1.090 |  |
| raycluster-historyserver-cpu-worker-n6lzs/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 239.8 |
| raycluster-historyserver-cpu-worker-n6lzs/ray-worker | job | 72 | 1017.9 | 1060.9 | 0.993 | 2.119 |  |
| raycluster-historyserver-cpu-worker-n6lzs/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1068.6 |
| raycluster-historyserver-head-jnls9/collector | flush | 1 | 106.0 | 142.8 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-jnls9/collector | job | 72 | 145.7 | 282.6 | 0.463 | 1.514 |  |
| raycluster-historyserver-head-jnls9/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 303.8 |
| raycluster-historyserver-head-jnls9/ray-head | flush | 1 | 2285.7 | 2333.2 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-jnls9/ray-head | job | 72 | 4177.2 | 4248.5 | 0.658 | 1.788 |  |
| raycluster-historyserver-head-jnls9/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 4252.5 |
| rayjob-bench-lv6h5/ray-job-submitter | job | 68 | 97.9 | 108.3 | 0.037 | 0.953 |  |
| rayjob-bench-lv6h5/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 109.9 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 266.32 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 169 | 39.24 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 300.89 MiB | 98.5% |
| logs | 4.67 MiB | 1.5% |
| node_events | 6.91 KiB | 0.0% |
| **total** | **305.57 MiB** | (174 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 434339 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 434325 |
| events per task (k) | 4.34 |
| raw JSONL bytes | 300.90 MiB |
| stored event bytes | 300.90 MiB |
| avg raw bytes/event | 726 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 68971b40ae6eba0fa7bf142727e826088839cc90d37c3f91499ba1c7 | 234279 | 174.51 MiB | 100007 | 9740 | 7496.5 |
| e036c20d734d78e8614e412aece6cdaa74d7b5d1232b328b7a76e887 | 200060 | 126.39 MiB | 100000 | 7286 | 6363.8 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 2 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 100005 |
| TASK_LIFECYCLE_EVENT | 234228 |
| TASK_PROFILE_EVENT | 100090 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 6ms / 84ms / 84ms (errors: 0) |
| /enter_cluster cold load | 1m4.574s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 1.014s | 1.039s | 1.039s | 411 B | 0 |
| /api/jobs/ | 25ms | 318ms | 318ms | 1.31 KiB | 0 |
| /nodes?view=summary | 25ms | 327ms | 327ms | 2.77 KiB | 0 |
| /events | 25ms | 319ms | 319ms | 3.21 KiB | 0 |

