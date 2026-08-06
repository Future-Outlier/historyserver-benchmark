# History Server Benchmark Report

- Date: 2026-08-06T13:29:48-05:00
- Tasks: 50000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_11-29-50_163143_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 55s |
| driver-measured wall | 20.7s |
| driver-measured rate | 2411.8 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-8mvkw | 1 | 63.20 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-rcx62 | 5 | 86.66 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 99.2 | 0.000 | 0.000 |
| collector (head) | job | 55 | 126.4 | 0.278 | 0.708 |
| collector (worker) | flush | 1 | 86.7 | 0.000 | 0.000 |
| collector (worker) | job | 55 | 113.3 | 0.275 | 0.486 |
| historyserver | historyserver | 47 | 981.5 | 1.144 | 2.007 |
| ray-head | job | 55 | 3598.8 | 0.717 | 1.169 |
| ray-worker | job | 55 | 982.9 | 0.894 | 2.035 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7768d9678d-gpttn/historyserver | historyserver | 45 | 992.2 | 1113.1 | 1.186 | 2.100 |  |
| historyserver-demo-7768d9678d-gpttn/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1119.0 |
| raycluster-historyserver-cpu-worker-8mvkw/collector | flush | 1 | 77.6 | 143.7 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-8mvkw/collector | job | 53 | 114.6 | 172.5 | 0.220 | 0.951 |  |
| raycluster-historyserver-cpu-worker-8mvkw/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 179.0 |
| raycluster-historyserver-cpu-worker-8mvkw/ray-worker | job | 53 | 966.5 | 1006.4 | 0.848 | 2.093 |  |
| raycluster-historyserver-cpu-worker-8mvkw/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1011.9 |
| raycluster-historyserver-head-rcx62/collector | flush | 1 | 95.8 | 186.2 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-rcx62/collector | job | 53 | 146.9 | 225.0 | 0.314 | 1.128 |  |
| raycluster-historyserver-head-rcx62/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 228.1 |
| raycluster-historyserver-head-rcx62/ray-head | job | 53 | 3558.1 | 3622.8 | 0.576 | 2.013 |  |
| raycluster-historyserver-head-rcx62/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3627.8 |
| rayjob-bench-ldjmz/ray-job-submitter | job | 46 | 97.8 | 108.3 | 0.063 | 0.994 |  |
| rayjob-bench-ldjmz/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.1 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 152.37 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 149.86 MiB | 98.4% |
| logs | 2.50 MiB | 1.6% |
| node_events | 6.91 KiB | 0.0% |
| **total** | **152.37 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 215924 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 215910 |
| events per task (k) | 4.32 |
| raw JSONL bytes | 149.86 MiB |
| stored event bytes | 149.86 MiB |
| avg raw bytes/event | 728 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 50005 |
| distinct taskIds in benchmark job `03000000` | **50001 / 50000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 1146fcc0aec5e89820835354ee892566757e113d0335c92df43be01e | 115881 | 86.66 MiB | 50007 | 9283 | 6449.2 |
| b10b09e8fde19ab265e80557918ffa0c24eb96713f2345ac97ee41f5 | 100043 | 63.20 MiB | 50000 | 5930 | 5387.3 |

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
| TASK_LIFECYCLE_EVENT | 115845 |
| TASK_PROFILE_EVENT | 50058 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 9ms / 119ms / 119ms (errors: 0) |
| /enter_cluster cold load | 39.265s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 464ms | 532ms | 532ms | 408 B | 0 |
| /api/jobs/ | 13ms | 120ms | 120ms | 1.31 KiB | 0 |
| /nodes?view=summary | 12ms | 115ms | 115ms | 2.77 KiB | 0 |
| /events | 13ms | 123ms | 123ms | 2.12 KiB | 0 |

