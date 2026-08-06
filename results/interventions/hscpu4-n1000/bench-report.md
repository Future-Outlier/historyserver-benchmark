# History Server Benchmark Report

- Date: 2026-08-05T20:54:55-05:00
- Tasks: 1000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_18-54-57_406262_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 30s |
| driver-measured wall | 1.1s |
| driver-measured rate | 910.0 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-qh9bq | 1 | 1.26 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-pxh2m | 5 | 1.74 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 30 | 73.6 | 0.013 | 0.019 |
| collector (worker) | job | 30 | 65.4 | 0.017 | 0.023 |
| historyserver | historyserver | 4 | 99.4 | 0.339 | 0.819 |
| ray-head | job | 30 | 3126.7 | 0.185 | 0.185 |
| ray-worker | job | 30 | 536.6 | 0.175 | 1.713 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-d978c6465-xqrzz/historyserver | historyserver | 4 | 97.4 | 99.4 | 0.377 | 0.774 |  |
| historyserver-demo-d978c6465-xqrzz/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 99.7 |
| raycluster-historyserver-cpu-worker-qh9bq/collector | job | 29 | 64.4 | 66.7 | 0.010 | 0.251 |  |
| raycluster-historyserver-cpu-worker-qh9bq/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 68.5 |
| raycluster-historyserver-cpu-worker-qh9bq/ray-worker | job | 29 | 557.9 | 586.8 | 0.118 | 2.009 |  |
| raycluster-historyserver-cpu-worker-qh9bq/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 609.5 |
| raycluster-historyserver-head-pxh2m/collector | job | 29 | 72.5 | 75.3 | 0.013 | 0.312 |  |
| raycluster-historyserver-head-pxh2m/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 76.5 |
| raycluster-historyserver-head-pxh2m/ray-head | job | 29 | 3206.7 | 3274.9 | 0.216 | 1.192 |  |
| raycluster-historyserver-head-pxh2m/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3278.4 |
| rayjob-bench-g2qzf/ray-job-submitter | job | 23 | 97.8 | 108.3 | 0.132 | 0.996 |  |
| rayjob-bench-g2qzf/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.2 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 147 | 3.31 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 2.99 MiB | 90.4% |
| logs | 318.41 KiB | 9.4% |
| node_events | 6.90 KiB | 0.2% |
| **total** | **3.31 MiB** | (148 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 4309 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 4295 |
| events per task (k) | 4.29 |
| raw JSONL bytes | 3.00 MiB |
| stored event bytes | 3.00 MiB |
| avg raw bytes/event | 730 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 1005 |
| distinct taskIds in benchmark job `03000000` | **1001 / 1000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 9bfd43848aef3ac5b57cd2f7583dba8d59df37d863e0ac7f503cc6a9 | 2309 | 1.74 MiB | 1006 | 1268 | 228.1 |
| 789c2885fa6b5696496d21396028c4a35cc2af95cac0cbfed6d900a4 | 2000 | 1.26 MiB | 1000 | 2000 | 200.0 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 1 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 1005 |
| TASK_LIFECYCLE_EVENT | 2274 |
| TASK_PROFILE_EVENT | 1015 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 5ms / 93ms / 93ms (errors: 0) |
| /enter_cluster cold load | 689ms (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=1000 | 36ms | 40ms | 40ms | 442.60 KiB | 0 |
| /api/v0/tasks/summarize | 5ms | 13ms | 13ms | 658 B | 0 |
| /api/jobs/ | 1ms | 2ms | 2ms | 1.31 KiB | 0 |
| /nodes?view=summary | 2ms | 5ms | 5ms | 2.76 KiB | 0 |
| /events | 1ms | 2ms | 2ms | 1.04 KiB | 0 |

