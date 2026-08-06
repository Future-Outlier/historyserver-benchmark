# History Server Benchmark Report

- Date: 2026-08-05T18:03:06-05:00
- Tasks: 50000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_16-03-08_488728_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 40s |
| driver-measured wall | 17.5s |
| driver-measured rate | 2855.3 tasks/s |
| flush (cluster deletion incl. final upload) | 3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-vpxmw | 0 | 0 B | 0 | 0 | 0 |
| raycluster-historyserver-head-2sqj2 | 0 | 0 B | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 97.8 | 0.000 | 0.000 |
| collector (head) | job | 40 | 133.0 | 0.425 | 0.644 |
| collector (worker) | flush | 1 | 110.9 | 0.000 | 0.000 |
| collector (worker) | job | 40 | 115.3 | 0.293 | 0.647 |
| historyserver | historyserver | 125 | 952.6 | 0.490 | 0.515 |
| ray-head | job | 40 | 2903.1 | 0.840 | 0.908 |
| ray-worker | job | 40 | 804.5 | 1.650 | 2.014 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-6ffn5/historyserver | historyserver | 121 | 1131.7 | 1173.6 | 0.490 | 0.535 |  |
| historyserver-demo-7f67bfd478-6ffn5/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1187.6 |
| raycluster-historyserver-cpu-worker-vpxmw/collector | flush | 1 | 110.3 | 176.4 | 0.817 | 0.817 |  |
| raycluster-historyserver-cpu-worker-vpxmw/collector | job | 39 | 113.7 | 174.1 | 0.288 | 1.052 |  |
| raycluster-historyserver-cpu-worker-vpxmw/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 179.9 |
| raycluster-historyserver-cpu-worker-vpxmw/ray-worker | job | 39 | 967.7 | 1007.6 | 0.984 | 2.117 |  |
| raycluster-historyserver-cpu-worker-vpxmw/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1021.1 |
| raycluster-historyserver-head-2sqj2/collector | flush | 1 | 94.0 | 185.9 | 0.804 | 0.804 |  |
| raycluster-historyserver-head-2sqj2/collector | job | 39 | 144.6 | 218.3 | 0.407 | 1.172 |  |
| raycluster-historyserver-head-2sqj2/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 230.7 |
| raycluster-historyserver-head-2sqj2/ray-head | job | 39 | 2869.3 | 2933.9 | 0.671 | 2.268 |  |
| raycluster-historyserver-head-2sqj2/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2939.0 |
| rayjob-bench-mc7hd/ray-job-submitter | job | 35 | 97.6 | 108.1 | 0.070 | 0.935 |  |
| rayjob-bench-mc7hd/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.4 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 153.33 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 150.90 MiB | 98.4% |
| logs | 2.42 MiB | 1.6% |
| node_events | 6.90 KiB | 0.0% |
| **total** | **153.33 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 218214 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 218200 |
| events per task (k) | 4.36 |
| raw JSONL bytes | 150.91 MiB |
| stored event bytes | 150.91 MiB |
| avg raw bytes/event | 725 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds | 50005 / 50000 expected |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 2450ba751774344b218a65a3b171d94208899e5f64595d4c7a8d89b9 | 118177 | 87.76 MiB | 50006 | 9943 | 7484.6 |
| 2fae99d61695221e03ac523f63f1aa960c02996ec7a6dfe1f4d9cc2c | 100037 | 63.15 MiB | 50000 | 7238 | 6432.6 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 1 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 50005 |
| TASK_LIFECYCLE_EVENT | 118149 |
| TASK_PROFILE_EVENT | 50045 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 5ms / 142ms / 142ms (errors: 0) |
| /enter_cluster cold load | 1m37.847s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 2.286s | 3.49s | 3.49s | 408 B | 0 |
| /api/jobs/ | 46ms | 542ms | 542ms | 1.22 KiB | 0 |
| /nodes?view=summary | 89ms | 587ms | 587ms | 2.76 KiB | 0 |
| /events | 38ms | 598ms | 598ms | 2.13 KiB | 0 |

