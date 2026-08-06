# History Server Benchmark Report

- Date: 2026-08-05T18:18:22-05:00
- Tasks: 20000 (wave 2000, num_cpus=0.05), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_16-18-24_590270_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 35s |
| driver-measured wall | 12.1s |
| driver-measured rate | 1659.4 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-6cz42 | 0 | 0 B | 0 | 0 | 0 |
| raycluster-historyserver-head-4mcds | 0 | 0 B | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 35 | 92.8 | 0.210 | 0.334 |
| collector (worker) | job | 35 | 118.1 | 0.182 | 0.182 |
| historyserver | historyserver | 52 | 435.7 | 0.497 | 0.509 |
| ray-head | job | 35 | 2683.7 | 0.413 | 0.534 |
| ray-worker | job | 35 | 486.3 | 0.898 | 1.535 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-bzk22/historyserver | historyserver | 49 | 494.0 | 532.7 | 0.489 | 0.529 |  |
| historyserver-demo-7f67bfd478-bzk22/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 533.8 |
| raycluster-historyserver-cpu-worker-6cz42/collector | flush | 1 | 116.0 | 117.2 | 0.591 | 0.591 |  |
| raycluster-historyserver-cpu-worker-6cz42/collector | job | 34 | 113.6 | 140.7 | 0.134 | 0.916 |  |
| raycluster-historyserver-cpu-worker-6cz42/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 147.9 |
| raycluster-historyserver-cpu-worker-6cz42/ray-worker | job | 34 | 1975.9 | 2049.0 | 0.863 | 2.125 |  |
| raycluster-historyserver-cpu-worker-6cz42/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2053.4 |
| raycluster-historyserver-head-4mcds/collector | flush | 1 | 93.0 | 132.2 | 0.630 | 0.630 |  |
| raycluster-historyserver-head-4mcds/collector | job | 34 | 105.0 | 129.8 | 0.202 | 0.906 |  |
| raycluster-historyserver-head-4mcds/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 134.7 |
| raycluster-historyserver-head-4mcds/ray-head | flush | 1 | 914.1 | 939.8 | 2.021 | 2.021 |  |
| raycluster-historyserver-head-4mcds/ray-head | job | 34 | 2631.7 | 2695.3 | 0.414 | 1.568 |  |
| raycluster-historyserver-head-4mcds/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2698.2 |
| rayjob-bench-9njm6/ray-job-submitter | job | 29 | 97.6 | 108.1 | 0.086 | 0.964 |  |
| rayjob-bench-9njm6/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 109.9 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 267 | 63.61 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 62.28 MiB | 97.9% |
| logs | 1.32 MiB | 2.1% |
| node_events | 6.90 KiB | 0.0% |
| **total** | **63.61 MiB** | (268 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 91269 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 91255 |
| events per task (k) | 4.56 |
| raw JSONL bytes | 62.29 MiB |
| stored event bytes | 62.29 MiB |
| avg raw bytes/event | 716 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds | 20005 / 20000 expected |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 5728b854338ae256fba7ddb6a4c7544bd1b89f01fb18901665dbf3b4 | 51235 | 37.02 MiB | 20006 | 5655 | 4538.0 |
| 0c7351fed83dbd13832b67b9a6aecc366fb2b2fed52995ca7ebc8a50 | 40034 | 25.27 MiB | 20000 | 3703 | 3459.6 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 1 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 20005 |
| TASK_LIFECYCLE_EVENT | 51210 |
| TASK_PROFILE_EVENT | 20039 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 153ms / 153ms (errors: 0) |
| /enter_cluster cold load | 40.858s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=20000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 778ms | 891ms | 891ms | 408 B | 0 |
| /api/jobs/ | 8ms | 203ms | 203ms | 1.23 KiB | 0 |
| /nodes?view=summary | 8ms | 203ms | 203ms | 2.76 KiB | 0 |
| /events | 6ms | 364ms | 364ms | 1.77 KiB | 0 |

