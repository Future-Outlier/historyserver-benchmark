# History Server Benchmark Report

- Date: 2026-08-05T18:24:10-05:00
- Tasks: 50000 (wave 2000, num_cpus=0.2), compression=true
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_16-24-12_771154_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 45s |
| driver-measured wall | 18.9s |
| driver-measured rate | 2645.3 tasks/s |
| flush (cluster deletion incl. final upload) | 4s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-qb75q | 0 | 0 B | 0 | 0 | 0 |
| raycluster-historyserver-head-zddjf | 0 | 0 B | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 2 | 101.7 | 0.056 | 0.056 |
| collector (head) | job | 45 | 126.5 | 0.426 | 0.750 |
| collector (worker) | flush | 2 | 114.8 | 0.000 | 0.000 |
| collector (worker) | job | 45 | 114.8 | 0.315 | 0.363 |
| historyserver | historyserver | 136 | 939.3 | 0.495 | 0.509 |
| ray-head | job | 45 | 2832.1 | 0.678 | 1.009 |
| ray-worker | job | 45 | 969.5 | 0.993 | 1.978 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-2t8gm/historyserver | historyserver | 130 | 1065.8 | 1136.3 | 0.495 | 0.522 |  |
| historyserver-demo-7f67bfd478-2t8gm/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1154.8 |
| raycluster-historyserver-cpu-worker-qb75q/collector | flush | 2 | 120.1 | 191.1 | 0.812 | 1.004 |  |
| raycluster-historyserver-cpu-worker-qb75q/collector | job | 43 | 116.0 | 177.9 | 0.262 | 1.018 |  |
| raycluster-historyserver-cpu-worker-qb75q/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 191.4 |
| raycluster-historyserver-cpu-worker-qb75q/ray-worker | job | 43 | 976.2 | 1013.0 | 0.941 | 2.076 |  |
| raycluster-historyserver-cpu-worker-qb75q/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1016.0 |
| raycluster-historyserver-head-zddjf/collector | flush | 3 | 98.3 | 198.0 | 0.860 | 1.006 |  |
| raycluster-historyserver-head-zddjf/collector | job | 43 | 144.3 | 216.4 | 0.393 | 1.140 |  |
| raycluster-historyserver-head-zddjf/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 229.6 |
| raycluster-historyserver-head-zddjf/ray-head | job | 43 | 2784.5 | 2847.9 | 0.636 | 2.601 |  |
| raycluster-historyserver-head-zddjf/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2852.3 |
| rayjob-bench-v5xjl/ray-job-submitter | job | 36 | 97.9 | 108.3 | 0.068 | 0.944 |  |
| rayjob-bench-v5xjl/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.1 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 16.33 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 13.85 MiB | 84.8% |
| logs | 2.48 MiB | 15.2% |
| node_events | 1.65 KiB | 0.0% |
| **total** | **16.34 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 220117 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 220103 |
| events per task (k) | 4.40 |
| raw JSONL bytes | 151.81 MiB |
| stored event bytes | 13.85 MiB |
| avg raw bytes/event | 723 |
| compression ratio (stored/raw) | 0.091 |
| distinct taskDefinitionEvent taskIds | 50005 / 50000 expected |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 5d6ead81c7fe7d9613874f42909bfddbdc3247edddce016b1423ee14 | 120083 | 88.66 MiB | 50007 | 9845 | 7039.2 |
| ab718ef8b2c337d1d159d6f0ac749e20bfdffcdc1801601286e400c6 | 100034 | 63.15 MiB | 50000 | 7020 | 5966.3 |

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
| TASK_LIFECYCLE_EVENT | 120045 |
| TASK_PROFILE_EVENT | 50051 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 133ms / 133ms (errors: 0) |
| /enter_cluster cold load | 1m45.774s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 2.469s | 2.618s | 2.618s | 408 B | 0 |
| /api/jobs/ | 44ms | 578ms | 578ms | 1.22 KiB | 0 |
| /nodes?view=summary | 62ms | 630ms | 630ms | 2.76 KiB | 0 |
| /events | 54ms | 584ms | 584ms | 2.12 KiB | 0 |

