# History Server Benchmark Report

- Date: 2026-08-05T18:01:31-05:00
- Tasks: 10000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_16-01-33_123559_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 30s |
| driver-measured wall | 4.7s |
| driver-measured rate | 2112.6 tasks/s |
| flush (cluster deletion incl. final upload) | 1s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-r67hn | 0 | 0 B | 0 | 0 | 0 |
| raycluster-historyserver-head-4sslk | 0 | 0 B | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 30 | 90.9 | 0.143 | 0.340 |
| collector (worker) | job | 30 | 90.3 | 0.136 | 0.145 |
| historyserver | historyserver | 37 | 259.4 | 0.469 | 0.504 |
| ray-head | job | 30 | 2446.8 | 0.373 | 0.435 |
| ray-worker | job | 30 | 458.8 | 0.517 | 0.994 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-9x8kl/historyserver | historyserver | 35 | 290.9 | 294.5 | 0.480 | 0.530 |  |
| historyserver-demo-7f67bfd478-9x8kl/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 295.0 |
| raycluster-historyserver-cpu-worker-r67hn/collector | flush | 1 | 91.2 | 92.8 | 0.348 | 0.348 |  |
| raycluster-historyserver-cpu-worker-r67hn/collector | job | 29 | 88.4 | 102.9 | 0.080 | 0.793 |  |
| raycluster-historyserver-cpu-worker-r67hn/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 109.7 |
| raycluster-historyserver-cpu-worker-r67hn/ray-worker | job | 29 | 900.1 | 938.8 | 0.394 | 2.116 |  |
| raycluster-historyserver-cpu-worker-r67hn/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 948.9 |
| raycluster-historyserver-head-4sslk/collector | flush | 1 | 90.0 | 112.8 | 0.402 | 0.402 |  |
| raycluster-historyserver-head-4sslk/collector | job | 29 | 101.1 | 111.4 | 0.118 | 0.716 |  |
| raycluster-historyserver-head-4sslk/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 117.3 |
| raycluster-historyserver-head-4sslk/ray-head | flush | 1 | 1016.9 | 1044.7 | 1.373 | 1.373 |  |
| raycluster-historyserver-head-4sslk/ray-head | job | 29 | 2541.2 | 2602.7 | 0.344 | 1.244 |  |
| raycluster-historyserver-head-4sslk/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2605.2 |
| rayjob-bench-tc5jj/ray-job-submitter | job | 23 | 97.6 | 108.9 | 0.113 | 0.955 |  |
| rayjob-bench-tc5jj/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.4 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 30.82 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 30.09 MiB | 97.6% |
| logs | 747.53 KiB | 2.4% |
| node_events | 6.54 KiB | 0.0% |
| **total** | **30.82 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 43448 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 43435 |
| events per task (k) | 4.34 |
| raw JSONL bytes | 30.09 MiB |
| stored event bytes | 30.09 MiB |
| avg raw bytes/event | 726 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds | 10005 / 10000 expected |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 50a2a26e3db392e98d92de186d350117bbeab00481a01f4977ba936e | 23446 | 17.47 MiB | 10006 | 5201 | 2341.9 |
| a36c8604f36a41f595c5a9797c7ea753d63cae9df9b7f4eee897b2fa | 20002 | 12.63 MiB | 10000 | 6381 | 2000.2 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 4 |
| ACTOR_TASK_DEFINITION_EVENT | 1 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 10005 |
| TASK_LIFECYCLE_EVENT | 23410 |
| TASK_PROFILE_EVENT | 10019 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 6ms / 162ms / 162ms (errors: 0) |
| /enter_cluster cold load | 19.862s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=10000 | 995ms | 1.093s | 1.093s | 4.32 MiB | 0 |
| /api/v0/tasks/summarize | 375ms | 493ms | 493ms | 533 B | 0 |
| /api/jobs/ | 4ms | 171ms | 171ms | 1.22 KiB | 0 |
| /nodes?view=summary | 4ms | 103ms | 103ms | 2.76 KiB | 0 |
| /events | 17ms | 146ms | 146ms | 1.04 KiB | 0 |

