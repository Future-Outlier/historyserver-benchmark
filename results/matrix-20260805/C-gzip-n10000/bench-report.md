# History Server Benchmark Report

- Date: 2026-08-05T18:22:34-05:00
- Tasks: 10000 (wave 2000, num_cpus=0.2), compression=true
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_16-22-35_798706_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 30s |
| driver-measured wall | 5.2s |
| driver-measured rate | 1930.3 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-m29vg | 0 | 0 B | 0 | 0 | 0 |
| raycluster-historyserver-head-2zsrw | 0 | 0 B | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 30 | 107.1 | 0.220 | 0.220 |
| collector (worker) | job | 30 | 98.4 | 0.100 | 0.119 |
| historyserver | historyserver | 37 | 268.7 | 0.469 | 0.502 |
| ray-head | job | 30 | 2490.6 | 0.368 | 0.456 |
| ray-worker | job | 30 | 461.7 | 0.464 | 1.094 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-vlf74/historyserver | historyserver | 35 | 293.9 | 322.0 | 0.478 | 0.521 |  |
| historyserver-demo-7f67bfd478-vlf74/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 326.8 |
| raycluster-historyserver-cpu-worker-m29vg/collector | job | 29 | 97.1 | 111.1 | 0.081 | 0.950 |  |
| raycluster-historyserver-cpu-worker-m29vg/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 113.1 |
| raycluster-historyserver-cpu-worker-m29vg/ray-worker | job | 29 | 890.7 | 930.1 | 0.427 | 2.091 |  |
| raycluster-historyserver-cpu-worker-m29vg/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 951.4 |
| raycluster-historyserver-head-2zsrw/collector | flush | 1 | 66.2 | 87.2 | 0.646 | 0.646 |  |
| raycluster-historyserver-head-2zsrw/collector | job | 29 | 105.5 | 125.6 | 0.129 | 0.847 |  |
| raycluster-historyserver-head-2zsrw/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 127.6 |
| raycluster-historyserver-head-2zsrw/ray-head | job | 29 | 2539.0 | 2606.0 | 0.345 | 1.217 |  |
| raycluster-historyserver-head-2zsrw/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2609.7 |
| rayjob-bench-jv6jt/ray-job-submitter | job | 22 | 97.6 | 108.0 | 0.114 | 0.962 |  |
| rayjob-bench-jv6jt/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.0 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 3.56 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 2.83 MiB | 79.4% |
| logs | 748.50 KiB | 20.5% |
| node_events | 1.66 KiB | 0.0% |
| **total** | **3.56 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 45407 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 45393 |
| events per task (k) | 4.54 |
| raw JSONL bytes | 31.03 MiB |
| stored event bytes | 2.83 MiB |
| avg raw bytes/event | 717 |
| compression ratio (stored/raw) | 0.091 |
| distinct taskDefinitionEvent taskIds | 10005 / 10000 expected |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| c929e4ce9a8a4d21fab24c96eb716690093e67ebedb4126cd9853acd | 25396 | 18.40 MiB | 10006 | 6404 | 2536.8 |
| 01e2064ac75e672e8bdfcd15080b95e2c5b8613209409fc163ccbb3d | 20011 | 12.63 MiB | 10000 | 5654 | 2001.1 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 1 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 10005 |
| TASK_LIFECYCLE_EVENT | 25364 |
| TASK_PROFILE_EVENT | 10023 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 5ms / 149ms / 149ms (errors: 0) |
| /enter_cluster cold load | 20.688s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=10000 | 986ms | 1.176s | 1.176s | 4.32 MiB | 0 |
| /api/v0/tasks/summarize | 301ms | 396ms | 396ms | 533 B | 0 |
| /api/jobs/ | 4ms | 161ms | 161ms | 1.22 KiB | 0 |
| /nodes?view=summary | 5ms | 170ms | 170ms | 2.76 KiB | 0 |
| /events | 13ms | 155ms | 155ms | 1.04 KiB | 0 |

