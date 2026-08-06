# History Server Benchmark Report

- Date: 2026-08-06T13:23:01-05:00
- Tasks: 100000 (wave 10000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_11-23-03_353788_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m25s |
| driver-measured wall | 40.5s |
| driver-measured rate | 2467.0 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-7zh4c | 2 | 126.40 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-lnrqh | 6 | 203.93 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 122.2 | 0.000 | 0.000 |
| collector (head) | job | 85 | 125.8 | 0.541 | 0.812 |
| collector (worker) | job | 85 | 112.9 | 0.362 | 0.648 |
| historyserver | historyserver | 86 | 1928.7 | 1.137 | 1.922 |
| ray-head | flush | 1 | 3488.7 | 0.000 | 0.000 |
| ray-head | job | 85 | 3795.2 | 0.714 | 1.256 |
| ray-worker | job | 85 | 1023.9 | 1.039 | 2.010 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7768d9678d-x928h/historyserver | historyserver | 83 | 1483.2 | 1927.9 | 1.258 | 2.911 |  |
| historyserver-demo-7768d9678d-x928h/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1929.6 |
| raycluster-historyserver-cpu-worker-7zh4c/collector | flush | 1 | 108.9 | 128.3 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-7zh4c/collector | job | 81 | 113.6 | 234.2 | 0.309 | 1.984 |  |
| raycluster-historyserver-cpu-worker-7zh4c/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 240.5 |
| raycluster-historyserver-cpu-worker-7zh4c/ray-worker | flush | 1 | 102.5 | 119.4 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-7zh4c/ray-worker | job | 81 | 1018.7 | 1059.2 | 1.035 | 2.706 |  |
| raycluster-historyserver-cpu-worker-7zh4c/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1063.9 |
| raycluster-historyserver-head-lnrqh/collector | flush | 2 | 119.6 | 198.6 | 1.128 | 1.128 |  |
| raycluster-historyserver-head-lnrqh/collector | job | 81 | 138.7 | 259.4 | 0.513 | 1.880 |  |
| raycluster-historyserver-head-lnrqh/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 264.1 |
| raycluster-historyserver-head-lnrqh/ray-head | flush | 1 | 3038.6 | 3101.8 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-lnrqh/ray-head | job | 81 | 3749.3 | 3818.2 | 0.736 | 2.301 |  |
| raycluster-historyserver-head-lnrqh/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3822.5 |
| rayjob-bench-4wg4j/ray-job-submitter | job | 75 | 97.9 | 108.6 | 0.040 | 0.995 |  |
| rayjob-bench-4wg4j/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 109.9 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 236.94 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 170 | 98.08 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 330.33 MiB | 98.6% |
| logs | 4.68 MiB | 1.4% |
| node_events | 6.91 KiB | 0.0% |
| **total** | **335.02 MiB** | (175 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 495954 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 495940 |
| events per task (k) | 4.96 |
| raw JSONL bytes | 330.33 MiB |
| stored event bytes | 330.33 MiB |
| avg raw bytes/event | 698 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 2393fca2a778820b3518dabebf3b5990bd9aa51a794a0d7887f31f5b | 295865 | 203.93 MiB | 100008 | 22732 | 8668.5 |
| c42e5b0c6f5ec3528bfebebcc1c25551c76e08e67a60b7a3efb88d3a | 200089 | 126.40 MiB | 100000 | 6380 | 5600.1 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 3 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 100005 |
| TASK_LIFECYCLE_EVENT | 295846 |
| TASK_PROFILE_EVENT | 100086 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 86ms / 86ms (errors: 0) |
| /enter_cluster cold load | 1m11.683s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 892ms | 1.269s | 1.269s | 536 B | 0 |
| /api/jobs/ | 26ms | 236ms | 236ms | 1.31 KiB | 0 |
| /nodes?view=summary | 28ms | 753ms | 753ms | 2.77 KiB | 0 |
| /events | 26ms | 338ms | 338ms | 3.57 KiB | 0 |

