# History Server Benchmark Report

- Date: 2026-08-06T15:12:46-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_13-12-48_505869_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m20s |
| driver-measured wall | 35.8s |
| driver-measured rate | 2791.3 tasks/s |
| flush (cluster deletion incl. final upload) | 3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-sspg7 | 2 | 126.40 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-8jdc9 | 6 | 174.03 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 80 | 144.7 | 0.469 | 0.820 |
| collector (worker) | job | 80 | 112.4 | 0.342 | 0.557 |
| historyserver | historyserver | 81 | 1903.7 | 1.128 | 2.033 |
| ray-head | flush | 1 | 4110.6 | 0.000 | 0.000 |
| ray-head | job | 80 | 4396.5 | 0.663 | 1.215 |
| ray-worker | flush | 1 | 548.9 | 0.000 | 0.000 |
| ray-worker | job | 80 | 1014.8 | 1.181 | 2.108 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7b56559c6b-2whg4/historyserver | historyserver | 77 | 1511.6 | 1971.8 | 1.245 | 2.640 |  |
| historyserver-demo-7b56559c6b-2whg4/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1976.0 |
| raycluster-historyserver-cpu-worker-sspg7/collector | flush | 1 | 82.0 | 83.1 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-sspg7/collector | job | 76 | 116.2 | 240.4 | 0.323 | 1.510 |  |
| raycluster-historyserver-cpu-worker-sspg7/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 245.2 |
| raycluster-historyserver-cpu-worker-sspg7/ray-worker | flush | 1 | 421.7 | 445.8 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-sspg7/ray-worker | job | 76 | 1017.4 | 1059.5 | 0.988 | 2.100 |  |
| raycluster-historyserver-cpu-worker-sspg7/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1063.8 |
| raycluster-historyserver-head-8jdc9/collector | flush | 1 | 129.3 | 176.5 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-8jdc9/collector | job | 76 | 145.9 | 278.2 | 0.473 | 2.045 |  |
| raycluster-historyserver-head-8jdc9/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 279.0 |
| raycluster-historyserver-head-8jdc9/ray-head | flush | 1 | 3635.2 | 3693.1 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-8jdc9/ray-head | job | 76 | 4349.7 | 4418.3 | 0.647 | 2.254 |  |
| raycluster-historyserver-head-8jdc9/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 4421.5 |
| rayjob-bench-5fqgk/ray-job-submitter | job | 71 | 98.0 | 108.5 | 0.039 | 0.947 |  |
| rayjob-bench-5fqgk/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.9 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 253.04 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 170 | 52.07 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 300.42 MiB | 98.5% |
| logs | 4.68 MiB | 1.5% |
| node_events | 6.55 KiB | 0.0% |
| **total** | **305.11 MiB** | (175 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 433348 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 433335 |
| events per task (k) | 4.33 |
| raw JSONL bytes | 300.43 MiB |
| stored event bytes | 300.43 MiB |
| avg raw bytes/event | 727 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 3a1c52f3447c4278359b2116f4362812488ffece4d112ace427caf0e | 233271 | 174.03 MiB | 100008 | 9327 | 7355.9 |
| 3b6f3a666cd1b53e2efa8b0360f50ac88746cf15750b8ababa44b21e | 200077 | 126.40 MiB | 100000 | 7138 | 6281.7 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 4 |
| ACTOR_TASK_DEFINITION_EVENT | 3 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 100005 |
| TASK_LIFECYCLE_EVENT | 233232 |
| TASK_PROFILE_EVENT | 100095 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 8ms / 82ms / 82ms (errors: 0) |
| /enter_cluster cold load | 1m7.9s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 865ms | 1.213s | 1.213s | 536 B | 0 |
| /api/jobs/ | 16ms | 250ms | 250ms | 1.31 KiB | 0 |
| /nodes?view=summary | 18ms | 269ms | 269ms | 2.77 KiB | 0 |
| /events | 16ms | 234ms | 234ms | 3.20 KiB | 0 |

