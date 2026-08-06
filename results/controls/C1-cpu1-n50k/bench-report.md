# History Server Benchmark Report

- Date: 2026-08-06T12:53:52-05:00
- Tasks: 50000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_10-53-53_948407_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 50s |
| driver-measured wall | 17.5s |
| driver-measured rate | 2858.8 tasks/s |
| flush (cluster deletion incl. final upload) | 3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-l2j54 | 1 | 63.20 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-txhsc | 5 | 88.38 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 108.6 | 0.000 | 0.000 |
| collector (head) | job | 50 | 148.1 | 0.292 | 0.633 |
| collector (worker) | flush | 1 | 86.7 | 0.000 | 0.000 |
| collector (worker) | job | 50 | 107.3 | 0.268 | 0.559 |
| historyserver | historyserver | 53 | 1156.5 | 0.938 | 1.010 |
| ray-head | job | 50 | 3391.7 | 0.537 | 1.155 |
| ray-worker | job | 50 | 542.4 | 0.983 | 1.877 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-bd55659d8-jvn29/historyserver | historyserver | 51 | 1270.5 | 1462.5 | 0.940 | 1.021 |  |
| historyserver-demo-bd55659d8-jvn29/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1476.5 |
| raycluster-historyserver-cpu-worker-l2j54/collector | flush | 1 | 86.1 | 152.2 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-l2j54/collector | job | 48 | 116.3 | 168.7 | 0.238 | 1.026 |  |
| raycluster-historyserver-cpu-worker-l2j54/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 175.8 |
| raycluster-historyserver-cpu-worker-l2j54/ray-worker | job | 48 | 973.4 | 1013.4 | 0.800 | 2.115 |  |
| raycluster-historyserver-cpu-worker-l2j54/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1018.1 |
| raycluster-historyserver-head-txhsc/collector | flush | 1 | 93.0 | 185.1 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-txhsc/collector | job | 48 | 129.2 | 208.2 | 0.342 | 1.116 |  |
| raycluster-historyserver-head-txhsc/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 229.7 |
| raycluster-historyserver-head-txhsc/ray-head | job | 48 | 3351.4 | 3414.8 | 0.565 | 1.995 |  |
| raycluster-historyserver-head-txhsc/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3418.7 |
| rayjob-bench-9hbrw/ray-job-submitter | job | 42 | 97.9 | 108.3 | 0.060 | 0.961 |  |
| rayjob-bench-9hbrw/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 109.9 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 167 | 154.07 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 151.57 MiB | 98.4% |
| logs | 2.50 MiB | 1.6% |
| node_events | 6.55 KiB | 0.0% |
| **total** | **154.07 MiB** | (168 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 219515 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 219502 |
| events per task (k) | 4.39 |
| raw JSONL bytes | 151.58 MiB |
| stored event bytes | 151.58 MiB |
| avg raw bytes/event | 724 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 50005 |
| distinct taskIds in benchmark job `03000000` | **50001 / 50000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| d67a4245abf79809e293c9bcfb1c11ce3e4e59c3e3caf3f1e2fe0929 | 119473 | 88.38 MiB | 50007 | 9457 | 7621.2 |
| 3763b996ac38ade741f45ddca6d798139bbb0cfa2f28958ad04e867b | 100042 | 63.20 MiB | 50000 | 7178 | 6438.1 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 4 |
| ACTOR_TASK_DEFINITION_EVENT | 2 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 50005 |
| TASK_LIFECYCLE_EVENT | 119440 |
| TASK_PROFILE_EVENT | 50055 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 103ms / 103ms (errors: 0) |
| /enter_cluster cold load | 38.229s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 962ms | 1.171s | 1.171s | 408 B | 0 |
| /api/jobs/ | 18ms | 316ms | 316ms | 1.31 KiB | 0 |
| /nodes?view=summary | 120ms | 207ms | 207ms | 2.77 KiB | 0 |
| /events | 35ms | 217ms | 217ms | 2.13 KiB | 0 |

