# History Server Benchmark Report

- Date: 2026-08-05T19:50:06-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_17-50-07_892832_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m15s |
| driver-measured wall | 32.3s |
| driver-measured rate | 3098.4 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-6qj2h | 1 | 126.30 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-jxtd5 | 6 | 171.76 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 75 | 150.9 | 0.488 | 0.820 |
| collector (worker) | job | 75 | 118.5 | 0.386 | 0.747 |
| historyserver | historyserver | 1273 | 2059.3 | 0.476 | 0.530 |
| ray-head | job | 75 | 3905.2 | 0.653 | 1.201 |
| ray-worker | job | 75 | 1038.2 | 1.275 | 1.968 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-7vfqh/historyserver | historyserver | 1222 | 1998.5 | 2221.8 | 0.476 | 0.628 |  |
| historyserver-demo-7f67bfd478-7vfqh/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2234.0 |
| raycluster-historyserver-cpu-worker-6qj2h/collector | job | 72 | 116.3 | 233.7 | 0.326 | 1.015 |  |
| raycluster-historyserver-cpu-worker-6qj2h/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 242.6 |
| raycluster-historyserver-cpu-worker-6qj2h/ray-worker | job | 72 | 1029.3 | 1068.6 | 0.964 | 2.117 |  |
| raycluster-historyserver-cpu-worker-6qj2h/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1071.6 |
| raycluster-historyserver-head-jxtd5/collector | job | 72 | 144.5 | 297.4 | 0.465 | 1.639 |  |
| raycluster-historyserver-head-jxtd5/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 299.3 |
| raycluster-historyserver-head-jxtd5/ray-head | job | 72 | 3859.0 | 3927.9 | 0.642 | 2.610 |  |
| raycluster-historyserver-head-jxtd5/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3932.3 |
| rayjob-bench-8wpmx/ray-job-submitter | job | 65 | 97.6 | 108.1 | 0.035 | 0.963 |  |
| rayjob-bench-8wpmx/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.7 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 265.81 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 164 | 36.90 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 298.05 MiB | 98.5% |
| logs | 4.66 MiB | 1.5% |
| node_events | 6.17 KiB | 0.0% |
| **total** | **302.71 MiB** | (169 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 431548 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 431536 |
| events per task (k) | 4.32 |
| raw JSONL bytes | 298.05 MiB |
| stored event bytes | 298.05 MiB |
| avg raw bytes/event | 724 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 98271 |
| distinct taskIds in benchmark job `03000000` | **98267 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 0839d8f01d6e9b0a2f75adc2c3829dd50e5c15ece2c38e6c95a095ed | 231483 | 171.76 MiB | 98273 | 10201 | 7740.8 |
| e7e71337d2e20b21f8e3ae858905d17f76b99e930fb4872d848ee77a | 200065 | 126.30 MiB | 100000 | 7332 | 6642.9 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 3 |
| ACTOR_TASK_DEFINITION_EVENT | 2 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 98271 |
| TASK_LIFECYCLE_EVENT | 233172 |
| TASK_PROFILE_EVENT | 100091 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 4ms / 128ms / 128ms (errors: 0) |
| /enter_cluster cold load | 21m9.784s (HTTP 0) |

- NOTE: first enter_cluster attempt: status=0 err=Get "http://localhost:30080/enter_cluster/test-ns-n7hrh/raycluster/raycluster-historyserver/session_2026-08-05_17-50-07_892832_1": context deadline exceeded (Client.Timeout exceeded while awaiting headers) after 5m0s; probing for warm hit
- NOTE: warm-probe gave up: last status=0 err=Get "http://localhost:30080/enter_cluster/test-ns-n7hrh/raycluster/raycluster-historyserver/session_2026-08-05_17-50-07_892832_1": context deadline exceeded (Client.Timeout exceeded while awaiting headers)
- NOTE: enter_cluster never succeeded within 20m0s; warm reads below are expected to 503
