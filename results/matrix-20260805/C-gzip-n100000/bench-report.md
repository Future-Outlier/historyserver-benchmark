# History Server Benchmark Report

- Date: 2026-08-05T18:27:46-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=true
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_16-27-47_936374_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 55s |
| driver-measured wall | 34.7s |
| driver-measured rate | 2879.5 tasks/s |
| flush (cluster deletion incl. final upload) | 3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-6d2np | 0 | 0 B | 0 | 0 | 0 |
| raycluster-historyserver-head-wphhq | 1 | 142.10 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 123.1 | 0.000 | 0.000 |
| collector (head) | job | 55 | 128.0 | 0.577 | 0.852 |
| collector (worker) | flush | 1 | 104.4 | 0.000 | 0.000 |
| collector (worker) | job | 55 | 110.4 | 0.347 | 0.405 |
| historyserver | historyserver | 301 | 1885.8 | 0.497 | 0.515 |
| ray-head | job | 55 | 3075.7 | 0.989 | 1.314 |
| ray-worker | job | 55 | 1040.5 | 1.691 | 2.015 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-l65sd/historyserver | historyserver | 289 | 1639.7 | 1892.5 | 0.498 | 0.530 |  |
| historyserver-demo-7f67bfd478-l65sd/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1893.4 |
| raycluster-historyserver-cpu-worker-6d2np/collector | flush | 2 | 105.9 | 250.3 | 1.098 | 1.197 |  |
| raycluster-historyserver-cpu-worker-6d2np/collector | job | 53 | 124.2 | 242.5 | 0.455 | 1.125 |  |
| raycluster-historyserver-cpu-worker-6d2np/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 251.1 |
| raycluster-historyserver-cpu-worker-6d2np/ray-worker | job | 53 | 1020.7 | 1062.8 | 1.389 | 2.115 |  |
| raycluster-historyserver-cpu-worker-6d2np/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1072.4 |
| raycluster-historyserver-head-wphhq/collector | flush | 1 | 89.4 | 126.3 | 0.774 | 0.774 |  |
| raycluster-historyserver-head-wphhq/collector | job | 53 | 146.0 | 316.3 | 0.682 | 2.090 |  |
| raycluster-historyserver-head-wphhq/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 318.6 |
| raycluster-historyserver-head-wphhq/ray-head | job | 53 | 3040.1 | 3104.7 | 0.893 | 1.992 |  |
| raycluster-historyserver-head-wphhq/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3109.0 |
| rayjob-bench-t8brd/ray-job-submitter | job | 48 | 97.6 | 108.1 | 0.052 | 0.955 |  |
| rayjob-bench-t8brd/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.7 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 2 | 11.95 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 171 | 20.14 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 27.47 MiB | 85.6% |
| logs | 4.61 MiB | 14.4% |
| node_events | 1.64 KiB | 0.0% |
| **total** | **32.09 MiB** | (174 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 435674 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 435660 |
| events per task (k) | 4.36 |
| raw JSONL bytes | 301.44 MiB |
| stored event bytes | 27.47 MiB |
| avg raw bytes/event | 726 |
| compression ratio (stored/raw) | 0.091 |
| distinct taskDefinitionEvent taskIds | 100005 / 100000 expected |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 76b6f0eaa510cbf7b11f6d5c046c0a8d21a33987b79293dd9de863e3 | 235592 | 175.14 MiB | 100007 | 9792 | 7462.5 |
| 6c5b3599174a7689337335410ba6f26cc1843bb207eb489dd6335047 | 200082 | 126.31 MiB | 100000 | 7167 | 6414.9 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 2 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 100005 |
| TASK_LIFECYCLE_EVENT | 235569 |
| TASK_PROFILE_EVENT | 100084 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 5ms / 119ms / 119ms (errors: 0) |
| /enter_cluster cold load | 5m0.001s (HTTP 0) |

- NOTE: enter_cluster error: Get "http://localhost:30080/enter_cluster/test-ns-vwczf/raycluster/raycluster-historyserver/session_2026-08-05_16-27-47_936374_1": context deadline exceeded (Client.Timeout exceeded while awaiting headers)
- NOTE: enter_cluster returned 0 after 5m0.001s; warm reads below are expected to 503
