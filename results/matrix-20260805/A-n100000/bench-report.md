# History Server Benchmark Report

- Date: 2026-08-05T18:06:22-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_16-06-24_407636_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 55s |
| driver-measured wall | 32.4s |
| driver-measured rate | 3084.0 tasks/s |
| flush (cluster deletion incl. final upload) | 1s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-grcxz | 0 | 0 B | 0 | 0 | 0 |
| raycluster-historyserver-head-8lwmp | 1 | 142.92 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 55 | 152.2 | 0.545 | 0.733 |
| collector (worker) | job | 55 | 117.9 | 0.423 | 0.728 |
| historyserver | historyserver | 302 | 2188.8 | 0.497 | 0.535 |
| ray-head | job | 55 | 3219.2 | 0.859 | 1.155 |
| ray-worker | job | 55 | 1043.4 | 1.566 | 2.000 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-wzqrp/historyserver | historyserver | 290 | 2201.6 | 2435.3 | 0.498 | 0.530 |  |
| historyserver-demo-7f67bfd478-wzqrp/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2447.1 |
| raycluster-historyserver-cpu-worker-grcxz/collector | flush | 1 | 96.2 | 230.9 | 0.962 | 0.962 |  |
| raycluster-historyserver-cpu-worker-grcxz/collector | job | 53 | 117.5 | 235.6 | 0.439 | 1.075 |  |
| raycluster-historyserver-cpu-worker-grcxz/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 248.4 |
| raycluster-historyserver-cpu-worker-grcxz/ray-worker | flush | 1 | 102.5 | 119.5 | 0.217 | 0.217 |  |
| raycluster-historyserver-cpu-worker-grcxz/ray-worker | job | 53 | 1027.2 | 1070.0 | 1.328 | 2.118 |  |
| raycluster-historyserver-cpu-worker-grcxz/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1078.7 |
| raycluster-historyserver-head-8lwmp/collector | flush | 1 | 122.5 | 154.1 | 0.312 | 0.312 |  |
| raycluster-historyserver-head-8lwmp/collector | job | 53 | 145.0 | 306.2 | 0.654 | 2.137 |  |
| raycluster-historyserver-head-8lwmp/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 306.5 |
| raycluster-historyserver-head-8lwmp/ray-head | flush | 1 | 2483.4 | 2539.3 | 0.675 | 0.675 |  |
| raycluster-historyserver-head-8lwmp/ray-head | job | 53 | 3191.4 | 3256.0 | 0.869 | 1.855 |  |
| raycluster-historyserver-head-8lwmp/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3257.3 |
| rayjob-bench-vj4hg/ray-job-submitter | job | 49 | 97.6 | 108.1 | 0.050 | 0.961 |  |
| rayjob-bench-vj4hg/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.2 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 3 | 142.92 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 170 | 159.88 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 298.18 MiB | 98.5% |
| logs | 4.61 MiB | 1.5% |
| node_events | 6.17 KiB | 0.0% |
| **total** | **302.80 MiB** | (174 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 429670 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 429658 |
| events per task (k) | 4.30 |
| raw JSONL bytes | 298.18 MiB |
| stored event bytes | 298.18 MiB |
| avg raw bytes/event | 728 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds | 99540 / 100000 expected |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| b4edc1699478340bb3210cb83ad7935b77469cd532ce8b4a12874e7d | 229595 | 171.88 MiB | 99542 | 10000 | 7849.9 |
| 8b8621becc43ae10cc68191565e5d3e674d1491f0c20570644acc025 | 200075 | 126.30 MiB | 100000 | 7106 | 6561.1 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 3 |
| ACTOR_TASK_DEFINITION_EVENT | 2 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 99540 |
| TASK_LIFECYCLE_EVENT | 230030 |
| TASK_PROFILE_EVENT | 100086 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 6ms / 203ms / 203ms (errors: 0) |
| /enter_cluster cold load | 5m0.001s (HTTP 0) |

- NOTE: enter_cluster error: Get "http://localhost:30080/enter_cluster/test-ns-9lwrj/raycluster/raycluster-historyserver/session_2026-08-05_16-06-24_407636_1": context deadline exceeded (Client.Timeout exceeded while awaiting headers)
- NOTE: enter_cluster returned 0 after 5m0.001s; warm reads below are expected to 503
