# History Server Benchmark Report

- Date: 2026-08-05T19:15:44-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_17-15-46_116687_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m25s |
| driver-measured wall | 40.1s |
| driver-measured rate | 2494.5 tasks/s |
| flush (cluster deletion incl. final upload) | 3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-pw5ld | 2 | 126.31 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-h9stz | 6 | 174.18 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 81.6 | 0.000 | 0.000 |
| collector (head) | job | 85 | 147.9 | 0.408 | 0.699 |
| collector (worker) | job | 85 | 111.3 | 0.312 | 0.519 |
| historyserver | historyserver | 302 | 1832.7 | 0.497 | 0.537 |
| ray-head | flush | 1 | 2913.0 | 0.000 | 0.000 |
| ray-head | job | 85 | 2944.1 | 0.730 | 1.211 |
| ray-worker | job | 85 | 1056.9 | 1.044 | 2.008 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-nthhb/historyserver | historyserver | 289 | 1525.1 | 1872.3 | 0.498 | 0.537 |  |
| historyserver-demo-7f67bfd478-nthhb/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1872.9 |
| raycluster-historyserver-cpu-worker-pw5ld/collector | flush | 1 | 87.1 | 111.1 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-pw5ld/collector | job | 81 | 115.5 | 224.7 | 0.308 | 1.885 |  |
| raycluster-historyserver-cpu-worker-pw5ld/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 234.9 |
| raycluster-historyserver-cpu-worker-pw5ld/ray-worker | flush | 1 | 102.6 | 119.4 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-pw5ld/ray-worker | job | 81 | 1056.9 | 1101.1 | 1.031 | 2.108 |  |
| raycluster-historyserver-cpu-worker-pw5ld/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1105.9 |
| raycluster-historyserver-head-h9stz/collector | flush | 1 | 75.2 | 128.4 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-h9stz/collector | job | 81 | 146.8 | 290.5 | 0.422 | 1.550 |  |
| raycluster-historyserver-head-h9stz/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 292.0 |
| raycluster-historyserver-head-h9stz/ray-head | flush | 1 | 2242.1 | 2303.8 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-h9stz/ray-head | job | 81 | 2891.9 | 2969.2 | 0.656 | 2.466 |  |
| raycluster-historyserver-head-h9stz/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2972.9 |
| rayjob-bench-9lq6t/ray-job-submitter | job | 75 | 97.6 | 108.1 | 0.035 | 0.972 |  |
| rayjob-bench-9lq6t/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.2 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 231.54 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 170 | 73.63 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 300.48 MiB | 98.5% |
| logs | 4.69 MiB | 1.5% |
| node_events | 6.17 KiB | 0.0% |
| **total** | **305.17 MiB** | (175 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 433654 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 433642 |
| events per task (k) | 4.34 |
| raw JSONL bytes | 300.48 MiB |
| stored event bytes | 300.48 MiB |
| avg raw bytes/event | 727 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 2ca51c9919987f0b940e50e8217b15e7731063987b3df4ca02698af0 | 233572 | 174.18 MiB | 100008 | 9083 | 6962.2 |
| 8d268ed93aed38fd7587b6d647552803dd6811e4572a48abd9935705 | 200082 | 126.31 MiB | 100000 | 6630 | 5920.7 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 3 |
| ACTOR_TASK_DEFINITION_EVENT | 3 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 100005 |
| TASK_LIFECYCLE_EVENT | 233530 |
| TASK_PROFILE_EVENT | 100104 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 121ms / 121ms (errors: 0) |
| /enter_cluster cold load | 5m0.001s (HTTP 0) |

- NOTE: first enter_cluster attempt: status=0 err=Get "http://localhost:30080/enter_cluster/test-ns-s6dwk/raycluster/raycluster-historyserver/session_2026-08-05_17-15-46_116687_1": context deadline exceeded (Client.Timeout exceeded while awaiting headers) after 5m0s; probing for warm hit
- NOTE: warm-probe gave up: last status=0 err=Get "http://localhost:30080/enter_cluster/test-ns-s6dwk/raycluster/raycluster-historyserver/session_2026-08-05_17-15-46_116687_1": context deadline exceeded (Client.Timeout exceeded while awaiting headers)
- NOTE: enter_cluster never succeeded within 3m0s; warm reads below are expected to 503
