# History Server Benchmark Report

- Date: 2026-08-05T18:57:09-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_16-57-10_845994_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m0s |
| driver-measured wall | 37.1s |
| driver-measured rate | 2692.3 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-hhkx8 | 2 | 126.30 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-s6488 | 6 | 175.77 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 0.0 | 0.000 | 0.000 |
| collector (head) | job | 60 | 146.4 | 0.600 | 0.828 |
| collector (worker) | job | 60 | 114.3 | 0.427 | 0.617 |
| historyserver | historyserver | 909 | 1941.5 | 0.458 | 0.530 |
| ray-head | flush | 1 | 0.0 | 0.000 | 0.000 |
| ray-head | job | 60 | 3512.4 | 0.934 | 1.215 |
| ray-worker | job | 60 | 1027.8 | 1.614 | 1.971 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-qpfd9/historyserver | historyserver | 870 | 1983.7 | 2085.2 | 0.453 | 0.535 |  |
| historyserver-demo-7f67bfd478-qpfd9/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2085.9 |
| raycluster-historyserver-cpu-worker-hhkx8/collector | flush | 1 | 102.6 | 116.3 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-hhkx8/collector | job | 57 | 115.8 | 233.6 | 0.436 | 2.051 |  |
| raycluster-historyserver-cpu-worker-hhkx8/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 243.8 |
| raycluster-historyserver-cpu-worker-hhkx8/ray-worker | flush | 1 | 102.6 | 118.3 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-hhkx8/ray-worker | job | 57 | 1028.2 | 1069.4 | 1.373 | 2.108 |  |
| raycluster-historyserver-cpu-worker-hhkx8/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1073.8 |
| raycluster-historyserver-head-s6488/collector | flush | 1 | 120.5 | 166.8 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-s6488/collector | job | 57 | 145.5 | 272.3 | 0.600 | 1.533 |  |
| raycluster-historyserver-head-s6488/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 293.6 |
| raycluster-historyserver-head-s6488/ray-head | flush | 1 | 2807.8 | 2863.9 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-s6488/ray-head | job | 57 | 3465.8 | 3534.2 | 0.887 | 2.066 |  |
| raycluster-historyserver-head-s6488/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3535.9 |
| rayjob-bench-gblgb/ray-job-submitter | job | 52 | 97.6 | 108.1 | 0.050 | 0.991 |  |
| rayjob-bench-gblgb/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.4 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 246.37 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 165 | 60.30 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 302.06 MiB | 98.5% |
| logs | 4.61 MiB | 1.5% |
| node_events | 6.90 KiB | 0.0% |
| **total** | **306.68 MiB** | (170 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 436980 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 436966 |
| events per task (k) | 4.37 |
| raw JSONL bytes | 302.07 MiB |
| stored event bytes | 302.07 MiB |
| avg raw bytes/event | 725 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds | 100005 / 100000 expected |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| c04b543d10769334a6fc913365cfce2bedec2657300deccfd7f5b03a | 236912 | 175.77 MiB | 100007 | 9420 | 7035.5 |
| 3dd7fe4a935a60f05fbc28663052d664a2ea2eed2586d9292ca1df95 | 200068 | 126.30 MiB | 100000 | 6641 | 5932.1 |

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
| TASK_LIFECYCLE_EVENT | 236875 |
| TASK_PROFILE_EVENT | 100084 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 83ms / 83ms (errors: 0) |
| /enter_cluster cold load | 15m7.324s (HTTP 0) |

- NOTE: first enter_cluster attempt: status=0 err=Get "http://localhost:30080/enter_cluster/test-ns-6g6l2/raycluster/raycluster-historyserver/session_2026-08-05_16-57-10_845994_1": context deadline exceeded (Client.Timeout exceeded while awaiting headers) after 5m0s; probing for warm hit
- NOTE: warm-probe gave up: last status=0 err=Get "http://localhost:30080/enter_cluster/test-ns-6g6l2/raycluster/raycluster-historyserver/session_2026-08-05_16-57-10_845994_1": dial tcp [::1]:30080: connect: connection refused
- NOTE: enter_cluster never succeeded within 15m0s; warm reads below are expected to 503
