# History Server Benchmark Report

- Date: 2026-08-06T13:59:16-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_11-59-17_917219_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m20s |
| driver-measured wall | 37.6s |
| driver-measured rate | 2660.8 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-ddp9j | 2 | 126.40 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-2mqnh | 6 | 174.90 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 131.1 | 0.000 | 0.000 |
| collector (head) | job | 80 | 131.1 | 0.485 | 0.770 |
| collector (worker) | job | 80 | 115.6 | 0.339 | 0.524 |
| historyserver | historyserver | 1527 | 4107.6 | 0.476 | 0.537 |
| ray-head | job | 80 | 3955.8 | 0.718 | 1.199 |
| ray-worker | job | 80 | 1029.7 | 1.064 | 2.057 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-67d55cff85-p2zn7/historyserver | historyserver | 1467 | 3746.8 | 4121.1 | 0.476 | 0.531 |  |
| historyserver-demo-67d55cff85-p2zn7/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 4121.8 |
| raycluster-historyserver-cpu-worker-ddp9j/collector | flush | 1 | 112.5 | 113.5 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-ddp9j/collector | job | 76 | 114.9 | 235.4 | 0.321 | 1.784 |  |
| raycluster-historyserver-cpu-worker-ddp9j/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 247.3 |
| raycluster-historyserver-cpu-worker-ddp9j/ray-worker | flush | 1 | 102.5 | 119.5 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-ddp9j/ray-worker | job | 76 | 1015.3 | 1054.9 | 1.034 | 2.110 |  |
| raycluster-historyserver-cpu-worker-ddp9j/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1059.7 |
| raycluster-historyserver-head-2mqnh/collector | flush | 1 | 130.4 | 185.2 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-2mqnh/collector | job | 76 | 144.9 | 256.4 | 0.495 | 1.563 |  |
| raycluster-historyserver-head-2mqnh/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 275.7 |
| raycluster-historyserver-head-2mqnh/ray-head | flush | 1 | 3179.5 | 3240.6 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-2mqnh/ray-head | job | 76 | 3896.8 | 3963.3 | 0.675 | 2.208 |  |
| raycluster-historyserver-head-2mqnh/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3965.6 |
| rayjob-bench-fs95q/ray-job-submitter | job | 72 | 98.0 | 108.5 | 0.038 | 0.980 |  |
| rayjob-bench-fs95q/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 109.9 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 242.08 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 170 | 63.90 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 301.28 MiB | 98.5% |
| logs | 4.68 MiB | 1.5% |
| node_events | 6.91 KiB | 0.0% |
| **total** | **305.97 MiB** | (175 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 435154 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 435140 |
| events per task (k) | 4.35 |
| raw JSONL bytes | 301.29 MiB |
| stored event bytes | 301.29 MiB |
| avg raw bytes/event | 726 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| ce2ef96af9711ffcdda1d27dbe75784897675832656bc6b7b1fa9be8 | 235081 | 174.90 MiB | 100008 | 9126 | 6956.4 |
| 3b94a5ba3d741ac419a7e7718d432deb97d0dc2f77dde0469ccde0f5 | 200073 | 126.40 MiB | 100000 | 7315 | 5909.2 |

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
| TASK_LIFECYCLE_EVENT | 235042 |
| TASK_PROFILE_EVENT | 100090 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 139ms / 139ms (errors: 0) |
| /enter_cluster cold load | 25m24.812s (HTTP 0) |

- NOTE: first enter_cluster attempt: status=0 err=Get "http://localhost:30080/enter_cluster/test-ns-4zz46/raycluster/raycluster-historyserver/session_2026-08-06_11-59-17_917219_1": EOF after 4m30s; probing for warm hit
- NOTE: warm-probe gave up: last status=0 err=Get "http://localhost:30080/enter_cluster/test-ns-4zz46/raycluster/raycluster-historyserver/session_2026-08-06_11-59-17_917219_1": context deadline exceeded (Client.Timeout exceeded while awaiting headers)
- NOTE: NOT A MEASUREMENT: enter_cluster never returned 200 within 25m0s, so enterColdLatency is the probe budget, not a load time
