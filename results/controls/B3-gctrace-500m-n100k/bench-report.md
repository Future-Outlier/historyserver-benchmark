# History Server Benchmark Report

- Date: 2026-08-06T13:31:59-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_11-32-01_850718_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m25s |
| driver-measured wall | 42.8s |
| driver-measured rate | 2338.3 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-k9jtk | 2 | 126.40 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-tgpqn | 6 | 175.46 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 143.0 | 0.000 | 0.000 |
| collector (head) | job | 85 | 143.0 | 0.448 | 0.810 |
| collector (worker) | job | 85 | 116.6 | 0.335 | 0.607 |
| historyserver | historyserver | 1519 | 1774.9 | 0.478 | 0.537 |
| ray-head | flush | 1 | 3250.9 | 0.000 | 0.000 |
| ray-head | job | 85 | 3862.7 | 0.734 | 1.170 |
| ray-worker | job | 85 | 1032.8 | 1.177 | 2.026 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-6785ff6464-zlj2n/historyserver | historyserver | 1457 | 1717.4 | 1817.8 | 0.478 | 0.711 |  |
| historyserver-demo-6785ff6464-zlj2n/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1830.6 |
| raycluster-historyserver-cpu-worker-k9jtk/collector | flush | 1 | 64.4 | 86.6 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-k9jtk/collector | job | 80 | 117.8 | 222.5 | 0.311 | 1.529 |  |
| raycluster-historyserver-cpu-worker-k9jtk/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 237.6 |
| raycluster-historyserver-cpu-worker-k9jtk/ray-worker | flush | 1 | 102.6 | 120.7 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-k9jtk/ray-worker | job | 80 | 1018.2 | 1061.9 | 1.095 | 2.096 |  |
| raycluster-historyserver-cpu-worker-k9jtk/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1066.2 |
| raycluster-historyserver-head-tgpqn/collector | flush | 2 | 75.1 | 132.0 | 1.034 | 1.034 |  |
| raycluster-historyserver-head-tgpqn/collector | job | 80 | 143.6 | 260.3 | 0.443 | 1.711 |  |
| raycluster-historyserver-head-tgpqn/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 271.2 |
| raycluster-historyserver-head-tgpqn/ray-head | flush | 1 | 3597.8 | 3659.4 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-tgpqn/ray-head | job | 80 | 3819.7 | 3885.3 | 0.686 | 2.027 |  |
| raycluster-historyserver-head-tgpqn/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3889.4 |
| rayjob-bench-wzpzh/ray-job-submitter | job | 75 | 97.8 | 108.2 | 0.039 | 0.983 |  |
| rayjob-bench-wzpzh/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 109.9 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 227.14 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 170 | 79.41 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 301.86 MiB | 98.5% |
| logs | 4.69 MiB | 1.5% |
| node_events | 6.91 KiB | 0.0% |
| **total** | **306.55 MiB** | (175 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 436355 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 436341 |
| events per task (k) | 4.36 |
| raw JSONL bytes | 301.86 MiB |
| stored event bytes | 301.86 MiB |
| avg raw bytes/event | 725 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| a17d3cad2abef6d000063920c29c25b77905d5cb11d950fccbddc58c | 236271 | 175.46 MiB | 100008 | 9181 | 6202.3 |
| b01c3707fef5fbe9153a59f41d195c622230e9fece719d810b226f9d | 200084 | 126.40 MiB | 100000 | 5931 | 5137.4 |

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
| TASK_LIFECYCLE_EVENT | 236230 |
| TASK_PROFILE_EVENT | 100103 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 7ms / 179ms / 179ms (errors: 0) |
| /enter_cluster cold load | 25m16.082s (HTTP 0) |

- NOTE: first enter_cluster attempt: status=0 err=Get "http://localhost:30080/enter_cluster/test-ns-25kkd/raycluster/raycluster-historyserver/session_2026-08-06_11-32-01_850718_1": EOF after 5m2s; probing for warm hit
- NOTE: warm-probe gave up: last status=0 err=Get "http://localhost:30080/enter_cluster/test-ns-25kkd/raycluster/raycluster-historyserver/session_2026-08-06_11-32-01_850718_1": context deadline exceeded (Client.Timeout exceeded while awaiting headers)
- NOTE: NOT A MEASUREMENT: enter_cluster never returned 200 within 25m0s, so enterColdLatency is the probe budget, not a load time
