# History Server Benchmark Report

- Date: 2026-08-06T14:37:16-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_12-37-18_286308_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m20s |
| driver-measured wall | 36.1s |
| driver-measured rate | 2767.6 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-z4cjr | 1 | 126.40 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-t99mj | 6 | 173.89 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 80 | 148.1 | 0.496 | 0.708 |
| collector (worker) | job | 80 | 115.4 | 0.341 | 0.733 |
| historyserver | historyserver | 78 | 2097.3 | 1.181 | 1.471 |
| ray-head | job | 80 | 4001.8 | 0.634 | 1.138 |
| ray-worker | job | 80 | 1045.5 | 1.127 | 2.052 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-65887b79cf-772p2/historyserver | historyserver | 74 | 1717.3 | 2142.6 | 1.206 | 2.578 |  |
| historyserver-demo-65887b79cf-772p2/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2143.2 |
| raycluster-historyserver-cpu-worker-z4cjr/collector | flush | 1 | 78.1 | 79.1 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-z4cjr/collector | job | 76 | 118.6 | 242.4 | 0.319 | 1.111 |  |
| raycluster-historyserver-cpu-worker-z4cjr/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 249.7 |
| raycluster-historyserver-cpu-worker-z4cjr/ray-worker | flush | 1 | 102.5 | 118.7 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-z4cjr/ray-worker | job | 76 | 1016.4 | 1054.9 | 1.001 | 2.096 |  |
| raycluster-historyserver-cpu-worker-z4cjr/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1058.9 |
| raycluster-historyserver-head-t99mj/collector | flush | 1 | 128.6 | 168.2 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-t99mj/collector | job | 76 | 145.6 | 275.9 | 0.440 | 1.752 |  |
| raycluster-historyserver-head-t99mj/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 297.1 |
| raycluster-historyserver-head-t99mj/ray-head | flush | 1 | 3256.5 | 3317.1 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-t99mj/ray-head | job | 76 | 3947.4 | 4014.7 | 0.651 | 2.294 |  |
| raycluster-historyserver-head-t99mj/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 4018.9 |
| rayjob-bench-8qtrs/ray-job-submitter | job | 71 | 98.0 | 108.5 | 0.039 | 0.988 |  |
| rayjob-bench-8qtrs/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.1 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 263.14 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 169 | 41.83 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 300.29 MiB | 98.5% |
| logs | 4.68 MiB | 1.5% |
| node_events | 6.18 KiB | 0.0% |
| **total** | **304.97 MiB** | (174 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 433061 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 433049 |
| events per task (k) | 4.33 |
| raw JSONL bytes | 300.29 MiB |
| stored event bytes | 300.29 MiB |
| avg raw bytes/event | 727 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 798af29623fefdc7636d0651c9d5161824270dde1b5b69259e8db43c | 232972 | 173.89 MiB | 100008 | 9896 | 7412.9 |
| c9ec772937e5a5fb1605efa822543839920bca3b0c05bb6fe11b3c62 | 200089 | 126.40 MiB | 100000 | 7110 | 6285.6 |

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
| TASK_LIFECYCLE_EVENT | 232946 |
| TASK_PROFILE_EVENT | 100095 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 6ms / 87ms / 87ms (errors: 0) |
| /enter_cluster cold load | 1m4.663s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 900ms | 1.38s | 1.38s | 536 B | 0 |
| /api/jobs/ | 16ms | 235ms | 235ms | 1.31 KiB | 0 |
| /nodes?view=summary | 17ms | 237ms | 237ms | 2.77 KiB | 0 |
| /events | 16ms | 239ms | 239ms | 3.21 KiB | 0 |

