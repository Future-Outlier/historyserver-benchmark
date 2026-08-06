# History Server Benchmark Report

- Date: 2026-08-05T20:15:11-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_18-15-13_075830_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m15s |
| driver-measured wall | 31.9s |
| driver-measured rate | 3138.4 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-w96ct | 1 | 126.30 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-z9qv8 | 6 | 169.78 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 75 | 132.9 | 0.461 | 0.753 |
| collector (worker) | job | 75 | 112.7 | 0.327 | 0.623 |
| historyserver | historyserver | 75 | 1869.7 | 1.141 | 1.433 |
| ray-head | job | 75 | 3892.1 | 0.609 | 1.163 |
| ray-worker | job | 75 | 1032.8 | 1.126 | 1.866 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-85f7c7ff99-smfzq/historyserver | historyserver | 72 | 1672.7 | 1977.8 | 1.161 | 2.215 |  |
| historyserver-demo-85f7c7ff99-smfzq/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1979.6 |
| raycluster-historyserver-cpu-worker-w96ct/collector | job | 72 | 116.9 | 236.4 | 0.330 | 1.121 |  |
| raycluster-historyserver-cpu-worker-w96ct/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 241.5 |
| raycluster-historyserver-cpu-worker-w96ct/ray-worker | job | 72 | 1022.3 | 1063.5 | 0.959 | 2.105 |  |
| raycluster-historyserver-cpu-worker-w96ct/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1073.0 |
| raycluster-historyserver-head-z9qv8/collector | flush | 1 | 132.8 | 164.7 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-z9qv8/collector | job | 72 | 145.4 | 280.4 | 0.465 | 1.735 |  |
| raycluster-historyserver-head-z9qv8/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 298.8 |
| raycluster-historyserver-head-z9qv8/ray-head | flush | 1 | 1965.2 | 2004.3 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-z9qv8/ray-head | job | 72 | 3846.2 | 3915.8 | 0.644 | 2.318 |  |
| raycluster-historyserver-head-z9qv8/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3920.8 |
| rayjob-bench-vpg5q/ray-job-submitter | job | 68 | 97.9 | 108.3 | 0.035 | 0.947 |  |
| rayjob-bench-vpg5q/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.0 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 266.19 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 169 | 34.55 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 296.07 MiB | 98.4% |
| logs | 4.66 MiB | 1.6% |
| node_events | 6.17 KiB | 0.0% |
| **total** | **300.74 MiB** | (174 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 427420 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 427408 |
| events per task (k) | 4.27 |
| raw JSONL bytes | 296.08 MiB |
| stored event bytes | 296.08 MiB |
| avg raw bytes/event | 726 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 98326 |
| distinct taskIds in benchmark job `03000000` | **98322 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| f5af13b82fd7552853c1e71093ef0274f18f29825052afd136048571 | 227353 | 169.78 MiB | 98328 | 9356 | 7623.6 |
| 75ab5efa91ed1592e95c7e2610cd546ec9ee06f48b01091fc652bfc1 | 200067 | 126.30 MiB | 100000 | 7447 | 6661.4 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 3 |
| ACTOR_TASK_DEFINITION_EVENT | 2 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 98326 |
| TASK_LIFECYCLE_EVENT | 228987 |
| TASK_PROFILE_EVENT | 100093 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 4ms / 87ms / 87ms (errors: 0) |
| /enter_cluster cold load | 1m2.67s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 803ms | 943ms | 943ms | 423 B | 0 |
| /api/jobs/ | 24ms | 185ms | 185ms | 1.31 KiB | 0 |
| /nodes?view=summary | 23ms | 192ms | 192ms | 2.76 KiB | 0 |
| /events | 24ms | 182ms | 182ms | 2.84 KiB | 0 |

