# History Server Benchmark Report

- Date: 2026-08-06T14:30:49-05:00
- Tasks: 100000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_12-30-51_276018_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 1m20s |
| driver-measured wall | 35.8s |
| driver-measured rate | 2796.0 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-2rgsn | 2 | 126.39 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-6wln2 | 6 | 174.88 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 142.7 | 0.000 | 0.000 |
| collector (head) | job | 80 | 142.7 | 0.548 | 0.802 |
| collector (worker) | job | 80 | 111.3 | 0.334 | 0.593 |
| historyserver | historyserver | 88 | 1844.1 | 1.071 | 1.298 |
| ray-head | job | 80 | 3958.9 | 0.656 | 1.155 |
| ray-worker | job | 80 | 1051.4 | 1.196 | 2.043 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7d676b97c8-gpp5l/historyserver | historyserver | 86 | 1697.1 | 2048.6 | 1.121 | 1.514 |  |
| historyserver-demo-7d676b97c8-gpp5l/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2049.2 |
| raycluster-historyserver-cpu-worker-2rgsn/collector | job | 77 | 113.9 | 241.3 | 0.314 | 1.954 |  |
| raycluster-historyserver-cpu-worker-2rgsn/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 245.8 |
| raycluster-historyserver-cpu-worker-2rgsn/ray-worker | job | 77 | 1018.9 | 1061.2 | 0.979 | 2.096 |  |
| raycluster-historyserver-cpu-worker-2rgsn/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1065.7 |
| raycluster-historyserver-head-6wln2/collector | flush | 1 | 66.0 | 122.9 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-6wln2/collector | job | 77 | 145.1 | 259.4 | 0.481 | 1.910 |  |
| raycluster-historyserver-head-6wln2/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 264.0 |
| raycluster-historyserver-head-6wln2/ray-head | job | 77 | 3902.4 | 3968.6 | 0.646 | 2.269 |  |
| raycluster-historyserver-head-6wln2/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3971.9 |
| rayjob-bench-spg4g/ray-job-submitter | job | 72 | 97.8 | 108.7 | 0.039 | 0.976 |  |
| rayjob-bench-spg4g/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.4 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 4 | 241.45 MiB | 0 | 0 B | 0 |
| flush (T2-T1) | 170 | 64.50 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 301.27 MiB | 98.5% |
| logs | 4.68 MiB | 1.5% |
| node_events | 6.55 KiB | 0.0% |
| **total** | **305.96 MiB** | (175 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 435117 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 435104 |
| events per task (k) | 4.35 |
| raw JSONL bytes | 301.27 MiB |
| stored event bytes | 301.27 MiB |
| avg raw bytes/event | 726 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 100005 |
| distinct taskIds in benchmark job `03000000` | **100001 / 100000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 1001fe5d3cb251e6e03a6b9421b493905e42f223ef80329dbb92550f | 235048 | 174.88 MiB | 100008 | 10202 | 7513.0 |
| 87ef53c376700f238251ec0b73a7257b9ce77a461e7223a2c51a618a | 200069 | 126.39 MiB | 100000 | 7159 | 6363.7 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 4 |
| ACTOR_TASK_DEFINITION_EVENT | 3 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 100005 |
| TASK_LIFECYCLE_EVENT | 235001 |
| TASK_PROFILE_EVENT | 100095 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 6ms / 87ms / 87ms (errors: 0) |
| /enter_cluster cold load | 1m7.179s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=100000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 1.628s | 1.695s | 1.695s | 536 B | 0 |
| /api/jobs/ | 23ms | 297ms | 297ms | 1.31 KiB | 0 |
| /nodes?view=summary | 28ms | 385ms | 385ms | 2.77 KiB | 0 |
| /events | 239ms | 376ms | 376ms | 3.56 KiB | 0 |

