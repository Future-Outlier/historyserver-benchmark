# History Server Benchmark Report

- Date: 2026-08-06T14:42:37-05:00
- Tasks: 50000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_12-42-39_321285_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 55s |
| driver-measured wall | 18.2s |
| driver-measured rate | 2746.9 tasks/s |
| flush (cluster deletion incl. final upload) | 3s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-jv44h | 1 | 63.20 MiB | 0 | 0 | 0 |
| raycluster-historyserver-head-pnvsz | 5 | 87.87 MiB | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | flush | 1 | 127.0 | 0.000 | 0.000 |
| collector (head) | job | 55 | 125.0 | 0.429 | 0.643 |
| collector (worker) | flush | 1 | 108.9 | 0.000 | 0.000 |
| collector (worker) | job | 55 | 109.5 | 0.230 | 0.367 |
| historyserver | historyserver | 41 | 926.3 | 1.061 | 1.389 |
| ray-head | job | 55 | 3788.7 | 0.542 | 0.890 |
| ray-worker | job | 55 | 982.7 | 0.715 | 1.399 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-6f87d5d7c9-2bgr9/historyserver | historyserver | 39 | 1111.2 | 1115.0 | 1.164 | 1.673 |  |
| historyserver-demo-6f87d5d7c9-2bgr9/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1116.5 |
| raycluster-historyserver-cpu-worker-jv44h/collector | flush | 1 | 108.4 | 174.5 | 0.000 | 0.000 |  |
| raycluster-historyserver-cpu-worker-jv44h/collector | job | 53 | 116.8 | 172.2 | 0.215 | 1.015 |  |
| raycluster-historyserver-cpu-worker-jv44h/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 180.2 |
| raycluster-historyserver-cpu-worker-jv44h/ray-worker | job | 53 | 978.3 | 1015.3 | 0.743 | 2.107 |  |
| raycluster-historyserver-cpu-worker-jv44h/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1018.4 |
| raycluster-historyserver-head-pnvsz/collector | flush | 1 | 125.9 | 217.4 | 0.000 | 0.000 |  |
| raycluster-historyserver-head-pnvsz/collector | job | 53 | 143.7 | 213.9 | 0.338 | 1.135 |  |
| raycluster-historyserver-head-pnvsz/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 230.7 |
| raycluster-historyserver-head-pnvsz/ray-head | job | 53 | 3756.0 | 3824.4 | 0.511 | 1.821 |  |
| raycluster-historyserver-head-pnvsz/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 3828.8 |
| rayjob-bench-bgbmb/ray-job-submitter | job | 45 | 97.9 | 108.3 | 0.058 | 0.930 |  |
| rayjob-bench-bgbmb/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.4 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 153.57 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 151.06 MiB | 98.4% |
| logs | 2.51 MiB | 1.6% |
| node_events | 6.55 KiB | 0.0% |
| **total** | **153.57 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 218457 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 218444 |
| events per task (k) | 4.37 |
| raw JSONL bytes | 151.07 MiB |
| stored event bytes | 151.07 MiB |
| avg raw bytes/event | 725 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 50005 |
| distinct taskIds in benchmark job `03000000` | **50001 / 50000 expected** |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| 7021b3d3eb19d6870c5f08a876e7a89b05a3402e6af03ab605c251e6 | 118418 | 87.87 MiB | 50007 | 9122 | 7265.1 |
| e204ea920afc660d141428a4b6fa0dcb791e4d6f171d833fc50494c0 | 100039 | 63.20 MiB | 50000 | 7145 | 6170.6 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 4 |
| ACTOR_TASK_DEFINITION_EVENT | 2 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 50005 |
| TASK_LIFECYCLE_EVENT | 118381 |
| TASK_PROFILE_EVENT | 50056 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 6ms / 82ms / 82ms (errors: 0) |
| /enter_cluster cold load | 31.772s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 583ms | 807ms | 807ms | 408 B | 0 |
| /api/jobs/ | 12ms | 113ms | 113ms | 1.31 KiB | 0 |
| /nodes?view=summary | 102ms | 121ms | 121ms | 2.77 KiB | 0 |
| /events | 13ms | 102ms | 102ms | 2.12 KiB | 0 |

