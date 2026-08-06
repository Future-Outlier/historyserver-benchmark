# History Server Benchmark Report

- Date: 2026-08-05T18:14:34-05:00
- Tasks: 20000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-05_16-14-35_959183_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 30s |
| driver-measured wall | 8.0s |
| driver-measured rate | 2500.6 tasks/s |
| flush (cluster deletion incl. final upload) | 2s |

## Collector

| pod | uploads | uploaded bytes | disk-pressure 503s | queue-full | upload failures |
|---|---|---|---|---|---|
| raycluster-historyserver-cpu-worker-swk8j | 0 | 0 B | 0 | 0 | 0 |
| raycluster-historyserver-head-cdg4p | 0 | 0 B | 0 | 0 | 0 |

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|
| collector (head) | job | 30 | 104.8 | 0.230 | 0.420 |
| collector (worker) | job | 30 | 91.2 | 0.248 | 0.248 |
| historyserver | historyserver | 48 | 477.6 | 0.483 | 0.521 |
| ray-head | job | 30 | 2590.5 | 0.422 | 0.449 |
| ray-worker | job | 30 | 491.6 | 0.909 | 0.909 |

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-7f67bfd478-8whdc/historyserver | historyserver | 46 | 479.9 | 511.8 | 0.488 | 0.536 |  |
| historyserver-demo-7f67bfd478-8whdc/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 524.2 |
| raycluster-historyserver-cpu-worker-swk8j/collector | flush | 1 | 105.6 | 133.0 | 0.455 | 0.455 |  |
| raycluster-historyserver-cpu-worker-swk8j/collector | job | 29 | 114.2 | 135.3 | 0.158 | 1.030 |  |
| raycluster-historyserver-cpu-worker-swk8j/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 138.4 |
| raycluster-historyserver-cpu-worker-swk8j/ray-worker | job | 29 | 939.5 | 978.9 | 0.630 | 2.109 |  |
| raycluster-historyserver-cpu-worker-swk8j/ray-worker | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 981.3 |
| raycluster-historyserver-head-cdg4p/collector | flush | 1 | 102.7 | 140.6 | 0.430 | 0.430 |  |
| raycluster-historyserver-head-cdg4p/collector | job | 29 | 137.4 | 170.3 | 0.236 | 1.153 |  |
| raycluster-historyserver-head-cdg4p/collector | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 172.7 |
| raycluster-historyserver-head-cdg4p/ray-head | flush | 1 | 937.5 | 970.2 | 1.330 | 1.330 |  |
| raycluster-historyserver-head-cdg4p/ray-head | job | 29 | 2727.3 | 2787.3 | 0.479 | 1.235 |  |
| raycluster-historyserver-head-cdg4p/ray-head | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 2789.7 |
| rayjob-bench-s9mlr/ray-job-submitter | job | 25 | 97.6 | 108.1 | 0.101 | 0.955 |  |
| rayjob-bench-s9mlr/ray-job-submitter | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 110.2 |

## Storage delta (bucket snapshots)

| window | added objs | added bytes | changed objs | changed bytes | deleted objs |
|---|---|---|---|---|---|
| during-job (T1-T0) | 0 | 0 B | 0 | 0 B | 0 |
| flush (T2-T1) | 172 | 61.77 MiB | 0 | 0 B | 0 |


## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| fetched_endpoints | 475 B | 0.0% |
| job_events | 60.61 MiB | 98.1% |
| logs | 1.15 MiB | 1.9% |
| node_events | 6.90 KiB | 0.0% |
| **total** | **61.77 MiB** | (173 objects) |

- Session marker present: true

## Event statistics

| metric | value |
|---|---|
| total events | 87813 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 87799 |
| events per task (k) | 4.39 |
| raw JSONL bytes | 60.62 MiB |
| stored event bytes | 60.62 MiB |
| avg raw bytes/event | 724 |
| compression ratio (stored/raw) | 1.000 |
| distinct taskDefinitionEvent taskIds | 20005 / 20000 expected |

### Per-node attribution (whose aggregator emitted the events)

| node | events | raw bytes | distinct taskIds | peak 1s events | peak 10s-avg events/s |
|---|---|---|---|---|---|
| e44c42cf45e2fa8d708a72a136a289caaf7f618682b9d2a7b2a79152 | 47793 | 35.36 MiB | 20006 | 8787 | 4776.3 |
| 5b44ea6a3ba408e612314e22c5fb008610793ff7fca43df5485599ac | 40020 | 25.26 MiB | 20000 | 6628 | 4002.0 |

| event type | count |
|---|---|
| ACTOR_DEFINITION_EVENT | 2 |
| ACTOR_LIFECYCLE_EVENT | 5 |
| ACTOR_TASK_DEFINITION_EVENT | 1 |
| DRIVER_JOB_DEFINITION_EVENT | 1 |
| DRIVER_JOB_LIFECYCLE_EVENT | 2 |
| NODE_DEFINITION_EVENT | 2 |
| NODE_LIFECYCLE_EVENT | 2 |
| TASK_DEFINITION_EVENT | 20005 |
| TASK_LIFECYCLE_EVENT | 47763 |
| TASK_PROFILE_EVENT | 20030 |

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 9ms / 119ms / 119ms (errors: 0) |
| /enter_cluster cold load | 37.596s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=20000 | 0s | 0s | 0s | 0 B | 10 |
| /api/v0/tasks/summarize | 835ms | 987ms | 987ms | 408 B | 0 |
| /api/jobs/ | 10ms | 260ms | 260ms | 1.22 KiB | 0 |
| /nodes?view=summary | 10ms | 285ms | 285ms | 2.76 KiB | 0 |
| /events | 9ms | 270ms | 270ms | 1.40 KiB | 0 |

