# History Server Benchmark Report

- Date: 2026-08-06T16:55:12-05:00
- Tasks: 50000 (wave 2000, num_cpus=0.2), compression=false
- Node bench-control-plane: v1.33.1, Debian GNU/Linux 12 (bookworm), cpu=14, mem=32813556Ki
- Session: `session_2026-08-06_14-35-51_963761_1`

## Load generation

| metric | value |
|---|---|
| RayJob wall clock (k8s-observed) | 0s |
| flush (cluster deletion incl. final upload) | 0s |

## Collector

## Container resources — kubelet summary API (working_set, k8s semantics, ~10s)

| class | phase | samples | peak working set (MiB) | avg cores | peak cores |
|---|---|---|---|---|---|

## Container resources — cgroup v2 direct (anon = pure heap, 1s; lifetime peak = kernel memory.peak)

| container | phase | samples | peak anon (MiB) | peak current (MiB) | avg cores | peak cores | lifetime peak (MiB) |
|---|---|---|---|---|---|---|---|
| historyserver-demo-6fddb7cf6c-7w598/historyserver | historyserver | 45 | 1112.1 | 1115.1 | 0.956 | 1.056 |  |
| historyserver-demo-6fddb7cf6c-7w598/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1116.2 |

## Storage footprint (session prefix)

| category | bytes | share |
|---|---|---|
| **total** | **0 B** | (0 objects) |

- Session marker present: false

## Event statistics

| metric | value |
|---|---|
| total events | 0 |
| task-scoped events (TASK_* + ACTOR_TASK_*) | 0 |
| events per task (k) | 0.00 |
| raw JSONL bytes | 0 B |
| stored event bytes | 0 B |
| avg raw bytes/event | 0 |
| compression ratio (stored/raw) | 0.000 |
| distinct taskDefinitionEvent taskIds (all jobs) | 0 |
| distinct taskIds in benchmark job `` | **0 / 0 expected** |

| event type | count |
|---|---|

## History server

| metric | value |
|---|---|
| GET /clusters p50 / p95 / max | 6ms / 82ms / 82ms (errors: 0) |
| /enter_cluster cold load | 41.156s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 3 |
| /api/v0/tasks/summarize | 881ms | 974ms | 974ms | 408 B | 0 |
| /api/jobs/ | 210ms | 247ms | 247ms | 1.31 KiB | 0 |
| /nodes?view=summary | 20ms | 33ms | 33ms | 2.77 KiB | 0 |
| /events | 220ms | 268ms | 268ms | 2.48 KiB | 0 |

