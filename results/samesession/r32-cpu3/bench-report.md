# History Server Benchmark Report

- Date: 2026-08-06T17:04:52-05:00
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
| historyserver-demo-65887b79cf-llscn/historyserver | historyserver | 36 | 1183.2 | 1186.4 | 1.078 | 1.912 |  |
| historyserver-demo-65887b79cf-llscn/historyserver | lifetime | 0 | 0.0 | 0.0 | 0.000 | 0.000 | 1187.6 |

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
| GET /clusters p50 / p95 / max | 6ms / 80ms / 80ms (errors: 0) |
| /enter_cluster cold load | 32.874s (HTTP 200) |

| warm endpoint | p50 | p95 | max | resp bytes | errors |
|---|---|---|---|---|---|
| /api/v0/tasks?limit=50000 | 0s | 0s | 0s | 0 B | 3 |
| /api/v0/tasks/summarize | 458ms | 600ms | 600ms | 408 B | 0 |
| /api/jobs/ | 11ms | 14ms | 14ms | 1.31 KiB | 0 |
| /nodes?view=summary | 12ms | 16ms | 16ms | 2.77 KiB | 0 |
| /events | 131ms | 153ms | 153ms | 2.48 KiB | 0 |

