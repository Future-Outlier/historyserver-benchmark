#!/usr/bin/env python3
"""Write the immutable expected configuration for a formal benchmark sweep."""

from __future__ import annotations

import argparse
import json
import pathlib


BENCHMARK_S3_BUCKET = "ray-historyserver-benchmark"


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def parse_bool(value: str) -> bool:
    normalized = value.lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise argparse.ArgumentTypeError("must be true or false")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument(
        "--kind", choices=("by-task", "by-rate", "collector-cap"), default="by-task"
    )
    parser.add_argument(
        "--task-count", action="append", required=True, type=positive_int
    )
    parser.add_argument("--repeats", required=True, type=positive_int)
    parser.add_argument("--wave-size", required=True, type=positive_int)
    parser.add_argument("--task-num-cpus", required=True)
    parser.add_argument("--ray-image", required=True)
    parser.add_argument("--compression", required=True, type=parse_bool)
    parser.add_argument("--shutdown-after-job", required=True, type=parse_bool)
    parser.add_argument("--job-ttl-seconds", required=True, type=nonnegative_int)
    parser.add_argument("--drain-sleep-seconds", required=True, type=nonnegative_int)
    parser.add_argument("--warm-iterations", required=True, type=positive_int)
    parser.add_argument("--hs-cpu-request", required=True)
    parser.add_argument("--hs-cpu-limit", required=True)
    parser.add_argument("--hs-args", required=True)
    parser.add_argument("--skip-history-server", required=True, type=parse_bool)
    parser.add_argument("--drivers", required=True, type=positive_int)
    parser.add_argument("--collector-cpu-request")
    parser.add_argument("--collector-cpu-limit")
    parser.add_argument("--collector-memory-request")
    parser.add_argument("--collector-memory-limit")
    parser.add_argument(
        "--target-task-rate",
        action="append",
        required=True,
        type=nonnegative_int,
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if len(set(args.task_count)) != len(args.task_count):
        raise SystemExit("task counts must be unique")
    if len(set(args.target_task_rate)) != len(args.target_task_rate):
        raise SystemExit("target task rates must be unique")
    if args.kind == "by-task" and len(args.target_task_rate) != 1:
        raise SystemExit("by-task matrices require exactly one target task rate")
    if args.kind == "by-rate" and len(args.task_count) != 1:
        raise SystemExit("by-rate matrices require exactly one task count")
    if args.kind == "collector-cap" and (
        len(args.task_count) != 1 or len(args.target_task_rate) != 1
    ):
        raise SystemExit(
            "collector-cap matrices require exactly one task count and target task rate"
        )
    collector_resources = {
        "CollectorCPURequest": args.collector_cpu_request,
        "CollectorCPU": args.collector_cpu_limit,
        "CollectorMemoryRequest": args.collector_memory_request,
        "CollectorMemoryLimit": args.collector_memory_limit,
    }
    if args.kind == "collector-cap":
        if any(not value for value in collector_resources.values()):
            raise SystemExit("collector-cap matrices require all Collector resources")
    elif any(value is not None for value in collector_resources.values()):
        raise SystemExit("Collector resources are only valid for collector-cap matrices")
    if args.output.exists():
        raise SystemExit(f"refusing to overwrite {args.output}")

    arms = []
    for repeat in range(1, args.repeats + 1):
        if args.kind == "by-task":
            dimensions = (
                (task_count, args.target_task_rate[0], f"n{task_count}")
                for task_count in args.task_count
            )
        else:
            dimensions = (
                (
                    args.task_count[0],
                    target_task_rate,
                    "unpaced" if target_task_rate == 0 else f"rate{target_task_rate}",
                )
                for target_task_rate in args.target_task_rate
            )
        for task_count, target_task_rate, arm_prefix in dimensions:
            config = {
                "TaskCount": task_count,
                "WaveSize": args.wave_size,
                "TaskNumCPUs": args.task_num_cpus,
                "RayImage": args.ray_image,
                "Compression": args.compression,
                "ShutdownAfterJob": args.shutdown_after_job,
                "JobTTLSeconds": args.job_ttl_seconds,
                "DrainSleepSec": args.drain_sleep_seconds,
                "WarmIterations": args.warm_iterations,
                "HSCPURequest": args.hs_cpu_request,
                "HSCPULimit": args.hs_cpu_limit,
                "HSArgs": args.hs_args,
                "SkipHistoryServer": args.skip_history_server,
                "Drivers": args.drivers,
                "TargetTaskRate": target_task_rate,
                "S3Bucket": BENCHMARK_S3_BUCKET,
            }
            if args.kind == "collector-cap":
                config.update(collector_resources)
            arms.append(
                {
                    "name": f"{arm_prefix}-r{repeat}",
                    "repeat": repeat,
                    "config": config,
                }
            )

    document = {
        "schemaVersion": 4,
        "kind": args.kind,
        "arms": arms,
    }
    with args.output.open("x") as stream:
        json.dump(document, stream, indent=2, sort_keys=True)
        stream.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
