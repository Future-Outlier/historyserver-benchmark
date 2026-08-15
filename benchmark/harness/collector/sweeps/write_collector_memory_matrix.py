#!/usr/bin/env python3
"""Write the immutable A/B/C Collector memory benchmark matrix.

The matrix is deliberately opinionated.  A formal campaign must not accept
ambient environment overrides for the workload, resource controls, or run
order: changing any of them creates a different experiment.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import random


SCHEMA_VERSION = 1
MATRIX_KIND = "collector-memory-abc"
REPEATS = 3
RANDOM_SEED = 20260813
RAY_IMAGE = "rayproject/ray:2.56.0"
COLLECTOR_IMAGE = "collector:v0.1.0"
S3_BUCKET = "ray-historyserver-benchmark"
DISCOVERY_MEMORY_LIMIT = "1Gi"
MEMORY_LIMIT_CANDIDATES = ("192Mi", "256Mi", "512Mi")
RATES_A = (1000, 2000, 3000, 5000)
RATES_B = (2000, 5000)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=pathlib.Path)
    return parser.parse_args()


def common_config(memory_limit: str) -> dict[str, object]:
    return {
        "publisherMode": "serial",
        "batchEvents": 1000,
        "fixtureSchema": "single-task-lifecycle-category-v1",
        "compressionEnabled": True,
        "rotationIntervalSeconds": 300,
        "rotationCheckSeconds": 30,
        "maxFileSizeMiB": 100,
        # Keep disk-pressure backpressure out of a memory-limit experiment.
        # Deliberately larger than production's 200 MiB. This experiment is
        # about memory boundedness, so disk-pressure backpressure is isolated.
        "maxDiskMiB": 1024,
        "baselineSeconds": 10,
        "sampleIntervalMilliseconds": 250,
        "collectorRole": "Worker",
        "freshPod": True,
        "rayImage": RAY_IMAGE,
        "collectorImage": COLLECTOR_IMAGE,
        "s3Bucket": S3_BUCKET,
        "cpuRequest": "100m",
        "cpuLimit": "2",
        "memoryRequest": "128Mi",
        "memoryLimit": memory_limit,
    }


def arm(
    experiment: str,
    rate: int,
    repeat: int,
    ingest_seconds: int,
    idle_seconds: int,
    memory_limit: str,
) -> dict[str, object]:
    prefix = f"{experiment}-rate{rate}"
    if experiment == "C":
        prefix = f"C-limit{memory_limit}"
    config = common_config(memory_limit)
    config.update(
        {
            "experiment": experiment,
            "targetEventsPerSecond": rate,
            "ingestSeconds": ingest_seconds,
            "idleSeconds": idle_seconds,
            "plannedEvents": rate * ingest_seconds,
            "idleGate": (
                "none"
                if experiment != "B"
                else "reclaim-after-delete"
                if rate == max(RATES_B)
                else "retained-file-plateau-negative-control"
            ),
        }
    )
    return {
        "name": f"{prefix}-r{repeat}",
        "repeat": repeat,
        "config": config,
    }


def build_matrix() -> dict[str, object]:
    arms: list[dict[str, object]] = []
    ab_order: list[str] = []
    for repeat in range(1, REPEATS + 1):
        repeat_arms: list[dict[str, object]] = []
        repeat_arms.extend(
            arm("A", rate, repeat, 90, 15, DISCOVERY_MEMORY_LIMIT)
            for rate in RATES_A
        )
        repeat_arms.extend(
            arm("B", rate, repeat, 30, 60, DISCOVERY_MEMORY_LIMIT)
            for rate in RATES_B
        )
        # Deterministic randomization preserves reproducibility while revisiting
        # every rate over time instead of measuring three adjacent repeats.
        random.Random(RANDOM_SEED + repeat).shuffle(repeat_arms)
        arms.extend(repeat_arms)
        ab_order.extend(str(item["name"]) for item in repeat_arms)

    c_groups: list[dict[str, object]] = []
    for memory_limit in MEMORY_LIMIT_CANDIDATES:
        names: list[str] = []
        for repeat in range(1, REPEATS + 1):
            item = arm("C", 5000, repeat, 90, 15, memory_limit)
            arms.append(item)
            names.append(str(item["name"]))
        c_groups.append({"memoryLimit": memory_limit, "arms": names})

    return {
        "schemaVersion": SCHEMA_VERSION,
        "kind": MATRIX_KIND,
        "randomSeed": RANDOM_SEED,
        "repeats": REPEATS,
        "executionOrderAB": ab_order,
        "candidateGroupsC": c_groups,
        "arms": arms,
        "acceptance": {
            "targetRateToleranceFraction": 0.05,
            "minimumCThroughputRatio": 0.95,
            "maximumCLatencyP99Ratio": 1.20,
            "maximumMemoryPeakToLimitRatio": 0.80,
            "maximumAnonWindowGrowthMiB": 8,
            "maximumAnonWindowGrowthFraction": 0.10,
            "maximumThreeRunPeakSpreadFraction": 0.10,
            "minimumPostDeleteObservationSeconds": 20,
            "minimumSampleCoverageFraction": 0.95,
        },
        "artifactContract": {
            "reportFile": "collector-memory-report.json",
            "oneReportRecursivelyUnderEachArmDirectory": True,
            "statusFile": "status.txt",
            "successSentinelPrefix": "SWEEP-SUCCEEDED ",
        },
    }


def main() -> int:
    args = parse_args()
    if args.output.exists():
        raise SystemExit(f"refusing to overwrite {args.output}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        json.dump(build_matrix(), stream, indent=2, sort_keys=True)
        stream.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
