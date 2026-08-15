#!/usr/bin/env python3
"""Fail closed when benchmark provenance or run artifacts are incomplete."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import pathlib
import re
import sys
from typing import Optional

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
IMAGE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
CONTAINER_IMAGE_ID_RE = re.compile(r"(?:^|@)(sha256:[0-9a-f]{64})$")
STATUS_RE = re.compile(r"^(?P<arm>\S+) rc=(?P<rc>\d+)\b")
TEN_SECONDS_NANO = 10_000_000_000
FORMAL_RATE_TARGETS = {0, 250, 500, 1000, 2000, 3000}
# The prior uncapped formal rate sweep's six high-load role/run observations
# (target=3000 and unpaced) bottomed out at 4,102.1 events/s. Round down so a
# capped run must reach the same empirical high-load regime without requiring
# it to reproduce the single noisiest 5,260.2 events/s window exactly.
COLLECTOR_CAP_MIN_PEAK_EVENTS_PER_SECOND = 4000.0
BENCHMARK_S3_BUCKET = "ray-historyserver-benchmark"
TARGET_RATE_PACING_BLOCK = "\n".join(
    (
        "        if target > 0:",
        "            behind = (i + 1) / target - (time.time() - t0)",
        "            if behind > 0:",
        "                time.sleep(behind)",
    )
)
TARGET_RATE_PACING_SLEEP_LINE = "                time.sleep(behind)"

REQUIRED_PROVENANCE = {
    "repo_head",
    "tracked_diff_sha256",
    "benchmark_source_sha256",
    "historyserver_source_sha256",
    "operator_sha256",
    "raycluster_manifest_sha256",
    "historyserver_manifest_sha256",
    "expected_matrix_sha256",
    "ray_image_requested",
    "ray_runtime_id",
    "ray_runtime_repo_digests",
    "ray_runtime_version",
    "ray_runtime_commit",
    "collector_image_requested",
    "collector_build_source_sha256",
    "collector_runtime_id",
    "collector_runtime_repo_digests",
    "collector_runtime_build_source_sha256",
    "historyserver_image_requested",
    "historyserver_build_source_sha256",
    "historyserver_runtime_id",
    "historyserver_runtime_repo_digests",
    "historyserver_runtime_build_source_sha256",
}

SHA256_FIELDS = {
    "tracked_diff_sha256",
    "benchmark_source_sha256",
    "historyserver_source_sha256",
    "operator_sha256",
    "raycluster_manifest_sha256",
    "historyserver_manifest_sha256",
    "expected_matrix_sha256",
    "collector_build_source_sha256",
    "collector_runtime_build_source_sha256",
    "historyserver_build_source_sha256",
    "historyserver_runtime_build_source_sha256",
}

FINAL_PROVENANCE_FIELDS = {
    "revalidation_status",
    "repo_head",
    "tracked_diff_sha256",
    "benchmark_source_sha256",
    "historyserver_source_sha256",
    "operator_sha256",
    "raycluster_manifest_sha256",
    "historyserver_manifest_sha256",
    "expected_matrix_sha256",
    "ray_runtime_id",
    "ray_runtime_version",
    "ray_runtime_commit",
    "collector_build_source_sha256",
    "collector_runtime_id",
    "collector_runtime_build_source_sha256",
    "historyserver_build_source_sha256",
    "historyserver_runtime_id",
    "historyserver_runtime_build_source_sha256",
}

EXPECTED_MATRIX_V1_CONFIG_FIELDS = {
    "TaskCount",
    "WaveSize",
    "TaskNumCPUs",
    "RayImage",
    "Compression",
    "ShutdownAfterJob",
    "JobTTLSeconds",
    "DrainSleepSec",
    "WarmIterations",
    "HSCPURequest",
    "HSCPULimit",
    "HSArgs",
    "Drivers",
    "TargetTaskRate",
}
EXPECTED_MATRIX_V2_CONFIG_FIELDS = EXPECTED_MATRIX_V1_CONFIG_FIELDS | {
    "SkipHistoryServer",
}
EXPECTED_MATRIX_V3_CONFIG_FIELDS = EXPECTED_MATRIX_V2_CONFIG_FIELDS | {
    "CollectorCPURequest",
    "CollectorCPU",
    "CollectorMemoryRequest",
    "CollectorMemoryLimit",
}
EXPECTED_MATRIX_V4_CONFIG_FIELDS = EXPECTED_MATRIX_V2_CONFIG_FIELDS | {
    "S3Bucket",
}

WARM_ENDPOINTS_WITHOUT_TASKS = (
    "/api/v0/tasks/summarize",
    "/api/jobs/",
    "/nodes?view=summary",
    "/events",
)

DECODE_ERROR_PATTERNS = (
    "failed to unmarshal task lifecycle event",
    "Failed to store events",
    "TASK_LIFECYCLE_EVENT must have at least one state transition",
)


class ValidationError(RuntimeError):
    pass


def normalize_container_image_id(value: object) -> Optional[str]:
    """Return the full digest from CRI's bare or repository-qualified image ID."""
    if not isinstance(value, str):
        return None
    match = CONTAINER_IMAGE_ID_RE.search(value)
    return match.group(1) if match else None


def normalize_local_image_reference(value: object) -> Optional[str]:
    """Canonicalize the two CRI spellings of an unqualified local image tag."""
    if not isinstance(value, str) or not value:
        return None
    prefix = "docker.io/library/"
    return value[len(prefix) :] if value.startswith(prefix) else value


def validate_task_window_sequence(
    label: str,
    windows: dict[int, tuple[int, int, int, int]],
    errors: list[str],
) -> None:
    outstanding = 0
    for start in sorted(windows):
        end, _submitted, _finished, backlog_delta = windows[start]
        if start % TEN_SECONDS_NANO != 0:
            errors.append(f"{label} window start is not epoch-aligned to 10s: {start}")
        if end - start != TEN_SECONDS_NANO:
            errors.append(f"{label} window at {start} is not 10 seconds")
        outstanding += backlog_delta
        if outstanding < 0:
            errors.append(
                f"{label} cumulative task backlog is negative at {start}: "
                f"{outstanding}"
            )
    if outstanding != 0:
        errors.append(f"{label} final task backlog is not zero: {outstanding}")


def read_event_id_counts(
    value: object,
    label: str,
    total_field: str,
    errors: list[str],
) -> Optional[dict[str, int]]:
    if not isinstance(value, dict):
        errors.append(f"{label} is not an object")
        return None

    counts: dict[str, int] = {}
    for field in (
        total_field,
        "distinctEventIDs",
        "missingEventIDs",
        "duplicateEventIDs",
    ):
        observed = value.get(field)
        if type(observed) is not int or observed < 0:
            errors.append(
                f"{label}.{field} must be a non-negative integer: {observed!r}"
            )
        else:
            counts[field] = observed
    if len(counts) != 4:
        return None

    if (
        counts["distinctEventIDs"]
        + counts["missingEventIDs"]
        + counts["duplicateEventIDs"]
        != counts[total_field]
    ):
        errors.append(
            f"{label} event-ID counts are inconsistent: "
            f"{total_field}={counts[total_field]}, "
            f"distinct={counts['distinctEventIDs']}, "
            f"missing={counts['missingEventIDs']}, "
            f"duplicate extras={counts['duplicateEventIDs']}"
        )
    return counts


def validate_event_id_integrity(
    events: object,
    per_node: object,
    errors: list[str],
) -> None:
    global_counts = read_event_id_counts(
        events,
        "storage.events",
        "totalEvents",
        errors,
    )

    node_counts: list[dict[str, int]] = []
    if not isinstance(per_node, list):
        errors.append("storage.events.perNode is not a list for event-ID validation")
    else:
        for index, node in enumerate(per_node):
            counts = read_event_id_counts(
                node,
                f"storage.events.perNode[{index}]",
                "events",
                errors,
            )
            if counts is not None:
                node_counts.append(counts)

    if (
        global_counts is not None
        and isinstance(per_node, list)
        and len(node_counts) == len(per_node)
    ):
        node_events = sum(counts["events"] for counts in node_counts)
        node_missing = sum(counts["missingEventIDs"] for counts in node_counts)
        node_distinct = sum(counts["distinctEventIDs"] for counts in node_counts)
        node_duplicates = sum(counts["duplicateEventIDs"] for counts in node_counts)

        if node_events != global_counts["totalEvents"]:
            errors.append(
                "per-node event total does not match storage.events.totalEvents: "
                f"per-node={node_events}, global={global_counts['totalEvents']}"
            )
        if node_missing != global_counts["missingEventIDs"]:
            errors.append(
                "per-node missing event-ID total does not match the global count: "
                f"per-node={node_missing}, global={global_counts['missingEventIDs']}"
            )

        cross_node_duplicate_extras = (
            node_distinct - global_counts["distinctEventIDs"]
        )
        if cross_node_duplicate_extras < 0:
            errors.append(
                "per-node/global event-ID distinct counts are inconsistent: "
                f"per-node={node_distinct}, "
                f"global={global_counts['distinctEventIDs']}"
            )
        elif (
            node_duplicates + cross_node_duplicate_extras
            != global_counts["duplicateEventIDs"]
        ):
            errors.append(
                "per-node/global duplicate event-ID counts are inconsistent: "
                f"within-node extras={node_duplicates}, "
                f"cross-node extras={cross_node_duplicate_extras}, "
                f"global extras={global_counts['duplicateEventIDs']}"
            )

    if global_counts is not None:
        if global_counts["missingEventIDs"] != 0:
            errors.append(
                "storage.events.missingEventIDs="
                f"{global_counts['missingEventIDs']}, expected 0"
            )
        if global_counts["duplicateEventIDs"] != 0:
            errors.append(
                "storage.events.duplicateEventIDs="
                f"{global_counts['duplicateEventIDs']}, expected 0"
            )
        if global_counts["distinctEventIDs"] != global_counts["totalEvents"]:
            errors.append(
                "storage.events.distinctEventIDs must equal totalEvents: "
                f"distinct={global_counts['distinctEventIDs']}, "
                f"total={global_counts['totalEvents']}"
            )


def reject_duplicate_json_keys(pairs):
    values = {}
    for key, value in pairs:
        if key in values:
            raise ValidationError(f"JSON object contains duplicate key {key!r}")
        values[key] = value
    return values


def read_key_value_file(path: pathlib.Path, label: str) -> dict[str, str]:
    if not path.is_file():
        raise ValidationError(f"missing {path}")
    try:
        lines = path.read_text().splitlines()
    except OSError as exc:
        raise ValidationError(f"cannot read {label}: {exc}") from exc

    values: dict[str, str] = {}
    for line_number, line in enumerate(lines, start=1):
        if not line:
            continue
        if "=" not in line:
            raise ValidationError(
                f"{label} line {line_number} is not key=value: {line!r}"
            )
        key, value = line.split("=", 1)
        if not key:
            raise ValidationError(f"{label} line {line_number} has an empty key")
        if key in values:
            raise ValidationError(f"{label} contains duplicate key {key!r}")
        values[key] = value
    return values


def read_provenance(root: pathlib.Path) -> dict[str, str]:
    path = root / "provenance.txt"
    values = read_key_value_file(path, "provenance")

    missing = sorted(REQUIRED_PROVENANCE - values.keys())
    if missing:
        raise ValidationError(f"provenance fields missing: {missing}")
    empty = sorted(key for key in REQUIRED_PROVENANCE if not values[key])
    if empty:
        raise ValidationError(f"provenance fields empty: {empty}")
    invalid_sha = sorted(
        key for key in SHA256_FIELDS if not SHA256_RE.fullmatch(values[key])
    )
    if invalid_sha:
        raise ValidationError(f"invalid SHA-256 fields: {invalid_sha}")
    for key in ("ray_runtime_id", "collector_runtime_id", "historyserver_runtime_id"):
        if not IMAGE_ID_RE.fullmatch(values[key]):
            raise ValidationError(
                f"{key} is not a full sha256 image ID: {values[key]!r}"
            )
    for key in ("ray_runtime_version", "ray_runtime_commit"):
        if values[key] == "<none>":
            raise ValidationError(f"{key} is not present in the runtime image")
    for component in ("collector", "historyserver"):
        build_key = f"{component}_build_source_sha256"
        runtime_key = f"{component}_runtime_build_source_sha256"
        if values[build_key] != values[runtime_key]:
            raise ValidationError(
                f"{component} build source is not bound to its runtime image: "
                f"build={values[build_key]!r}, runtime={values[runtime_key]!r}"
            )

    matrix_path = root / "expected-matrix.json"
    if not matrix_path.is_file():
        raise ValidationError(f"missing {matrix_path}")
    try:
        matrix_sha256 = hashlib.sha256(matrix_path.read_bytes()).hexdigest()
    except OSError as exc:
        raise ValidationError(f"cannot hash expected matrix: {exc}") from exc
    if matrix_sha256 != values["expected_matrix_sha256"]:
        raise ValidationError(
            "expected matrix SHA-256 does not match provenance: "
            f"captured={values['expected_matrix_sha256']!r}, actual={matrix_sha256!r}"
        )
    return values


def read_final_provenance(
    root: pathlib.Path, initial: dict[str, str]
) -> dict[str, str]:
    path = root / "provenance-final.txt"
    values = read_key_value_file(path, "final provenance")

    missing = sorted(FINAL_PROVENANCE_FIELDS - values.keys())
    if missing:
        raise ValidationError(f"final provenance fields missing: {missing}")
    empty = sorted(key for key in FINAL_PROVENANCE_FIELDS if not values[key])
    if empty:
        raise ValidationError(f"final provenance fields empty: {empty}")
    if values["revalidation_status"] != "valid":
        raise ValidationError(
            "final provenance was not revalidated: "
            f"{values['revalidation_status']!r}"
        )

    for key in FINAL_PROVENANCE_FIELDS - {"revalidation_status"}:
        if values[key] != initial[key]:
            raise ValidationError(
                f"final provenance {key} changed: "
                f"initial={initial[key]!r}, final={values[key]!r}"
            )
    return values


def read_expected_matrix(
    root: pathlib.Path,
    expected_arms: Optional[int] = None,
    *,
    allow_legacy_schema: bool = False,
) -> dict[str, dict]:
    path = root / "expected-matrix.json"
    try:
        document = json.loads(
            path.read_text(), object_pairs_hook=reject_duplicate_json_keys
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError(f"cannot read expected matrix: {exc}") from exc
    if not isinstance(document, dict):
        raise ValidationError("expected matrix root must be an object")
    schema_version = document.get("schemaVersion")
    kind = document.get("kind")
    if (
        type(schema_version) is not int
        or schema_version not in {1, 2, 3, 4}
        or kind not in {"by-task", "by-rate", "collector-cap"}
    ):
        raise ValidationError(
            "expected matrix must have schemaVersion=1, 2, 3, or 4 and a supported kind"
        )
    if schema_version < 4 and not allow_legacy_schema:
        raise ValidationError(
            f"legacy schemaVersion={schema_version} requires explicit "
            "--allow-legacy-schema for offline validation"
        )
    if schema_version == 3 and kind != "collector-cap":
        raise ValidationError("schemaVersion=3 is reserved for kind='collector-cap'")
    if schema_version in {1, 2} and kind == "collector-cap":
        raise ValidationError("collector-cap requires schemaVersion=3 or 4")
    expected_config_fields = {
        1: EXPECTED_MATRIX_V1_CONFIG_FIELDS,
        2: EXPECTED_MATRIX_V2_CONFIG_FIELDS,
        3: EXPECTED_MATRIX_V3_CONFIG_FIELDS,
        4: EXPECTED_MATRIX_V4_CONFIG_FIELDS
        | (EXPECTED_MATRIX_V3_CONFIG_FIELDS - EXPECTED_MATRIX_V2_CONFIG_FIELDS if kind == "collector-cap" else set()),
    }[schema_version]
    arms = document.get("arms")
    if not isinstance(arms, list) or not arms:
        raise ValidationError("expected matrix arms must be a non-empty list")
    if expected_arms is not None and len(arms) != expected_arms:
        raise ValidationError(
            f"expected matrix has {len(arms)} arms, command expected {expected_arms}"
        )

    by_name: dict[str, dict] = {}
    dimension_field = "TaskCount" if kind == "by-task" else "TargetTaskRate"
    repeats_by_dimension: dict[int, set[int]] = {}
    campaign_config = None
    for index, arm in enumerate(arms):
        if not isinstance(arm, dict):
            raise ValidationError(f"expected matrix arm {index} is not an object")
        name = arm.get("name")
        repeat = arm.get("repeat")
        raw_config = arm.get("config")
        if not isinstance(name, str) or not name:
            raise ValidationError(
                f"expected matrix arm {index} has invalid name {name!r}"
            )
        if name in by_name:
            raise ValidationError(f"expected matrix contains duplicate arm {name!r}")
        if type(repeat) is not int or repeat <= 0:
            raise ValidationError(
                f"expected matrix arm {name!r} has invalid repeat {repeat!r}"
            )
        if not isinstance(raw_config, dict):
            raise ValidationError(f"expected matrix arm {name!r} has no config object")
        fields = set(raw_config)
        if fields != expected_config_fields:
            raise ValidationError(
                f"expected matrix arm {name!r} config fields differ: "
                f"missing={sorted(expected_config_fields - fields)}, "
                f"extra={sorted(fields - expected_config_fields)}"
            )
        config = dict(raw_config)
        if schema_version == 1:
            # v1 predates Collector-only runs. Its exact old field set means
            # every legacy arm semantically ran the History Server phase.
            config["SkipHistoryServer"] = False
        if schema_version == 4 and config.get("S3Bucket") != BENCHMARK_S3_BUCKET:
            raise ValidationError(
                f"expected matrix arm {name!r} S3Bucket={config.get('S3Bucket')!r}, "
                f"want {BENCHMARK_S3_BUCKET!r}"
            )

        integer_fields = {
            "TaskCount": True,
            "WaveSize": True,
            "JobTTLSeconds": False,
            "DrainSleepSec": False,
            "WarmIterations": True,
            "Drivers": True,
            "TargetTaskRate": False,
        }
        for field, positive in integer_fields.items():
            value = config[field]
            if type(value) is not int or value < (1 if positive else 0):
                raise ValidationError(
                    f"expected matrix arm {name!r} has invalid {field}={value!r}"
                )
        for field in (
            "TaskNumCPUs",
            "RayImage",
            "HSCPURequest",
            "HSCPULimit",
            "HSArgs",
        ):
            if not isinstance(config[field], str) or not config[field]:
                raise ValidationError(
                    f"expected matrix arm {name!r} has invalid {field}={config[field]!r}"
                )
        if kind == "collector-cap":
            for field in (
                "CollectorCPURequest",
                "CollectorCPU",
                "CollectorMemoryRequest",
                "CollectorMemoryLimit",
            ):
                if not isinstance(config[field], str) or not config[field]:
                    raise ValidationError(
                        f"expected matrix arm {name!r} has invalid {field}={config[field]!r}"
                    )
        for field in ("Compression", "ShutdownAfterJob", "SkipHistoryServer"):
            if type(config[field]) is not bool:
                raise ValidationError(
                    f"expected matrix arm {name!r} has invalid {field}={config[field]!r}"
                )

        task_count = config["TaskCount"]
        target_task_rate = config["TargetTaskRate"]
        if kind == "by-task":
            expected_name = f"n{task_count}-r{repeat}"
        else:
            prefix = "unpaced" if target_task_rate == 0 else f"rate{target_task_rate}"
            expected_name = f"{prefix}-r{repeat}"
        if name != expected_name:
            raise ValidationError(
                f"expected matrix arm name {name!r} does not match "
                f"{dimension_field}={config[dimension_field]}, repeat={repeat}"
            )
        if config["Compression"] is not True:
            raise ValidationError(
                f"formal {kind} arm {name!r} must enable compression"
            )
        if config["ShutdownAfterJob"] is not True or config["JobTTLSeconds"] != 30:
            raise ValidationError(
                f"formal {kind} arm {name!r} must use "
                "shutdownAfterJob=true and TTL=30"
            )
        if config["DrainSleepSec"] != 0:
            raise ValidationError(
                f"formal {kind} arm {name!r} must use DrainSleepSec=0"
            )
        # Legacy v1 by-rate artifacts ran the History Server and remain
        # readable. Schema v2 and later encode the Collector-only contract.
        expected_skip_history_server = kind in {"by-rate", "collector-cap"} and schema_version >= 2
        if config["SkipHistoryServer"] is not expected_skip_history_server:
            raise ValidationError(
                f"formal {kind} arm {name!r} must use "
                f"SkipHistoryServer={expected_skip_history_server}"
            )
        if config["HSCPURequest"] != config["HSCPULimit"]:
            raise ValidationError(
                f"formal {kind} arm {name!r} must use HS CPU request=limit"
            )
        if kind in {"by-rate", "collector-cap"}:
            fixed_rate_config = {
                "TaskCount": 50000,
                "WaveSize": 2000,
                "TaskNumCPUs": "0.5",
                "RayImage": "rayproject/ray:2.56.0",
                "Compression": True,
                "ShutdownAfterJob": True,
                "JobTTLSeconds": 30,
                "DrainSleepSec": 0,
                "WarmIterations": 3,
                "HSCPURequest": "4",
                "HSCPULimit": "4",
                "HSArgs": "--session-process-timeout=30m",
                "SkipHistoryServer": schema_version >= 2,
                "Drivers": 1,
            }
            if kind == "collector-cap":
                fixed_rate_config.update(
                    {
                        "CollectorCPURequest": "150m",
                        "CollectorCPU": "1200m",
                        "CollectorMemoryRequest": "160Mi",
                        "CollectorMemoryLimit": "192Mi",
                    }
                )
            changed = sorted(
                field
                for field, wanted in fixed_rate_config.items()
                if config[field] != wanted or type(config[field]) is not type(wanted)
            )
            if changed:
                raise ValidationError(
                    f"formal {kind} arm {name!r} changes fixed config: {changed}"
                )
        invariant_config = {
            field: value
            for field, value in config.items()
            if field != dimension_field
        }
        if campaign_config is None:
            campaign_config = invariant_config
        elif invariant_config != campaign_config:
            changed = sorted(
                field
                for field in invariant_config
                if invariant_config[field] != campaign_config[field]
            )
            raise ValidationError(
                f"formal {kind} arm {name!r} changes campaign-wide config: {changed}"
            )
        repeats_by_dimension.setdefault(config[dimension_field], set()).add(repeat)
        normalized_arm = dict(arm)
        normalized_arm["config"] = config
        normalized_arm["_schemaVersion"] = schema_version
        by_name[name] = normalized_arm

    if kind == "by-task":
        expected_dimensions = {1000, 10000, 50000, 100000}
        if set(repeats_by_dimension) != expected_dimensions:
            raise ValidationError(
                "formal by-task matrix task counts must be 1k/10k/50k/100k, "
                f"found {sorted(repeats_by_dimension)}"
            )
    elif kind == "by-rate":
        expected_dimensions = FORMAL_RATE_TARGETS
        if set(repeats_by_dimension) != expected_dimensions:
            raise ValidationError(
                "formal by-rate matrix target task rates must be "
                "unpaced/250/500/1000/2000/3000, found "
                f"{sorted(repeats_by_dimension)}"
            )
    else:
        if set(repeats_by_dimension) != {3000}:
            raise ValidationError(
                "formal collector-cap matrix target task rate must be 3000, found "
                f"{sorted(repeats_by_dimension)}"
            )
    for dimension, repeats in repeats_by_dimension.items():
        if repeats != {1, 2, 3}:
            raise ValidationError(
                f"{dimension_field}={dimension} repeats must be 1/2/3, "
                f"found {sorted(repeats)}"
            )
    return by_name


def read_status(root: pathlib.Path, expected_arm_names: set[str]) -> set[str]:
    path = root / "status.txt"
    if not path.is_file():
        raise ValidationError(f"missing {path}")
    statuses: dict[str, int] = {}
    for line in path.read_text().splitlines():
        match = STATUS_RE.match(line)
        if not match:
            continue
        arm = match.group("arm")
        if arm in statuses:
            raise ValidationError(f"duplicate status for arm {arm!r}")
        statuses[arm] = int(match.group("rc"))
    actual_names = set(statuses)
    if actual_names != expected_arm_names:
        raise ValidationError(
            "status arm set does not match expected matrix: "
            f"missing={sorted(expected_arm_names - actual_names)}, "
            f"extra={sorted(actual_names - expected_arm_names)}"
        )
    failed = {arm: rc for arm, rc in statuses.items() if rc != 0}
    if failed:
        raise ValidationError(f"non-zero arm statuses: {failed}")
    return set(statuses)


def validate_csv_artifact(
    path: pathlib.Path, required_columns: set[str]
) -> tuple[list[dict[str, str]], list[str]]:
    if not path.is_file():
        return [], [f"{path.name} is missing"]
    try:
        with path.open(newline="") as stream:
            reader = csv.DictReader(stream)
            columns = set(reader.fieldnames or [])
            missing = sorted(required_columns - columns)
            if missing:
                return [], [f"{path.name} columns missing: {missing}"]
            rows = list(reader)
    except (OSError, csv.Error) as exc:
        return [], [f"cannot read {path.name}: {exc}"]
    if not rows:
        return [], [f"{path.name} has no data rows"]
    return rows, []


def validate_report(
    path: pathlib.Path,
    provenance: dict[str, str],
    expected_config: dict,
    expected_matrix_schema_version: int,
) -> list[str]:
    errors: list[str] = []
    try:
        report = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return [f"cannot read report JSON: {exc}"]

    if report.get("completed") is not True:
        errors.append(f"completed is not exactly true: {report.get('completed')!r}")

    config = report.get("config")
    if not isinstance(config, dict):
        errors.append(f"report config is missing or invalid: {config!r}")
        config = {}
    for field, wanted in expected_config.items():
        if (
            field == "SkipHistoryServer"
            and expected_matrix_schema_version == 1
            and field not in config
        ):
            observed_config = False
        else:
            observed_config = config.get(field)
        if observed_config != wanted or type(observed_config) is not type(wanted):
            errors.append(
                f"config.{field} does not match expected matrix: "
                f"observed={observed_config!r}, expected={wanted!r}"
            )
    report_ray_image = config.get("RayImage")
    if report_ray_image != provenance["ray_image_requested"]:
        errors.append(
            "report RayImage does not match captured runtime request: "
            f"report={report_ray_image!r}, "
            f"provenance={provenance['ray_image_requested']!r}"
        )

    driver_path = path.parent / "driver.py"
    try:
        driver = driver_path.read_text()
    except OSError as exc:
        errors.append(f"cannot read rendered RayJob driver: {exc}")
        driver = ""
    if driver:
        sleep_lines = [
            line for line in driver.splitlines() if "time.sleep(" in line
        ]
        target_task_rate = expected_config["TargetTaskRate"]
        if target_task_rate == 0:
            if sleep_lines:
                errors.append("formal rendered RayJob driver contains time.sleep")
        elif (
            driver.count(TARGET_RATE_PACING_BLOCK) != 1
            or sleep_lines != [TARGET_RATE_PACING_SLEEP_LINE]
        ):
            errors.append(
                "formal paced RayJob driver must contain only the exact "
                "target-rate pacing sleep"
            )
        if "__" in driver:
            errors.append(
                "formal rendered RayJob driver contains a template placeholder"
            )
        driver_fragments = (
            f"T = {expected_config['TaskCount']}",
            f"WAVE = {expected_config['WaveSize']}",
            f"TARGET = {expected_config['TargetTaskRate']}",
            f"DRIVERS = {expected_config['Drivers']}",
            "@ray.remote(" f"num_cpus={expected_config['TaskNumCPUs']}, max_retries=0)",
        )
        missing_driver_fragments = [
            fragment for fragment in driver_fragments if fragment not in driver
        ]
        if missing_driver_fragments:
            errors.append(
                "formal rendered RayJob driver does not match expected matrix: "
                f"missing={missing_driver_fragments!r}"
            )

    rayjob_lifecycle = report.get("rayJobLifecycle")
    expected_rayjob_lifecycle = {
        "ownedCluster": True,
        "shutdownAfterJobFinishes": True,
        "ttlSecondsAfterFinished": 30,
    }
    if not isinstance(rayjob_lifecycle, dict):
        errors.append(
            f"report rayJobLifecycle is missing or invalid: {rayjob_lifecycle!r}"
        )
    else:
        retry_fields = {"rayJobBackoffLimit", "submitterBackoffLimit"}
        if retry_fields & set(rayjob_lifecycle):
            expected_rayjob_lifecycle.update(
                {
                    "rayJobBackoffLimit": 0,
                    "submitterBackoffLimit": 0,
                }
            )
        if set(rayjob_lifecycle) != set(expected_rayjob_lifecycle):
            errors.append(
                "report rayJobLifecycle fields differ from one complete formal "
                f"schema: observed={sorted(rayjob_lifecycle)!r}, "
                f"expected={sorted(expected_rayjob_lifecycle)!r}"
            )
        for field, wanted in expected_rayjob_lifecycle.items():
            observed_lifecycle = rayjob_lifecycle.get(field)
            if observed_lifecycle != wanted or type(observed_lifecycle) is not type(
                wanted
            ):
                errors.append(
                    f"rayJobLifecycle.{field} does not match the created formal "
                    f"RayJob: observed={observed_lifecycle!r}, expected={wanted!r}"
                )

    storage = report.get("storage") or {}
    if storage.get("markerPresent") is not True:
        errors.append("session marker is missing")
    events = storage.get("events") or {}
    expected = events.get("expectedTasks")
    observed = events.get("benchTaskIDs")
    if not isinstance(expected, int) or expected <= 0:
        errors.append(f"expectedTasks is invalid: {expected!r}")
    elif expected != expected_config["TaskCount"]:
        errors.append(
            "storage.events.expectedTasks does not match expected matrix: "
            f"observed={expected!r}, expected={expected_config['TaskCount']!r}"
        )
    if observed != expected:
        errors.append(
            f"benchTaskIDs mismatch: observed={observed!r}, expected={expected!r}"
        )

    validity = events.get("benchTaskValidity")
    if not isinstance(validity, dict):
        errors.append("storage.events.benchTaskValidity is missing")
    else:
        expected_counts = {
            "expectedTaskIDs": expected,
            "observedTaskIDs": expected,
            "observedAttempts": expected,
            "attemptZero": expected,
            "finishedAttempts": expected,
            "submittedToWorkerAttempts": expected,
            "finishedTransitionAttempts": expected,
        }
        for field, wanted in expected_counts.items():
            if validity.get(field) != wanted:
                errors.append(
                    f"benchTaskValidity.{field}={validity.get(field)!r}, "
                    f"expected {wanted!r}"
                )
        for field in (
            "malformedDefinitions",
            "missingDefinitionAttemptFields",
            "missingLifecycleAttemptFields",
            "invalidLifecycleTransitions",
            "outOfRangeLifecycleTransitions",
            "ambiguousLifecycleTransitions",
            "missingLifecycleAttempts",
            "nonFinishedAttempts",
        ):
            if validity.get(field) != 0:
                errors.append(
                    f"benchTaskValidity.{field}={validity.get(field)!r}, expected 0"
                )
        if validity.get("valid") is not True:
            errors.append("benchTaskValidity.valid is not exactly true")
        if validity.get("problems") != []:
            errors.append(
                "benchTaskValidity.problems must be present and empty: "
                f"{validity.get('problems')!r}"
            )

    task_windows = events.get("taskLifecycleWindows")
    task_json_by_start: dict[int, tuple[int, int, int, int]] = {}
    json_submitted = 0
    json_finished = 0
    if not isinstance(task_windows, list) or not task_windows:
        errors.append("storage.events.taskLifecycleWindows is missing or empty")
        task_windows = []
    for index, window in enumerate(task_windows):
        if not isinstance(window, dict):
            errors.append(f"taskLifecycleWindows[{index}] is not an object: {window!r}")
            continue
        values = {
            field: window.get(field)
            for field in (
                "windowStartUnixNano",
                "windowEndUnixNano",
                "submittedToWorkerAttempts",
                "finishedAttempts",
                "backlogDelta",
            )
        }
        invalid_fields = [
            field for field, value in values.items() if type(value) is not int
        ]
        if invalid_fields:
            errors.append(
                f"taskLifecycleWindows[{index}] has non-integer fields: {invalid_fields}"
            )
            continue
        if (
            values["windowEndUnixNano"] - values["windowStartUnixNano"]
            != TEN_SECONDS_NANO
        ):
            errors.append(f"taskLifecycleWindows[{index}] is not a 10-second window")
        if values["submittedToWorkerAttempts"] < 0 or values["finishedAttempts"] < 0:
            errors.append(f"taskLifecycleWindows[{index}] has a negative attempt count")
        if values["backlogDelta"] != (
            values["submittedToWorkerAttempts"] - values["finishedAttempts"]
        ):
            errors.append(f"taskLifecycleWindows[{index}].backlogDelta is inconsistent")
        start = values["windowStartUnixNano"]
        task_window = (
            values["windowEndUnixNano"],
            values["submittedToWorkerAttempts"],
            values["finishedAttempts"],
            values["backlogDelta"],
        )
        if start in task_json_by_start:
            errors.append(f"taskLifecycleWindows contains duplicate start {start}")
        else:
            task_json_by_start[start] = task_window
        json_submitted += values["submittedToWorkerAttempts"]
        json_finished += values["finishedAttempts"]
    if json_submitted != expected or json_finished != expected:
        errors.append(
            "taskLifecycleWindows totals do not match expected tasks: "
            f"submitted={json_submitted}, finished={json_finished}, expected={expected!r}"
        )
    validate_task_window_sequence("taskLifecycleWindows", task_json_by_start, errors)

    lifecycle_rows, csv_errors = validate_csv_artifact(
        path.parent / "task_lifecycle_10s.csv",
        {
            "window_start_unix_nano",
            "window_end_unix_nano",
            "submitted_to_worker_attempts",
            "finished_attempts",
            "backlog_delta",
        },
    )
    errors.extend(csv_errors)
    csv_submitted = 0
    csv_finished = 0
    csv_counts_valid = bool(lifecycle_rows)
    task_csv_by_start: dict[int, tuple[int, int, int, int]] = {}
    for index, row in enumerate(lifecycle_rows):
        try:
            submitted = int(row["submitted_to_worker_attempts"])
            finished = int(row["finished_attempts"])
            backlog = int(row["backlog_delta"])
            start = int(row["window_start_unix_nano"])
            end = int(row["window_end_unix_nano"])
        except (KeyError, TypeError, ValueError) as exc:
            errors.append(f"task_lifecycle_10s.csv row {index + 2} is invalid: {exc}")
            csv_counts_valid = False
            continue
        if submitted < 0 or finished < 0 or end - start != TEN_SECONDS_NANO:
            errors.append(
                f"task_lifecycle_10s.csv row {index + 2} has invalid counts or window"
            )
            csv_counts_valid = False
        if backlog != submitted - finished:
            errors.append(
                f"task_lifecycle_10s.csv row {index + 2} has inconsistent backlog_delta"
            )
            csv_counts_valid = False
        task_window = (end, submitted, finished, backlog)
        if start in task_csv_by_start:
            errors.append(f"task_lifecycle_10s.csv contains duplicate start {start}")
            csv_counts_valid = False
        else:
            task_csv_by_start[start] = task_window
        csv_submitted += submitted
        csv_finished += finished
    if csv_counts_valid and (
        csv_submitted != json_submitted or csv_finished != json_finished
    ):
        errors.append(
            "task_lifecycle_10s.csv totals do not match report windows: "
            f"csv submitted={csv_submitted}, csv finished={csv_finished}, "
            f"report submitted={json_submitted}, report finished={json_finished}"
        )
    if csv_counts_valid and task_csv_by_start != task_json_by_start:
        errors.append(
            "task_lifecycle_10s.csv windows do not match report taskLifecycleWindows"
        )
    validate_task_window_sequence("task_lifecycle_10s.csv", task_csv_by_start, errors)

    skip_history_server = expected_config["SkipHistoryServer"]
    hs = report.get("historyServer") or {}
    if skip_history_server:
        if hs.get("enterMeasured") not in (None, False):
            errors.append(
                "Collector-only report unexpectedly measured a History Server "
                f"cold load: {hs.get('enterMeasured')!r}"
            )
        if hs.get("enterStatus") not in (None, 0):
            errors.append(
                "Collector-only report unexpectedly has a History Server status: "
                f"{hs.get('enterStatus')!r}"
            )
        if hs.get("warmEndpoints") not in (None, []):
            errors.append(
                "Collector-only report unexpectedly has History Server warm endpoints"
            )
    else:
        if hs.get("enterMeasured") is not True or hs.get("enterStatus") != 200:
            errors.append(
                "History Server cold load is not a measured HTTP 200: "
                f"measured={hs.get('enterMeasured')!r}, "
                f"status={hs.get('enterStatus')!r}"
            )
        warm_iterations = config.get("WarmIterations")
        if type(warm_iterations) is not int or warm_iterations <= 0:
            errors.append(
                "config.WarmIterations must be a positive integer: "
                f"{warm_iterations!r}"
            )
        task_count = expected_config["TaskCount"]
        expected_warm_endpoints = (
            f"/api/v0/tasks?limit={min(task_count, 10000)}",
            *WARM_ENDPOINTS_WITHOUT_TASKS,
        )
        warm_endpoints = hs.get("warmEndpoints") or []
        if not isinstance(warm_endpoints, list):
            errors.append(f"warmEndpoints is not a list: {warm_endpoints!r}")
            warm_endpoints = []
        observed_warm_endpoints = [
            endpoint.get("endpoint") if isinstance(endpoint, dict) else None
            for endpoint in warm_endpoints
        ]
        if tuple(observed_warm_endpoints) != expected_warm_endpoints:
            errors.append(
                "warmEndpoints do not match the exact expected query set: "
                f"observed={observed_warm_endpoints!r}, "
                f"expected={list(expected_warm_endpoints)!r}"
            )
        for endpoint in warm_endpoints:
            if not isinstance(endpoint, dict):
                errors.append(f"warm endpoint entry is not an object: {endpoint!r}")
                continue
            if endpoint.get("errors") != 0:
                errors.append(
                    f"warm endpoint {endpoint.get('endpoint')!r} has "
                    f"{endpoint.get('errors')!r} errors"
                )

    per_node = events.get("perNode")
    storage_by_node: dict[str, dict] = {}
    if not isinstance(per_node, list) or len(per_node) != 2:
        errors.append(
            "storage.events.perNode must contain exactly two node entries: "
            f"{per_node!r}"
        )
        per_node = []
    for index, node in enumerate(per_node):
        if not isinstance(node, dict):
            errors.append(f"storage.events.perNode[{index}] is not an object")
            continue
        node_id = node.get("nodeID")
        node_events = node.get("events")
        raw_bytes = node.get("rawBytes")
        if not isinstance(node_id, str) or not node_id:
            errors.append(
                f"storage.events.perNode[{index}] has invalid nodeID={node_id!r}"
            )
            continue
        if node_id in storage_by_node:
            errors.append(f"storage.events.perNode repeats NodeID {node_id!r}")
            continue
        if (
            type(node_events) is not int
            or node_events < 0
            or type(raw_bytes) is not int
            or raw_bytes < 0
        ):
            errors.append(
                f"storage.events.perNode[{index}] has invalid "
                f"events/rawBytes={node_events!r}/{raw_bytes!r}"
            )
            continue
        storage_by_node[node_id] = node

    if (
        expected_matrix_schema_version >= 2
        and expected_config["SkipHistoryServer"] is True
    ):
        validate_event_id_integrity(events, per_node, errors)

    collectors = report.get("collectorLogs") or []
    if len(collectors) != 2:
        errors.append(
            f"expected exactly 2 collectorLogs entries, found {len(collectors)}"
        )
    collector_pods: set[str] = set()
    collector_roles: dict[str, str] = {}
    collectors_by_pod: dict[str, dict] = {}
    collector_node_ids: dict[str, str] = {}
    require_runtime_image_binding = (
        expected_matrix_schema_version >= 2 and skip_history_server is True
    )
    require_collector_cap_evidence = "CollectorCPURequest" in expected_config
    expected_collector_image = provenance["collector_image_requested"]
    expected_collector_image_id = provenance["collector_runtime_id"]
    for collector in collectors:
        if not isinstance(collector, dict):
            errors.append(f"collectorLogs entry is not an object: {collector!r}")
            continue
        pod = collector.get("pod")
        role = collector.get("role")
        if not isinstance(pod, str) or not pod:
            errors.append(f"collector has invalid pod name: {pod!r}")
        elif pod in collector_pods:
            errors.append(f"collectorLogs repeats pod {pod!r}")
        else:
            collector_pods.add(pod)
            collector_roles[pod] = role
            collectors_by_pod[pod] = collector
        if require_runtime_image_binding:
            collector_image = collector.get("image")
            if normalize_local_image_reference(collector_image) != normalize_local_image_reference(
                expected_collector_image
            ):
                errors.append(
                    f"collector {pod!r} image does not match captured runtime request: "
                    f"observed={collector_image!r}, expected={expected_collector_image!r}"
                )
            collector_image_id = collector.get("imageID")
            normalized_image_id = normalize_container_image_id(collector_image_id)
            if normalized_image_id is None:
                errors.append(
                    f"collector {pod!r} has invalid imageID={collector_image_id!r}"
                )
            elif normalized_image_id != expected_collector_image_id:
                errors.append(
                    f"collector {pod!r} imageID does not match provenance "
                    f"collector_runtime_id: observed={normalized_image_id!r}, "
                    f"expected={expected_collector_image_id!r}"
                )
        if collector.get("uploadFailures") != 0:
            errors.append(
                f"collector {collector.get('pod')!r} uploadFailures="
                f"{collector.get('uploadFailures')!r}"
            )
        if collector.get("rotationQueueFull") != 0:
            errors.append(
                f"collector {collector.get('pod')!r} rotationQueueFull="
                f"{collector.get('rotationQueueFull')!r}"
            )
        if collector.get("logStreamComplete") is not True:
            errors.append(
                f"collector {collector.get('pod')!r} log stream is incomplete: "
                f"complete={collector.get('logStreamComplete')!r}, "
                f"error={collector.get('logStreamError')!r}"
            )
        if collector.get("logStreamTimedOut") is not False:
            errors.append(
                f"collector {collector.get('pod')!r} log stream timed out: "
                f"{collector.get('logStreamTimedOut')!r}"
            )
        if collector.get("logStreamError", "") != "":
            errors.append(
                f"collector {collector.get('pod')!r} log stream failed: "
                f"{collector.get('logStreamError')!r}"
            )
        if collector.get("gracefulShutdownComplete") is not True:
            errors.append(
                f"collector {collector.get('pod')!r} has no explicit graceful "
                "shutdown-complete marker"
            )
        container_id = collector.get("containerID")
        if not isinstance(container_id, str) or not container_id:
            errors.append(
                f"collector {collector.get('pod')!r} has invalid "
                f"containerID={container_id!r}"
            )
        restart_count = collector.get("restartCount")
        if type(restart_count) is not int or restart_count != 0:
            errors.append(
                f"collector {collector.get('pod')!r} restartCount="
                f"{restart_count!r}, expected integer 0"
            )
        if require_collector_cap_evidence:
            expected_resources = {
                "cpuRequest": expected_config["CollectorCPURequest"],
                "cpuLimit": expected_config["CollectorCPU"],
                "memoryRequest": expected_config["CollectorMemoryRequest"],
                "memoryLimit": expected_config["CollectorMemoryLimit"],
            }
            mismatched_resources = {
                field: (collector.get(field), wanted)
                for field, wanted in expected_resources.items()
                if collector.get(field) != wanted
            }
            if mismatched_resources:
                errors.append(
                    f"collector {pod!r} Pod resources do not match cap matrix: "
                    f"{mismatched_resources!r}"
                )
            if collector.get("cgroupMemoryObserved") is not True:
                errors.append(
                    f"collector {pod!r} has no cgroup memory.max/memory.events evidence"
                )
            if collector.get("cgroupMemoryMaxBytes") != 192 * 1024 * 1024:
                errors.append(
                    f"collector {pod!r} cgroup memory.max is not 192Mi: "
                    f"{collector.get('cgroupMemoryMaxBytes')!r}"
                )
            if collector.get("memoryEventsOOM") != 0 or collector.get(
                "memoryEventsOOMKill"
            ) != 0:
                errors.append(
                    f"collector {pod!r} has memory pressure events: "
                    f"oom={collector.get('memoryEventsOOM')!r}, "
                    f"oom_kill={collector.get('memoryEventsOOMKill')!r}"
                )
            if collector.get("cgroupMemoryReadErrors") != 0:
                errors.append(
                    f"collector {pod!r} cgroup memory evidence has read errors: "
                    f"{collector.get('cgroupMemoryReadErrors')!r}"
                )

        ingress_windows = collector.get("ingressWindows")
        if not isinstance(ingress_windows, list) or not ingress_windows:
            errors.append(
                f"collector {collector.get('pod')!r} ingressWindows is missing or empty"
            )
            continue
        ingress_node_ids: set[str] = set()
        ingress_events = 0
        ingress_valid = True
        for index, window in enumerate(ingress_windows):
            if not isinstance(window, dict):
                errors.append(
                    f"collector {collector.get('pod')!r} ingressWindows[{index}] "
                    "is not an object"
                )
                ingress_valid = False
                continue
            node_id = window.get("nodeID")
            window_events = window.get("events")
            window_bytes = window.get("bytes")
            if not isinstance(node_id, str) or not node_id:
                errors.append(
                    f"collector {collector.get('pod')!r} ingressWindows[{index}] "
                    f"has invalid NodeID={node_id!r}"
                )
                ingress_valid = False
            else:
                ingress_node_ids.add(node_id)
            if (
                type(window_events) is not int
                or window_events < 0
                or type(window_bytes) is not int
                or window_bytes < 0
            ):
                errors.append(
                    f"collector {collector.get('pod')!r} ingressWindows[{index}] "
                    f"has invalid events/bytes={window_events!r}/{window_bytes!r}"
                )
                ingress_valid = False
                continue
            ingress_events += window_events
        if len(ingress_node_ids) != 1:
            errors.append(
                f"collector {collector.get('pod')!r} ingressWindows must contain "
                f"exactly one NodeID, found {sorted(ingress_node_ids)}"
            )
            ingress_valid = False
        if not ingress_valid:
            continue

        node_id = next(iter(ingress_node_ids))
        if isinstance(pod, str) and pod:
            collector_node_ids[pod] = node_id
        stored = storage_by_node.get(node_id)
        if stored is None:
            errors.append(
                f"collector {collector.get('pod')!r} NodeID {node_id!r} is "
                "missing from storage.events.perNode"
            )
            continue
        if ingress_events != stored["events"]:
            errors.append(
                f"collector {collector.get('pod')!r} ingress event total does not "
                f"match storage for NodeID {node_id!r}: "
                f"ingress={ingress_events}, storage={stored['events']}"
            )
        uploaded_bytes = collector.get("uploadedBytes")
        if type(uploaded_bytes) is not int or uploaded_bytes != stored["rawBytes"]:
            errors.append(
                f"collector {collector.get('pod')!r} uploadedBytes does not match "
                f"storage rawBytes for NodeID {node_id!r}: "
                f"uploaded={uploaded_bytes!r}, storage={stored['rawBytes']}"
            )
    if len(collector_pods) != 2:
        errors.append(
            f"collectorLogs must name two distinct pods, found {sorted(collector_pods)}"
        )
    if set(collector_roles.values()) != {"head", "worker"}:
        errors.append(
            "collectorLogs roles must be head+worker, found "
            f"{sorted(set(collector_roles.values()))}"
        )
    if len(set(collector_node_ids.values())) != len(collector_node_ids):
        errors.append(
            "collectorLogs must map each pod to a different NodeID: "
            f"{collector_node_ids!r}"
        )
    if storage_by_node and set(collector_node_ids.values()) != set(storage_by_node):
        errors.append(
            "collector ingress NodeIDs do not exactly match storage.events.perNode: "
            f"collectors={sorted(set(collector_node_ids.values()))}, "
            f"storage={sorted(storage_by_node)}"
        )

    collector_terminations: dict[str, dict] = {}
    pod_terminations = report.get("podTerminations") or []
    if not isinstance(pod_terminations, list):
        errors.append(f"podTerminations is not a list: {pod_terminations!r}")
        pod_terminations = []
    for termination in pod_terminations:
        if not isinstance(termination, dict):
            errors.append(f"podTerminations entry is not an object: {termination!r}")
            continue
        pod = termination.get("pod")
        if termination.get("container") != "collector" or pod not in collector_pods:
            continue
        if pod in collector_terminations:
            errors.append(f"podTerminations repeats collector pod {pod!r}")
            continue
        collector_terminations[pod] = termination
    for pod, termination in collector_terminations.items():
        collector = collectors_by_pod[pod]
        observed = termination.get("observed")
        termination_restart_count = termination.get("restartCount")
        identity_matches = (
            termination.get("containerID") == collector.get("containerID")
            and type(termination_restart_count) is int
            and termination_restart_count == collector.get("restartCount")
        )
        if observed is False:
            invalid_termination = (
                not identity_matches
                or termination.get("source") not in (None, "")
            )
        else:
            invalid_termination = (
                observed is not True
                or not identity_matches
                or termination.get("source") != "current"
                or type(termination.get("exitCode")) is not int
                or termination.get("exitCode") != 0
                or termination.get("reason") != "Completed"
            )
        if invalid_termination:
            errors.append(
                f"collector {pod!r} termination evidence is non-current, "
                f"mismatched, or non-zero: {termination!r}"
            )

    windows = report.get("collectorWindows") or []
    if not windows:
        errors.append("collectorWindows is empty")
    valid_window_pods = {
        row.get("pod")
        for row in windows
        if isinstance(row, dict) and row.get("validForSizing") is True
    }
    collector_json_by_key: dict[tuple[int, str, str], tuple[int, int, int, int]] = {}
    collector_tasks_by_start: dict[int, tuple[int, int, int]] = {}
    for index, window in enumerate(windows):
        if not isinstance(window, dict):
            errors.append(f"collectorWindows[{index}] is not an object: {window!r}")
            continue
        pod = window.get("pod")
        node_id = window.get("nodeID")
        if not isinstance(pod, str) or not pod or not isinstance(node_id, str):
            errors.append(
                f"collectorWindows[{index}] has invalid pod/nodeID: {pod!r}/{node_id!r}"
            )
            continue
        numeric = {
            field: window.get(field)
            for field in (
                "windowStartUnixNano",
                "windowEndUnixNano",
                "submittedToWorkerAttempts",
                "finishedAttempts",
                "backlogDelta",
            )
        }
        invalid_fields = [
            field for field, value in numeric.items() if type(value) is not int
        ]
        if invalid_fields:
            errors.append(
                f"collectorWindows[{index}] has non-integer fields: {invalid_fields}"
            )
            continue
        start = numeric["windowStartUnixNano"]
        end = numeric["windowEndUnixNano"]
        if start % TEN_SECONDS_NANO != 0:
            errors.append(
                f"collectorWindows[{index}] start is not epoch-aligned to 10s"
            )
        if end - start != TEN_SECONDS_NANO:
            errors.append(f"collectorWindows[{index}] is not a 10-second window")
        task_values = (
            numeric["submittedToWorkerAttempts"],
            numeric["finishedAttempts"],
            numeric["backlogDelta"],
        )
        expected_task_values = task_csv_by_start.get(start, (0, 0, 0, 0))[1:]
        if task_values != expected_task_values:
            errors.append(
                f"collectorWindows[{index}] task counts do not match "
                f"task_lifecycle_10s.csv: observed={task_values!r}, "
                f"expected={expected_task_values!r}"
            )
        previous_task_values = collector_tasks_by_start.setdefault(start, task_values)
        if previous_task_values != task_values:
            errors.append(
                f"collectorWindows repeats inconsistent task counts at start {start}"
            )
        key = (start, pod, node_id)
        if key in collector_json_by_key:
            errors.append(f"collectorWindows contains duplicate row {key!r}")
        else:
            collector_json_by_key[key] = (end, *task_values)

    if task_csv_by_start and collector_json_by_key:
        matched_starts = set(task_csv_by_start).intersection(collector_tasks_by_start)
        matched_submitted = sum(task_csv_by_start[start][1] for start in matched_starts)
        matched_finished = sum(task_csv_by_start[start][2] for start in matched_starts)
        if matched_submitted != expected or matched_finished != expected:
            errors.append(
                "Collector windows do not contain every task lifecycle bucket: "
                f"matched submitted={matched_submitted}, "
                f"finished={matched_finished}, expected={expected}"
            )
        task_range_start = min(task_csv_by_start)
        task_range_end = max(window[0] for window in task_csv_by_start.values())
        collector_range_start = min(key[0] for key in collector_json_by_key)
        collector_range_end = max(
            window[0] for window in collector_json_by_key.values()
        )
        range_gap = max(
            task_range_start - collector_range_end,
            collector_range_start - task_range_end,
            0,
        )
        if range_gap > TEN_SECONDS_NANO:
            errors.append(
                "task lifecycle and Collector measurement ranges are more than "
                f"one 10-second bucket apart: gap_ns={range_gap}"
            )

    gates = report.get("collectorIngressGates") or []
    if len(gates) != 2:
        errors.append(
            f"expected exactly 2 collectorIngressGates entries, found {len(gates)}"
        )
    gate_roles: set[str] = set()
    gate_node_ids: list[str] = []
    gate_pods: set[str] = set()
    for gate in gates:
        pod = gate.get("pod")
        role = gate.get("role")
        if isinstance(pod, str) and pod:
            gate_pods.add(pod)
        if isinstance(role, str):
            gate_roles.add(role)
        node_ids = gate.get("nodeIDs")
        if (
            not isinstance(node_ids, list)
            or len(node_ids) != 1
            or not isinstance(node_ids[0], str)
            or not node_ids[0]
        ):
            errors.append(
                f"collector gate {pod!r} does not have exactly one NodeID: {node_ids!r}"
            )
        else:
            gate_node_ids.append(node_ids[0])
        if gate.get("valid") is not True:
            errors.append(
                f"collector gate {pod!r} is invalid: {gate.get('problems')!r}"
            )
        if gate.get("logStreamComplete") is not True:
            errors.append(f"collector gate {pod!r} has an incomplete log stream")
        if gate.get("gracefulShutdownComplete") is not True:
            errors.append(
                f"collector gate {pod!r} has no explicit graceful "
                "shutdown-complete marker"
            )
        if gate.get("problems") != []:
            errors.append(
                "collector gate problems must be present and empty for "
                f"{pod!r}: {gate.get('problems')!r}"
            )
        if gate.get("rejectedRequests") != 0:
            errors.append(
                f"collector gate {pod!r} rejectedRequests="
                f"{gate.get('rejectedRequests')!r}"
            )
        if gate.get("rotationQueueFull") != 0:
            errors.append(
                f"collector gate {pod!r} rotationQueueFull="
                f"{gate.get('rotationQueueFull')!r}"
            )
        if require_collector_cap_evidence:
            peak_events_per_second = gate.get("peakEventsPerSecond")
            if (
                type(peak_events_per_second) not in (int, float)
                or not math.isfinite(float(peak_events_per_second))
                or peak_events_per_second
                < COLLECTOR_CAP_MIN_PEAK_EVENTS_PER_SECOND
            ):
                errors.append(
                    f"collector cap gate {pod!r} peakEventsPerSecond="
                    f"{peak_events_per_second!r}, want at least "
                    f"{COLLECTOR_CAP_MIN_PEAK_EVENTS_PER_SECOND:.1f}"
                )
        if pod not in valid_window_pods:
            errors.append(f"collector {pod!r} has no valid-for-sizing ingress window")
        if pod in collector_roles and role != collector_roles[pod]:
            errors.append(
                f"collector {pod!r} role mismatch: "
                f"log={collector_roles[pod]!r}, gate={role!r}"
            )
        if (
            pod in collector_node_ids
            and isinstance(node_ids, list)
            and len(node_ids) == 1
            and node_ids[0] != collector_node_ids[pod]
        ):
            errors.append(
                f"collector {pod!r} NodeID mismatch: "
                f"log={collector_node_ids[pod]!r}, gate={node_ids[0]!r}"
            )
    if gate_pods != collector_pods:
        errors.append(
            "collector gate pods do not match collector logs: "
            f"gates={sorted(gate_pods)}, logs={sorted(collector_pods)}"
        )
    if gate_roles != {"head", "worker"}:
        errors.append(
            f"collector gate roles must be head+worker, found {sorted(gate_roles)}"
        )
    if len(gate_node_ids) == 2 and len(set(gate_node_ids)) != 2:
        errors.append(f"head and worker collectors share one NodeID: {gate_node_ids!r}")

    window_rows, csv_errors = validate_csv_artifact(
        path.parent / "collector_ingress_cgroup_10s.csv",
        {
            "window_start_unix_nano",
            "window_end_unix_nano",
            "submitted_to_worker_attempts",
            "finished_attempts",
            "backlog_delta",
            "pod",
            "ray_node_id",
            "events",
            "cpu_coverage_ratio",
            "valid_for_sizing",
        },
    )
    errors.extend(csv_errors)
    if window_rows:
        csv_valid_pods = {
            row.get("pod")
            for row in window_rows
            if row.get("valid_for_sizing") == "true"
        }
        if csv_valid_pods != collector_pods:
            errors.append(
                "collector_ingress_cgroup_10s.csv valid pods do not match "
                f"collectors: csv={sorted(csv_valid_pods)}, "
                f"collectors={sorted(collector_pods)}"
            )
        collector_csv_by_key: dict[tuple[int, str, str], tuple[int, int, int, int]] = {}
        csv_tasks_by_start: dict[int, tuple[int, int, int]] = {}
        for index, row in enumerate(window_rows):
            pod = row.get("pod")
            node_id = row.get("ray_node_id")
            try:
                start = int(row["window_start_unix_nano"])
                end = int(row["window_end_unix_nano"])
                task_values = (
                    int(row["submitted_to_worker_attempts"]),
                    int(row["finished_attempts"]),
                    int(row["backlog_delta"]),
                )
            except (KeyError, TypeError, ValueError) as exc:
                errors.append(
                    f"collector_ingress_cgroup_10s.csv row {index + 2} is invalid: {exc}"
                )
                continue
            if not isinstance(pod, str) or not isinstance(node_id, str):
                errors.append(
                    f"collector_ingress_cgroup_10s.csv row {index + 2} "
                    "has an invalid pod or NodeID"
                )
                continue
            if end - start != TEN_SECONDS_NANO:
                errors.append(
                    f"collector_ingress_cgroup_10s.csv row {index + 2} "
                    "is not a 10-second window"
                )
            if start % TEN_SECONDS_NANO != 0:
                errors.append(
                    f"collector_ingress_cgroup_10s.csv row {index + 2} start "
                    "is not epoch-aligned to 10s"
                )
            expected_task_values = task_csv_by_start.get(start, (0, 0, 0, 0))[1:]
            if task_values != expected_task_values:
                errors.append(
                    f"collector_ingress_cgroup_10s.csv row {index + 2} task "
                    f"counts do not match task_lifecycle_10s.csv: "
                    f"observed={task_values!r}, expected={expected_task_values!r}"
                )
            previous_task_values = csv_tasks_by_start.setdefault(start, task_values)
            if previous_task_values != task_values:
                errors.append(
                    "collector_ingress_cgroup_10s.csv repeats inconsistent task "
                    f"counts at start {start}"
                )
            key = (start, pod, node_id)
            if key in collector_csv_by_key:
                errors.append(
                    "collector_ingress_cgroup_10s.csv contains duplicate row "
                    f"{key!r}"
                )
            else:
                collector_csv_by_key[key] = (end, *task_values)
        if collector_csv_by_key != collector_json_by_key:
            errors.append(
                "collector_ingress_cgroup_10s.csv task/window rows do not match "
                "report collectorWindows"
            )

    gate_rows, csv_errors = validate_csv_artifact(
        path.parent / "collector_ingress_gate.csv",
        {
            "pod",
            "role",
            "node_ids",
            "log_stream_complete",
            "graceful_shutdown_complete",
            "valid",
            "problems",
        },
    )
    errors.extend(csv_errors)
    if gate_rows:
        csv_gate_pods = {row.get("pod") for row in gate_rows}
        if csv_gate_pods != collector_pods:
            errors.append(
                "collector_ingress_gate.csv pods do not match collectors: "
                f"csv={sorted(csv_gate_pods)}, collectors={sorted(collector_pods)}"
            )
        if len(gate_rows) != 2:
            errors.append(
                "collector_ingress_gate.csv must contain exactly two rows, "
                f"found {len(gate_rows)}"
            )
        if any(row.get("valid") != "true" for row in gate_rows):
            errors.append("collector_ingress_gate.csv contains an invalid row")
        if any(row.get("log_stream_complete") != "true" for row in gate_rows):
            errors.append(
                "collector_ingress_gate.csv contains an incomplete log stream"
            )
        if any(
            row.get("graceful_shutdown_complete") != "true" for row in gate_rows
        ):
            errors.append(
                "collector_ingress_gate.csv contains a missing graceful "
                "shutdown-complete marker"
            )
        csv_roles = {row.get("role") for row in gate_rows}
        if csv_roles != {"head", "worker"}:
            errors.append(
                "collector_ingress_gate.csv roles must be head+worker, "
                f"found {sorted(csv_roles)}"
            )

    hs_log = path.parent / "historyserver.log"
    if skip_history_server:
        if hs_log.exists():
            errors.append(
                "historyserver.log must be absent when SkipHistoryServer=true"
            )
    elif not hs_log.is_file():
        errors.append("historyserver.log is missing")
    else:
        log_text = hs_log.read_text(errors="replace")
        for pattern in DECODE_ERROR_PATTERNS:
            if pattern in log_text:
                errors.append(f"historyserver.log contains {pattern!r}")
    return errors


def validate_sweep(
    root: pathlib.Path, expected_arms: int, *, allow_legacy_schema: bool = False
) -> None:
    provenance = read_provenance(root)
    read_final_provenance(root, provenance)
    matrix = read_expected_matrix(
        root, expected_arms, allow_legacy_schema=allow_legacy_schema
    )
    arms = read_status(root, set(matrix))

    reports_by_arm: dict[str, list[pathlib.Path]] = {arm: [] for arm in arms}
    for report in root.glob("*/*/bench-report.json"):
        arm = report.relative_to(root).parts[0]
        if arm in reports_by_arm:
            reports_by_arm[arm].append(report)

    failures: list[str] = []
    for arm in sorted(arms):
        reports = reports_by_arm[arm]
        if len(reports) != 1:
            failures.append(f"{arm}: expected exactly one report, found {len(reports)}")
            continue
        for error in validate_report(
            reports[0],
            provenance,
            matrix[arm]["config"],
            matrix[arm]["_schemaVersion"],
        ):
            failures.append(f"{arm}: {error}")
    if failures:
        raise ValidationError("\n".join(failures))


def validate_single_arm(
    root: pathlib.Path, arm: str, *, allow_legacy_schema: bool = False
) -> None:
    provenance = read_provenance(root)
    matrix = read_expected_matrix(root, allow_legacy_schema=allow_legacy_schema)
    if arm not in matrix:
        raise ValidationError(f"single arm {arm!r} is not in the expected matrix")

    reports = list((root / arm).glob("*/bench-report.json"))
    if len(reports) != 1:
        raise ValidationError(
            f"{arm}: expected exactly one report, found {len(reports)}"
        )
    errors = validate_report(
        reports[0],
        provenance,
        matrix[arm]["config"],
        matrix[arm]["_schemaVersion"],
    )
    if errors:
        raise ValidationError("\n".join(f"{arm}: {error}" for error in errors))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=pathlib.Path)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--provenance-only", action="store_true")
    mode.add_argument("--expected-arms", type=int)
    mode.add_argument("--single-arm")
    parser.add_argument(
        "--allow-legacy-schema",
        action="store_true",
        help="allow schema v1-v3 only when validating archived artifacts offline",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.provenance_only:
            read_provenance(args.root)
            read_expected_matrix(
                args.root, allow_legacy_schema=args.allow_legacy_schema
            )
        elif args.single_arm is not None:
            validate_single_arm(
                args.root,
                args.single_arm,
                allow_legacy_schema=args.allow_legacy_schema,
            )
        else:
            validate_sweep(
                args.root,
                args.expected_arms,
                allow_legacy_schema=args.allow_legacy_schema,
            )
    except ValidationError as exc:
        print(f"GATE-FAILED: {exc}", file=sys.stderr)
        return 1
    if args.provenance_only:
        print("PROVENANCE-VALID")
    elif args.single_arm is not None:
        print(f"ARM-VALID {args.single_arm}")
    else:
        print("SWEEP-VALID")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
