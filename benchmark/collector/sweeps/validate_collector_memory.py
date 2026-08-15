#!/usr/bin/env python3
"""Fail-closed validator for the formal Collector memory A/B/C campaign.

The Go harness is the evidence producer.  Each arm must contain exactly one
``collector-memory-report.json`` plus the raw cgroup-v2 and event-spool CSVs in
the same immutable run directory.  This validator binds that evidence to the
exact matrix, independently recomputes memory gates from raw CSV rows, and
never trusts a producer-supplied pass/fail boolean.

Report contract: schemaVersion=1; completed=true; config contains armName,
matrixSHA256, kind, rateEventsPerSecond, ingestSeconds, idleSeconds, repeat and
the exact resources; replay/remote/collectorLogs/runtime/cgroupSampler/
memoryDetail/eventSpool are the JSON fields emitted by
TestCollectorMemoryBenchmark.  Raw CSV schemas are defined by
WriteMemoryDetailCSV and WriteEventSpoolCSV.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import pathlib
import re
import statistics
import sys
from typing import Any, Optional

import write_collector_memory_matrix as matrix_contract

MIB = 1024 * 1024
FULL_SHA256 = re.compile(r"^[0-9a-f]{64}$")
FULL_GIT_OID = re.compile(r"^[0-9a-f]{40,64}$")
FULL_IMAGE_ID = re.compile(r"^sha256:[0-9a-f]{64}$")
REQUIRED_PROVENANCE = {
    "repo_head", "tracked_diff_sha256", "benchmark_source_sha256",
    "operator_sha256", "expected_matrix_sha256", "runner_sha256",
    "generator_sha256", "validator_sha256", "ray_image_requested",
    "ray_runtime_id", "ray_runtime_version", "ray_runtime_commit",
    "collector_image_requested", "collector_build_source_sha256",
    "collector_runtime_id", "collector_runtime_build_source_sha256",
}
RESOURCE_FAILURE_REASONS = {
    "collector-oom", "collector-restart", "rate-sag", "disk-pressure-503",
    "rotation-queue-full", "memory-max-hit", "psi-full-pressure",
}


class ValidationError(RuntimeError):
    pass


def normalize_docker_hub_reference(value: Any) -> Optional[str]:
    if not isinstance(value, str) or not value:
        return None
    normalized = value.removeprefix("docker.io/")
    if normalized.startswith("library/") and "/" not in normalized[len("library/"):]:
        normalized = normalized.removeprefix("library/")
    return normalized or None


def docker_hub_references_equal(left: Any, right: Any) -> bool:
    normalized_left = normalize_docker_hub_reference(left)
    normalized_right = normalize_docker_hub_reference(right)
    return bool(normalized_left and normalized_right and normalized_left == normalized_right)


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError(f"JSON contains duplicate key {key!r}")
        result[key] = value
    return result


def load_json(path: pathlib.Path) -> Any:
    if not path.is_file():
        raise ValidationError(f"missing {path}")
    try:
        return json.loads(path.read_text(), object_pairs_hook=reject_duplicate_keys)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError(f"cannot read {path}: {exc}") from exc


def write_json_exclusive(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
    except OSError as exc:
        raise ValidationError(f"cannot create immutable verdict {path}: {exc}") from exc


def sha256(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_kv(path: pathlib.Path, required: set[str]) -> dict[str, str]:
    if not path.is_file():
        raise ValidationError(f"missing {path}")
    values: dict[str, str] = {}
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if not line or "=" not in line:
            raise ValidationError(f"{path.name}:{number} is not key=value")
        key, value = line.split("=", 1)
        if not key or not value or key in values:
            raise ValidationError(f"{path.name}:{number} has empty/duplicate field")
        values[key] = value
    missing = required - values.keys()
    if missing:
        raise ValidationError(f"{path.name} missing fields {sorted(missing)}")
    return values


def read_provenance(root: pathlib.Path, final: bool = False) -> dict[str, str]:
    path = root / ("provenance-final.txt" if final else "provenance.txt")
    required = REQUIRED_PROVENANCE | ({"revalidation_status"} if final else set())
    values = read_kv(path, required)
    sha_fields = {
        "tracked_diff_sha256", "benchmark_source_sha256", "operator_sha256",
        "expected_matrix_sha256", "runner_sha256",
        "generator_sha256", "validator_sha256", "collector_build_source_sha256",
        "collector_runtime_build_source_sha256",
    }
    for field in sha_fields:
        if not FULL_SHA256.fullmatch(values[field]):
            raise ValidationError(f"{path.name} {field} is not full SHA-256")
    if not FULL_GIT_OID.fullmatch(values["repo_head"]):
        raise ValidationError(f"{path.name} repo_head is not a full Git object ID")
    for field in ("ray_runtime_id", "collector_runtime_id"):
        if not FULL_IMAGE_ID.fullmatch(values[field]):
            raise ValidationError(f"{path.name} {field} is not full image ID")
    if values["collector_build_source_sha256"] != values["collector_runtime_build_source_sha256"]:
        raise ValidationError("Collector image build-source label does not match checkout")
    if final and values["revalidation_status"] != "valid":
        raise ValidationError("final provenance is not marked valid")
    return values


def read_matrix(root: pathlib.Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    doc = load_json(root / "expected-matrix.json")
    if doc != matrix_contract.build_matrix():
        raise ValidationError("expected matrix differs from exact A/B/C contract")
    by_name: dict[str, dict[str, Any]] = {}
    for arm in doc["arms"]:
        name = arm.get("name")
        if not isinstance(name, str) or name in by_name:
            raise ValidationError("matrix has invalid/duplicate arm")
        by_name[name] = arm
    return doc, by_name


def parse_quantity_bytes(quantity: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)(Mi|Gi)", quantity)
    if not match:
        raise ValidationError(f"unsupported memory quantity {quantity!r}")
    return int(match.group(1)) * (MIB if match.group(2) == "Mi" else 1024 * MIB)


def parse_rfc3339_ns(text: str) -> int:
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})",
        text,
    )
    if not match:
        raise ValidationError(f"invalid upload timestamp {text!r}")
    try:
        base = dt.datetime.fromisoformat(match.group(1) + match.group(3).replace("Z", "+00:00"))
        delta = base.astimezone(dt.timezone.utc) - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
        seconds = delta.days * 86400 + delta.seconds
        fraction = int((match.group(2) or "").ljust(9, "0"))
        return seconds * 1_000_000_000 + fraction
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"invalid upload timestamp {text!r}") from exc


def parse_rfc3339_interval_ns(text: str) -> tuple[int, int]:
    lower = parse_rfc3339_ns(text)
    match = re.search(r":\d{2}(?:\.(\d{1,9}))?(?:Z|[+-]\d{2}:\d{2})$", text)
    if not match:
        raise ValidationError(f"invalid upload timestamp precision {text!r}")
    fractional_digits = len(match.group(1) or "")
    quantum = 10 ** (9 - fractional_digits) if fractional_digits < 9 else 1
    return lower, lower + quantum


def read_csv(path: pathlib.Path, fields: set[str]) -> list[dict[str, str]]:
    if not path.is_file():
        raise ValidationError(f"missing {path}")
    with path.open(newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None or set(reader.fieldnames) != fields:
            raise ValidationError(f"{path.name} header differs: {reader.fieldnames}")
        rows = list(reader)
    if not rows:
        raise ValidationError(f"{path.name} has no samples")
    return rows


MEMORY_FIELDS = {
    "time_nano", "container_id", "container", "current_bytes", "peak_bytes",
    "anon_bytes", "file_bytes", "file_dirty_bytes", "file_writeback_bytes",
    "kernel_bytes", "slab_bytes", "memory_max", "memory_events_low",
    "memory_events_high", "memory_events_max", "memory_events_oom",
    "memory_events_oom_kill", "psi_some_total_usec", "psi_full_total_usec",
}
SPOOL_FIELDS = {
    "time_nano", "pod_uid", "pod", "total_bytes", "raw_jsonl_bytes",
    "gzip_bytes", "tmp_bytes", "other_bytes", "file_count", "valid", "error",
}
INTEGER_MEMORY_FIELDS = MEMORY_FIELDS - {"container_id", "container", "memory_max"}
INTEGER_SPOOL_FIELDS = SPOOL_FIELDS - {"pod_uid", "pod", "valid", "error"}


def integerize(rows: list[dict[str, str]], fields: set[str], label: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        copy: dict[str, Any] = dict(row)
        for field in fields:
            try:
                copy[field] = int(row[field])
            except ValueError as exc:
                raise ValidationError(f"{label}[{index}].{field} is not integer") from exc
            if copy[field] < 0:
                raise ValidationError(f"{label}[{index}].{field} is negative")
        result.append(copy)
    return result


def phase_map(report: dict[str, Any]) -> dict[str, int]:
    phases = report.get("phases")
    if not isinstance(phases, list):
        raise ValidationError("phases is not an array")
    result: dict[str, int] = {}
    for point in phases:
        if not isinstance(point, dict) or set(point) != {"name", "timeNano"}:
            raise ValidationError("phase point schema differs")
        name, timestamp = point["name"], point["timeNano"]
        if name in result or type(timestamp) is not int or timestamp <= 0:
            raise ValidationError("phase point is duplicate/invalid")
        result[name] = timestamp
    if list(result) != ["baseline", "ingest", "idle", "shutdown", "complete"]:
        raise ValidationError(f"phase order differs: {list(result)}")
    if list(result.values()) != sorted(result.values()):
        raise ValidationError("phase timestamps are not monotonic")
    return result


def validate_clock_skew(report: dict[str, Any], name: str) -> None:
    evidence = report.get("clockSkew")
    if not isinstance(evidence, dict) or set(evidence) != {"before", "after"}:
        raise ValidationError(f"{name}: clockSkew schema differs")
    fields = {
        "hostBeforeUnixNano", "nodeUnixNano", "hostAfterUnixNano",
        "roundTripNano", "skewNano",
    }
    for label in ("before", "after"):
        sample = evidence.get(label)
        if not isinstance(sample, dict) or set(sample) != fields:
            raise ValidationError(f"{name}: clockSkew.{label} schema differs")
        if any(type(sample[field]) is not int for field in fields):
            raise ValidationError(f"{name}: clockSkew.{label} contains non-integer")
        before = sample["hostBeforeUnixNano"]
        node = sample["nodeUnixNano"]
        after = sample["hostAfterUnixNano"]
        round_trip = after - before
        midpoint = before + round_trip // 2
        skew = node - midpoint
        skew_lower = node - after
        skew_upper = node - before
        if (
            before <= 0
            or node <= 0
            or after < before
            or sample["roundTripNano"] != round_trip
            or sample["skewNano"] != skew
            or round_trip <= 0
            or round_trip > 250_000_000
            or abs(skew_lower) > 1_000_000_000
            or abs(skew_upper) > 1_000_000_000
        ):
            raise ValidationError(
                f"{name}: clockSkew.{label} outside exact 250ms RTT/1s uncertainty gate"
            )


def clock_skew_bounds(report: dict[str, Any], name: str) -> tuple[int, int]:
    """Return the conservative node-minus-host clock interval for the arm."""
    evidence = report["clockSkew"]
    lowers = [sample["nodeUnixNano"] - sample["hostAfterUnixNano"]
              for sample in (evidence["before"], evidence["after"])]
    uppers = [sample["nodeUnixNano"] - sample["hostBeforeUnixNano"]
              for sample in (evidence["before"], evidence["after"])]
    if max(lowers) > min(uppers):
        raise ValidationError(f"{name}: pre/post clock-skew intervals do not overlap")
    return min(lowers), max(uppers)


def median_window(rows: list[dict[str, Any]], field: str, start: int, end: int) -> float:
    values = [row[field] for row in rows if start <= row["time_nano"] < end]
    if not values:
        raise ValidationError(f"no {field} samples in [{start},{end})")
    return float(statistics.median(values))


def bounded(current: float, previous: float, abs_mib: float, fraction: float) -> bool:
    return current - previous <= max(abs_mib * MIB, fraction * previous)


def expected_report_config(arm: dict[str, Any], matrix_sha: str, out_dir: pathlib.Path) -> dict[str, Any]:
    cfg = arm["config"]
    return {
        "armName": arm["name"], "matrixSHA256": matrix_sha,
        "kind": {"A": "continuous", "B": "ingest-idle", "C": "limit"}[cfg["experiment"]],
        "rateEventsPerSecond": cfg["targetEventsPerSecond"],
        "repeat": arm["repeat"], "cpuRequest": cfg["cpuRequest"],
        "cpuLimit": cfg["cpuLimit"], "memoryRequest": cfg["memoryRequest"],
        "memoryLimit": cfg["memoryLimit"], "kindNode": "bench-control-plane",
        "outDir": str(out_dir), "ingestSeconds": float(cfg["ingestSeconds"]),
        "idleSeconds": float(cfg["idleSeconds"]),
    }


def locate_arm(root: pathlib.Path, arm: str) -> tuple[pathlib.Path, dict[str, Any]]:
    reports = list((root / arm).glob("*/collector-memory-report.json"))
    if len(reports) != 1:
        raise ValidationError(f"{arm}: expected exactly one report, found {len(reports)}")
    report = load_json(reports[0])
    if not isinstance(report, dict):
        raise ValidationError(f"{arm}: report is not object")
    return reports[0].parent, report


def validate_arm(root: pathlib.Path, arm: dict[str, Any], provenance: dict[str, str]) -> dict[str, Any]:
    name = arm["name"]
    run_dir, report = locate_arm(root, name)
    cfg = arm["config"]
    errors: list[str] = []
    check = lambda condition, message: errors.append(message) if not condition else None

    check(report.get("schemaVersion") == 1, "schemaVersion != 1")
    check(report.get("completed") is True and not report.get("error"), "report not completed")
    check(report.get("config") == expected_report_config(arm, provenance["expected_matrix_sha256"], root / name), "config/matrix binding differs")
    phases = phase_map(report)
    validate_clock_skew(report, name)
    clock_evidence = report["clockSkew"]
    check(
        clock_evidence["before"]["hostAfterUnixNano"] <= min(phases.values())
        and max(phases.values()) <= clock_evidence["after"]["hostBeforeUnixNano"],
        "clock probes do not bracket phase timeline",
    )
    skew_lower, skew_upper = clock_skew_bounds(report, name)
    check(abs((phases["ingest"] - phases["baseline"]) / 1e9 - cfg["baselineSeconds"]) <= 1.5, "baseline duration differs")
    check(abs((phases["idle"] - phases["ingest"]) / 1e9 - cfg["ingestSeconds"]) <= 2.0, "ingest duration differs")
    check(abs((phases["shutdown"] - phases["idle"]) / 1e9 - cfg["idleSeconds"]) <= 1.5, "idle/tail duration differs")

    replay = report.get("replay")
    remote = report.get("remote")
    remote_preflight = report.get("remotePreflight")
    runtime = report.get("runtime")
    ray_runtime = report.get("rayRuntime")
    logs = report.get("collectorLogs")
    if not all(isinstance(value, dict) for value in (replay, remote, remote_preflight, runtime, ray_runtime)) or not isinstance(logs, list) or len(logs) != 1:
        raise ValidationError(f"{name}: replay/remote/remotePreflight/runtime/rayRuntime/collectorLogs schema differs")
    log = logs[0]
    if not isinstance(log, dict):
        raise ValidationError(f"{name}: collector log is not object")
    planned = cfg["plannedEvents"]
    exact_replay = {
        "schema_version": 1, "target_events_per_second": cfg["targetEventsPerSecond"],
        "batch_size": cfg["batchEvents"], "planned_events": planned,
        "sent_events": planned, "accepted_events": planned,
    }
    for key, expected in exact_replay.items():
        check(replay.get(key) == expected, f"replay.{key} differs")
    check(replay.get("jsonl_bytes_per_event") == 895, "replay.jsonl_bytes_per_event differs")
    check(
        replay.get("fixture_gzip_ratio_definition") == "compressed_bytes/raw_jsonl_bytes",
        "replay.fixture_gzip_ratio_definition differs",
    )
    fixture_gzip_ratio = replay.get("fixture_gzip_ratio")
    check(
        type(fixture_gzip_ratio) in (int, float)
        and 0.08 <= float(fixture_gzip_ratio) <= 0.12,
        "replay.fixture_gzip_ratio outside [0.08,0.12] compressed/raw gate",
    )
    achieved = replay.get("achieved_events_per_second")
    check(type(achieved) in (int, float) and math.isfinite(float(achieved)), "achieved rate invalid")
    if type(achieved) in (int, float):
        check(abs(float(achieved) / cfg["targetEventsPerSecond"] - 1) <= 0.05, "achieved rate outside +/-5%")
    check(replay.get("non_200_responses") == 0 and replay.get("retries") == 0, "publisher rejection/retry")
    check(remote.get("lines") == planned and remote.get("uniqueEventIDs") == planned, "remote event count differs")
    check(remote.get("duplicateEventIDs") == remote.get("malformedLines") == remote.get("unexpectedEventIDs") == 0, "remote corruption")
    check(remote.get("rawJSONLBytes") == replay.get("expected_jsonl_bytes"), "remote bytes differ")
    check(
        isinstance(remote_preflight.get("prefix"), str)
        and bool(remote_preflight["prefix"])
        and remote_preflight.get("empty") is True
        and remote_preflight.get("objects") == 0
        and remote_preflight.get("bytes") == 0,
        "remote preflight prefix was not empty",
    )

    for evidence, label in ((runtime, "runtime"), (log, "collectorLog")):
        for key in ("cpuRequest", "cpuLimit", "memoryRequest", "memoryLimit"):
            check(evidence.get(key) == cfg[key], f"{label}.{key} differs")
        check(evidence.get("restartCount") == 0, f"{label} restarted")
    check(
        docker_hub_references_equal(runtime.get("image"), provenance["collector_image_requested"]),
        "Collector image differs",
    )
    check(provenance["collector_runtime_id"] in str(runtime.get("imageID", "")), "Collector image ID differs")
    check(runtime.get("containerID") and runtime.get("containerID") == log.get("containerID"), "container identity differs")
    check(runtime.get("terminationGracePeriodSeconds") == 120, "termination grace period != 120s")
    check(
        docker_hub_references_equal(ray_runtime.get("image"), provenance["ray_image_requested"]),
        "Ray image differs",
    )
    check(provenance["ray_runtime_id"] in str(ray_runtime.get("imageID", "")), "Ray image ID differs")
    check(ray_runtime.get("containerID") and ray_runtime.get("restartCount") == 0, "Ray runtime identity/restart differs")
    check(ray_runtime.get("podUID") == runtime.get("podUID"), "Ray and Collector pod UID differ")
    check(log.get("gracefulShutdownComplete") is True and log.get("logStreamComplete") is True, "Collector shutdown/log incomplete")
    for key in ("diskPressure503s", "rotationQueueFull", "uploadFailures", "memoryEventsOOM", "memoryEventsOOMKill", "cgroupMemoryReadErrors"):
        check(log.get(key) == 0, f"collectorLog.{key} != 0")
    check(log.get("cgroupMemoryMaxBytes") == parse_quantity_bytes(cfg["memoryLimit"]), "cgroup memory.max differs")
    check(log.get("uploadedBytes") == replay.get("expected_jsonl_bytes"), "uploaded bytes differ")
    sampler = report.get("cgroupSampler", {})
    detail = report.get("memoryDetail", {})
    spool_gate = report.get("eventSpool", {})
    check(sampler.get("streamComplete") is True, "cgroup stream incomplete")
    check(detail.get("observed") is True and detail.get("readErrors") == 0 and detail.get("samples", 0) >= 4, "memory detail gate failed")
    check(spool_gate.get("invalidSamples") == 0 and spool_gate.get("samples", 0) >= 4, "spool gate failed")

    memory = integerize(read_csv(run_dir / "collector_memory_samples.csv", MEMORY_FIELDS), INTEGER_MEMORY_FIELDS, "memory")
    memory = [row for row in memory if row["container_id"] == runtime.get("containerID")]
    if not memory:
        raise ValidationError(f"{name}: no samples for runtime container")
    memory.sort(key=lambda row: row["time_nano"])
    check(all(a["time_nano"] < b["time_nano"] for a, b in zip(memory, memory[1:])), "memory timestamps not strictly increasing")
    check(all(row["memory_max"] == str(parse_quantity_bytes(cfg["memoryLimit"])) for row in memory), "raw memory.max differs")
    observed = [row for row in memory if phases["baseline"] <= row["time_nano"] < phases["shutdown"]]
    expected_span = phases["shutdown"] - phases["baseline"]
    observed_span = observed[-1]["time_nano"] - observed[0]["time_nano"]
    max_gap = max(
        b["time_nano"] - a["time_nano"] for a, b in zip(observed, observed[1:])
    )
    # A sample no more than one second inside either boundary brackets the
    # phase; timestamp span prevents duplicated/bursty rows from manufacturing
    # nominal count coverage.
    check(observed[0]["time_nano"] - phases["baseline"] <= 1_000_000_000, "memory series does not bracket baseline")
    check(phases["shutdown"] - observed[-1]["time_nano"] <= 1_000_000_000, "memory series does not bracket shutdown")
    check(observed_span / expected_span >= 0.95, "memory timestamp coverage below 95%")
    check(max_gap <= 1_000_000_000, "memory sample gap exceeds 1s")
    # Resource safety covers the whole container lifecycle, including the
    # shutdown compression/upload burst after the measured ingest window.
    for key in ("memory_events_oom", "memory_events_oom_kill"):
        check(memory[-1][key] - memory[0][key] == 0, f"{key} increased")
    # max/reclaim and PSI are candidate-sizing outcomes for C, not malformed
    # evidence. Keep A/B discovery pressure-free, but let a complete C arm fail
    # this candidate and proceed to the next larger limit.
    if cfg["experiment"] != "C":
        for key in ("memory_events_max",):
            check(memory[-1][key] - memory[0][key] == 0, f"{key} increased")

    end = phases["idle"] if cfg["experiment"] in {"A", "C"} else phases["shutdown"]
    current_anon = median_window(memory, "anon_bytes", end - 30_000_000_000, end)
    previous_anon = median_window(memory, "anon_bytes", end - 60_000_000_000, end - 30_000_000_000)
    check(bounded(current_anon, previous_anon, 8, 0.10), "anonymous memory grew in final window")

    spool = integerize(read_csv(run_dir / "event_spool_samples.csv", SPOOL_FIELDS), INTEGER_SPOOL_FIELDS, "spool")
    spool = [row for row in spool if row["pod_uid"] == runtime.get("podUID")]
    check(bool(spool), "no spool samples for runtime pod")
    check(all(row["valid"] == "true" and not row["error"] for row in spool), "invalid spool sample")
    spool.sort(key=lambda row: row["time_nano"])
    check(
        all(a["time_nano"] < b["time_nano"] for a, b in zip(spool, spool[1:])),
        "spool timestamps not strictly increasing",
    )
    active_spool = [
        row for row in spool
        if phases["baseline"] <= row["time_nano"] < phases["shutdown"]
    ]
    if len(active_spool) < 2:
        check(False, "spool series missing active lifecycle coverage")
    else:
        spool_expected_span = phases["shutdown"] - phases["baseline"]
        spool_observed_span = active_spool[-1]["time_nano"] - active_spool[0]["time_nano"]
        spool_max_gap = max(
            b["time_nano"] - a["time_nano"]
            for a, b in zip(active_spool, active_spool[1:])
        )
        check(
            active_spool[0]["time_nano"] - phases["baseline"] <= 1_000_000_000,
            "spool series does not bracket baseline",
        )
        check(
            phases["shutdown"] - active_spool[-1]["time_nano"] <= 1_000_000_000,
            "spool series does not bracket shutdown",
        )
        check(
            spool_observed_span / spool_expected_span >= 0.95,
            "spool timestamp coverage below 95%",
        )
        check(spool_max_gap <= 1_000_000_000, "spool sample gap exceeds 1s")
    uploads = log.get("uploadTimeline")
    if not isinstance(uploads, list):
        errors.append("uploadTimeline not array")
        uploads = []
    upload_times: list[int] = []
    for point in uploads:
        if not isinstance(point, dict):
            errors.append("uploadTimeline entry is not an object")
            continue
        lower, upper = parse_rfc3339_interval_ns(point.get("time", ""))
        exact = point.get("unixNano")
        check(type(exact) is int and exact > 0, "uploadTimeline exact unixNano missing")
        if type(exact) is int and exact > 0:
            check(lower <= exact < upper, "uploadTimeline unixNano outside log timestamp bucket")
            upload_times.append(exact)
    final_cgroup = log.get("finalCgroupMemory")
    final_fields = {
        "time", "unixNano", "currentBytes", "peakBytes",
        "eventsMax", "eventsOOM", "eventsOOMKill", "psiFullTotalUsec",
    }
    final_exact = None
    if not isinstance(final_cgroup, dict) or set(final_cgroup) != final_fields:
        check(False, "final cgroup memory evidence missing or schema differs")
        final_cgroup = {}
    else:
        final_exact = final_cgroup.get("unixNano")
        final_time = final_cgroup.get("time")
        numeric_fields = (
            "unixNano", "currentBytes", "peakBytes",
            "eventsMax", "eventsOOM", "eventsOOMKill", "psiFullTotalUsec",
        )
        check(all(type(final_cgroup.get(field)) is int for field in numeric_fields), "final cgroup memory evidence contains non-integer")
        check(type(final_time) is str and bool(final_time), "final cgroup memory timestamp missing")
        if type(final_time) is str and final_time and type(final_exact) is int:
            lower, upper = parse_rfc3339_interval_ns(final_time)
            check(lower <= final_exact < upper, "final cgroup unixNano outside log timestamp bucket")
        check(type(final_exact) is int and final_exact > 0, "final cgroup unixNano invalid")
        check(
            type(final_cgroup.get("currentBytes")) is int
            and type(final_cgroup.get("peakBytes")) is int
            and 0 < final_cgroup["currentBytes"] <= final_cgroup["peakBytes"],
            "final cgroup current/peak invalid",
        )
        check(
            final_cgroup.get("eventsOOM") == 0
            and final_cgroup.get("eventsOOMKill") == 0,
            "final cgroup OOM events are nonzero",
        )
        check(
            final_cgroup.get("eventsMax", -1) >= memory[-1]["memory_events_max"],
            "final cgroup memory.events.max precedes sampled counter",
        )
        check(
            final_cgroup.get("psiFullTotalUsec", -1) >= memory[-1]["psi_full_total_usec"],
            "final cgroup PSI full total precedes sampled counter",
        )
        if cfg["experiment"] != "C":
            check(final_cgroup.get("eventsMax") == 0, "final cgroup memory.events.max is nonzero")
            check(final_cgroup.get("psiFullTotalUsec") == 0, "final cgroup PSI full pressure is nonzero")
        check(not upload_times or final_exact >= max(upload_times), "final cgroup evidence precedes final upload")
    main_return = log.get("mainReturn")
    main_return_fields = {"time", "unixNano", "exitCode", "reason"}
    if not isinstance(main_return, dict) or set(main_return) != main_return_fields:
        check(False, "collector main-return evidence missing or schema differs")
    else:
        return_exact = main_return.get("unixNano")
        return_time = main_return.get("time")
        check(
            type(return_exact) is int and return_exact > 0
            and main_return.get("exitCode") == 0
            and main_return.get("reason") == "Completed",
            "collector main-return evidence is not clean completion",
        )
        if type(return_time) is str and return_time and type(return_exact) is int:
            lower, upper = parse_rfc3339_interval_ns(return_time)
            check(lower <= return_exact < upper, "collector main-return unixNano outside log timestamp bucket")
        else:
            check(False, "collector main-return timestamp missing")
        if isinstance(final_cgroup, dict):
            check(
                type(final_cgroup.get("unixNano")) is int
                and type(return_exact) is int
                and return_exact >= final_cgroup["unixNano"],
                "collector main return precedes final cgroup evidence",
            )
    shutdown_node_lower = phases["shutdown"] + skew_lower
    shutdown_node_upper = phases["shutdown"] + skew_upper
    if type(final_exact) is int:
        check(final_exact >= shutdown_node_upper, "final cgroup evidence precedes conservative shutdown boundary")
        check(
            0 <= final_exact - memory[-1]["time_nano"] <= 1_000_000_000,
            "memory series ends more than 1s before final cgroup evidence",
        )
    ambiguous_shutdown_uploads = [
        stamp for stamp in upload_times
        if shutdown_node_lower <= stamp < shutdown_node_upper
    ]
    check(not ambiguous_shutdown_uploads, "upload timing is ambiguous across shutdown clock-skew interval")
    post_shutdown_uploads = [stamp for stamp in upload_times if stamp >= shutdown_node_upper]
    if post_shutdown_uploads:
        last_upload = max(post_shutdown_uploads)
        terminal = memory[-1]
        check(
            terminal["time_nano"] >= shutdown_node_lower,
            "memory series does not reach the shutdown drain",
        )
        before_shutdown = [row for row in memory if row["time_nano"] <= shutdown_node_lower]
        if before_shutdown:
            drain = [before_shutdown[-1]] + [
                row for row in memory
                if shutdown_node_lower < row["time_nano"] <= terminal["time_nano"]
            ]
            check(
                len(drain) >= 2 and all(
                    b["time_nano"] - a["time_nano"] <= 1_000_000_000
                    for a, b in zip(drain, drain[1:])
                ),
                "memory series has >1s gap during shutdown drain",
            )
    pre_shutdown_uploads = [stamp for stamp in upload_times if stamp < shutdown_node_lower]
    if cfg["experiment"] == "B":
        during_idle = report.get("duringIdle")
        check(
            isinstance(during_idle, dict)
            and type(during_idle.get("addedObjects")) is int
            and during_idle["addedObjects"] >= 0,
            "duringIdle storage evidence invalid",
        )
        if cfg["idleGate"] == "retained-file-plateau-negative-control":
            check(not pre_shutdown_uploads, "negative control uploaded before shutdown")
            check(
                isinstance(during_idle, dict) and during_idle.get("addedObjects") == 0,
                "negative control stored objects before shutdown",
            )
            f1 = median_window(memory, "file_bytes", phases["shutdown"] - 30_000_000_000, phases["shutdown"])
            f0 = median_window(memory, "file_bytes", phases["shutdown"] - 60_000_000_000, phases["shutdown"] - 30_000_000_000)
            check(bounded(f1, f0, 10, 0.10) and f1 >= 0.8 * f0, "negative-control file charge not retained/flat")
        else:
            check(bool(pre_shutdown_uploads), "high-rate B did not upload before shutdown")
            check(
                isinstance(during_idle, dict) and during_idle.get("addedObjects", 0) > 0,
                "high-rate B storage snapshot has no pre-shutdown upload",
            )
            event_bytes = lambda row: row["raw_jsonl_bytes"] + row["gzip_bytes"] + row["tmp_bytes"]
            before = [event_bytes(row) for row in spool if phases["ingest"] <= row["time_nano"] < phases["idle"]]
            after = [event_bytes(row) for row in spool if phases["idle"] <= row["time_nano"] < phases["shutdown"]]
            check(bool(before) and max(before) >= 100 * MIB, "high-rate B never crossed 100MiB")
            # A fresh active file can legitimately retain the ~28 MiB written
            # after the 100 MiB file rotated. Include raw/gzip/tmp event files,
            # but exclude unrelated Ray logs classified as other_bytes.
            check(bool(after) and min(after) <= 0.30 * max(before), "high-rate B spool did not reclaim")
            if pre_shutdown_uploads:
                last = max(pre_shutdown_uploads)
                check(
                    shutdown_node_lower - last >= 20_000_000_000,
                    "less than 20s observed after upload/delete",
                )

    if errors:
        raise ValidationError("\n".join(f"{name}: {error}" for error in errors))
    report["_memory_rows"] = memory
    # memory.peak is kernel-maintained lifetime high-water evidence.  Sampled
    # memory.current can miss a short OOM-dangerous peak between 250ms reads.
    sampled_peak = max(row["peak_bytes"] for row in memory)
    final_peak = final_cgroup.get("peakBytes", 0) if isinstance(final_cgroup, dict) else 0
    check(final_peak >= sampled_peak, "final cgroup memory.peak is below sampled memory.peak")
    if errors:
        raise ValidationError("\n".join(f"{name}: {error}" for error in errors))
    report["_peak_current"] = final_peak
    adjacent = max(memory, key=lambda row: (row["current_bytes"], row["time_nano"]))
    report["_peak_composition"] = {
        "timeNano": adjacent["time_nano"],
        "anonBytes": adjacent["anon_bytes"],
        "fileBytes": adjacent["file_bytes"],
        "fileDirtyBytes": adjacent["file_dirty_bytes"],
        "fileWritebackBytes": adjacent["file_writeback_bytes"],
        "kernelBytes": adjacent["kernel_bytes"],
        "slabBytes": adjacent["slab_bytes"],
        "currentBytes": adjacent["current_bytes"],
        "lifetimePeakBytes": report["_peak_current"],
    }
    final_events_max = final_cgroup.get("eventsMax")
    report["_events_max_delta"] = final_events_max if type(final_events_max) is int else -1
    final_psi = final_cgroup.get("psiFullTotalUsec")
    report["_psi_full_delta"] = final_psi if type(final_psi) is int else -1
    return report


def classify_candidate_hard_failure(
    root: pathlib.Path,
    expected: dict[str, Any],
    provenance: dict[str, str],
    go_test_rc: int,
) -> dict[str, Any]:
    """Classify a nonzero C-arm process exit without hiding integrity loss.

    A resource-limited candidate may legitimately make the Go test nonzero.
    Only explicit cgroup/Collector pressure or rate-sag evidence is recoverable;
    remote corruption, accepted-event loss, wrong images/resources, Ray restart,
    sampler corruption, or upload failure remain campaign-fatal.
    """
    name = expected["name"]
    cfg = expected["config"]
    if cfg["experiment"] != "C" or type(go_test_rc) is not int or go_test_rc <= 0:
        raise ValidationError(f"{name}: hard-failure classifier requires C arm and rc>0")
    run_dir, report = locate_arm(root, name)
    if report.get("schemaVersion") != 1:
        raise ValidationError(f"{name}: hard-failure report schema differs")
    validate_clock_skew(report, name)
    if report.get("config") != expected_report_config(
        expected, provenance["expected_matrix_sha256"], root / name
    ):
        raise ValidationError(f"{name}: hard-failure config/matrix binding differs")
    replay = report.get("replay")
    remote = report.get("remote")
    remote_preflight = report.get("remotePreflight")
    runtime = report.get("runtime")
    ray_runtime = report.get("rayRuntime")
    logs = report.get("collectorLogs")
    if not all(isinstance(value, dict) for value in (replay, remote, remote_preflight, runtime, ray_runtime)) \
            or not isinstance(logs, list) or len(logs) != 1 or not isinstance(logs[0], dict):
        raise ValidationError(f"{name}: hard-failure identity/count evidence is incomplete")
    log = logs[0]
    fatal: list[str] = []
    check = lambda condition, message: fatal.append(message) if not condition else None
    for evidence, label in ((runtime, "runtime"), (log, "collectorLog")):
        for key in ("cpuRequest", "cpuLimit", "memoryRequest", "memoryLimit"):
            check(evidence.get(key) == cfg[key], f"{label}.{key} differs")
    check(
        docker_hub_references_equal(runtime.get("image"), provenance["collector_image_requested"]),
        "Collector image differs",
    )
    check(provenance["collector_runtime_id"] in str(runtime.get("imageID", "")), "Collector image ID differs")
    check(runtime.get("containerID") and runtime.get("containerID") == log.get("containerID"), "Collector identity differs")
    check(runtime.get("terminationGracePeriodSeconds") == 120, "termination grace period != 120s")
    check(
        docker_hub_references_equal(ray_runtime.get("image"), provenance["ray_image_requested"]),
        "Ray image differs",
    )
    check(provenance["ray_runtime_id"] in str(ray_runtime.get("imageID", "")), "Ray image ID differs")
    check(ray_runtime.get("containerID") and ray_runtime.get("restartCount") == 0, "Ray restarted/identity missing")
    check(ray_runtime.get("podUID") == runtime.get("podUID"), "Ray and Collector pod UID differ")
    check(log.get("cgroupMemoryMaxBytes") == parse_quantity_bytes(cfg["memoryLimit"]), "cgroup memory.max differs")
    check(log.get("uploadFailures") == 0, "upload failure is integrity loss")
    check(log.get("cgroupMemoryReadErrors") == 0, "cgroup read errors")
    check(replay.get("retries") in (0, None), "publisher retried")
    for key in ("duplicateEventIDs", "malformedLines", "unexpectedEventIDs"):
        check(remote.get(key) == 0, f"remote.{key} is integrity loss")
    check(
        isinstance(remote_preflight.get("prefix"), str)
        and bool(remote_preflight["prefix"])
        and remote_preflight.get("empty") is True
        and remote_preflight.get("objects") == 0
        and remote_preflight.get("bytes") == 0,
        "remote preflight prefix was not empty",
    )
    accepted = replay.get("accepted_events")
    expected_bytes = replay.get("expected_jsonl_bytes")
    if type(accepted) is int and accepted > 0:
        check(remote.get("lines") == accepted, "accepted event lines were lost")
        check(remote.get("uniqueEventIDs") == accepted, "accepted event IDs were lost")
        check(type(expected_bytes) is int and remote.get("rawJSONLBytes") == expected_bytes, "accepted event bytes were lost")
        check(log.get("uploadedBytes") == expected_bytes, "accepted bytes were not uploaded")

    memory = integerize(
        read_csv(run_dir / "collector_memory_samples.csv", MEMORY_FIELDS),
        INTEGER_MEMORY_FIELDS,
        "memory",
    )
    collector_rows = [row for row in memory if str(row["container"]).endswith("/collector")]
    if not collector_rows:
        fatal.append("no Collector cgroup rows")
    check(all(row["memory_max"] == str(parse_quantity_bytes(cfg["memoryLimit"])) for row in collector_rows), "raw memory.max differs")
    sampler = report.get("cgroupSampler", {})
    detail = report.get("memoryDetail", {})
    check(sampler.get("streamComplete") is True, "cgroup stream incomplete")
    check(detail.get("observed") is True and detail.get("readErrors") == 0, "memory detail incomplete")
    if fatal:
        raise ValidationError("\n".join(f"{name}: {message}" for message in fatal))

    reasons: set[str] = set()
    if max((row["memory_events_oom"] + row["memory_events_oom_kill"] for row in collector_rows), default=0) > 0 \
            or int(log.get("memoryEventsOOM", 0)) > 0 or int(log.get("memoryEventsOOMKill", 0)) > 0:
        reasons.add("collector-oom")
    if int(runtime.get("restartCount", 0)) > 0 or int(log.get("restartCount", 0)) > 0:
        reasons.add("collector-restart")
    achieved = replay.get("achieved_events_per_second")
    if type(achieved) in (int, float) and math.isfinite(float(achieved)) \
            and float(achieved) < 0.95 * cfg["targetEventsPerSecond"]:
        reasons.add("rate-sag")
    if int(log.get("diskPressure503s", 0)) > 0:
        reasons.add("disk-pressure-503")
    if int(log.get("rotationQueueFull", 0)) > 0:
        reasons.add("rotation-queue-full")
    final_memory = ((report.get("collectorLogs") or [{}])[0].get("finalCgroupMemory") or {})
    final_events_max = final_memory.get("eventsMax")
    if max((row["memory_events_max"] for row in collector_rows), default=0) > 0 \
            or (type(final_events_max) is int and final_events_max > 0):
        reasons.add("memory-max-hit")
    final_psi = final_memory.get("psiFullTotalUsec")
    if max((row["psi_full_total_usec"] for row in collector_rows), default=0) > 0 \
            or (type(final_psi) is int and final_psi > 0):
        reasons.add("psi-full-pressure")
    if not reasons:
        raise ValidationError(f"{name}: nonzero Go test rc has no attributable resource failure")
    if replay.get("non_200_responses", 0) not in (0, None) and not reasons.intersection({
        "collector-oom", "collector-restart", "disk-pressure-503", "rotation-queue-full",
    }):
        raise ValidationError(f"{name}: publisher rejection has no Collector resource cause")
    report_path = run_dir / "collector-memory-report.json"
    return {
        "schemaVersion": 1,
        "armName": name,
        "memoryLimit": cfg["memoryLimit"],
        "classification": "resource-failure",
        "goTestRC": go_test_rc,
        "reasons": sorted(reasons),
        "reportSHA256": sha256(report_path),
    }


def candidate_arm_verdict_path(root: pathlib.Path, name: str) -> pathlib.Path:
    return root / "candidate-arm-verdicts" / f"{name}.json"


def validate_resource_failure_verdict_schema(verdict: dict[str, Any], name: str) -> None:
    reasons = verdict.get("reasons")
    if (
        verdict.get("schemaVersion") != 1
        or verdict.get("armName") != name
        or verdict.get("classification") != "resource-failure"
        or type(verdict.get("goTestRC")) is not int
        or verdict["goTestRC"] <= 0
        or not isinstance(reasons, list)
        or not reasons
        or any(not isinstance(reason, str) or reason not in RESOURCE_FAILURE_REASONS for reason in reasons)
        or reasons != sorted(set(reasons))
        or not FULL_SHA256.fullmatch(str(verdict.get("reportSHA256", "")))
    ):
        raise ValidationError(f"{name}: candidate resource-failure verdict schema differs")


def read_candidate_arm_verdict(
    root: pathlib.Path,
    expected: dict[str, Any],
    provenance: dict[str, str],
) -> Optional[dict[str, Any]]:
    path = candidate_arm_verdict_path(root, expected["name"])
    if not path.exists():
        return None
    verdict = load_json(path)
    if not isinstance(verdict, dict):
        raise ValidationError(f"{expected['name']}: candidate arm verdict is not object")
    validate_resource_failure_verdict_schema(verdict, expected["name"])
    recomputed = classify_candidate_hard_failure(
        root, expected, provenance, verdict.get("goTestRC")
    )
    if verdict != recomputed:
        raise ValidationError(f"{expected['name']}: candidate arm verdict differs from evidence")
    return verdict


def validate_provenance(root: pathlib.Path) -> dict[str, str]:
    provenance = read_provenance(root)
    if provenance["expected_matrix_sha256"] != sha256(root / "expected-matrix.json"):
        raise ValidationError("provenance matrix SHA differs")
    return provenance


def candidate_evaluation(root: pathlib.Path, memory_limit: str) -> dict[str, Any]:
    matrix, by_name = read_matrix(root)
    acceptance = matrix["acceptance"]
    provenance = validate_provenance(root)
    discovery = [validate_arm(root, by_name[f"A-rate5000-r{r}"], provenance) for r in range(1, 4)]
    group = next((item for item in matrix["candidateGroupsC"] if item["memoryLimit"] == memory_limit), None)
    if group is None:
        raise ValidationError(f"unknown candidate {memory_limit}")
    ref_rate = statistics.median(item["replay"]["achieved_events_per_second"] for item in discovery)
    ref_p99 = statistics.median(item["replay"]["latency_p99_ms"] for item in discovery)
    limit = parse_quantity_bytes(memory_limit)
    errors: list[str] = []
    reports: list[dict[str, Any]] = []
    arm_results: list[dict[str, Any]] = []
    for name in group["arms"]:
        hard = read_candidate_arm_verdict(root, by_name[name], provenance)
        if hard is not None:
            reason = ",".join(hard["reasons"])
            errors.append(f"{name}: hard resource failure: {reason}")
            arm_results.append({
                "armName": name,
                "classification": "resource-failure",
                "goTestRC": hard["goTestRC"],
                "reasons": hard["reasons"],
                "reportSHA256": hard["reportSHA256"],
            })
            continue
        item = validate_arm(root, by_name[name], provenance)
        reports.append(item)
        arm_results.append({
            "armName": name,
            "classification": "valid-run",
            "goTestRC": 0,
            "reasons": [],
            "reportSHA256": sha256(locate_arm(root, name)[0] / "collector-memory-report.json"),
            "peakComposition": item["_peak_composition"],
        })
    for item in reports:
        name = item["config"]["armName"]
        if item["replay"]["achieved_events_per_second"] < acceptance["minimumCThroughputRatio"] * ref_rate:
            errors.append(f"{name}: throughput below 95% of discovery")
        if item["replay"]["latency_p99_ms"] > acceptance["maximumCLatencyP99Ratio"] * ref_p99:
            errors.append(f"{name}: p99 above 120% of discovery")
        if item["_peak_current"] > acceptance["maximumMemoryPeakToLimitRatio"] * limit:
            errors.append(f"{name}: headroom-including-reclaimable-cache: kernel memory.peak above 80% of limit")
        if item["_events_max_delta"] != 0:
            errors.append(f"{name}: memory.events.max increased")
        if item["_psi_full_delta"] != 0:
            errors.append(f"{name}: PSI full pressure increased")
    if len(reports) == 3:
        peaks = [item["_peak_current"] for item in reports]
        median_peak = statistics.median(peaks)
        spread = (max(peaks) - min(peaks)) / median_peak if median_peak > 0 else math.inf
        if spread > acceptance["maximumThreeRunPeakSpreadFraction"]:
            errors.append(
                f"{memory_limit}: three-run memory.peak spread {spread:.3f} exceeds 0.10 of median"
            )
    return {
        "schemaVersion": 1,
        "memoryLimit": memory_limit,
        "verdict": "failed" if errors else "passed",
        "errors": errors,
        "arms": arm_results,
    }


def candidate_errors(root: pathlib.Path, memory_limit: str) -> list[str]:
    return candidate_evaluation(root, memory_limit)["errors"]


def read_status(root: pathlib.Path) -> tuple[dict[str, int], list[str]]:
    values: dict[str, int] = {}
    sentinels: list[str] = []
    path = root / "status.txt"
    if not path.is_file():
        raise ValidationError("status.txt missing")
    for line in path.read_text().splitlines():
        if line.startswith("SWEEP-SUCCEEDED "):
            sentinels.append(line)
            continue
        match = re.fullmatch(r"([^ ]+) rc=([0-9]+) duration=([0-9]+)s", line)
        if not match or match[1] in values:
            raise ValidationError(f"invalid/duplicate status {line!r}")
        values[match[1]] = int(match[2])
    return values, sentinels


def candidate_verdict_path(root: pathlib.Path, memory_limit: str) -> pathlib.Path:
    return root / f"candidate-{memory_limit}.json"


def read_candidate_verdict(root: pathlib.Path, memory_limit: str) -> dict[str, Any]:
    recorded = load_json(candidate_verdict_path(root, memory_limit))
    if not isinstance(recorded, dict):
        raise ValidationError(f"candidate {memory_limit} verdict is not object")
    recomputed = candidate_evaluation(root, memory_limit)
    if recorded != recomputed:
        raise ValidationError(f"candidate {memory_limit} verdict differs from evidence")
    return recorded


def validate_campaign(root: pathlib.Path, full: bool) -> dict[str, Any]:
    matrix, by_name = read_matrix(root)
    provenance = validate_provenance(root)
    final = read_provenance(root, final=True)
    if any(final[key] != provenance[key] for key in REQUIRED_PROVENANCE):
        raise ValidationError("final provenance changed")
    statuses, sentinels = read_status(root)
    if (full and len(sentinels) != 1) or (not full and sentinels):
        raise ValidationError("success sentinel state differs")
    required_ab = set(matrix["executionOrderAB"])
    for name in sorted(required_ab):
        validate_arm(root, by_name[name], provenance)
    completion = load_json(root / "completion.json")
    candidates = list(matrix_contract.MEMORY_LIMIT_CANDIDATES)
    selected = completion.get("selectedMemoryLimit") if isinstance(completion, dict) else None
    if selected not in candidates:
        raise ValidationError("completion selectedMemoryLimit invalid")
    expected = set(required_ab)
    recorded_candidates: list[dict[str, Any]] = []
    for limit in candidates[: candidates.index(selected) + 1]:
        group = next(item for item in matrix["candidateGroupsC"] if item["memoryLimit"] == limit)
        expected.update(group["arms"])
        verdict = read_candidate_verdict(root, limit)
        recorded_candidates.append(verdict)
        failures = verdict["errors"]
        if limit == selected and failures:
            raise ValidationError("selected candidate failed:\n" + "\n".join(failures))
        if limit != selected and not failures:
            raise ValidationError(f"lower candidate {limit} passed but campaign continued")
    found = {path.relative_to(root).parts[0] for path in root.glob("*/*/collector-memory-report.json")}
    if found != expected or set(statuses) != expected:
        raise ValidationError("executed report/status set differs")
    if any(statuses[name] != 0 for name in required_ab):
        raise ValidationError("an A/B discovery arm had nonzero status")
    arm_outcomes = {
        arm_result["armName"]: arm_result
        for verdict in recorded_candidates
        for arm_result in verdict["arms"]
    }
    for name in expected - required_ab:
        result = arm_outcomes.get(name)
        if result is None:
            raise ValidationError(f"missing exact candidate arm outcome for {name}")
        wanted_rc = result["goTestRC"]
        if statuses[name] != wanted_rc:
            raise ValidationError(f"{name}: status rc differs from candidate outcome")
        if result["classification"] == "valid-run" and wanted_rc != 0:
            raise ValidationError(f"{name}: valid-run outcome has nonzero rc")
        if result["classification"] == "resource-failure" and wanted_rc <= 0:
            raise ValidationError(f"{name}: resource-failure outcome lacks nonzero rc")
    if completion.get("schemaVersion") != 1 or completion.get("executedArms") != sorted(expected):
        raise ValidationError("completion executedArms differs")
    if completion.get("candidateVerdicts") != recorded_candidates:
        raise ValidationError("completion candidateVerdicts differ")
    return completion


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=pathlib.Path)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--provenance-only", action="store_true")
    mode.add_argument("--single-arm")
    mode.add_argument("--classify-candidate-arm")
    mode.add_argument("--candidate", choices=matrix_contract.MEMORY_LIMIT_CANDIDATES)
    mode.add_argument("--campaign", action="store_true")
    mode.add_argument("--full", action="store_true")
    parser.add_argument("--go-test-rc", type=int)
    parser.add_argument("--verdict-output", type=pathlib.Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        matrix, arms = read_matrix(args.root)
        provenance = validate_provenance(args.root)
        if args.provenance_only:
            print("PROVENANCE-VALID")
        elif args.single_arm:
            if args.single_arm not in arms:
                raise ValidationError(f"unknown arm {args.single_arm}")
            validate_arm(args.root, arms[args.single_arm], provenance)
            print(f"ARM-VALID {args.single_arm}")
        elif args.classify_candidate_arm:
            if args.classify_candidate_arm not in arms or args.go_test_rc is None or args.verdict_output is None:
                raise ValidationError("hard-failure classification requires known arm, --go-test-rc, and --verdict-output")
            verdict = classify_candidate_hard_failure(
                args.root, arms[args.classify_candidate_arm], provenance, args.go_test_rc
            )
            write_json_exclusive(args.verdict_output, verdict)
            print("CANDIDATE-ARM-RESOURCE-FAIL " + args.classify_candidate_arm + " " + ",".join(verdict["reasons"]))
            return 2
        elif args.candidate:
            verdict = candidate_evaluation(args.root, args.candidate)
            if args.verdict_output is None:
                raise ValidationError("candidate evaluation requires --verdict-output")
            write_json_exclusive(args.verdict_output, verdict)
            if verdict["errors"]:
                print(f"CANDIDATE-FAIL {args.candidate}\n" + "\n".join(verdict["errors"]))
                return 2
            print(f"CANDIDATE-PASS {args.candidate}")
        else:
            validate_campaign(args.root, args.full)
            print("SWEEP-VALID" if args.full else "CAMPAIGN-VALID")
        return 0
    except ValidationError as exc:
        print(f"GATE-FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
