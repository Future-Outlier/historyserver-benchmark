#!/usr/bin/env python3
"""Fail-closed validator for formal History Server-only campaigns.

This module intentionally spells out the Go report contract and reads source
identity from the immutable expected matrix.  A
missing field is not interpreted as zero or success: formal artifacts must make
every correctness, lifecycle, resource, and provenance assertion explicit.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import math
import pathlib
import re
import sys
from collections import defaultdict
from typing import Any, Iterable


MATRIX_SCHEMA_VERSION = 5
MATRIX_KINDS = ("hs-cpu", "hs-memory")
SOURCE_TASK_NAME = "bench_task"
SOURCE_ALLOWED_ATTEMPTS = (1_000, 5_000, 10_000, 50_000)
SOURCE_BASELINE_WAVE_SIZE = 2_000
SOURCE_N50_WAVE_SIZE = 2_000
SOURCE_N50_TARGET_TASK_RATE = 500
SOURCE_BASELINE_PACING_VARIANT = "baseline-wave2000-v1"
SOURCE_N50_PACING_VARIANT = "single-driver-rate500-wave2000-v2"
SOURCE_N50_REJECTED_BASELINE_REPORT_SHA256 = (
    "7873ec98d6e1d376a5994a86aaaa18ee41b6f5e1a124f67249589c152ad94264"
)
SOURCE_N50_REJECTED_ATTEMPT_REPORT_SHA256S = (
    "2c191e0133dce4a95a19fe7835daf0628784276fd421de608ed8fd796d3915ef",
    "01274cdc416f6a6709706389042e5aa8e6ab382677b1886b9bf2b8459d47e677",
)
SOURCE_N50_COLLECTOR_CPU_REQUEST = "100m"
SOURCE_N50_COLLECTOR_CPU_LIMIT = "2"
SOURCE_N50_DRIVER_RATE_MIN = 490.0
SOURCE_N50_DRIVER_RATE_MAX = 510.0
SOURCE_FINGERPRINT_ALGORITHM = "s3-key-size-etag-content-sha256-v1"
SOURCE_BUCKET = "ray-historyserver-benchmark"
SOURCE_STORAGE_DIFF_LABELS = ("during-job (T1-T0)", "flush (T2-T1)")
SOURCE_STORAGE_ISOLATION_LABEL = "full-lifecycle (T2-pre-start)"
TASK_LOG_METADATA_ALGORITHM = "task-log-metadata-sha256-v1"
TASK_LOG_METADATA_COUNT_KEYS = (
    "nil",
    "present",
    "structurallyInvalid",
    "incompleteNonNil",
    "stdoutExactResolvable",
    "stderrExactResolvable",
    "legacyWholeWorkerFallback",
)
MEASUREMENT_CLAIM = {
    "replay": True,
    "taskList": True,
    "logsFile": False,
}

CPU_VALUES = ("500m", "1", "2", "4")
CPU_INTERLEAVED_ORDER = (
    ("500m", "1", "4", "2"),
    ("1", "2", "500m", "4"),
    ("2", "4", "1", "500m"),
)
CPU_REPEATS = 3
CPU_MEMORY = "8Gi"
MEMORY_REPEATS = 5

COLD_SLO_SECONDS = 120
PROCESS_TIMEOUT_SECONDS = 10 * 60
CLIENT_TIMEOUT_SECONDS = 12 * 60
SESSION_SETTLE_SECONDS = 0
WARM_QUERY_CONCURRENCY = 1
WARM_ITERATIONS = 1
WARM_TASK_LIMIT_CAP = 10_000
HS_ARGS = "--session-cache-size=1,--session-process-timeout=10m"
S3_LOCAL_PORT = 19_003

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
IMAGE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
QUALIFIED_IMAGE_ID_RE = re.compile(
    r"(?:(?:docker-pullable|docker|containerd)://)?"
    r"[A-Za-z0-9][A-Za-z0-9._:/-]*@(?P<digest>sha256:[0-9a-f]{64})"
)
RUNTIME_IMAGE_ID_RE = re.compile(
    r"(?:docker|containerd)://(?P<digest>sha256:[0-9a-f]{64})"
)
ARM_STATUS_RE = re.compile(r"^(?P<name>[A-Za-z0-9-]+) rc=0 duration=[1-9][0-9]*s$")
DNS1123_LABEL_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
RAY_SESSION_ID_RE = re.compile(r"^session_[A-Za-z0-9][A-Za-z0-9_.-]*$")

# Contract with the Go harness.  Keep field names here so changes cannot be
# silently accepted by loose .get(..., 0) behavior.
REPORT_TOP_LEVEL_REQUIRED = {
    "config",
    "namespace",
    "executionNamespace",
    "executionNamespaceUID",
    "clusterName",
    "sessionID",
    "historyServer",
    "cgroupSampler",
    "cgroups",
    "completed",
    "hsPodEvidence",
    "hsValidation",
    "sourceSessionFingerprint",
    "rayJobLifecycle",
}
CONFIG_REQUIRED = {
    "TaskCount",
    "HSOnly",
    "HSSourceObjectCount",
    "HSSourceTotalBytes",
    "S3Bucket",
    "S3LocalPort",
    "HSCPURequest",
    "HSCPULimit",
    "HSMemoryRequest",
    "HSMemoryLimit",
    "HSArgs",
    "HSColdSLO",
    "HSEnterTimeout",
    "HSWarmWait",
    "HSSessionSettle",
    "WarmIterations",
    "HSStrictCold",
    "HSQueryConcurrency",
    "KindNode",
    "SkipHistoryServer",
    "ExecutionIdentityFile",
    "HSSourceRayJobOwned",
    "HSSourceShutdownAfterJob",
    "HSSourceJobTTLSeconds",
    "HSSourceTaskLogMetadataAlgorithm",
    "HSSourceTaskLogMetadataSHA256",
    "HSSourceTaskLogMetadataAttempts",
    "HSSourceTaskLogMetadataNil",
    "HSSourceTaskLogMetadataPresent",
    "HSSourceTaskLogMetadataStructurallyInvalid",
    "HSSourceTaskLogMetadataIncompleteNonNil",
    "HSSourceTaskLogMetadataStdoutExactResolvable",
    "HSSourceTaskLogMetadataStderrExactResolvable",
    "HSSourceTaskLogMetadataLegacyWholeWorkerFallback",
}
POD_EVIDENCE_REQUIRED = {
    "executionNamespace",
    "podName",
    "podUID",
    "containerName",
    "containerID",
    "image",
    "imageID",
    "ready",
    "running",
    "restartCount",
    "cpuRequest",
    "cpuLimit",
    "memoryRequest",
    "memoryLimit",
    "oomKilled",
    "terminationReasons",
    "cgroupObserved",
    "cgroupMemoryMax",
    "cgroupMemoryMaxBytes",
    "memoryEventsOOM",
    "memoryEventsOOMKill",
    "cgroupReadErrors",
    "cgroupReadErrorFields",
    "valid",
    "problems",
}
HS_VALIDATION_REQUIRED = {
    "scope",
    "expectedTaskAttempts",
    "taskCountQuery",
    "warmTaskQuery",
    "fullReplay",
    "logs",
    "lifetimeMemoryPeakBytes",
    "measurementValid",
    "meetsColdSLO",
    "valid",
    "problems",
}
TASK_QUERY_REQUIRED = {
    "endpoint",
    "concurrency",
    "limit",
    "httpStatus",
    "latency",
    "responseResult",
    "rows",
    "numFiltered",
    "distinctTaskIDs",
    "attemptZero",
    "finished",
    "valid",
    "problems",
}
WARM_TASK_QUERY_REQUIRED = TASK_QUERY_REQUIRED | {
    "taskLogMetadata",
    "expectedProjectionSHA256",
    "projectionMatches",
}
ERROR_COUNT_KEYS = {
    "decompress",
    "getContent",
    "read",
    "decode",
    "store",
    "taskLifecycleUnmarshal",
    "emptyLifecycle",
    "logEvents",
    "listObjects",
    "getObject",
    "readObject",
    "bucketCreateMissing",
    "bucketCreateAttempt",
    "bucketCreateSuccess",
}
TASK_COUNT_ENDPOINT = (
    "/api/v0/tasks?detail=false&filter_keys=task_name&"
    "filter_predicates=%3D&filter_values=bench_task&limit=0"
)
FINGERPRINT_REQUIRED = {
    "algorithm",
    "bucket",
    "start",
    "end",
    "objectCount",
    "totalBytes",
}
TASK_LOG_METADATA_PROVENANCE_KEYS = (
    "source_task_log_metadata_algorithm",
    "source_task_log_metadata_sha256",
    "source_task_log_metadata_attempts",
    "source_task_log_metadata_nil",
    "source_task_log_metadata_present",
    "source_task_log_metadata_structurally_invalid",
    "source_task_log_metadata_incomplete_non_nil",
    "source_task_log_metadata_stdout_exact_resolvable",
    "source_task_log_metadata_stderr_exact_resolvable",
    "source_task_log_metadata_legacy_whole_worker_fallback",
)

PROVENANCE_INITIAL_REQUIRED = {
    "campaign_kind",
    "repo_head",
    "tracked_diff_sha256",
    "benchmark_source_sha256",
    "historyserver_source_sha256",
    "historyserver_manifest_sha256",
    "expected_matrix_sha256",
    "source_report_sha256",
    "source_provenance_sha256",
    "source_session",
    "source_bucket",
    "source_fingerprint_algorithm",
    "historyserver_image_requested",
    "historyserver_build_source_sha256",
    "historyserver_runtime_id",
    "historyserver_runtime_build_source_sha256",
    "source_collector_runtime_id",
    "source_rayjob_owned",
    "source_shutdown_after_job",
    "source_job_ttl_seconds",
} | set(TASK_LOG_METADATA_PROVENANCE_KEYS)
PROVENANCE_FINAL_EXTRA_REQUIRED = {
    "revalidation_status",
    "source_fingerprint_sha256",
}
ARM_PROVENANCE_REQUIRED = {
    "arm",
    "expected_matrix_sha256",
    "initial_provenance_sha256",
    "historyserver_runtime_id",
    "historyserver_runtime_build_source_sha256",
    "source_collector_runtime_id",
    "source_rayjob_owned",
    "source_shutdown_after_job",
    "source_job_ttl_seconds",
} | set(TASK_LOG_METADATA_PROVENANCE_KEYS)
GENERATED_SOURCE_PROVENANCE_REQUIRED = {
    "source_completion_sha256",
    "source_rayjob_backoff_limit",
    "source_submitter_backoff_limit",
}

COLLECTOR_REPORT_FIELDS = (
    "collectorLogs",
    "storageDiffs",
    "collectorWindows",
    "collectorIngressGates",
    "timeline",
    "podTerminations",
)
FORBIDDEN_ARTIFACT_PATTERNS = (
    "collector_ingress",
    "collector.log",
    "collector-gate",
    "rayjob-rendered",
    "task_lifecycle_10s",
)


class ValidationError(RuntimeError):
    pass


@dataclasses.dataclass(frozen=True)
class ArmResult:
    name: str
    repeat: int
    cpu: str
    memory: str
    cold_latency_nanoseconds: int
    meets_cold_slo: bool
    lifetime_memory_peak_bytes: int
    execution_namespace: str
    pod_uid: str
    container_id: str
    source_fingerprint_sha256: str


@dataclasses.dataclass(frozen=True)
class CampaignResult:
    root: pathlib.Path
    matrix: dict[str, Any]
    matrix_sha256: str
    provenance: dict[str, str]
    final_provenance: dict[str, str] | None
    arms: tuple[ArmResult, ...]
    source_fingerprint_sha256: str


@dataclasses.dataclass(frozen=True)
class CPUSelection:
    cpu: str
    discovery_max_lifetime_memory_peak_bytes: int


def fail(message: str) -> None:
    raise ValidationError(message)


def require(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def require_object(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{context} must be an object")
    return value


def require_fields(value: dict[str, Any], fields: Iterable[str], context: str) -> None:
    missing = sorted(set(fields) - set(value))
    if missing:
        fail(f"{context} fields missing: {missing}")


def json_exact_equal(left: Any, right: Any) -> bool:
    return json.dumps(left, sort_keys=True, separators=(",", ":")) == json.dumps(
        right,
        sort_keys=True,
        separators=(",", ":"),
    )


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            fail(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def read_json(path: pathlib.Path, context: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(), object_pairs_hook=unique_object)
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read {context} {path}: {exc}")
    return require_object(value, context)


def file_sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        fail(f"cannot hash {path}: {exc}")
    return digest.hexdigest()


def read_key_value(path: pathlib.Path, context: str) -> dict[str, str]:
    try:
        lines = path.read_text().splitlines()
    except OSError as exc:
        fail(f"cannot read {context} {path}: {exc}")
    result: dict[str, str] = {}
    for line_number, line in enumerate(lines, start=1):
        if not line or "=" not in line:
            fail(f"{context} line {line_number} is not key=value")
        key, value = line.split("=", 1)
        if not key or not value:
            fail(f"{context} line {line_number} has an empty key or value")
        if key in result:
            fail(f"{context} contains duplicate key {key!r}")
        result[key] = value
    return result


def validate_sha256(value: str, context: str) -> None:
    require(bool(SHA256_RE.fullmatch(value)), f"{context} is not a SHA-256")


def normalize_container_image_id(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    if IMAGE_ID_RE.fullmatch(value):
        return value
    for pattern in (QUALIFIED_IMAGE_ID_RE, RUNTIME_IMAGE_ID_RE):
        match = pattern.fullmatch(value)
        if match:
            return match.group("digest")
    return None


def source_parts(source: dict[str, Any]) -> tuple[str, str, str]:
    values = (source.get("namespace"), source.get("clusterName"), source.get("sessionID"))
    require(
        isinstance(values[0], str)
        and len(values[0]) <= 63
        and bool(DNS1123_LABEL_RE.fullmatch(values[0])),
        "matrix source namespace must be one Kubernetes DNS-1123 label",
    )
    require(
        isinstance(values[1], str)
        and len(values[1]) <= 253
        and all(
            len(label) <= 63 and bool(DNS1123_LABEL_RE.fullmatch(label))
            for label in values[1].split(".")
        ),
        "matrix source cluster must be one Kubernetes DNS-1123 subdomain",
    )
    require(
        isinstance(values[2], str)
        and len(values[2]) <= 255
        and bool(RAY_SESSION_ID_RE.fullmatch(values[2])),
        "matrix source session must be one safe Ray session_ path segment",
    )
    return values


def expected_source_generation_controls(
    attempts: int,
) -> tuple[int, int, str, str | None, tuple[str, ...], str | None, str | None]:
    if attempts == 50_000:
        return (
            SOURCE_N50_WAVE_SIZE,
            SOURCE_N50_TARGET_TASK_RATE,
            SOURCE_N50_PACING_VARIANT,
            SOURCE_N50_REJECTED_BASELINE_REPORT_SHA256,
            SOURCE_N50_REJECTED_ATTEMPT_REPORT_SHA256S,
            SOURCE_N50_COLLECTOR_CPU_REQUEST,
            SOURCE_N50_COLLECTOR_CPU_LIMIT,
        )
    return (
        SOURCE_BASELINE_WAVE_SIZE,
        0,
        SOURCE_BASELINE_PACING_VARIANT,
        None,
        (),
        None,
        None,
    )


def validate_source_generation(source: dict[str, Any], attempts: int) -> None:
    generation = source.get("sourceGeneration")
    if generation is None:
        # Schema v5 matrices produced before the separately labelled N=50k
        # pacing variant remain verifiable for the already completed lower-N
        # campaigns. A 50k source may never use that legacy escape hatch.
        require(
            attempts in (1_000, 5_000, 10_000),
            "matrix N=50k sourceGeneration is required",
        )
        return
    generation = require_object(generation, "matrix source.sourceGeneration")
    legacy_fields = {
        "waveSize",
        "drivers",
        "targetTaskRate",
        "pacingVariant",
        "driverTasks",
        "driverWallSec",
        "driverRateTPS",
        "lineageSHA256",
        "rejectedBaselineReportSHA256",
    }
    expected_fields = legacy_fields | {
        "rejectedAttemptReportSHA256s",
        "collectorCPURequest",
        "collectorCPULimit",
    }
    legacy_lower = attempts in (1_000, 5_000, 10_000) and set(generation) == legacy_fields
    require(
        legacy_lower or set(generation) == expected_fields,
        "matrix source.sourceGeneration fields differ",
    )
    (
        expected_wave,
        expected_rate,
        expected_variant,
        expected_rejected,
        expected_rejected_attempts,
        expected_collector_request,
        expected_collector_limit,
    ) = (
        expected_source_generation_controls(attempts)
    )
    expected_exact = {
        "waveSize": expected_wave,
        "drivers": 1,
        "targetTaskRate": expected_rate,
        "pacingVariant": expected_variant,
        "driverTasks": attempts,
        "rejectedBaselineReportSHA256": expected_rejected,
    }
    if not legacy_lower:
        expected_exact.update(
            {
                "rejectedAttemptReportSHA256s": list(expected_rejected_attempts),
                "collectorCPURequest": expected_collector_request,
                "collectorCPULimit": expected_collector_limit,
            }
        )
    for field, wanted in expected_exact.items():
        if field in {
            "waveSize",
            "drivers",
            "targetTaskRate",
            "driverTasks",
        }:
            require(
                type(generation.get(field)) is int,
                f"matrix source.sourceGeneration.{field} must be an integer",
            )
        require(
            generation.get(field) == wanted,
            f"matrix source.sourceGeneration.{field} differs",
        )
    for field in ("driverWallSec", "driverRateTPS"):
        observed = generation.get(field)
        require(
            not isinstance(observed, bool)
            and isinstance(observed, (int, float))
            and math.isfinite(observed)
            and observed > 0,
            f"matrix source.sourceGeneration.{field} must be finite and positive",
        )
    if attempts == 50_000:
        require(
            SOURCE_N50_DRIVER_RATE_MIN
            <= float(generation["driverRateTPS"])
            <= SOURCE_N50_DRIVER_RATE_MAX,
            "matrix source.sourceGeneration.driverRateTPS is outside the "
            "preregistered pacing band",
        )
    lineage_sha = generation.get("lineageSHA256")
    require(
        isinstance(lineage_sha, str) and bool(SHA256_RE.fullmatch(lineage_sha)),
        "matrix source.sourceGeneration.lineageSHA256 is invalid",
    )


def expected_attempts(source: dict[str, Any]) -> int:
    attempts = source.get("expectedBenchmarkAttempts")
    require(
        type(attempts) is int and attempts in SOURCE_ALLOWED_ATTEMPTS,
        "matrix source expected attempts are unsupported",
    )
    return attempts


def warm_task_limit(source: dict[str, Any]) -> int:
    return min(expected_attempts(source), WARM_TASK_LIMIT_CAP)


def warm_task_endpoint(limit: int) -> str:
    return (
        "/api/v0/tasks?detail=true&filter_keys=task_name&"
        f"filter_predicates=%3D&filter_values=bench_task&limit={limit}"
    )


def full_replay_exact(attempts: int) -> dict[str, int]:
    return {
        "expectedAttempts": attempts,
        "distinctTaskIDs": attempts,
        "observedAttempts": attempts,
        "attemptZero": attempts,
        "finished": attempts,
    }


def validate_task_log_metadata(
    value: Any,
    attempts: int,
    context: str,
) -> dict[str, Any]:
    metadata = require_object(value, context)
    expected_fields = {"algorithm", "sha256", "attempts", "counts", "valid", "problems"}
    require(set(metadata) == expected_fields, f"{context} fields differ")
    require(
        metadata["algorithm"] == TASK_LOG_METADATA_ALGORITHM,
        f"{context}.algorithm differs",
    )
    require(isinstance(metadata["sha256"], str), f"{context}.sha256 missing")
    validate_sha256(metadata["sha256"], f"{context}.sha256")
    require(
        type(metadata["attempts"]) is int and metadata["attempts"] == attempts,
        f"{context}.attempts differs",
    )
    counts = require_object(metadata["counts"], f"{context}.counts")
    require(
        set(counts) == set(TASK_LOG_METADATA_COUNT_KEYS),
        f"{context}.counts fields differ",
    )
    for key in TASK_LOG_METADATA_COUNT_KEYS:
        count = counts[key]
        require(
            type(count) is int and 0 <= count <= attempts,
            f"{context}.counts.{key} is invalid",
        )
    require(
        counts["nil"] + counts["present"] == attempts,
        f"{context} nil+present does not equal attempts",
    )
    for key in (
        "structurallyInvalid",
        "incompleteNonNil",
        "stdoutExactResolvable",
        "stderrExactResolvable",
    ):
        require(
            counts[key] <= counts["present"],
            f"{context}.counts.{key} exceeds present",
        )
    require(
        counts["legacyWholeWorkerFallback"] <= counts["nil"],
        f"{context}.counts.legacyWholeWorkerFallback exceeds nil",
    )
    require(
        counts["structurallyInvalid"] == 0,
        f"{context} contains structurally invalid TaskLogInfo",
    )
    require(metadata["valid"] is True, f"{context}.valid is not true")
    require(metadata["problems"] == [], f"{context}.problems is not empty")
    return metadata


def expected_task_log_metadata_provenance(source: dict[str, Any]) -> dict[str, str]:
    metadata = source["taskLogMetadata"]
    counts = metadata["counts"]
    return {
        "source_task_log_metadata_algorithm": metadata["algorithm"],
        "source_task_log_metadata_sha256": metadata["sha256"],
        "source_task_log_metadata_attempts": str(metadata["attempts"]),
        "source_task_log_metadata_nil": str(counts["nil"]),
        "source_task_log_metadata_present": str(counts["present"]),
        "source_task_log_metadata_structurally_invalid": str(
            counts["structurallyInvalid"]
        ),
        "source_task_log_metadata_incomplete_non_nil": str(
            counts["incompleteNonNil"]
        ),
        "source_task_log_metadata_stdout_exact_resolvable": str(
            counts["stdoutExactResolvable"]
        ),
        "source_task_log_metadata_stderr_exact_resolvable": str(
            counts["stderrExactResolvable"]
        ),
        "source_task_log_metadata_legacy_whole_worker_fallback": str(
            counts["legacyWholeWorkerFallback"]
        ),
    }


def validate_source(source: dict[str, Any], kind: str) -> None:
    namespace, cluster, session = source_parts(source)
    require(
        isinstance(source.get("namespaceUID"), str) and bool(source["namespaceUID"]),
        "matrix source namespaceUID is missing",
    )
    attempts = expected_attempts(source)
    validate_source_generation(source, attempts)
    expected = {
        "spec": f"{namespace}/{cluster}/{session}",
        "bucket": SOURCE_BUCKET,
        "benchmarkTaskName": SOURCE_TASK_NAME,
        "fingerprintAlgorithm": SOURCE_FINGERPRINT_ALGORITHM,
    }
    for field, wanted in expected.items():
        require(source.get(field) == wanted, f"matrix source.{field} != {wanted!r}")
    for field in ("expectedObjectCount", "expectedTotalBytes"):
        value = source.get(field)
        require(
            type(value) is int and value > 0,
            f"matrix source.{field} must be a positive integer",
        )
    require(attempts == source["expectedBenchmarkAttempts"], "matrix source attempts differ")
    validate_task_log_metadata(
        source.get("taskLogMetadata"),
        attempts,
        "matrix source.taskLogMetadata",
    )
    source_report_sha = source.get("sourceReportSHA256")
    require(isinstance(source_report_sha, str), "matrix sourceReportSHA256 missing")
    validate_sha256(source_report_sha, "matrix sourceReportSHA256")
    source_provenance_sha = source.get("sourceProvenanceSHA256")
    require(
        isinstance(source_provenance_sha, str),
        "matrix sourceProvenanceSHA256 missing",
    )
    validate_sha256(source_provenance_sha, "matrix sourceProvenanceSHA256")
    require(
        bool(IMAGE_ID_RE.fullmatch(str(source.get("collectorRuntimeID", "")))),
        "matrix source collectorRuntimeID is invalid",
    )
    require(
        isinstance(source.get("collectorImageRequested"), str)
        and bool(source["collectorImageRequested"]),
        "matrix source collectorImageRequested is missing",
    )
    lifecycle = source.get("rayJobLifecycle")
    if source.get("sourceGeneration") is None:
        wanted_lifecycle = {
            "ownedCluster": True,
            "shutdownAfterJobFinishes": True,
            "ttlSecondsAfterFinished": 30,
        }
    else:
        wanted_lifecycle = {
            "ownedCluster": True,
            "shutdownAfterJobFinishes": True,
            "ttlSecondsAfterFinished": 30,
            "rayJobBackoffLimit": 0,
            "submitterBackoffLimit": 0,
        }
    require(
        json_exact_equal(lifecycle, wanted_lifecycle),
        "matrix source RayJob lifecycle differs from the formal source contract",
    )

    def validate_isolation_diff(diff: Any, wanted_label: str, context: str) -> None:
        require(isinstance(diff, dict), f"matrix source.{context} must be an object")
        require(
            diff.get("label") == wanted_label,
            f"matrix source.{context}.label must be {wanted_label!r}",
        )
        require(
            type(diff.get("deletedObjects")) is int
            and diff["deletedObjects"] == 0,
            f"matrix source.{context}.deletedObjects must be zero",
        )
        for field in ("unexpectedKeys", "unexpectedChangedKeys", "deletedKeys"):
            require(
                diff.get(field) == [],
                f"matrix source.{context}.{field} must be []",
            )

    storage_diffs = source.get("storageDiffs")
    require(
        isinstance(storage_diffs, list)
        and len(storage_diffs) == len(SOURCE_STORAGE_DIFF_LABELS),
        "matrix source.storageDiffs must contain exactly two phase diffs",
    )
    seen_labels: set[str] = set()
    for index, diff in enumerate(storage_diffs):
        require(
            isinstance(diff, dict),
            f"matrix source.storageDiffs[{index}] must be an object",
        )
        label = diff.get("label")
        require(
            label in SOURCE_STORAGE_DIFF_LABELS and label not in seen_labels,
            "matrix source.storageDiffs labels must be exact and unique",
        )
        seen_labels.add(label)
        validate_isolation_diff(diff, label, f"storageDiffs[{index}]")
    require(
        seen_labels == set(SOURCE_STORAGE_DIFF_LABELS),
        "matrix source.storageDiffs labels are incomplete",
    )
    validate_isolation_diff(
        source.get("storageIsolation"),
        SOURCE_STORAGE_ISOLATION_LABEL,
        "storageIsolation",
    )
    if kind == "hs-memory":
        baseline = source.get("baselineFingerprintSHA256")
        require(isinstance(baseline, str), "memory matrix baseline fingerprint missing")
        validate_sha256(baseline, "memory matrix baseline fingerprint")


def expected_policy(source: dict[str, Any]) -> dict[str, Any]:
    return {
        "cacheSize": 1,
        "serverTimeoutSeconds": PROCESS_TIMEOUT_SECONDS,
        "clientTimeoutSeconds": CLIENT_TIMEOUT_SECONDS,
        "coldSLOSeconds": COLD_SLO_SECONDS,
        "strictColdNoRetry": True,
        "freshPodPerArm": True,
        "sessionSettleSeconds": SESSION_SETTLE_SECONDS,
        "warmQueryConcurrency": WARM_QUERY_CONCURRENCY,
        "warmIterations": WARM_ITERATIONS,
        "warmTaskLimit": warm_task_limit(source),
        "warmTaskDetail": True,
        "s3LocalPort": S3_LOCAL_PORT,
        "historyServerLocalPort": 30_080,
        "executionNamespaceIdentityFile": "execution-namespace.json",
        "waitForNamespaceDeletion": True,
    }


def validate_matrix(matrix: dict[str, Any], expected_kind: str | None) -> None:
    require(
        type(matrix.get("schemaVersion")) is int
        and matrix["schemaVersion"] == MATRIX_SCHEMA_VERSION,
        "wrong matrix schemaVersion",
    )
    kind = matrix.get("kind")
    require(kind in MATRIX_KINDS, f"invalid matrix kind {kind!r}")
    if expected_kind is not None:
        require(kind == expected_kind, f"expected matrix kind {expected_kind}, got {kind}")
    source = require_object(matrix.get("source"), "matrix source")
    validate_source(source, kind)
    require(
        json_exact_equal(matrix.get("claim"), MEASUREMENT_CLAIM),
        "matrix claim differs",
    )
    require(
        json_exact_equal(matrix.get("policy"), expected_policy(source)),
        "matrix policy is not the formal HS policy",
    )

    arms = matrix.get("arms")
    require(isinstance(arms, list) and arms, "matrix arms must be a non-empty list")
    expected_configs: list[tuple[str, int, int, str, str]] = []
    if kind == "hs-cpu":
        selection = require_object(matrix.get("selectionPolicy"), "selectionPolicy")
        require(selection.get("cpuOrder") == list(CPU_VALUES), "CPU selection order differs")
        require(selection.get("requiredValidRepeats") == CPU_REPEATS, "CPU repeats differ")
        for repeat, cpus in enumerate(CPU_INTERLEAVED_ORDER, start=1):
            for sequence, cpu in enumerate(cpus, start=1):
                expected_configs.append((f"cpu-{cpu}-r{repeat}", repeat, sequence, cpu, CPU_MEMORY))
    else:
        evidence = require_object(matrix.get("selectionEvidence"), "selectionEvidence")
        confirmation = require_object(matrix.get("confirmationPolicy"), "confirmationPolicy")
        require(confirmation.get("requiredValidRepeats") == MEMORY_REPEATS, "memory repeats differ")
        require(confirmation.get("requestEqualsLimit") is True, "memory request must equal limit")
        require(confirmation.get("stopOnAnyInvalidArm") is True, "memory campaign must stop on invalid arm")
        for key in (
            "cpuCampaignExpectedMatrixSHA256",
            "cpuCampaignFinalProvenanceSHA256",
        ):
            require(isinstance(evidence.get(key), str), f"selectionEvidence.{key} missing")
            validate_sha256(evidence[key], f"selectionEvidence.{key}")
        selected_cpu = evidence.get("selectedCPU")
        require(selected_cpu in CPU_VALUES, "selected CPU is invalid")
        peak = evidence.get("discoveryMaxLifetimeMemoryPeakBytes")
        candidate = evidence.get("candidateMemoryBytes")
        quantity = evidence.get("candidateMemoryQuantity")
        require(isinstance(peak, int) and peak > 0, "discovery peak must be positive")
        require(evidence.get("headroomNumerator") == 5, "memory headroom numerator differs")
        require(evidence.get("headroomDenominator") == 4, "memory headroom denominator differs")
        require(evidence.get("roundBytes") == 32 * 1024 * 1024, "memory rounding differs")
        round_bytes = 32 * 1024 * 1024
        expected_candidate = ((peak * 5 + 4 * round_bytes - 1) // (4 * round_bytes)) * round_bytes
        require(candidate == expected_candidate, "candidate memory formula mismatch")
        require(quantity == f"{candidate // (1024 * 1024)}Mi", "candidate memory quantity mismatch")
        for repeat in range(1, MEMORY_REPEATS + 1):
            expected_configs.append(
                (f"memory-{quantity}-r{repeat}", repeat, 1, selected_cpu, quantity)
            )

    require(len(arms) == len(expected_configs), "matrix arm count differs from formal design")
    kind_node: str | None = None
    seen: set[str] = set()
    for index, (arm, expected) in enumerate(zip(arms, expected_configs, strict=True)):
        arm = require_object(arm, f"matrix arm {index}")
        name, repeat, sequence, cpu, memory = expected
        require(arm.get("name") == name, f"matrix arm {index} name/order differs")
        require(name not in seen, f"duplicate matrix arm {name}")
        seen.add(name)
        require(arm.get("repeat") == repeat, f"matrix arm {name} repeat differs")
        require(arm.get("sequenceInRepeat") == sequence, f"matrix arm {name} sequence differs")
        config = require_object(arm.get("config"), f"matrix arm {name} config")
        require_fields(
            config,
            CONFIG_REQUIRED - {"ExecutionIdentityFile"},
            f"matrix arm {name} config",
        )
        expected_config = {
            "TaskCount": source["expectedBenchmarkAttempts"],
            "HSOnly": source["spec"],
            "HSSourceObjectCount": source["expectedObjectCount"],
            "HSSourceTotalBytes": source["expectedTotalBytes"],
            "S3Bucket": SOURCE_BUCKET,
            "S3LocalPort": S3_LOCAL_PORT,
            "HSCPURequest": cpu,
            "HSCPULimit": cpu,
            "HSMemoryRequest": memory,
            "HSMemoryLimit": memory,
            "HSArgs": HS_ARGS,
            "HSColdSLO": COLD_SLO_SECONDS * 1_000_000_000,
            "HSEnterTimeout": CLIENT_TIMEOUT_SECONDS * 1_000_000_000,
            "HSWarmWait": 0,
            "HSSessionSettle": SESSION_SETTLE_SECONDS * 1_000_000_000,
            "WarmIterations": WARM_ITERATIONS,
            "HSStrictCold": True,
            "HSQueryConcurrency": WARM_QUERY_CONCURRENCY,
            "KindNode": config.get("KindNode"),
            "SkipHistoryServer": False,
            "HSSourceRayJobOwned": True,
            "HSSourceShutdownAfterJob": True,
            "HSSourceJobTTLSeconds": 30,
            "HSSourceTaskLogMetadataAlgorithm": source["taskLogMetadata"]["algorithm"],
            "HSSourceTaskLogMetadataSHA256": source["taskLogMetadata"]["sha256"],
            "HSSourceTaskLogMetadataAttempts": source["taskLogMetadata"]["attempts"],
            "HSSourceTaskLogMetadataNil": source["taskLogMetadata"]["counts"]["nil"],
            "HSSourceTaskLogMetadataPresent": source["taskLogMetadata"]["counts"]["present"],
            "HSSourceTaskLogMetadataStructurallyInvalid": source[
                "taskLogMetadata"
            ]["counts"]["structurallyInvalid"],
            "HSSourceTaskLogMetadataIncompleteNonNil": source["taskLogMetadata"][
                "counts"
            ]["incompleteNonNil"],
            "HSSourceTaskLogMetadataStdoutExactResolvable": source[
                "taskLogMetadata"
            ]["counts"]["stdoutExactResolvable"],
            "HSSourceTaskLogMetadataStderrExactResolvable": source[
                "taskLogMetadata"
            ]["counts"]["stderrExactResolvable"],
            "HSSourceTaskLogMetadataLegacyWholeWorkerFallback": source[
                "taskLogMetadata"
            ]["counts"]["legacyWholeWorkerFallback"],
        }
        if source.get("sourceGeneration") is not None:
            expected_config.update(
                {
                    "HSSourceRayJobBackoffLimit": 0,
                    "HSSourceSubmitterBackoffLimit": 0,
                }
            )
        require(
            json_exact_equal(config, expected_config),
            f"matrix arm {name} config differs from formal controls",
        )
        require(isinstance(config["KindNode"], str) and config["KindNode"], "KindNode is empty")
        if kind_node is None:
            kind_node = config["KindNode"]
        require(config["KindNode"] == kind_node, "KindNode changes within matrix")


def read_provenance(root: pathlib.Path, matrix: dict[str, Any]) -> dict[str, str]:
    values = read_key_value(root / "provenance.txt", "initial provenance")
    required = set(PROVENANCE_INITIAL_REQUIRED)
    if matrix["source"].get("sourceGeneration") is not None:
        required |= GENERATED_SOURCE_PROVENANCE_REQUIRED
    require_fields(values, required, "initial provenance")
    require(values["campaign_kind"] == matrix["kind"], "provenance campaign kind differs")
    require(values["source_session"] == matrix["source"]["spec"], "provenance source session differs")
    require(values["source_bucket"] == matrix["source"]["bucket"], "provenance source bucket differs")
    require(
        values["source_fingerprint_algorithm"] == SOURCE_FINGERPRINT_ALGORITHM,
        "provenance fingerprint algorithm differs",
    )
    require(values["source_report_sha256"] == matrix["source"]["sourceReportSHA256"], "source report hash differs")
    require(
        values["source_provenance_sha256"]
        == matrix["source"]["sourceProvenanceSHA256"],
        "source provenance hash differs",
    )
    require(
        values["source_collector_runtime_id"] == matrix["source"]["collectorRuntimeID"],
        "source Collector runtime image ID differs",
    )
    require(values["source_rayjob_owned"] == "true", "source RayJob ownership provenance differs")
    require(values["source_shutdown_after_job"] == "true", "source RayJob shutdown provenance differs")
    require(values["source_job_ttl_seconds"] == "30", "source RayJob TTL provenance differs")
    if matrix["source"].get("sourceGeneration") is not None:
        require(
            values["source_rayjob_backoff_limit"] == "0",
            "source RayJob retry provenance differs",
        )
        require(
            values["source_submitter_backoff_limit"] == "0",
            "source submitter retry provenance differs",
        )
        validate_sha256(
            values["source_completion_sha256"],
            "provenance source_completion_sha256",
        )
    for key, wanted in expected_task_log_metadata_provenance(matrix["source"]).items():
        require(values[key] == wanted, f"provenance {key} differs")
    require(values["expected_matrix_sha256"] == file_sha256(root / "expected-matrix.json"), "matrix hash differs")
    for key in (
        "tracked_diff_sha256",
        "benchmark_source_sha256",
        "historyserver_source_sha256",
        "historyserver_manifest_sha256",
        "expected_matrix_sha256",
        "source_report_sha256",
        "source_provenance_sha256",
        "historyserver_build_source_sha256",
        "historyserver_runtime_build_source_sha256",
    ):
        validate_sha256(values[key], f"provenance {key}")
    require(bool(re.fullmatch(r"[0-9a-f]{40,64}", values["repo_head"])), "repo_head invalid")
    require(bool(IMAGE_ID_RE.fullmatch(values["historyserver_runtime_id"])), "runtime image ID invalid")
    require(
        values["historyserver_build_source_sha256"]
        == values["historyserver_runtime_build_source_sha256"],
        "History Server image is not bound to current source",
    )
    if matrix["kind"] == "hs-memory":
        evidence = matrix["selectionEvidence"]
        for key, matrix_key in (
            ("cpu_campaign_expected_matrix_sha256", "cpuCampaignExpectedMatrixSHA256"),
            ("cpu_campaign_final_provenance_sha256", "cpuCampaignFinalProvenanceSHA256"),
        ):
            require(values.get(key) == evidence[matrix_key], f"provenance {key} differs")
    return values


def read_final_provenance(
    root: pathlib.Path,
    initial: dict[str, str],
    required: bool,
) -> dict[str, str] | None:
    path = root / "provenance-final.txt"
    if not path.exists() and not required:
        return None
    values = read_key_value(path, "final provenance")
    require_fields(
        values,
        set(initial) | PROVENANCE_FINAL_EXTRA_REQUIRED,
        "final provenance",
    )
    require(values["revalidation_status"] == "valid", "provenance was not revalidated")
    validate_sha256(values["source_fingerprint_sha256"], "final source fingerprint")
    for key, expected in initial.items():
        require(values.get(key) == expected, f"final provenance changed {key}")
    return values


def parse_memory_bytes(quantity: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)(Mi|Gi)", quantity)
    if not match:
        fail(f"unsupported memory quantity {quantity!r}")
    multiplier = 1024 * 1024 if match.group(2) == "Mi" else 1024 * 1024 * 1024
    return int(match.group(1)) * multiplier


def validate_pod_evidence(
    evidence: dict[str, Any],
    config: dict[str, Any],
    arm_name: str,
    report_execution_namespace: str,
    historyserver_image_requested: str,
    historyserver_runtime_id: str,
    source_namespace: str,
) -> tuple[str, str, str]:
    require_fields(evidence, POD_EVIDENCE_REQUIRED, f"{arm_name} hsPodEvidence")
    namespace = evidence["executionNamespace"]
    require(isinstance(namespace, str) and namespace, f"{arm_name} execution namespace empty")
    require(namespace != source_namespace, f"{arm_name} reused source namespace")
    require(namespace == report_execution_namespace, f"{arm_name} execution namespace fields differ")
    require(isinstance(evidence["podName"], str) and evidence["podName"], f"{arm_name} pod name empty")
    require(evidence["containerName"] == "historyserver", f"{arm_name} wrong container name")
    pod_uid = evidence["podUID"]
    container_id = evidence["containerID"]
    require(isinstance(pod_uid, str) and pod_uid, f"{arm_name} pod UID empty")
    require(isinstance(container_id, str) and container_id, f"{arm_name} container ID empty")
    image_id = evidence["imageID"]
    require(isinstance(image_id, str) and image_id, f"{arm_name} image ID empty")
    normalized_image_id = normalize_container_image_id(image_id)
    require(normalized_image_id is not None, f"{arm_name} image ID has an invalid CRI form")
    require(
        normalized_image_id == historyserver_runtime_id,
        f"{arm_name} image ID differs from provenance historyserver_runtime_id",
    )
    require(evidence["image"] == historyserver_image_requested, f"{arm_name} pod image differs from provenance")
    require(evidence["ready"] is True, f"{arm_name} container not ready")
    require(evidence["running"] is True, f"{arm_name} container not running")
    require(evidence["restartCount"] == 0, f"{arm_name} container restarted")
    require(evidence["oomKilled"] is False, f"{arm_name} container was OOMKilled")
    require(evidence["terminationReasons"] in (None, []), f"{arm_name} has termination reasons")
    require(evidence["valid"] is True, f"{arm_name} pod evidence is invalid")
    require(evidence["problems"] == [], f"{arm_name} pod evidence has problems")

    resource_map = {
        "cpuRequest": "HSCPURequest",
        "cpuLimit": "HSCPULimit",
        "memoryRequest": "HSMemoryRequest",
        "memoryLimit": "HSMemoryLimit",
    }
    for observed, wanted in resource_map.items():
        require(evidence[observed] == config[wanted], f"{arm_name} observed {observed} differs")

    require(evidence["cgroupObserved"] is True, f"{arm_name} cgroup memory evidence missing")
    require(
        evidence["cgroupMemoryMaxBytes"] == parse_memory_bytes(config["HSMemoryLimit"]),
        f"{arm_name} cgroup memory.max differs",
    )
    require(
        evidence["cgroupMemoryMax"] == str(evidence["cgroupMemoryMaxBytes"]),
        f"{arm_name} cgroup memory.max string differs",
    )
    require(evidence["memoryEventsOOM"] == 0, f"{arm_name} memory.events oom is nonzero")
    require(evidence["memoryEventsOOMKill"] == 0, f"{arm_name} memory.events oom_kill is nonzero")
    require(evidence["cgroupReadErrors"] == 0, f"{arm_name} cgroup read errors nonzero")
    require(evidence["cgroupReadErrorFields"] in (None, []), f"{arm_name} cgroup read error fields nonempty")
    return namespace, pod_uid, container_id


def validate_cgroup(report: dict[str, Any], arm_name: str, peak_bytes: int) -> None:
    status = require_object(report["cgroupSampler"], f"{arm_name} cgroup sampler")
    expected_true = ("startAttempted", "started", "stopRequested", "streamEnded", "streamComplete")
    for field in expected_true:
        require(status.get(field) is True, f"{arm_name} cgroupSampler.{field} is not true")
    require(status.get("streamEndedBeforeStop") is False, f"{arm_name} cgroup stream ended early")
    require(not status.get("startError"), f"{arm_name} cgroup start error")
    require(not status.get("streamError"), f"{arm_name} cgroup stream error")

    rows = report["cgroups"]
    require(isinstance(rows, list) and rows, f"{arm_name} has no cgroup rows")
    lifetime = [
        row
        for row in rows
        if isinstance(row, dict)
        and row.get("phase") == "lifetime"
        and "historyserver" in str(row.get("container", ""))
    ]
    require(len(lifetime) == 1, f"{arm_name} must have one History Server lifetime cgroup row")
    row = lifetime[0]
    sampled = [
        candidate
        for candidate in rows
        if isinstance(candidate, dict)
        and candidate.get("container") == row.get("container")
        and candidate.get("phase") == "historyserver"
        and isinstance(candidate.get("samples"), int)
        and candidate["samples"] > 0
    ]
    require(len(sampled) == 1, f"{arm_name} has no sampled History Server cgroup phase")
    peak_mib = row.get("lifetimePeakMiB")
    require(isinstance(peak_mib, (int, float)) and peak_mib > 0, f"{arm_name} lifetime peak missing")
    require(
        row.get("lifetimePeakBytes") == peak_bytes,
        f"{arm_name} exact lifetimePeakBytes disagrees with hsValidation",
    )
    observed_from_float = float(peak_mib) * 1024 * 1024
    require(
        abs(observed_from_float - peak_bytes) <= 1024 * 1024,
        f"{arm_name} exact lifetime peak disagrees with cgroup row",
    )


def validate_hs_validation(
    validation: dict[str, Any],
    report: dict[str, Any],
    arm_name: str,
    matrix: dict[str, Any],
) -> tuple[int, int, bool]:
    require_fields(validation, HS_VALIDATION_REQUIRED, f"{arm_name} hsValidation")
    require(validation["measurementValid"] is True, f"{arm_name} measurement is invalid")
    require(validation["valid"] is True, f"{arm_name} hsValidation.valid is not true")
    require(validation["problems"] == [], f"{arm_name} hsValidation has problems")
    require(
        validation["scope"] == matrix["claim"],
        f"{arm_name} hsValidation.scope differs from matrix claim",
    )
    source = matrix["source"]
    attempts = expected_attempts(source)
    warm_limit = warm_task_limit(source)
    require(
        validation["expectedTaskAttempts"] == attempts,
        f"{arm_name} expected task attempts differs",
    )
    meets_slo = validation["meetsColdSLO"]
    require(isinstance(meets_slo, bool), f"{arm_name} meetsColdSLO is not boolean")
    peak = validation["lifetimeMemoryPeakBytes"]
    require(isinstance(peak, int) and peak > 0, f"{arm_name} lifetime peak invalid")

    history = require_object(report["historyServer"], f"{arm_name} historyServer")
    require_fields(
        history,
        {"enterMeasured", "enterStatus", "enterColdLatency", "enterAttempts", "notes"},
        f"{arm_name} historyServer",
    )
    latency = history["enterColdLatency"]
    require(history.get("enterAttempts") == 1, f"{arm_name} cold request attempt count differs")
    require(history.get("warmEndpoints") in (None, []), f"{arm_name} ran legacy warm endpoints")
    list_clusters = history.get("listClusters")
    require(
        list_clusters in (None, {})
        or (
            isinstance(list_clusters, dict)
            and not list_clusters.get("endpoint")
            and not list_clusters.get("p50")
            and not list_clusters.get("p95")
            and not list_clusters.get("max")
            and not list_clusters.get("lastBytes")
            and not list_clusters.get("errors")
        ),
        f"{arm_name} ran the legacy /clusters prewarm",
    )
    require(isinstance(latency, int) and latency > 0, f"{arm_name} cold latency invalid")
    cold_succeeded = history.get("enterMeasured") is True and history.get("enterStatus") == 200
    expected_meets_slo = cold_succeeded and latency <= COLD_SLO_SECONDS * 1_000_000_000
    require(meets_slo == expected_meets_slo, f"{arm_name} meetsColdSLO disagrees with cold result")
    require(cold_succeeded, f"{arm_name} cold request did not complete as a measured HTTP 200")
    require(history.get("notes") in (None, []), f"{arm_name} successful cold request has notes")
    if matrix["kind"] == "hs-memory":
        require(meets_slo, f"{arm_name} memory confirmation did not meet cold SLO")

    count_query = require_object(validation["taskCountQuery"], f"{arm_name} task count query")
    require_fields(count_query, TASK_QUERY_REQUIRED, f"{arm_name} task count query")
    warm = require_object(validation["warmTaskQuery"], f"{arm_name} warm task query")
    require_fields(warm, WARM_TASK_QUERY_REQUIRED, f"{arm_name} warm task query")
    expected_count = {
        "endpoint": TASK_COUNT_ENDPOINT,
        "concurrency": 1,
        "limit": 0,
        "httpStatus": 200,
        "responseResult": True,
        "rows": 0,
        "numFiltered": attempts,
        "distinctTaskIDs": 0,
        "attemptZero": 0,
        "finished": 0,
        "valid": True,
        "problems": [],
    }
    for field, wanted in expected_count.items():
        require(count_query[field] == wanted, f"{arm_name} taskCountQuery.{field} differs")
    require(
        isinstance(count_query["latency"], int) and count_query["latency"] > 0,
        f"{arm_name} task count query latency invalid",
    )

    expected_warm = {
        "endpoint": warm_task_endpoint(warm_limit),
        "concurrency": WARM_QUERY_CONCURRENCY,
        "limit": warm_limit,
        "httpStatus": 200,
        "responseResult": True,
        "rows": warm_limit,
        "numFiltered": attempts,
        "distinctTaskIDs": warm_limit,
        "attemptZero": warm_limit,
        "finished": warm_limit,
        "valid": True,
        "problems": [],
    }
    for field, wanted in expected_warm.items():
        require(warm[field] == wanted, f"{arm_name} warmTaskQuery.{field} differs")
    require(isinstance(warm["latency"], int) and warm["latency"] > 0, f"{arm_name} warm latency invalid")
    warm_metadata = validate_task_log_metadata(
        warm["taskLogMetadata"],
        warm_limit,
        f"{arm_name} warmTaskQuery.taskLogMetadata",
    )
    require(
        isinstance(warm["expectedProjectionSHA256"], str),
        f"{arm_name} warm expected projection hash missing",
    )
    validate_sha256(
        warm["expectedProjectionSHA256"],
        f"{arm_name} warm expected projection hash",
    )
    require(
        warm["expectedProjectionSHA256"] == warm_metadata["sha256"],
        f"{arm_name} warm expected projection hash differs",
    )
    require(
        warm["projectionMatches"] is True,
        f"{arm_name} warm metadata projection does not match",
    )

    replay = require_object(validation["fullReplay"], f"{arm_name} full replay")
    require(replay.get("status") == "processed", f"{arm_name} replay status differs")
    require(replay.get("valid") is True, f"{arm_name} full replay invalid")
    require(replay.get("problems") == [], f"{arm_name} full replay has problems")
    for field, wanted in full_replay_exact(attempts).items():
        require(field in replay, f"{arm_name} full replay missing {field}")
        require(replay[field] == wanted, f"{arm_name} fullReplay.{field} differs")
    replay_metadata = validate_task_log_metadata(
        replay.get("taskLogMetadata"),
        attempts,
        f"{arm_name} fullReplay.taskLogMetadata",
    )
    require(
        replay_metadata == source["taskLogMetadata"],
        f"{arm_name} full replay TaskLog metadata differs from source",
    )
    replay_errors = require_object(replay.get("errorCounts"), f"{arm_name} replay errors")
    require(set(replay_errors) == ERROR_COUNT_KEYS, f"{arm_name} replay error keys differ")
    require(all(value == 0 for value in replay_errors.values()), f"{arm_name} replay errors nonzero")
    require(replay.get("totalErrors") == 0, f"{arm_name} replay total errors nonzero")

    logs = require_object(validation["logs"], f"{arm_name} log validation")
    require(logs.get("valid") is True, f"{arm_name} log validation invalid")
    require(logs.get("problems") == [], f"{arm_name} log validation has problems")
    log_errors = require_object(logs.get("errorCounts"), f"{arm_name} log errors")
    require(set(log_errors) == ERROR_COUNT_KEYS, f"{arm_name} log error keys differ")
    require(all(value == 0 for value in log_errors.values()), f"{arm_name} log errors nonzero")
    require(logs.get("totalErrors") == 0, f"{arm_name} log total errors nonzero")
    return latency, peak, meets_slo


def validate_fingerprint(
    value: dict[str, Any], matrix: dict[str, Any], arm_name: str
) -> str:
    require_fields(value, FINGERPRINT_REQUIRED, f"{arm_name} source fingerprint")
    require(value["algorithm"] == SOURCE_FINGERPRINT_ALGORITHM, f"{arm_name} fingerprint algorithm differs")
    require(value["bucket"] == matrix["source"]["bucket"], f"{arm_name} fingerprint bucket differs")
    start = value["start"]
    end = value["end"]
    require(isinstance(start, str), f"{arm_name} fingerprint start missing")
    validate_sha256(start, f"{arm_name} fingerprint start")
    require(end == start, f"{arm_name} source session changed during arm")
    require(
        value["objectCount"] == matrix["source"]["expectedObjectCount"],
        f"{arm_name} source object count differs",
    )
    require(
        value["totalBytes"] == matrix["source"]["expectedTotalBytes"],
        f"{arm_name} source byte count differs",
    )
    expected = matrix["source"].get("baselineFingerprintSHA256")
    if expected is not None:
        require(start == expected, f"{arm_name} fingerprint differs from memory matrix baseline")
    return start


def find_report(arm_dir: pathlib.Path, arm_name: str) -> pathlib.Path:
    reports = list(arm_dir.glob("*/bench-report.json"))
    require(len(reports) == 1, f"{arm_name} must contain exactly one bench-report.json")
    return reports[0]


def validate_arm(
    root: pathlib.Path,
    matrix: dict[str, Any],
    matrix_sha: str,
    initial_provenance: dict[str, str],
    arm: dict[str, Any],
) -> ArmResult:
    name = arm["name"]
    arm_dir = root / name
    require(arm_dir.is_dir(), f"missing arm directory {name}")
    sentinel = arm_dir / "arm-sentinel.txt"
    try:
        sentinel_text = sentinel.read_text()
    except OSError as exc:
        fail(f"cannot read {name} arm sentinel: {exc}")
    require(sentinel_text == f"ARM-SUCCEEDED name={name} rc=0\n", f"{name} arm sentinel differs")

    arm_provenance_path = arm_dir / "arm-provenance.txt"
    arm_provenance = read_key_value(arm_provenance_path, f"{name} arm provenance")
    arm_required = set(ARM_PROVENANCE_REQUIRED)
    if matrix["source"].get("sourceGeneration") is not None:
        arm_required |= GENERATED_SOURCE_PROVENANCE_REQUIRED
    require_fields(arm_provenance, arm_required, f"{name} arm provenance")
    require(arm_provenance["arm"] == name, f"{name} arm provenance name differs")
    require(arm_provenance["expected_matrix_sha256"] == matrix_sha, f"{name} matrix binding differs")
    require(
        arm_provenance["initial_provenance_sha256"] == file_sha256(root / "provenance.txt"),
        f"{name} initial provenance binding differs",
    )
    for key in (
        "historyserver_runtime_id",
        "historyserver_runtime_build_source_sha256",
        "source_collector_runtime_id",
        "source_rayjob_owned",
        "source_shutdown_after_job",
        "source_job_ttl_seconds",
        *(GENERATED_SOURCE_PROVENANCE_REQUIRED if matrix["source"].get("sourceGeneration") is not None else ()),
        *TASK_LOG_METADATA_PROVENANCE_KEYS,
    ):
        require(arm_provenance[key] == initial_provenance[key], f"{name} {key} differs")
    for key, wanted in expected_task_log_metadata_provenance(matrix["source"]).items():
        require(arm_provenance[key] == wanted, f"{name} {key} differs from matrix")

    report_path = find_report(arm_dir, name)
    report = read_json(report_path, f"{name} report")
    require_fields(report, REPORT_TOP_LEVEL_REQUIRED, f"{name} report")
    require(report["completed"] is True, f"{name} report is not completed")
    source = matrix["source"]
    require(report["namespace"] == source["namespace"], f"{name} source namespace differs")
    require(
        isinstance(report["executionNamespace"], str) and report["executionNamespace"],
        f"{name} execution namespace missing",
    )
    require(
        isinstance(report["executionNamespaceUID"], str)
        and report["executionNamespaceUID"],
        f"{name} execution namespace UID missing",
    )
    require(report["clusterName"] == source["clusterName"], f"{name} source cluster differs")
    require(report["sessionID"] == source["sessionID"], f"{name} source session differs")
    for field in COLLECTOR_REPORT_FIELDS:
        require(not report.get(field), f"{name} contains Collector artifact field {field}")
    config = require_object(report["config"], f"{name} config")
    require_fields(config, CONFIG_REQUIRED, f"{name} config")
    expected_config = arm["config"]
    for field, wanted in expected_config.items():
        require(config.get(field) == wanted, f"{name} config.{field} differs from matrix")
    identity_path = arm_dir / "execution-namespace.json"
    identity = read_json(identity_path, f"{name} execution namespace identity")
    require(
        set(identity) == {"name", "uid"},
        f"{name} execution namespace identity fields differ",
    )
    require(identity["name"] == report["executionNamespace"], f"{name} namespace identity name differs")
    require(identity["uid"] == report["executionNamespaceUID"], f"{name} namespace identity UID differs")
    require(
        pathlib.Path(config["ExecutionIdentityFile"]).resolve() == identity_path.resolve(),
        f"{name} config.ExecutionIdentityFile is not the arm identity artifact",
    )
    cleanup_sentinel = arm_dir / "namespace-cleanup-sentinel.txt"
    try:
        cleanup_text = cleanup_sentinel.read_text()
    except OSError as exc:
        fail(f"cannot read {name} namespace cleanup sentinel: {exc}")
    require(
        cleanup_text
        == f"NAMESPACE-DELETED name={identity['name']} uid={identity['uid']} pods=0\n",
        f"{name} namespace cleanup sentinel differs",
    )
    port_cleanup_sentinel = arm_dir / "local-port-cleanup-sentinel.txt"
    try:
        port_cleanup_text = port_cleanup_sentinel.read_text()
    except OSError as exc:
        fail(f"cannot read {name} local port cleanup sentinel: {exc}")
    require(
        port_cleanup_text
        == "LOCAL-PORTS-FREE ports=19003,30080 consecutive=2\n",
        f"{name} local port cleanup sentinel differs",
    )
    require(
        json_exact_equal(report["rayJobLifecycle"], source["rayJobLifecycle"]),
        f"{name} source RayJob lifecycle evidence differs",
    )

    validation = require_object(report["hsValidation"], f"{name} hsValidation")
    latency, peak, meets_slo = validate_hs_validation(validation, report, name, matrix)
    namespace, pod_uid, container_id = validate_pod_evidence(
        require_object(report["hsPodEvidence"], f"{name} hsPodEvidence"),
        config,
        name,
        report["executionNamespace"],
        initial_provenance["historyserver_image_requested"],
        initial_provenance["historyserver_runtime_id"],
        source["namespace"],
    )
    validate_cgroup(report, name, peak)
    fingerprint = validate_fingerprint(
        require_object(report["sourceSessionFingerprint"], f"{name} source fingerprint"),
        matrix,
        name,
    )

    for path in arm_dir.rglob("*"):
        lowered = path.name.lower()
        for pattern in FORBIDDEN_ARTIFACT_PATTERNS:
            require(pattern not in lowered, f"{name} contains forbidden Collector artifact {path.name}")

    return ArmResult(
        name=name,
        repeat=arm["repeat"],
        cpu=config["HSCPURequest"],
        memory=config["HSMemoryRequest"],
        cold_latency_nanoseconds=latency,
        meets_cold_slo=meets_slo,
        lifetime_memory_peak_bytes=peak,
        execution_namespace=namespace,
        pod_uid=pod_uid,
        container_id=container_id,
        source_fingerprint_sha256=fingerprint,
    )


def validate_status(root: pathlib.Path, arm_names: list[str]) -> None:
    try:
        lines = (root / "status.txt").read_text().splitlines()
    except OSError as exc:
        fail(f"cannot read status.txt: {exc}")
    require(len(lines) == len(arm_names) + 1, "status line count differs")
    observed: list[str] = []
    for line in lines[:-1]:
        match = ARM_STATUS_RE.fullmatch(line)
        require(match is not None, f"invalid status row {line!r}")
        observed.append(match.group("name"))
    require(observed == arm_names, "status arm order differs from expected matrix")
    require(lines[-1] == f"SWEEP-SUCCEEDED arms={len(arm_names)}", "sweep sentinel differs")


def validate_parent_cpu_campaign(matrix: dict[str, Any], cpu_root: pathlib.Path) -> None:
    parent = validate_campaign(cpu_root, expected_kind="hs-cpu", require_final_provenance=True)
    selection = select_cpu(parent)
    require(selection is not None, "parent CPU campaign has no eligible CPU")
    evidence = matrix["selectionEvidence"]
    require(
        evidence["cpuCampaignExpectedMatrixSHA256"] == parent.matrix_sha256,
        "memory matrix parent expected-matrix hash differs",
    )
    require(
        evidence["cpuCampaignFinalProvenanceSHA256"]
        == file_sha256(cpu_root / "provenance-final.txt"),
        "memory matrix parent final provenance hash differs",
    )
    require(evidence["selectedCPU"] == selection.cpu, "memory matrix selected CPU differs")
    require(
        evidence["discoveryMaxLifetimeMemoryPeakBytes"]
        == selection.discovery_max_lifetime_memory_peak_bytes,
        "memory matrix discovery peak differs",
    )
    require(
        matrix["source"]["baselineFingerprintSHA256"]
        == parent.source_fingerprint_sha256,
        "memory matrix source fingerprint differs from CPU campaign",
    )


def validate_campaign(
    root: pathlib.Path,
    *,
    expected_kind: str | None = None,
    require_final_provenance: bool = True,
    parent_cpu_root: pathlib.Path | None = None,
) -> CampaignResult:
    root = root.resolve()
    matrix_path = root / "expected-matrix.json"
    matrix = read_json(matrix_path, "expected matrix")
    validate_matrix(matrix, expected_kind)
    matrix_sha = file_sha256(matrix_path)
    provenance = read_provenance(root, matrix)
    final = read_final_provenance(root, provenance, require_final_provenance)
    arms_config = matrix["arms"]
    arm_names = [arm["name"] for arm in arms_config]
    validate_status(root, arm_names)

    arms = tuple(
        validate_arm(root, matrix, matrix_sha, provenance, arm)
        for arm in arms_config
    )
    namespaces = [arm.execution_namespace for arm in arms]
    pod_uids = [arm.pod_uid for arm in arms]
    container_ids = [arm.container_id for arm in arms]
    require(len(set(namespaces)) == len(namespaces), "execution namespace was reused across arms")
    require(len(set(pod_uids)) == len(pod_uids), "History Server pod UID was reused across arms")
    require(len(set(container_ids)) == len(container_ids), "History Server container ID was reused across arms")
    fingerprints = {arm.source_fingerprint_sha256 for arm in arms}
    require(len(fingerprints) == 1, "source fingerprint changes across arms")
    fingerprint = next(iter(fingerprints))
    if final is not None:
        require(final["source_fingerprint_sha256"] == fingerprint, "final provenance fingerprint differs")
    if matrix["kind"] == "hs-memory":
        require(parent_cpu_root is not None, "memory validation requires --cpu-campaign-root")
        validate_parent_cpu_campaign(matrix, parent_cpu_root)
    return CampaignResult(
        root=root,
        matrix=matrix,
        matrix_sha256=matrix_sha,
        provenance=provenance,
        final_provenance=final,
        arms=arms,
        source_fingerprint_sha256=fingerprint,
    )


def validate_single_arm(root: pathlib.Path, arm_name: str) -> ArmResult:
    """Validate one completed arm before a sequential campaign continues."""
    root = root.resolve()
    matrix_path = root / "expected-matrix.json"
    matrix = read_json(matrix_path, "expected matrix")
    validate_matrix(matrix, None)
    matrix_sha = file_sha256(matrix_path)
    provenance = read_provenance(root, matrix)
    matches = [arm for arm in matrix["arms"] if arm.get("name") == arm_name]
    require(len(matches) == 1, f"matrix does not contain exactly one arm {arm_name!r}")
    return validate_arm(root, matrix, matrix_sha, provenance, matches[0])


def cpu_millicores(cpu: str) -> int:
    if cpu.endswith("m"):
        return int(cpu[:-1])
    return int(cpu) * 1000


def select_cpu(result: CampaignResult) -> CPUSelection | None:
    require(result.matrix["kind"] == "hs-cpu", "CPU selection requires hs-cpu campaign")
    by_cpu: dict[str, list[ArmResult]] = defaultdict(list)
    for arm in result.arms:
        by_cpu[arm.cpu].append(arm)
    for cpu in sorted(CPU_VALUES, key=cpu_millicores):
        arms = by_cpu[cpu]
        if len(arms) != CPU_REPEATS:
            continue
        if any(not arm.meets_cold_slo for arm in arms):
            continue
        return CPUSelection(
            cpu=cpu,
            discovery_max_lifetime_memory_peak_bytes=max(
                arm.lifetime_memory_peak_bytes for arm in arms
            ),
        )
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=pathlib.Path)
    parser.add_argument("--expected-kind", choices=MATRIX_KINDS)
    parser.add_argument("--artifacts-only", action="store_true")
    parser.add_argument("--cpu-campaign-root", type=pathlib.Path)
    parser.add_argument("--single-arm")
    output = parser.add_mutually_exclusive_group()
    output.add_argument("--print-source-fingerprint", action="store_true")
    output.add_argument("--print-selection-json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.single_arm:
            require(not args.artifacts_only, "--single-arm and --artifacts-only conflict")
            require(not args.print_selection_json, "--single-arm cannot select a CPU")
            arm = validate_single_arm(args.root, args.single_arm)
            if args.print_source_fingerprint:
                print(arm.source_fingerprint_sha256)
            else:
                print("HS-ARM-VALID")
            return 0
        result = validate_campaign(
            args.root,
            expected_kind=args.expected_kind,
            require_final_provenance=not args.artifacts_only,
            parent_cpu_root=args.cpu_campaign_root,
        )
        if args.print_source_fingerprint:
            print(result.source_fingerprint_sha256)
        elif args.print_selection_json:
            selection = select_cpu(result)
            require(selection is not None, "CPU campaign has no eligible CPU")
            print(
                json.dumps(
                    {
                        "cpu": selection.cpu,
                        "discoveryMaxLifetimeMemoryPeakBytes": (
                            selection.discovery_max_lifetime_memory_peak_bytes
                        ),
                    },
                    sort_keys=True,
                )
            )
        else:
            print("HS-SWEEP-VALID")
        return 0
    except ValidationError as exc:
        print(f"HS-SWEEP-INVALID: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
