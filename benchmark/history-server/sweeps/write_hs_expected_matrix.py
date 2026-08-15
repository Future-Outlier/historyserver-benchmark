#!/usr/bin/env python3
"""Write immutable expected matrices for formal History Server-only campaigns.

The CPU matrix fixes the byte-identical source session and every experimental
control before the first arm starts.  The memory matrix is intentionally a
separate document: it can only be derived from a fully validated CPU campaign,
and therefore binds the selected CPU, measured lifetime peak, source fingerprint,
and parent campaign hashes before any memory-confirmation arm starts.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import importlib.util
import json
import math
import os
import pathlib
import re
import stat
import sys
from typing import Any


LEGACY_SOURCE_MATRIX_SCHEMA_VERSION = 5
MATRIX_SCHEMA_VERSION = 6
SOURCE_COMPLETION_SCHEMA_VERSION = 1
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
SOURCE_EVENT_RATIO_ENVELOPES = {
    "TASK_LIFECYCLE_EVENT": (2.0, 2.5),
    "TASK_PROFILE_EVENT": (0.8, 1.1),
    "totalEvents": (3.8, 4.6),
    "rawJSONLBytes": (3_200.0, 3_900.0),
}
SOURCE_FINGERPRINT_ALGORITHM = "s3-key-size-etag-content-sha256-v1"
SOURCE_RAY_IMAGE = "rayproject/ray:2.56.0"
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
MEMORY_EXECUTION_CPU_OVERRIDE = "1"
MEMORY_SELECTION_MODE_DEFAULT = "automatic-policy"
MEMORY_SELECTION_MODE_OVERRIDE = "explicit-validation-override"
ISOLATED_REPEATS = 5
ISOLATED_CPU_REQUEST = "1"
ISOLATED_CPU_LIMIT = "2"
ISOLATED_MEMORY_REQUEST = "1Gi"
ISOLATED_MEMORY_LIMIT = "12Gi"
ISOLATED_PROTOCOL = "isolated-request-v1"
ISOLATED_PRE_COLD_IDLE_SECONDS = 10
ISOLATED_QUIET_GAP_SECONDS = 8
ISOLATED_HS_ENV = "GOMAXPROCS=2,GODEBUG=gctrace=1"

COLD_SLO_SECONDS = 120
PROCESS_TIMEOUT_SECONDS = 10 * 60
CLIENT_TIMEOUT_SECONDS = 12 * 60
SESSION_SETTLE_SECONDS = 0
CACHE_SIZE = 1
CACHE_MAX_BYTES = 2 * 1024 * 1024 * 1024
CACHE_TTL_SECONDS = 0
WARM_QUERY_CONCURRENCY = 1
WARM_ITERATIONS = 1
WARM_TASK_LIMIT = 10_000
MEMORY_HEADROOM_NUMERATOR = 5
MEMORY_HEADROOM_DENOMINATOR = 4
MEMORY_ROUND_BYTES = 32 * 1024 * 1024

LEGACY_HS_ARGS = "--session-cache-size=1,--session-process-timeout=10m"
HS_ARGS = (
    "--session-cache-size=1,"
    "--session-cache-max-bytes=2147483648,"
    "--session-cache-ttl=0s,"
    "--session-process-timeout=10m"
)
HS_PHASE_SEQUENCE = (
    "startup",
    "cold-load",
    "endpoint-test",
    "after-endpoint-test",
    "arm-end",
)
ISOLATED_HS_PHASE_SEQUENCE = (
    "startup-baseline", "cold-request", "post-cold-quiet", "count-request",
    "post-count-quiet", "detail-request", "post-detail-quiet", "arm-end",
)
S3_LOCAL_PORT = 19_003
CANONICAL_BUNDLE_ROOT = pathlib.Path("/private/tmp")
RENAME_EXCL = 0x00000004

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
DNS1123_LABEL_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
RAY_SESSION_ID_RE = re.compile(r"^session_[A-Za-z0-9][A-Za-z0-9_.-]*$")
IMAGE_ID_RE = re.compile(r"sha256:[0-9a-f]{64}")
QUALIFIED_IMAGE_ID_RE = re.compile(
    r"(?:(?:docker-pullable|docker|containerd)://)?"
    r"[A-Za-z0-9][A-Za-z0-9._:/-]*@(?P<digest>sha256:[0-9a-f]{64})"
)
RUNTIME_IMAGE_ID_RE = re.compile(
    r"(?:docker|containerd)://(?P<digest>sha256:[0-9a-f]{64})"
)
SOURCE_BUNDLE_FILENAMES = {
    "sourceReport": "source-report.json",
    "initialProvenance": "provenance.txt",
    "plannedConfig": "planned-config.json",
    "lineage": "lineage.json",
    "sourceContract": "source-contract.json",
    "finalProvenance": "provenance-final.txt",
    "expectedCPUMatrix": "expected-hs-cpu-matrix.json",
    "status": "status.txt",
}
SOURCE_COMPLETION_FILENAME = "source-completion.json"
FINAL_SOURCE_PROVENANCE_REQUIRED = {
    "revalidation_status",
    "source_report_sha256",
    "source_initial_provenance_sha256",
    "source_contract_sha256",
    "source_session",
    "source_task_count",
    "source_wave_size",
    "source_drivers",
    "source_target_task_rate",
    "source_driver_tasks",
    "source_driver_wall_sec",
    "source_driver_rate_tps",
    "source_pacing_variant",
    "source_rejected_baseline_report_sha256",
    "source_lineage_sha256",
    "planned_config_sha256",
    "source_rayjob_backoff_limit",
    "source_submitter_backoff_limit",
}
FINAL_N50_SOURCE_PROVENANCE_REQUIRED = {
    "source_rejected_attempt_report_sha256s",
    "source_collector_cpu_request",
    "source_collector_cpu_limit",
}


def read_provenance(path: pathlib.Path, *, require_revalidated: bool) -> dict[str, str]:
    if not path.is_file():
        raise SystemExit(f"source provenance does not exist: {path}")
    values: dict[str, str] = {}
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw or "=" not in raw:
            raise SystemExit(f"invalid source provenance line {line_number}")
        key, value = raw.split("=", 1)
        if not key or not value or key in values:
            raise SystemExit(f"invalid or duplicate source provenance key {key!r}")
        values[key] = value
    for key in ("collector_image_requested", "collector_runtime_id"):
        if not values.get(key):
            raise SystemExit(f"source provenance is missing {key}")
    if not IMAGE_ID_RE.fullmatch(values["collector_runtime_id"]):
        raise SystemExit("source provenance collector_runtime_id is invalid")
    if require_revalidated and values.get("revalidation_status") != "valid":
        raise SystemExit("source provenance was not finally revalidated")
    if require_revalidated:
        missing = sorted(FINAL_SOURCE_PROVENANCE_REQUIRED - set(values))
        if missing:
            raise SystemExit(f"source final provenance keys are missing: {missing}")
        if values.get("source_task_count") == "50000":
            missing = sorted(FINAL_N50_SOURCE_PROVENANCE_REQUIRED - set(values))
            if missing:
                raise SystemExit(f"N=50k source final provenance keys are missing: {missing}")
    return values


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


def normalize_local_image_reference(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    prefix = "docker.io/library/"
    return value[len(prefix) :] if value.startswith(prefix) else value


def validate_source_identity(namespace: Any, cluster: Any, session: Any) -> None:
    if (
        not isinstance(namespace, str)
        or len(namespace) > 63
        or not DNS1123_LABEL_RE.fullmatch(namespace)
    ):
        raise SystemExit("source namespace must be one Kubernetes DNS-1123 label")
    if (
        not isinstance(cluster, str)
        or len(cluster) > 253
        or any(
            len(label) > 63 or not DNS1123_LABEL_RE.fullmatch(label)
            for label in cluster.split(".")
        )
    ):
        raise SystemExit("source cluster must be one Kubernetes DNS-1123 subdomain")
    if (
        not isinstance(session, str)
        or len(session) > 255
        or not RAY_SESSION_ID_RE.fullmatch(session)
    ):
        raise SystemExit("source session must be one safe Ray session_ path segment")


def file_sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_task_log_metadata(
    value: Any,
    attempts: int,
    context: str,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SystemExit(f"{context} must be an object")
    expected_fields = {"algorithm", "sha256", "attempts", "counts", "valid", "problems"}
    if set(value) != expected_fields:
        raise SystemExit(f"{context} fields differ: observed={sorted(value)}")
    if value.get("algorithm") != TASK_LOG_METADATA_ALGORITHM:
        raise SystemExit(f"{context}.algorithm differs")
    digest = value.get("sha256")
    if not isinstance(digest, str) or not SHA256_RE.fullmatch(digest):
        raise SystemExit(f"{context}.sha256 is invalid")
    if type(value.get("attempts")) is not int or value["attempts"] != attempts:
        raise SystemExit(
            f"{context}.attempts differs: expected={attempts} observed={value.get('attempts')!r}"
        )
    counts = value.get("counts")
    if not isinstance(counts, dict) or set(counts) != set(TASK_LOG_METADATA_COUNT_KEYS):
        raise SystemExit(f"{context}.counts fields differ")
    for key in TASK_LOG_METADATA_COUNT_KEYS:
        count = counts[key]
        if type(count) is not int or count < 0 or count > attempts:
            raise SystemExit(f"{context}.counts.{key} is invalid: {count!r}")
    if counts["nil"] + counts["present"] != attempts:
        raise SystemExit(f"{context} nil+present does not equal attempts")
    for key in (
        "structurallyInvalid",
        "incompleteNonNil",
        "stdoutExactResolvable",
        "stderrExactResolvable",
    ):
        if counts[key] > counts["present"]:
            raise SystemExit(f"{context}.counts.{key} exceeds present")
    if counts["legacyWholeWorkerFallback"] > counts["nil"]:
        raise SystemExit(
            f"{context}.counts.legacyWholeWorkerFallback exceeds nil"
        )
    if counts["structurallyInvalid"] != 0:
        raise SystemExit(f"{context} contains structurally invalid TaskLogInfo")
    if value.get("valid") is not True or value.get("problems") != []:
        raise SystemExit(f"{context} is not a valid canonical summary")
    return {
        "algorithm": TASK_LOG_METADATA_ALGORITHM,
        "sha256": digest,
        "attempts": attempts,
        "counts": {key: counts[key] for key in TASK_LOG_METADATA_COUNT_KEYS},
        "valid": True,
        "problems": [],
    }


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
    if attempts in SOURCE_ALLOWED_ATTEMPTS:
        return (
            SOURCE_BASELINE_WAVE_SIZE,
            0,
            SOURCE_BASELINE_PACING_VARIANT,
            None,
            (),
            None,
            None,
        )
    raise SystemExit(f"unsupported source task count: {attempts!r}")


def build_source_generation(
    report: dict[str, Any],
    provenance: dict[str, str],
    attempts: int,
) -> dict[str, Any]:
    config = report.get("config")
    job = report.get("job")
    if not isinstance(config, dict):
        raise SystemExit("source report config is missing")
    if not isinstance(job, dict):
        raise SystemExit("source report job timing evidence is missing")

    (
        expected_wave, expected_rate, expected_variant, expected_rejected,
        expected_rejected_attempts, expected_collector_request, expected_collector_limit,
    ) = (
        expected_source_generation_controls(attempts)
    )
    exact_report_controls = {
        "config.WaveSize": (config.get("WaveSize"), expected_wave),
        "config.Drivers": (config.get("Drivers"), 1),
        "config.TargetTaskRate": (config.get("TargetTaskRate"), expected_rate),
        "config.CollectorCPURequest": (config.get("CollectorCPURequest"), expected_collector_request or ""),
        "config.CollectorCPU": (config.get("CollectorCPU"), expected_collector_limit or ""),
        "job.driverTasks": (job.get("driverTasks"), attempts),
    }
    for label, (observed, wanted) in exact_report_controls.items():
        if type(observed) is not type(wanted) or observed != wanted:
            raise SystemExit(
                f"source report {label} differs: "
                f"expected={wanted!r} observed={observed!r}"
            )

    for field in ("driverWallSec", "driverRateTPS"):
        observed = job.get(field)
        if (
            isinstance(observed, bool)
            or not isinstance(observed, (int, float))
            or not math.isfinite(observed)
            or observed <= 0
        ):
            raise SystemExit(f"source report job.{field} must be finite and positive")
    if attempts == 50_000 and not SOURCE_N50_DRIVER_RATE_MIN <= float(job["driverRateTPS"]) <= SOURCE_N50_DRIVER_RATE_MAX:
        raise SystemExit("source report job.driverRateTPS is outside the preregistered pacing band")

    extension_keys = {"source_rejected_attempt_report_sha256s", "source_collector_cpu_request", "source_collector_cpu_limit"}
    present_extension_keys = extension_keys & set(provenance)
    legacy_lower = attempts != 50_000 and not present_extension_keys
    if present_extension_keys and present_extension_keys != extension_keys:
        raise SystemExit("source provenance generation extension is incomplete")
    expected_provenance = {
        "source_task_count": str(attempts),
        "source_wave_size": str(expected_wave),
        "source_drivers": "1",
        "source_target_task_rate": str(expected_rate),
        "source_rayjob_backoff_limit": "0",
        "source_submitter_backoff_limit": "0",
        "source_pacing_variant": expected_variant,
        "source_rejected_baseline_report_sha256": (
            expected_rejected if expected_rejected is not None else "none"
        ),
        "source_rejected_attempt_report_sha256s": ",".join(expected_rejected_attempts) if expected_rejected_attempts else "none",
        "source_collector_cpu_request": expected_collector_request or "none",
        "source_collector_cpu_limit": expected_collector_limit or "none",
    }
    if legacy_lower:
        for key in extension_keys:
            expected_provenance.pop(key)
    for key, wanted in expected_provenance.items():
        observed = provenance.get(key)
        if observed != wanted:
            raise SystemExit(
                f"source provenance {key} differs: "
                f"expected={wanted!r} observed={observed!r}"
            )
    lineage_sha = provenance.get("source_lineage_sha256")
    if not isinstance(lineage_sha, str) or not SHA256_RE.fullmatch(lineage_sha):
        raise SystemExit("source provenance source_lineage_sha256 is invalid")

    generation = {
        "waveSize": expected_wave,
        "drivers": 1,
        "targetTaskRate": expected_rate,
        "pacingVariant": expected_variant,
        "driverTasks": attempts,
        "driverWallSec": job["driverWallSec"],
        "driverRateTPS": job["driverRateTPS"],
        "lineageSHA256": lineage_sha,
        "rejectedBaselineReportSHA256": expected_rejected,
    }
    if not legacy_lower:
        generation.update({
            "rejectedAttemptReportSHA256s": list(expected_rejected_attempts),
            "collectorCPURequest": expected_collector_request,
            "collectorCPULimit": expected_collector_limit,
        })
    return generation


def common_source(
    source_report: pathlib.Path,
    provenance: dict[str, str],
) -> dict[str, Any]:
    if not source_report.is_file():
        raise SystemExit(f"source report does not exist: {source_report}")
    try:
        report = json.loads(source_report.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot read source report {source_report}: {exc}") from exc

    namespace = report.get("namespace")
    namespace_uid = report.get("namespaceUID")
    cluster = report.get("clusterName")
    session = report.get("sessionID")
    validate_source_identity(namespace, cluster, session)
    if not isinstance(namespace_uid, str) or not namespace_uid:
        raise SystemExit("source report namespaceUID is missing")

    config = report.get("config")
    if not isinstance(config, dict):
        raise SystemExit("source report config is missing")
    storage = report.get("storage")
    if not isinstance(storage, dict):
        raise SystemExit("source report storage is missing")
    events = storage.get("events")
    if not isinstance(events, dict):
        raise SystemExit("source report storage.events is missing")
    validity = events.get("benchTaskValidity")
    if not isinstance(validity, dict):
        raise SystemExit("source report benchTaskValidity is missing")

    task_count = config.get("TaskCount")
    if type(task_count) is not int or task_count not in SOURCE_ALLOWED_ATTEMPTS:
        raise SystemExit(
            f"source report config.TaskCount must be one of {SOURCE_ALLOWED_ATTEMPTS}, observed={task_count!r}"
        )
    object_count = storage.get("objectCount")
    total_bytes = storage.get("totalBytes")
    if type(object_count) is not int or object_count <= 0:
        raise SystemExit(f"source report storage.objectCount must be positive, observed={object_count!r}")
    if type(total_bytes) is not int or total_bytes <= 0:
        raise SystemExit(f"source report storage.totalBytes must be positive, observed={total_bytes!r}")
    task_log_metadata = validate_task_log_metadata(
        events.get("taskLogMetadata"),
        task_count,
        "source report storage.events.taskLogMetadata",
    )

    exact_source_gates = {
        "completed": (report.get("completed"), True),
        "config.RayImage": (config.get("RayImage"), SOURCE_RAY_IMAGE),
        "config.Compression": (config.get("Compression"), True),
        "config.TaskNumCPUs": (config.get("TaskNumCPUs"), "0.5"),
        "config.DrainSleepSec": (config.get("DrainSleepSec"), 0),
        "config.ShutdownAfterJob": (config.get("ShutdownAfterJob"), True),
        "config.JobTTLSeconds": (config.get("JobTTLSeconds"), 30),
        "config.S3Bucket": (config.get("S3Bucket"), SOURCE_BUCKET),
        "config.SkipCleanup": (config.get("SkipCleanup"), True),
        "storage.markerPresent": (storage.get("markerPresent"), True),
        "storage.events.expectedTasks": (events.get("expectedTasks"), task_count),
        "storage.events.benchTaskIDs": (events.get("benchTaskIDs"), task_count),
        "benchTaskValidity.expectedTaskIDs": (validity.get("expectedTaskIDs"), task_count),
        "benchTaskValidity.observedTaskIDs": (validity.get("observedTaskIDs"), task_count),
        "benchTaskValidity.valid": (validity.get("valid"), True),
        "benchTaskValidity.observedAttempts": (
            validity.get("observedAttempts"),
            task_count,
        ),
        "benchTaskValidity.attemptZero": (
            validity.get("attemptZero"),
            task_count,
        ),
        "benchTaskValidity.finishedAttempts": (
            validity.get("finishedAttempts"),
            task_count,
        ),
        "benchTaskValidity.submittedToWorkerAttempts": (validity.get("submittedToWorkerAttempts"), task_count),
        "benchTaskValidity.finishedTransitionAttempts": (validity.get("finishedTransitionAttempts"), task_count),
        "benchTaskValidity.malformedDefinitions": (validity.get("malformedDefinitions"), 0),
        "benchTaskValidity.missingDefinitionAttemptFields": (validity.get("missingDefinitionAttemptFields"), 0),
        "benchTaskValidity.missingLifecycleAttemptFields": (validity.get("missingLifecycleAttemptFields"), 0),
        "benchTaskValidity.invalidLifecycleTransitions": (validity.get("invalidLifecycleTransitions"), 0),
        "benchTaskValidity.outOfRangeLifecycleTransitions": (validity.get("outOfRangeLifecycleTransitions"), 0),
        "benchTaskValidity.ambiguousLifecycleTransitions": (validity.get("ambiguousLifecycleTransitions"), 0),
        "benchTaskValidity.missingLifecycleAttempts": (validity.get("missingLifecycleAttempts"), 0),
        "benchTaskValidity.nonFinishedAttempts": (validity.get("nonFinishedAttempts"), 0),
        "benchTaskValidity.problems": (validity.get("problems"), []),
    }
    for label, (actual, wanted) in exact_source_gates.items():
        if not json_exact_equal(actual, wanted):
            raise SystemExit(
                f"source report {label} differs: expected={wanted!r} observed={actual!r}"
            )

    count_by_type = events.get("countByType")
    if not isinstance(count_by_type, dict):
        raise SystemExit("source report storage.events.countByType is missing")
    if count_by_type.get("TASK_DEFINITION_EVENT") != task_count + 5:
        raise SystemExit("source report TASK_DEFINITION_EVENT count differs")
    comparability_values = {
        "TASK_LIFECYCLE_EVENT": count_by_type.get("TASK_LIFECYCLE_EVENT"),
        "TASK_PROFILE_EVENT": count_by_type.get("TASK_PROFILE_EVENT"),
        "totalEvents": events.get("totalEvents"),
        "rawJSONLBytes": events.get("rawJSONLBytes"),
    }
    for field, observed in comparability_values.items():
        if type(observed) is not int or observed <= 0:
            raise SystemExit(f"source report {field} must be a positive integer")
        ratio = observed / task_count
        low, high = SOURCE_EVENT_RATIO_ENVELOPES[field]
        if ratio < low or ratio > high:
            raise SystemExit(f"source report {field} per-task mix differs")

    source_generation = build_source_generation(report, provenance, task_count)
    return {
        "namespace": namespace,
        "namespaceUID": namespace_uid,
        "clusterName": cluster,
        "sessionID": session,
        "spec": f"{namespace}/{cluster}/{session}",
        "bucket": SOURCE_BUCKET,
        "benchmarkTaskName": SOURCE_TASK_NAME,
        "expectedBenchmarkAttempts": task_count,
        "expectedObjectCount": object_count,
        "expectedTotalBytes": total_bytes,
        "fingerprintAlgorithm": SOURCE_FINGERPRINT_ALGORITHM,
        "sourceReportSHA256": file_sha256(source_report),
        "taskLogMetadata": task_log_metadata,
        "sourceGeneration": source_generation,
    }


def read_json_object(path: pathlib.Path, context: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot read {context} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SystemExit(f"{context} must be an object")
    return value


def source_contract_runtime_values(path: pathlib.Path) -> tuple[str, ...]:
    """Return the exact shell-facing source contract fields in fixed order."""

    document = read_json_object(path, "source contract")
    expected_top_fields = {
        "schemaVersion",
        "kind",
        "claim",
        "source",
        "collectorValidityRequired",
    }
    if set(document) != expected_top_fields:
        raise SystemExit("source contract top-level fields differ")
    if (
        type(document.get("schemaVersion")) is not int
        or document["schemaVersion"]
        not in (LEGACY_SOURCE_MATRIX_SCHEMA_VERSION, MATRIX_SCHEMA_VERSION)
    ):
        raise SystemExit("source contract schemaVersion differs")
    if document.get("kind") != "hs-source":
        raise SystemExit("source contract kind differs")
    if not json_exact_equal(document.get("claim"), MEASUREMENT_CLAIM):
        raise SystemExit("source contract claim differs")
    if document.get("collectorValidityRequired") is not True:
        raise SystemExit("source contract collector validity gate differs")

    source = document.get("source")
    if not isinstance(source, dict):
        raise SystemExit("source contract source must be an object")
    expected_source_fields = {
        "namespace",
        "namespaceUID",
        "clusterName",
        "sessionID",
        "spec",
        "bucket",
        "benchmarkTaskName",
        "expectedBenchmarkAttempts",
        "expectedObjectCount",
        "expectedTotalBytes",
        "fingerprintAlgorithm",
        "sourceReportSHA256",
        "taskLogMetadata",
        "sourceGeneration",
        "rayJobLifecycle",
        "collectorImageRequested",
        "collectorRuntimeID",
        "sourceProvenanceSHA256",
        "storageDiffs",
        "storageIsolation",
    }
    if set(source) != expected_source_fields:
        raise SystemExit("source contract source fields differ")
    validator = load_validator()
    try:
        validator.validate_source(source, "hs-source")
    except validator.ValidationError as exc:
        raise SystemExit(f"source contract is invalid: {exc}") from exc

    generation = source["sourceGeneration"]
    lifecycle = source["rayJobLifecycle"]
    rejected = generation["rejectedBaselineReportSHA256"]
    values = (
        document["kind"],
        source["spec"],
        str(source["expectedBenchmarkAttempts"]),
        source["bucket"],
        str(generation["waveSize"]),
        str(generation["drivers"]),
        str(generation["targetTaskRate"]),
        generation["pacingVariant"],
        str(generation["driverTasks"]),
        str(generation["driverWallSec"]),
        str(generation["driverRateTPS"]),
        generation["lineageSHA256"],
        rejected if rejected is not None else "none",
        str(lifecycle["rayJobBackoffLimit"]),
        str(lifecycle["submitterBackoffLimit"]),
    )
    for value in values:
        if not isinstance(value, str) or any(mark in value for mark in "\t\r\n"):
            raise SystemExit("source contract runtime value is not one safe field")
    return values


def json_exact_equal(left: Any, right: Any) -> bool:
    return json.dumps(left, sort_keys=True, separators=(",", ":")) == json.dumps(
        right,
        sort_keys=True,
        separators=(",", ":"),
    )


def _directory_identity(value: os.stat_result) -> tuple[int, int]:
    return value.st_dev, value.st_ino


def _directory_open_flags() -> int:
    if not hasattr(os, "O_DIRECTORY") or not hasattr(os, "O_NOFOLLOW"):
        raise SystemExit(
            "source bundle path validation requires O_DIRECTORY and O_NOFOLLOW"
        )
    return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def _lstat_directory_chain(
    path: pathlib.Path,
    context: str,
) -> tuple[tuple[int, int], ...]:
    """Snapshot the canonical bundle root and every lexical descendant."""

    if path.anchor != os.sep or ".." in path.parts:
        raise SystemExit(f"{context} must be one canonical absolute path: {path}")
    try:
        relative = path.relative_to(CANONICAL_BUNDLE_ROOT)
    except ValueError as exc:
        raise SystemExit(
            f"{context} must be below canonical bundle root "
            f"{CANONICAL_BUNDLE_ROOT}: {path}"
        ) from exc

    current = pathlib.Path(os.sep)
    components = [current]
    for component in CANONICAL_BUNDLE_ROOT.parts[1:] + relative.parts:
        current /= component
        components.append(current)

    identities: list[tuple[int, int]] = []
    for component in components:
        try:
            observed = os.lstat(component)
        except OSError as exc:
            raise SystemExit(
                f"{context} and every ancestor must be existing "
                f"symlink-free directories: {path}: {exc}"
            ) from exc
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise SystemExit(
                f"{context} and every ancestor must be existing "
                f"symlink-free directories: {path}"
            )
        identities.append(_directory_identity(observed))
    return tuple(identities)


def _open_directory_without_symlinks(
    path: pathlib.Path,
    context: str,
) -> int:
    """Open a canonical bundle directory through pinned, symlink-free fds.

    Opening each component relative to ``/`` is ideal on a normal host, but the
    macOS Codex sandbox rejects ``openat(root_fd, "private")`` even when the
    final directory below ``/private/tmp`` is writable.  ``/private/tmp`` is the
    one formal-bundle root, so open it directly and then walk only descendants
    with ``openat``.  Pre/post lexical identities reject replacement races while
    the returned descriptor keeps the final directory pinned.
    """

    flags = _directory_open_flags()
    before = _lstat_directory_chain(path, context)
    relative = path.relative_to(CANONICAL_BUNDLE_ROOT)
    descriptor: int | None = None
    try:
        descriptor = os.open(os.fspath(CANONICAL_BUNDLE_ROOT), flags)
        anchor_index = len(CANONICAL_BUNDLE_ROOT.parts) - 1
        if _directory_identity(os.fstat(descriptor)) != before[anchor_index]:
            raise SystemExit(
                f"{context} directory identity changed while opening: {path}"
            )
        for component in relative.parts:
            child_descriptor = os.open(component, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child_descriptor
        assert descriptor is not None
        opened_identity = _directory_identity(os.fstat(descriptor))
        after = _lstat_directory_chain(path, context)
        if before != after or opened_identity != after[-1]:
            raise SystemExit(
                f"{context} directory identity changed while opening: {path}"
            )
        result = descriptor
        descriptor = None
        return result
    except OSError as exc:
        raise SystemExit(
            f"{context} and every ancestor must be existing "
            f"symlink-free directories: {path}: {exc}"
        ) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _rename_directory_exclusive(
    source_name: str,
    destination_name: str,
    parent_descriptor: int,
    destination: pathlib.Path,
) -> None:
    """Atomically publish one directory without replacing any destination.

    The formal runner executes on macOS.  Darwin's ``renameatx_np`` with
    ``RENAME_EXCL`` is the required no-clobber primitive; an unavailable symbol
    or unsupported platform is a hard failure rather than an unsafe rename
    fallback.
    """

    if sys.platform != "darwin":
        raise SystemExit(
            "atomic source publication requires macOS renameatx_np(RENAME_EXCL)"
        )
    try:
        renameatx_np = ctypes.CDLL(None, use_errno=True).renameatx_np
    except (AttributeError, OSError) as exc:
        raise SystemExit(
            "atomic source publication requires available renameatx_np(RENAME_EXCL)"
        ) from exc
    renameatx_np.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    renameatx_np.restype = ctypes.c_int
    ctypes.set_errno(0)
    result = renameatx_np(
        parent_descriptor,
        os.fsencode(source_name),
        parent_descriptor,
        os.fsencode(destination_name),
        RENAME_EXCL,
    )
    if result == 0:
        return
    error = ctypes.get_errno()
    if error in (errno.EEXIST, errno.ENOTEMPTY):
        raise SystemExit(f"refusing to replace accepted source bundle: {destination}")
    raise SystemExit(
        "atomic source publication failed: "
        f"{source_name} -> {destination_name}: {os.strerror(error)}"
    )


def require_canonical_absolute_path(
    path: pathlib.Path,
    context: str,
    *,
    allow_missing_leaf: bool = False,
) -> tuple[tuple[int, int], tuple[int, int] | None]:
    """Return stable parent/leaf identities after a symlink-free openat walk."""

    if not path.is_absolute() or ".." in path.parts:
        raise SystemExit(f"{context} must be one canonical absolute path: {path}")
    if path == pathlib.Path("/"):
        descriptor = _open_directory_without_symlinks(path, context)
        try:
            identity = _directory_identity(os.fstat(descriptor))
            return identity, identity
        finally:
            os.close(descriptor)

    parent_descriptor = _open_directory_without_symlinks(path.parent, context)
    try:
        parent_identity = _directory_identity(os.fstat(parent_descriptor))
        try:
            leaf_descriptor = os.open(
                path.name,
                _directory_open_flags(),
                dir_fd=parent_descriptor,
            )
        except FileNotFoundError as exc:
            if allow_missing_leaf:
                return parent_identity, None
            raise SystemExit(
                f"{context} must be an existing symlink-free directory: {path}"
            ) from exc
        except OSError as exc:
            raise SystemExit(
                f"{context} must be an existing symlink-free directory: "
                f"{path}: {exc}"
            ) from exc
        try:
            leaf_identity = _directory_identity(os.fstat(leaf_descriptor))
        finally:
            os.close(leaf_descriptor)
        return parent_identity, leaf_identity
    finally:
        os.close(parent_descriptor)


def source_bundle_paths(
    source_report: pathlib.Path,
    source_provenance: pathlib.Path,
) -> dict[str, pathlib.Path]:
    bundle = source_provenance.parent
    require_canonical_absolute_path(bundle, "source bundle")
    if bundle.is_symlink() or not bundle.is_dir():
        raise SystemExit(f"source bundle is missing or is a symlink: {bundle}")
    if source_provenance.name != SOURCE_BUNDLE_FILENAMES["finalProvenance"]:
        raise SystemExit("final source provenance must use basename provenance-final.txt")
    expected_report = bundle / SOURCE_BUNDLE_FILENAMES["sourceReport"]
    if source_report.absolute() != expected_report.absolute():
        raise SystemExit("source report and final provenance are not in one canonical bundle")
    paths = {key: bundle / name for key, name in SOURCE_BUNDLE_FILENAMES.items()}
    for key, path in paths.items():
        if key in {"expectedCPUMatrix", "status"}:
            continue
        if path.is_symlink() or not path.is_file():
            raise SystemExit(f"source bundle {key} is missing or is a symlink: {path}")
    return paths


def expected_source_status(source: dict[str, Any]) -> str:
    generation = source["sourceGeneration"]
    return (
        f"SOURCE-ACCEPTED schema={SOURCE_COMPLETION_SCHEMA_VERSION} "
        f"spec={source['spec']} "
        f"task-count={source['expectedBenchmarkAttempts']} "
        f"wave-size={generation['waveSize']} "
        f"pacing-variant={generation['pacingVariant']} "
        "rayjob-backoff-limit=0 submitter-backoff-limit=0\n"
    )


def completion_document(bundle: pathlib.Path) -> dict[str, Any]:
    require_canonical_absolute_path(bundle, "source bundle")
    if bundle.is_symlink() or not bundle.is_dir():
        raise SystemExit(f"source bundle is missing or is a symlink: {bundle}")
    paths = {key: bundle / name for key, name in SOURCE_BUNDLE_FILENAMES.items()}
    for key, path in paths.items():
        if path.is_symlink() or not path.is_file():
            raise SystemExit(f"source bundle {key} is missing or is a symlink: {path}")

    source = full_collector_source(
        paths["sourceReport"],
        paths["finalProvenance"],
        require_revalidated=True,
    )
    matrix = read_json_object(paths["expectedCPUMatrix"], "expected CPU matrix")
    validator = load_validator()
    try:
        validator.validate_matrix(
            matrix,
            "hs-cpu",
            allow_legacy_source_bundle=True,
        )
    except validator.ValidationError as exc:
        raise SystemExit(f"expected CPU matrix is invalid: {exc}") from exc
    if not json_exact_equal(matrix.get("source"), source):
        raise SystemExit("expected CPU matrix source differs from accepted source")

    try:
        status = paths["status"].read_text(encoding="utf-8")
    except OSError as exc:
        raise SystemExit(f"cannot read source status {paths['status']}: {exc}") from exc
    expected_status = expected_source_status(source)
    if status != expected_status:
        raise SystemExit(
            "source status differs from the canonical terminal acceptance record"
        )

    lifecycle = source["rayJobLifecycle"]
    return {
        "schemaVersion": SOURCE_COMPLETION_SCHEMA_VERSION,
        "kind": "hs-source-completion",
        "status": "accepted",
        "publishedDirectory": "accepted",
        "claim": dict(MEASUREMENT_CLAIM),
        "sourceSpec": source["spec"],
        "taskCount": source["expectedBenchmarkAttempts"],
        "sourceGeneration": source["sourceGeneration"],
        "retryControls": {
            "rayJobBackoffLimit": lifecycle["rayJobBackoffLimit"],
            "submitterBackoffLimit": lifecycle["submitterBackoffLimit"],
        },
        "artifacts": {
            key: {
                "path": path.name,
                "sha256": file_sha256(path),
            }
            for key, path in paths.items()
        },
    }


def verify_source_completion(
    bundle: pathlib.Path,
    *,
    require_published: bool,
    expected_parent_identity: tuple[int, int] | None = None,
    expected_bundle_identity: tuple[int, int] | None = None,
) -> dict[str, Any]:
    initial_parent_identity, initial_bundle_identity = (
        require_canonical_absolute_path(bundle, "source bundle")
    )
    if (
        expected_parent_identity is not None
        and initial_parent_identity != expected_parent_identity
    ):
        raise SystemExit("source bundle parent identity changed before verification")
    if (
        expected_bundle_identity is not None
        and initial_bundle_identity != expected_bundle_identity
    ):
        raise SystemExit("source bundle identity changed before verification")
    if require_published and bundle.name != "accepted":
        raise SystemExit("published source bundle directory must be named accepted")
    completion_path = bundle / SOURCE_COMPLETION_FILENAME
    if completion_path.is_symlink() or not completion_path.is_file():
        raise SystemExit(
            f"source completion is missing or is a symlink: {completion_path}"
        )
    expected_names = set(SOURCE_BUNDLE_FILENAMES.values()) | {
        SOURCE_COMPLETION_FILENAME
    }
    try:
        observed_names = {path.name for path in bundle.iterdir()}
    except OSError as exc:
        raise SystemExit(f"cannot inventory source bundle {bundle}: {exc}") from exc
    if observed_names != expected_names:
        raise SystemExit(
            "source bundle files differ: "
            f"expected={sorted(expected_names)} observed={sorted(observed_names)}"
        )
    observed = read_json_object(completion_path, "source completion")
    expected = completion_document(bundle)
    if not json_exact_equal(observed, expected):
        raise SystemExit("source completion differs from the recomputed bundle manifest")
    final_parent_identity, final_bundle_identity = require_canonical_absolute_path(
        bundle,
        "source bundle",
    )
    if (
        final_parent_identity != initial_parent_identity
        or final_bundle_identity != initial_bundle_identity
    ):
        raise SystemExit("source bundle identity changed during verification")
    return observed


def publish_source_bundle(stage: pathlib.Path, accepted: pathlib.Path) -> None:
    stage_parent_identity, stage_identity = require_canonical_absolute_path(
        stage,
        "source staging directory",
    )
    accepted_parent_identity, accepted_identity = require_canonical_absolute_path(
        accepted,
        "accepted source directory",
        allow_missing_leaf=True,
    )
    if stage.name != ".accepted-staging" or accepted.name != "accepted":
        raise SystemExit(
            "source publish paths must use .accepted-staging and accepted basenames"
        )
    if stage.parent != accepted.parent:
        raise SystemExit("source publish paths must have the same parent directory")
    if stage_parent_identity != accepted_parent_identity:
        raise SystemExit("source publish parent identity differs between paths")
    if accepted_identity is not None:
        raise SystemExit(f"refusing to replace accepted source bundle: {accepted}")

    parent_descriptor = _open_directory_without_symlinks(
        stage.parent,
        "source publish parent",
    )
    stage_descriptor = -1
    try:
        pinned_parent_identity = _directory_identity(os.fstat(parent_descriptor))
        if pinned_parent_identity != stage_parent_identity:
            raise SystemExit("source publish parent identity changed before verification")
        try:
            stage_descriptor = os.open(
                stage.name,
                _directory_open_flags(),
                dir_fd=parent_descriptor,
            )
        except OSError as exc:
            raise SystemExit(
                f"cannot pin source staging directory without symlinks: {stage}: {exc}"
            ) from exc
        pinned_stage_identity = _directory_identity(os.fstat(stage_descriptor))
        if pinned_stage_identity != stage_identity:
            raise SystemExit("source staging identity changed before verification")
        try:
            os.stat(accepted.name, dir_fd=parent_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise SystemExit(f"refusing to replace accepted source bundle: {accepted}")

        verify_source_completion(
            stage,
            require_published=False,
            expected_parent_identity=pinned_parent_identity,
            expected_bundle_identity=pinned_stage_identity,
        )

        fresh_parent_identity, fresh_stage_identity = require_canonical_absolute_path(
            stage,
            "source staging directory",
        )
        if (
            fresh_parent_identity != pinned_parent_identity
            or fresh_stage_identity != pinned_stage_identity
        ):
            raise SystemExit("source publish path identity changed after verification")
        fresh_accepted_parent, fresh_accepted_identity = (
            require_canonical_absolute_path(
                accepted,
                "accepted source directory",
                allow_missing_leaf=True,
            )
        )
        if fresh_accepted_parent != pinned_parent_identity:
            raise SystemExit("accepted source parent identity changed after verification")
        if fresh_accepted_identity is not None:
            raise SystemExit(f"refusing to replace accepted source bundle: {accepted}")

        pinned_stage_now = os.stat(
            stage.name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISDIR(pinned_stage_now.st_mode)
            or _directory_identity(pinned_stage_now) != pinned_stage_identity
        ):
            raise SystemExit("source staging identity changed after verification")
        try:
            os.stat(accepted.name, dir_fd=parent_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise SystemExit(f"refusing to replace accepted source bundle: {accepted}")

        # Flush the pinned immutable artifacts and staging directory before the
        # one namespace-changing operation. The caller does no work after it.
        expected_names = sorted(
            set(SOURCE_BUNDLE_FILENAMES.values()) | {SOURCE_COMPLETION_FILENAME}
        )
        if sorted(os.listdir(stage_descriptor)) != expected_names:
            raise SystemExit("source staging inventory changed after verification")
        for name in expected_names:
            try:
                descriptor = os.open(
                    name,
                    os.O_RDONLY | os.O_NOFOLLOW,
                    dir_fd=stage_descriptor,
                )
            except OSError as exc:
                raise SystemExit(
                    f"cannot pin immutable source artifact {name}: {exc}"
                ) from exc
            try:
                if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                    raise SystemExit(
                        f"immutable source artifact is not a regular file: {name}"
                    )
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        os.fsync(stage_descriptor)

        # Re-open the lexical parent immediately before the fd-relative rename.
        # The parent fd remains the authority for rename; the formal protocol
        # additionally requires the runner-owned path to retain that identity.
        final_parent_descriptor = _open_directory_without_symlinks(
            stage.parent,
            "source publish parent",
        )
        try:
            if (
                _directory_identity(os.fstat(final_parent_descriptor))
                != pinned_parent_identity
            ):
                raise SystemExit("source publish parent identity changed before rename")
        finally:
            os.close(final_parent_descriptor)
        try:
            os.stat(accepted.name, dir_fd=parent_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise SystemExit(f"refusing to replace accepted source bundle: {accepted}")

        _rename_directory_exclusive(
            stage.name,
            accepted.name,
            parent_descriptor,
            accepted,
        )
        try:
            accepted_descriptor = os.open(
                accepted.name,
                _directory_open_flags(),
                dir_fd=parent_descriptor,
            )
        except OSError as exc:
            raise SystemExit(
                f"cannot pin published source directory without symlinks: "
                f"{accepted}: {exc}"
            ) from exc
        try:
            accepted_identity = _directory_identity(os.fstat(accepted_descriptor))
        finally:
            os.close(accepted_descriptor)
        if accepted_identity != pinned_stage_identity:
            raise SystemExit("published source identity differs from pinned staging")
    finally:
        if stage_descriptor >= 0:
            os.close(stage_descriptor)
        os.close(parent_descriptor)


def validate_final_source_bundle(
    source_report: pathlib.Path,
    source_provenance: pathlib.Path,
    provenance: dict[str, str],
    source: dict[str, Any],
) -> dict[str, pathlib.Path]:
    paths = source_bundle_paths(source_report, source_provenance)
    generation = source["sourceGeneration"]
    extended_generation = "rejectedAttemptReportSHA256s" in generation
    rejected = generation["rejectedBaselineReportSHA256"]
    expected_provenance = {
        "revalidation_status": "valid",
        "source_report_sha256": file_sha256(paths["sourceReport"]),
        "source_initial_provenance_sha256": file_sha256(
            paths["initialProvenance"]
        ),
        "source_contract_sha256": file_sha256(paths["sourceContract"]),
        "source_session": source["spec"],
        "source_task_count": str(source["expectedBenchmarkAttempts"]),
        "source_wave_size": str(generation["waveSize"]),
        "source_drivers": str(generation["drivers"]),
        "source_target_task_rate": str(generation["targetTaskRate"]),
        "source_driver_tasks": str(generation["driverTasks"]),
        "source_driver_wall_sec": str(generation["driverWallSec"]),
        "source_driver_rate_tps": str(generation["driverRateTPS"]),
        "source_pacing_variant": generation["pacingVariant"],
        "source_rejected_baseline_report_sha256": (
            rejected if rejected is not None else "none"
        ),
        "source_lineage_sha256": file_sha256(paths["lineage"]),
        "planned_config_sha256": file_sha256(paths["plannedConfig"]),
        "source_rayjob_backoff_limit": "0",
        "source_submitter_backoff_limit": "0",
    }
    if extended_generation:
        expected_provenance.update({
            "source_rejected_attempt_report_sha256s": (
                ",".join(generation["rejectedAttemptReportSHA256s"])
                if generation["rejectedAttemptReportSHA256s"]
                else "none"
            ),
            "source_collector_cpu_request": (
                generation["collectorCPURequest"] or "none"
            ),
            "source_collector_cpu_limit": (
                generation["collectorCPULimit"] or "none"
            ),
        })
    for key, wanted in expected_provenance.items():
        observed = provenance.get(key)
        if observed != wanted:
            raise SystemExit(
                f"source final provenance {key} differs: "
                f"expected={wanted!r} observed={observed!r}"
            )

    planned = read_json_object(paths["plannedConfig"], "source planned config")
    expected_planned = {
        "TaskCount": source["expectedBenchmarkAttempts"],
        "WaveSize": generation["waveSize"],
        "PacingVariant": generation["pacingVariant"],
        "RejectedBaselineReportSHA256": rejected,
        "LineageFile": SOURCE_BUNDLE_FILENAMES["lineage"],
        "Drivers": generation["drivers"],
        "TargetTaskRate": generation["targetTaskRate"],
        "RayJobBackoffLimit": 0,
        "SubmitterBackoffLimit": 0,
    }
    if extended_generation:
        expected_planned.update({
            "RejectedAttemptReportSHA256s": generation["rejectedAttemptReportSHA256s"],
            "CollectorCPURequest": generation["collectorCPURequest"],
            "CollectorCPULimit": generation["collectorCPULimit"],
        })
    for key, wanted in expected_planned.items():
        observed = planned.get(key)
        if observed != wanted or type(observed) is not type(wanted):
            raise SystemExit(
                f"source planned config {key} differs: "
                f"expected={wanted!r} observed={observed!r}"
            )

    lineage = read_json_object(paths["lineage"], "source lineage")
    expected_predecessor = None
    if rejected is not None:
        expected_predecessor = {
            "taskCount": 50_000,
            "waveSize": SOURCE_BASELINE_WAVE_SIZE,
            "reportSHA256": rejected,
            "verdict": "rejected-incomplete-source",
        }
    expected_current = {
        "taskCount": source["expectedBenchmarkAttempts"],
        "waveSize": generation["waveSize"],
        "drivers": generation["drivers"],
        "targetTaskRate": generation["targetTaskRate"],
        "pacingVariant": generation["pacingVariant"],
        "rayJobBackoffLimit": 0,
        "submitterBackoffLimit": 0,
    }
    if extended_generation:
        expected_current.update({"collectorCPURequest": generation["collectorCPURequest"], "collectorCPULimit": generation["collectorCPULimit"]})
    expected_lineage = {
        "schemaVersion": 1,
        "kind": "hs-source-generation-lineage",
        "current": expected_current,
        "rejectedPredecessor": expected_predecessor,
    }
    if extended_generation:
        expected_lineage["rejectedAttempts"] = [{
            "taskCount": 50_000, "waveSize": 100, "targetTaskRate": 0,
            "reportSHA256": digest, "verdict": "rejected-incomplete-source",
        } for digest in generation["rejectedAttemptReportSHA256s"]]
    if not json_exact_equal(lineage, expected_lineage):
        raise SystemExit("source lineage content differs from sourceGeneration")

    contract = read_json_object(paths["sourceContract"], "source contract")
    expected_contract_source = dict(source)
    expected_contract_source["sourceProvenanceSHA256"] = file_sha256(
        paths["initialProvenance"]
    )
    contract_schema = contract.get("schemaVersion")
    if (
        type(contract_schema) is not int
        or contract_schema
        not in (LEGACY_SOURCE_MATRIX_SCHEMA_VERSION, MATRIX_SCHEMA_VERSION)
    ):
        raise SystemExit("source contract differs: schemaVersion")
    expected_contract = {
        "schemaVersion": contract_schema,
        "kind": "hs-source",
        "claim": dict(MEASUREMENT_CLAIM),
        "source": expected_contract_source,
        "collectorValidityRequired": True,
    }
    if not json_exact_equal(contract, expected_contract):
        raise SystemExit("source contract differs from report and initial provenance")
    return paths


def full_collector_source(
    source_report: pathlib.Path,
    source_provenance: pathlib.Path,
    *,
    require_revalidated: bool,
) -> dict[str, Any]:
    """Validate the stricter source-generation contract, including Collector evidence."""
    provenance = read_provenance(
        source_provenance,
        require_revalidated=require_revalidated,
    )
    source = common_source(source_report, provenance)
    report = json.loads(source_report.read_text())
    config = report.get("config") or {}
    expected_target_rate = source["sourceGeneration"]["targetTaskRate"]
    for field, wanted in {
        "S3LocalPort": S3_LOCAL_PORT,
        "SkipHistoryServer": True,
        "TargetTaskRate": expected_target_rate,
    }.items():
        if not json_exact_equal(config.get(field), wanted):
            raise SystemExit(
                f"source report config.{field} differs: "
                f"expected={wanted!r} observed={config.get(field)!r}"
            )

    storage_diffs = report.get("storageDiffs")
    if not isinstance(storage_diffs, list) or len(storage_diffs) != len(
        SOURCE_STORAGE_DIFF_LABELS
    ):
        raise SystemExit(
            "source report must contain exactly the during-job and flush storageDiffs"
        )
    def validate_isolation_diff(diff: Any, wanted_label: str, context: str) -> None:
        if not isinstance(diff, dict):
            raise SystemExit(f"source report {context} is not an object")
        if diff.get("label") != wanted_label:
            raise SystemExit(
                f"source report {context}.label must be {wanted_label!r}"
            )
        if type(diff.get("deletedObjects")) is not int or diff["deletedObjects"] != 0:
            raise SystemExit(f"source report {wanted_label} deletedObjects must be zero")
        for field in (
            "unexpectedKeys",
            "unexpectedChangedKeys",
            "deletedKeys",
        ):
            if diff.get(field) != []:
                raise SystemExit(f"source report {wanted_label} {field} must be []")

    seen_labels: set[str] = set()
    for index, diff in enumerate(storage_diffs):
        if not isinstance(diff, dict):
            raise SystemExit(f"source report storageDiffs[{index}] is not an object")
        label = diff.get("label")
        if label not in SOURCE_STORAGE_DIFF_LABELS or label in seen_labels:
            raise SystemExit(
                "source report storageDiffs must contain each expected label exactly once"
            )
        seen_labels.add(label)
        validate_isolation_diff(diff, label, f"storageDiffs[{index}]")
    if seen_labels != set(SOURCE_STORAGE_DIFF_LABELS):
        raise SystemExit(
            "source report storageDiffs must contain each expected label exactly once"
        )
    storage_isolation = report.get("storageIsolation")
    validate_isolation_diff(
        storage_isolation,
        SOURCE_STORAGE_ISOLATION_LABEL,
        "storageIsolation",
    )

    logs = report.get("collectorLogs")
    if not isinstance(logs, list) or len(logs) != 2:
        raise SystemExit("source report must contain exactly two Collector log records")
    if {item.get("role") for item in logs if isinstance(item, dict)} != {"head", "worker"}:
        raise SystemExit("source report Collector logs must contain one head and one worker")
    for item in logs:
        role = item.get("role")
        exact_log_gates = {
            "logStreamComplete": True,
            "logStreamTimedOut": False,
            "gracefulShutdownComplete": True,
            "restartCount": 0,
            "uploadFailures": 0,
            "rotationQueueFull": 0,
        }
        for field, wanted in exact_log_gates.items():
            if not json_exact_equal(item.get(field), wanted):
                raise SystemExit(
                    f"source report {role} Collector {field} differs: "
                    f"expected={wanted!r} observed={item.get(field)!r}"
                )
        if not isinstance(item.get("containerID"), str) or not item["containerID"]:
            raise SystemExit(f"source report {role} Collector containerID is missing")
        if type(item.get("uploads")) is not int or item["uploads"] <= 0:
            raise SystemExit(f"source report {role} Collector has no successful upload")
        expected_collector_resources = {
            "cpuRequest": source["sourceGeneration"].get("collectorCPURequest") or "0",
            "cpuLimit": source["sourceGeneration"].get("collectorCPULimit") or "0",
        }
        for field, wanted in expected_collector_resources.items():
            if item.get(field) != wanted:
                raise SystemExit(f"source report {role} Collector {field} differs")
        observed_image = normalize_local_image_reference(item.get("image"))
        expected_image = normalize_local_image_reference(
            provenance["collector_image_requested"]
        )
        if observed_image != expected_image:
            raise SystemExit(
                f"source report {role} Collector image differs from provenance: "
                f"observed={observed_image!r} expected={expected_image!r}"
            )
        observed_image_id = normalize_container_image_id(item.get("imageID"))
        if observed_image_id != provenance["collector_runtime_id"]:
            raise SystemExit(
                f"source report {role} Collector imageID differs from provenance: "
                f"observed={observed_image_id!r} "
                f"expected={provenance['collector_runtime_id']!r}"
            )
        windows = item.get("ingressWindows")
        if not isinstance(windows, list) or not windows:
            raise SystemExit(f"source report {role} Collector has no ingress windows")
        for window in windows:
            if not isinstance(window, dict):
                raise SystemExit(f"source report {role} Collector ingress window is invalid")
            for field in (
                "rejectedRequests",
                "rejectedDraining",
                "rejectedDiskPressure",
                "rejectedBadRequest",
                "rejectedInternal",
                "rotationQueueFull",
            ):
                if type(window.get(field)) is not int or window[field] != 0:
                    raise SystemExit(
                        f"source report {role} Collector ingress {field} is nonzero"
                    )

    gates = report.get("collectorIngressGates")
    if not isinstance(gates, list) or len(gates) != 2:
        raise SystemExit("source report must contain exactly two Collector validity gates")
    if {item.get("role") for item in gates if isinstance(item, dict)} != {"head", "worker"}:
        raise SystemExit("source report Collector gates must contain one head and one worker")
    for item in gates:
        if item.get("valid") is not True or item.get("problems") != []:
            raise SystemExit(
                f"source report {item.get('role')} Collector validity gate failed: "
                f"{item.get('problems')!r}"
            )

    sampler = report.get("cgroupSampler")
    if not isinstance(sampler, dict):
        raise SystemExit("source report cgroupSampler is missing")
    for field in ("startAttempted", "started", "stopRequested", "streamEnded", "streamComplete"):
        if sampler.get(field) is not True:
            raise SystemExit(f"source report cgroupSampler.{field} is not true")
    if sampler.get("streamEndedBeforeStop") is not False:
        raise SystemExit("source report cgroup sampler ended before stop")
    if sampler.get("startError") or sampler.get("streamError"):
        raise SystemExit("source report cgroup sampler contains an error")

    errors = ((report.get("storage") or {}).get("events") or {}).get("errors")
    if errors not in (None, []):
        raise SystemExit(f"source report storage event scan has errors: {errors!r}")
    # Carry the validated evidence into every source/CPU/memory matrix so the
    # formal validator can enforce the same contract without trusting a label.
    source["storageDiffs"] = storage_diffs
    source["storageIsolation"] = storage_isolation
    lifecycle = report.get("rayJobLifecycle")
    wanted_lifecycle = {
        "ownedCluster": True,
        "shutdownAfterJobFinishes": True,
        "ttlSecondsAfterFinished": 30,
        "rayJobBackoffLimit": 0,
        "submitterBackoffLimit": 0,
    }
    if (
        not isinstance(lifecycle, dict)
        or set(lifecycle) != set(wanted_lifecycle)
        or any(
            lifecycle.get(field) != wanted
            or type(lifecycle.get(field)) is not type(wanted)
            for field, wanted in wanted_lifecycle.items()
        )
    ):
        raise SystemExit(
            f"source report rayJobLifecycle differs: "
            f"expected={wanted_lifecycle!r} observed={lifecycle!r}"
        )
    source["rayJobLifecycle"] = lifecycle
    source["collectorImageRequested"] = provenance["collector_image_requested"]
    source["collectorRuntimeID"] = provenance["collector_runtime_id"]
    if require_revalidated:
        validate_final_source_bundle(
            source_report,
            source_provenance,
            provenance,
            source,
        )
    source["sourceProvenanceSHA256"] = file_sha256(source_provenance)
    return source


def source_document(
    source_report: pathlib.Path,
    source_provenance: pathlib.Path | None = None,
) -> dict[str, Any]:
    if source_provenance is None:
        source_provenance = source_report.parent / SOURCE_BUNDLE_FILENAMES[
            "initialProvenance"
        ]
    return {
        "schemaVersion": MATRIX_SCHEMA_VERSION,
        "kind": "hs-source",
        "claim": dict(MEASUREMENT_CLAIM),
        "source": full_collector_source(
            source_report,
            source_provenance,
            require_revalidated=False,
        ),
        "collectorValidityRequired": True,
    }


def common_policy(source: dict[str, Any]) -> dict[str, Any]:
    return {
        "cacheSize": CACHE_SIZE,
        "cacheMaxBytes": CACHE_MAX_BYTES,
        "cacheTTLSeconds": CACHE_TTL_SECONDS,
        "serverTimeoutSeconds": PROCESS_TIMEOUT_SECONDS,
        "clientTimeoutSeconds": CLIENT_TIMEOUT_SECONDS,
        "coldSLOSeconds": COLD_SLO_SECONDS,
        "strictColdNoRetry": True,
        "freshPodPerArm": True,
        "sessionSettleSeconds": SESSION_SETTLE_SECONDS,
        "warmQueryConcurrency": WARM_QUERY_CONCURRENCY,
        "warmIterations": WARM_ITERATIONS,
        "warmTaskLimit": min(source["expectedBenchmarkAttempts"], WARM_TASK_LIMIT),
        "warmTaskDetail": True,
        "s3LocalPort": S3_LOCAL_PORT,
        "historyServerLocalPort": 30_080,
        "executionNamespaceIdentityFile": "execution-namespace.json",
        "waitForNamespaceDeletion": True,
        "historyServerPhases": list(HS_PHASE_SEQUENCE),
    }


def arm_config(source: dict[str, Any], cpu: str, memory: str, kind_node: str) -> dict[str, Any]:
    task_log_metadata = source["taskLogMetadata"]
    counts = task_log_metadata["counts"]
    return {
        "TaskCount": source["expectedBenchmarkAttempts"],
        "HSOnly": source["spec"],
        "HSSourceObjectCount": source["expectedObjectCount"],
        "HSSourceTotalBytes": source["expectedTotalBytes"],
        "S3Bucket": source["bucket"],
        "S3LocalPort": S3_LOCAL_PORT,
        "HSCPURequest": cpu,
        "HSCPULimit": cpu,
        "HSMemoryRequest": memory,
        "HSMemoryLimit": memory,
        "HSEnv": "",
        "HSArgs": HS_ARGS,
        "HSColdSLO": COLD_SLO_SECONDS * 1_000_000_000,
        "HSEnterTimeout": CLIENT_TIMEOUT_SECONDS * 1_000_000_000,
        "HSWarmWait": 0,
        "HSSessionSettle": SESSION_SETTLE_SECONDS * 1_000_000_000,
        "WarmIterations": WARM_ITERATIONS,
        "HSStrictCold": True,
        "HSQueryConcurrency": WARM_QUERY_CONCURRENCY,
        "HSProtocol": "",
        "HSPreColdIdle": 0,
        "HSRequestQuietGap": 0,
        "KindNode": kind_node,
        "SkipHistoryServer": False,
        "HSSourceRayJobOwned": True,
        "HSSourceShutdownAfterJob": True,
        "HSSourceJobTTLSeconds": 30,
        "HSSourceRayJobBackoffLimit": 0,
        "HSSourceSubmitterBackoffLimit": 0,
        "HSSourceTaskLogMetadataAlgorithm": task_log_metadata["algorithm"],
        "HSSourceTaskLogMetadataSHA256": task_log_metadata["sha256"],
        "HSSourceTaskLogMetadataAttempts": task_log_metadata["attempts"],
        "HSSourceTaskLogMetadataNil": counts["nil"],
        "HSSourceTaskLogMetadataPresent": counts["present"],
        "HSSourceTaskLogMetadataStructurallyInvalid": counts["structurallyInvalid"],
        "HSSourceTaskLogMetadataIncompleteNonNil": counts["incompleteNonNil"],
        "HSSourceTaskLogMetadataStdoutExactResolvable": counts["stdoutExactResolvable"],
        "HSSourceTaskLogMetadataStderrExactResolvable": counts["stderrExactResolvable"],
        "HSSourceTaskLogMetadataLegacyWholeWorkerFallback": counts[
            "legacyWholeWorkerFallback"
        ],
    }


def isolated_arm_config(source: dict[str, Any], kind_node: str) -> dict[str, Any]:
    config = arm_config(source, ISOLATED_CPU_REQUEST, ISOLATED_MEMORY_REQUEST, kind_node)
    config.update({
        "HSCPURequest": ISOLATED_CPU_REQUEST,
        "HSCPULimit": ISOLATED_CPU_LIMIT,
        "HSMemoryRequest": ISOLATED_MEMORY_REQUEST,
        "HSMemoryLimit": ISOLATED_MEMORY_LIMIT,
        "HSEnv": ISOLATED_HS_ENV,
        "HSProtocol": ISOLATED_PROTOCOL,
        "HSPreColdIdle": ISOLATED_PRE_COLD_IDLE_SECONDS * 1_000_000_000,
        "HSRequestQuietGap": ISOLATED_QUIET_GAP_SECONDS * 1_000_000_000,
    })
    return config


def isolated_document(source_report: pathlib.Path, kind_node: str, source_provenance: pathlib.Path) -> dict[str, Any]:
    source = full_collector_source(source_report, source_provenance, require_revalidated=True)
    if source["expectedBenchmarkAttempts"] not in SOURCE_ALLOWED_ATTEMPTS:
        raise SystemExit("isolated request source task count is not allowed")
    return {
        "schemaVersion": MATRIX_SCHEMA_VERSION,
        "kind": "hs-isolated-request",
        "claim": dict(MEASUREMENT_CLAIM),
        "source": source,
        "policy": {**common_policy(source), "historyServerPhases": list(ISOLATED_HS_PHASE_SEQUENCE)},
        "selectionPolicy": {
            "requiredValidRepeats": ISOLATED_REPEATS,
            "rule": "all five predeclared repeats must be measurement-valid; no post-hoc lifecycle selection",
        },
        "arms": [
            {"name": f"isolated-r{repeat}", "repeat": repeat,
             "sequenceInRepeat": 1, "config": isolated_arm_config(source, kind_node)}
            for repeat in range(1, ISOLATED_REPEATS + 1)
        ],
    }


def cpu_document(
    source_report: pathlib.Path,
    kind_node: str,
    source_provenance: pathlib.Path | None = None,
) -> dict[str, Any]:
    # CPU and the memory campaign derived from it must consume the same strict,
    # immutable Collector source contract as the standalone hs-source document.
    if source_provenance is None:
        source_provenance = source_report.parent / SOURCE_BUNDLE_FILENAMES[
            "finalProvenance"
        ]
    source = full_collector_source(
        source_report,
        source_provenance,
        require_revalidated=True,
    )
    arms: list[dict[str, Any]] = []
    for repeat, cpus in enumerate(CPU_INTERLEAVED_ORDER, start=1):
        for sequence, cpu in enumerate(cpus, start=1):
            arms.append(
                {
                    "name": f"cpu-{cpu}-r{repeat}",
                    "repeat": repeat,
                    "sequenceInRepeat": sequence,
                    "config": arm_config(source, cpu, CPU_MEMORY, kind_node),
                }
            )
    return {
        "schemaVersion": MATRIX_SCHEMA_VERSION,
        "kind": "hs-cpu",
        "claim": dict(MEASUREMENT_CLAIM),
        "source": source,
        "policy": common_policy(source),
        "selectionPolicy": {
            "cpuOrder": list(CPU_VALUES),
            "requiredValidRepeats": CPU_REPEATS,
            "rule": "smallest CPU with all repeats measurement-valid and cold SLO met",
        },
        "arms": arms,
    }


def load_validator() -> Any:
    path = pathlib.Path(__file__).with_name("validate_hs_sweep.py")
    spec = importlib.util.spec_from_file_location("validate_hs_sweep", path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot load validator from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def round_memory_candidate(peak_bytes: int) -> int:
    if peak_bytes <= 0:
        raise ValueError("peak bytes must be positive")
    # ceil(peak * 1.25 / 32Mi) * 32Mi, kept in integer arithmetic.
    scaled_numerator = peak_bytes * MEMORY_HEADROOM_NUMERATOR
    scaled_denominator = MEMORY_HEADROOM_DENOMINATOR * MEMORY_ROUND_BYTES
    units = (scaled_numerator + scaled_denominator - 1) // scaled_denominator
    return units * MEMORY_ROUND_BYTES


def memory_document(
    cpu_root: pathlib.Path,
    kind_node: str,
    execution_cpu_override: str | None = None,
) -> dict[str, Any]:
    validator = load_validator()
    result = validator.validate_campaign(
        cpu_root,
        expected_kind="hs-cpu",
        require_final_provenance=True,
    )
    default_selection = validator.select_cpu(result)
    if default_selection is None:
        raise SystemExit("CPU campaign has no eligible CPU")
    if execution_cpu_override not in (None, MEMORY_EXECUTION_CPU_OVERRIDE):
        raise SystemExit(
            "memory execution CPU override must be exactly "
            f"{MEMORY_EXECUTION_CPU_OVERRIDE!r}"
        )
    execution_cpu = default_selection.cpu
    selection_mode = MEMORY_SELECTION_MODE_DEFAULT
    if execution_cpu_override is not None:
        override_selection = validator.select_eligible_cpu(
            result, execution_cpu_override
        )
        if override_selection is None:
            raise SystemExit(
                f"CPU campaign has no eligible {execution_cpu_override} CPU override"
            )
        execution_cpu = execution_cpu_override
        selection_mode = MEMORY_SELECTION_MODE_OVERRIDE

    cpu_matrix = cpu_root / "expected-matrix.json"
    final_provenance = cpu_root / "provenance-final.txt"
    # The explicit execution override is a validation control only. Keep the
    # candidate limit derived from the default CPU-selection policy so the
    # override changes one experimental variable and cannot silently rewrite
    # the memory-sizing rule.
    peak_bytes = default_selection.discovery_max_lifetime_memory_peak_bytes
    candidate_bytes = round_memory_candidate(peak_bytes)
    if candidate_bytes % MEMORY_ROUND_BYTES != 0:
        raise AssertionError("candidate memory is not a 32Mi multiple")
    candidate_quantity = f"{candidate_bytes // (1024 * 1024)}Mi"

    source = dict(result.matrix["source"])
    source["baselineFingerprintSHA256"] = result.source_fingerprint_sha256
    arms = [
        {
            "name": f"memory-{candidate_quantity}-r{repeat}",
            "repeat": repeat,
            "sequenceInRepeat": 1,
            "config": arm_config(source, execution_cpu, candidate_quantity, kind_node),
        }
        for repeat in range(1, MEMORY_REPEATS + 1)
    ]
    return {
        "schemaVersion": MATRIX_SCHEMA_VERSION,
        "kind": "hs-memory",
        "claim": dict(MEASUREMENT_CLAIM),
        "source": source,
        "policy": common_policy(source),
        "selectionEvidence": {
            "cpuCampaignExpectedMatrixSHA256": file_sha256(cpu_matrix),
            "cpuCampaignFinalProvenanceSHA256": file_sha256(final_provenance),
            "selectedCPU": execution_cpu,
            "defaultSelectedCPU": default_selection.cpu,
            "candidateMemoryDiscoveryCPU": default_selection.cpu,
            "executionCPU": execution_cpu,
            "selectionMode": selection_mode,
            "cpuOverride": execution_cpu_override,
            "discoveryMaxLifetimeMemoryPeakBytes": peak_bytes,
            "headroomNumerator": MEMORY_HEADROOM_NUMERATOR,
            "headroomDenominator": MEMORY_HEADROOM_DENOMINATOR,
            "roundBytes": MEMORY_ROUND_BYTES,
            "candidateMemoryBytes": candidate_bytes,
            "candidateMemoryQuantity": candidate_quantity,
            "rule": "roundUp32Mi(1.25 * default-selected CPU discovery max lifetime memory.peak)",
        },
        "confirmationPolicy": {
            "requiredValidRepeats": MEMORY_REPEATS,
            "requestEqualsLimit": True,
            "stopOnAnyInvalidArm": True,
        },
        "arms": arms,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    cpu = subparsers.add_parser("cpu")
    cpu.add_argument("--output", required=True, type=pathlib.Path)
    cpu.add_argument("--source-report", required=True, type=pathlib.Path)
    cpu.add_argument("--source-provenance", required=True, type=pathlib.Path)
    cpu.add_argument("--kind-node", default="bench-control-plane")

    isolated = subparsers.add_parser("isolated")
    isolated.add_argument("--output", required=True, type=pathlib.Path)
    isolated.add_argument("--source-report", required=True, type=pathlib.Path)
    isolated.add_argument("--source-provenance", required=True, type=pathlib.Path)
    isolated.add_argument("--kind-node", default="bench-control-plane")

    source = subparsers.add_parser("source")
    source.add_argument("--output", required=True, type=pathlib.Path)
    source.add_argument("--source-report", required=True, type=pathlib.Path)
    source.add_argument("--source-provenance", required=True, type=pathlib.Path)

    status = subparsers.add_parser("status")
    status.add_argument("--output", required=True, type=pathlib.Path)
    status.add_argument("--source-report", required=True, type=pathlib.Path)
    status.add_argument("--source-provenance", required=True, type=pathlib.Path)

    memory = subparsers.add_parser("memory")
    memory.add_argument("--output", required=True, type=pathlib.Path)
    memory.add_argument("--cpu-campaign-root", required=True, type=pathlib.Path)
    memory.add_argument("--kind-node", default="bench-control-plane")
    memory.add_argument(
        "--execution-cpu-override",
        choices=(MEMORY_EXECUTION_CPU_OVERRIDE,),
    )

    completion = subparsers.add_parser("completion")
    completion.add_argument("--output", required=True, type=pathlib.Path)
    completion.add_argument("--accepted-dir", required=True, type=pathlib.Path)

    verify_completion = subparsers.add_parser("verify-completion")
    verify_completion.add_argument("--accepted-dir", required=True, type=pathlib.Path)
    verify_completion.add_argument("--allow-staging", action="store_true")

    publish = subparsers.add_parser("publish")
    publish.add_argument("--staging-dir", required=True, type=pathlib.Path)
    publish.add_argument("--accepted-dir", required=True, type=pathlib.Path)

    source_values = subparsers.add_parser("source-runtime-values")
    source_values.add_argument("--source-contract", required=True, type=pathlib.Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "verify-completion":
        verify_source_completion(
            args.accepted_dir,
            require_published=not args.allow_staging,
        )
        print("SOURCE-COMPLETION-VALID")
        return 0
    if args.command == "publish":
        publish_source_bundle(args.staging_dir, args.accepted_dir)
        return 0
    if args.command == "source-runtime-values":
        values = source_contract_runtime_values(args.source_contract)
        print("\t".join((*values, "SOURCE-CONTRACT-END")))
        return 0

    if args.output.exists():
        raise SystemExit(f"refusing to overwrite {args.output}")
    args.output.parent.mkdir(parents=True, exist_ok=True)

    if args.command == "status":
        source = full_collector_source(
            args.source_report,
            args.source_provenance,
            require_revalidated=True,
        )
        with args.output.open("x", encoding="utf-8") as stream:
            stream.write(expected_source_status(source))
        return 0

    if args.command == "source":
        document = source_document(args.source_report, args.source_provenance)
    elif args.command == "cpu":
        document = cpu_document(
            args.source_report,
            args.kind_node,
            args.source_provenance,
        )
    elif args.command == "isolated":
        document = isolated_document(args.source_report, args.kind_node, args.source_provenance)
    elif args.command == "memory":
        document = memory_document(
            args.cpu_campaign_root,
            args.kind_node,
            args.execution_cpu_override,
        )
    else:
        document = completion_document(args.accepted_dir)

    with args.output.open("x") as stream:
        json.dump(document, stream, indent=2, sort_keys=True)
        stream.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
