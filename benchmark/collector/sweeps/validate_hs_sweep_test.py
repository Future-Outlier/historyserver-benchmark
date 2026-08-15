#!/usr/bin/env python3
"""Unit and contract tests for formal History Server-only sweep artifacts."""

from __future__ import annotations

import hashlib
import errno
import json
import os
import pathlib
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import validate_hs_sweep
import write_hs_expected_matrix


SHA_A = "a" * 64
SHA_B = "b" * 64
IMAGE_ID = "sha256:" + "9" * 64
IMAGE = "historyserver:latest"
COLLECTOR_IMAGE_ID = "sha256:" + "6" * 64
COLLECTOR_IMAGE = "collector:latest"
SOURCE_LINEAGE_SHA = "d" * 64
SOURCE_NAMESPACE = "test-ns-vp8s9"
SOURCE_CLUSTER = "rayjob-bench-rjff2"
SOURCE_SESSION = "session_fixture"
SOURCE_OBJECTS = 143
SOURCE_BYTES = {
    1_000: 741_488,
    5_000: 2_500_000,
    10_000: 4_268_828,
    50_000: 19_582_280,
}


def sha256(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def zero_errors() -> dict[str, int]:
    return {key: 0 for key in sorted(validate_hs_sweep.ERROR_COUNT_KEYS)}


def task_log_metadata(attempts: int, *, warm: bool = False) -> dict:
    scope = "warm" if warm else "full"
    digest = hashlib.sha256(f"{scope}-task-log-{attempts}".encode()).hexdigest()
    return {
        "algorithm": validate_hs_sweep.TASK_LOG_METADATA_ALGORITHM,
        "sha256": digest,
        "attempts": attempts,
        "counts": {
            "nil": 0,
            "present": attempts,
            "structurallyInvalid": 0,
            "incompleteNonNil": attempts,
            "stdoutExactResolvable": 0,
            "stderrExactResolvable": 0,
            "legacyWholeWorkerFallback": 0,
        },
        "valid": True,
        "problems": [],
    }


def task_log_provenance(source: dict) -> dict[str, str]:
    return validate_hs_sweep.expected_task_log_metadata_provenance(source)


def safe_storage_diffs() -> list[dict]:
    return [
        {
            "label": "during-job (T1-T0)",
            "addedObjects": 2,
            "addedBytes": 1_000,
            # An overwrite inside this run's session is allowed. The Go
            # snapshot matcher proves it did not touch another session.
            "changedObjects": 1,
            "changedBytes": 0,
            "deletedObjects": 0,
            "unexpectedKeys": [],
            "unexpectedChangedKeys": [],
            "deletedKeys": [],
        },
        {
            "label": "flush (T2-T1)",
            "addedObjects": 2,
            "addedBytes": 500,
            "changedObjects": 0,
            "changedBytes": 0,
            "deletedObjects": 0,
            "unexpectedKeys": [],
            "unexpectedChangedKeys": [],
            "deletedKeys": [],
        },
    ]


def safe_storage_isolation() -> dict:
    return {
        "label": "full-lifecycle (T2-pre-start)",
        "addedObjects": 4,
        "addedBytes": 1_500,
        "changedObjects": 1,
        "changedBytes": 0,
        "deletedObjects": 0,
        "unexpectedKeys": [],
        "unexpectedChangedKeys": [],
        "deletedKeys": [],
    }


def source_report(
    root: pathlib.Path,
    task_count: int = 50_000,
    *,
    task_log: dict | None = None,
    session_id: str = SOURCE_SESSION,
    driver_wall_sec: float | None = None,
    compact_lineage: bool = False,
) -> pathlib.Path:
    root = root.resolve(strict=True)
    bundle = pathlib.Path(
        tempfile.mkdtemp(prefix=f"source-{task_count}-", dir=root)
    )
    path = bundle / "source-report.json"
    if task_log is None:
        task_log = task_log_metadata(task_count)
    (
        wave_size,
        target_task_rate,
        pacing_variant,
        rejected_baseline_sha,
        rejected_attempt_shas,
        collector_cpu_request,
        collector_cpu_limit,
    ) = (
        write_hs_expected_matrix.expected_source_generation_controls(task_count)
    )
    if driver_wall_sec is None:
        target_rate = 500 if task_count == 50_000 else 2_000
        driver_wall_sec = max(task_count / target_rate, 0.5)
    driver_rate_tps = task_count / driver_wall_sec
    path.write_text(
        json.dumps(
            {
                "namespace": SOURCE_NAMESPACE,
                "namespaceUID": "10000000-0000-0000-0000-000000000001",
                "clusterName": SOURCE_CLUSTER,
                "sessionID": session_id,
                "completed": True,
                "rayJobLifecycle": {
                    "ownedCluster": True,
                    "shutdownAfterJobFinishes": True,
                    "ttlSecondsAfterFinished": 30,
                    "rayJobBackoffLimit": 0,
                    "submitterBackoffLimit": 0,
                },
                "job": {
                    "driverTasks": task_count,
                    "driverWallSec": driver_wall_sec,
                    "driverRateTPS": driver_rate_tps,
                },
                "config": {
                    "TaskCount": task_count,
                    "RayImage": "rayproject/ray:2.56.0",
                    "Compression": True,
                    "WaveSize": wave_size,
                    "TaskNumCPUs": "0.5",
                    "DrainSleepSec": 0,
                    "ShutdownAfterJob": True,
                    "JobTTLSeconds": 30,
                    "Drivers": 1,
                    "S3Bucket": validate_hs_sweep.SOURCE_BUCKET,
                    "S3LocalPort": 19_003,
                    "SkipHistoryServer": True,
                    "SkipCleanup": True,
                    "TargetTaskRate": target_task_rate,
                    "CollectorCPURequest": collector_cpu_request or "",
                    "CollectorCPU": collector_cpu_limit or "",
                },
                "collectorLogs": [
                    {
                        "role": role,
                        "image": COLLECTOR_IMAGE,
                        "imageID": "docker.io/library/collector@" + COLLECTOR_IMAGE_ID,
                        "logStreamComplete": True,
                        "logStreamTimedOut": False,
                        "gracefulShutdownComplete": True,
                        "containerID": role + "-container-id",
                        "restartCount": 0,
                        "cpuRequest": collector_cpu_request or "0",
                        "cpuLimit": collector_cpu_limit or "0",
                        "uploads": 1,
                        "uploadFailures": 0,
                        "rotationQueueFull": 0,
                        "ingressWindows": [
                            {
                                "rejectedRequests": 0,
                                "rejectedDraining": 0,
                                "rejectedDiskPressure": 0,
                                "rejectedBadRequest": 0,
                                "rejectedInternal": 0,
                                "rotationQueueFull": 0,
                            }
                        ],
                    }
                    for role in ("head", "worker")
                ],
                "collectorIngressGates": [
                    {"role": role, "valid": True, "problems": []}
                    for role in ("head", "worker")
                ],
                "cgroupSampler": {
                    "startAttempted": True,
                    "started": True,
                    "stopRequested": True,
                    "streamEnded": True,
                    "streamEndedBeforeStop": False,
                    "streamComplete": True,
                    "startError": "",
                    "streamError": "",
                },
                "storageDiffs": safe_storage_diffs(),
                "storageIsolation": safe_storage_isolation(),
                "storage": {
                    "objectCount": SOURCE_OBJECTS,
                    "totalBytes": SOURCE_BYTES[task_count],
                    "markerPresent": True,
                    "events": {
                        "errors": None,
                        "expectedTasks": task_count,
                        "benchTaskIDs": task_count,
                        "countByType": {
                            "TASK_DEFINITION_EVENT": task_count + 5,
                            "TASK_LIFECYCLE_EVENT": int(task_count * 2.2),
                            "TASK_PROFILE_EVENT": int(task_count * 0.9),
                        },
                        "totalEvents": int(task_count * 4.1),
                        "rawJSONLBytes": task_count * 3_500,
                        "benchTaskValidity": {
                            "expectedTaskIDs": task_count,
                            "observedTaskIDs": task_count,
                            "observedAttempts": task_count,
                            "attemptZero": task_count,
                            "finishedAttempts": task_count,
                            "submittedToWorkerAttempts": task_count,
                            "finishedTransitionAttempts": task_count,
                            "malformedDefinitions": 0,
                            "missingDefinitionAttemptFields": 0,
                            "missingLifecycleAttemptFields": 0,
                            "invalidLifecycleTransitions": 0,
                            "outOfRangeLifecycleTransitions": 0,
                            "ambiguousLifecycleTransitions": 0,
                            "missingLifecycleAttempts": 0,
                            "nonFinishedAttempts": 0,
                            "valid": True,
                            "problems": [],
                        },
                        "taskLogMetadata": task_log,
                    },
                },
            },
            sort_keys=True,
        )
    )
    lineage = {
        "schemaVersion": 1,
        "kind": "hs-source-generation-lineage",
        "current": {
            "taskCount": task_count,
            "waveSize": wave_size,
            "drivers": 1,
            "targetTaskRate": target_task_rate,
            "pacingVariant": pacing_variant,
            "collectorCPURequest": "100m" if task_count == 50_000 else None,
            "collectorCPULimit": "2" if task_count == 50_000 else None,
            "rayJobBackoffLimit": 0,
            "submitterBackoffLimit": 0,
        },
        "rejectedPredecessor": (
            None
            if rejected_baseline_sha is None
            else {
                "taskCount": 50_000,
                "waveSize": 2_000,
                "reportSHA256": rejected_baseline_sha,
                "verdict": "rejected-incomplete-source",
            }
        ),
        "rejectedAttempts": (
            [
                {
                    "taskCount": 50_000,
                    "waveSize": 100,
                    "targetTaskRate": 0,
                    "reportSHA256": digest,
                    "verdict": "rejected-incomplete-source",
                }
                for digest in rejected_attempt_shas
            ]
            if task_count == 50_000 else []
        ),
    }
    lineage_text = json.dumps(
        lineage,
        indent=None if compact_lineage else 2,
        sort_keys=True,
        separators=(",", ":") if compact_lineage else None,
    )
    (bundle / "lineage.json").write_text(lineage_text + "\n")
    planned = {
        "TaskCount": task_count,
        "WaveSize": wave_size,
        "PacingVariant": pacing_variant,
        "RejectedBaselineReportSHA256": rejected_baseline_sha,
        "LineageFile": "lineage.json",
        "Drivers": 1,
        "TargetTaskRate": target_task_rate,
        "RejectedAttemptReportSHA256s": list(rejected_attempt_shas),
        "CollectorCPURequest": collector_cpu_request,
        "CollectorCPULimit": collector_cpu_limit,
        "RayJobBackoffLimit": 0,
        "SubmitterBackoffLimit": 0,
    }
    (bundle / "planned-config.json").write_text(
        json.dumps(planned, indent=2, sort_keys=True) + "\n"
    )
    initial = {
        "collector_image_requested": COLLECTOR_IMAGE,
        "collector_runtime_id": COLLECTOR_IMAGE_ID,
        "source_task_count": str(task_count),
        "source_wave_size": str(wave_size),
        "source_drivers": "1",
        "source_target_task_rate": str(target_task_rate),
        "source_rayjob_backoff_limit": "0",
        "source_submitter_backoff_limit": "0",
        "source_pacing_variant": pacing_variant,
        "source_rejected_baseline_report_sha256": rejected_baseline_sha or "none",
        "source_rejected_attempt_report_sha256s": (
            ",".join(rejected_attempt_shas) if rejected_attempt_shas else "none"
        ),
        "source_collector_cpu_request": collector_cpu_request or "none",
        "source_collector_cpu_limit": collector_cpu_limit or "none",
        "source_lineage_sha256": sha256(bundle / "lineage.json"),
        "planned_config_sha256": sha256(bundle / "planned-config.json"),
    }
    write_kv(bundle / "provenance.txt", initial)
    contract = write_hs_expected_matrix.source_document(
        path,
        bundle / "provenance.txt",
    )
    (bundle / "source-contract.json").write_text(
        json.dumps(contract, indent=2, sort_keys=True) + "\n"
    )
    generation = contract["source"]["sourceGeneration"]
    final = dict(initial)
    final.update(
        {
            "revalidation_status": "valid",
            "source_report_sha256": sha256(path),
            "source_initial_provenance_sha256": sha256(bundle / "provenance.txt"),
            "source_contract_sha256": sha256(bundle / "source-contract.json"),
            "source_session": contract["source"]["spec"],
            "source_driver_tasks": str(generation["driverTasks"]),
            "source_driver_wall_sec": str(generation["driverWallSec"]),
            "source_driver_rate_tps": str(generation["driverRateTPS"]),
        }
    )
    write_kv(bundle / "provenance-final.txt", final)
    return path


def source_staging_bundle(
    root: pathlib.Path,
    task_count: int = 50_000,
    **report_kwargs: object,
) -> pathlib.Path:
    root = root.resolve(strict=True)
    attempt = pathlib.Path(
        tempfile.mkdtemp(prefix="source-attempt-", dir=root)
    ).resolve(strict=True)
    report = source_report(attempt, task_count, **report_kwargs)
    stage = attempt / ".accepted-staging"
    os.rename(report.parent, stage)
    report = stage / "source-report.json"
    matrix = write_hs_expected_matrix.cpu_document(report, "bench-control-plane")
    (stage / "expected-hs-cpu-matrix.json").write_text(
        json.dumps(matrix, indent=2, sort_keys=True) + "\n"
    )
    source = matrix["source"]
    (stage / "status.txt").write_text(
        write_hs_expected_matrix.expected_source_status(source)
    )
    completion = write_hs_expected_matrix.completion_document(stage)
    (stage / "source-completion.json").write_text(
        json.dumps(completion, indent=2, sort_keys=True) + "\n"
    )
    write_hs_expected_matrix.verify_source_completion(
        stage,
        require_published=False,
    )
    return stage


def initial_provenance(
    root: pathlib.Path,
    matrix: dict,
    *,
    parent_cpu_root: pathlib.Path | None = None,
    image: str = IMAGE,
) -> dict[str, str]:
    values = {
        "campaign_kind": matrix["kind"],
        "repo_head": "1" * 40,
        "tracked_diff_sha256": "2" * 64,
        "benchmark_source_sha256": "3" * 64,
        "historyserver_source_sha256": "4" * 64,
        "historyserver_manifest_sha256": "5" * 64,
        "expected_matrix_sha256": sha256(root / "expected-matrix.json"),
        "source_report_sha256": matrix["source"]["sourceReportSHA256"],
        "source_provenance_sha256": matrix["source"]["sourceProvenanceSHA256"],
        "source_session": matrix["source"]["spec"],
        "source_bucket": matrix["source"]["bucket"],
        "source_fingerprint_algorithm": validate_hs_sweep.SOURCE_FINGERPRINT_ALGORITHM,
        "historyserver_image_requested": image,
        "historyserver_build_source_sha256": "8" * 64,
        "historyserver_runtime_id": IMAGE_ID,
        "historyserver_runtime_build_source_sha256": "8" * 64,
        "source_collector_runtime_id": matrix["source"]["collectorRuntimeID"],
        "source_rayjob_owned": "true",
        "source_shutdown_after_job": "true",
        "source_job_ttl_seconds": "30",
    }
    if matrix["source"].get("sourceGeneration") is not None:
        values.update(
            {
                "source_completion_sha256": "c" * 64,
                "source_rayjob_backoff_limit": "0",
                "source_submitter_backoff_limit": "0",
            }
        )
    values.update(task_log_provenance(matrix["source"]))
    if parent_cpu_root is not None:
        values.update(
            {
                "cpu_campaign_expected_matrix_sha256": sha256(
                    parent_cpu_root / "expected-matrix.json"
                ),
                "cpu_campaign_final_provenance_sha256": sha256(
                    parent_cpu_root / "provenance-final.txt"
                ),
            }
        )
    return values


def write_kv(path: pathlib.Path, values: dict[str, str]) -> None:
    path.write_text("".join(f"{key}={value}\n" for key, value in values.items()))


def valid_task_query(source: dict, *, warm: bool) -> dict:
    task_count = source["expectedBenchmarkAttempts"]
    warm_limit = min(task_count, validate_hs_sweep.WARM_TASK_LIMIT_CAP)
    if warm:
        metadata = task_log_metadata(warm_limit, warm=True)
        return {
            "endpoint": validate_hs_sweep.warm_task_endpoint(warm_limit),
            "concurrency": 1,
            "limit": warm_limit,
            "httpStatus": 200,
            "latency": 100_000_000,
            "responseResult": True,
            "rows": warm_limit,
            "numFiltered": task_count,
            "distinctTaskIDs": warm_limit,
            "attemptZero": warm_limit,
            "finished": warm_limit,
            "taskLogMetadata": metadata,
            "expectedProjectionSHA256": metadata["sha256"],
            "projectionMatches": True,
            "valid": True,
            "problems": [],
        }
    return {
        "endpoint": validate_hs_sweep.TASK_COUNT_ENDPOINT,
        "concurrency": 1,
        "limit": 0,
        "httpStatus": 200,
        "latency": 20_000_000,
        "responseResult": True,
        "rows": 0,
        "numFiltered": task_count,
        "distinctTaskIDs": 0,
        "attemptZero": 0,
        "finished": 0,
        "valid": True,
        "problems": [],
    }


def valid_report(
    arm: dict,
    index: int,
    source: dict,
    *,
    fingerprint: str = SHA_A,
    peak_bytes: int = 768 * 1024 * 1024,
) -> dict:
    config = dict(arm["config"])
    namespace = f"hs-formal-{index:02d}"
    pod_uid = f"00000000-0000-0000-0000-{index:012d}"
    container_id = f"{index + 1:064x}"
    memory_bytes = validate_hs_sweep.parse_memory_bytes(config["HSMemoryLimit"])
    task_count = source["expectedBenchmarkAttempts"]
    warm_limit = min(task_count, validate_hs_sweep.WARM_TASK_LIMIT_CAP)
    return {
        "startedAt": "2026-08-08T12:00:00Z",
        "config": config,
        "env": {"nodes": []},
        "namespace": source["namespace"],
        "executionNamespace": namespace,
        "executionNamespaceUID": f"20000000-0000-0000-0000-{index:012d}",
        "clusterName": source["clusterName"],
        "sessionID": source["sessionID"],
        "job": {},
        "rayJobLifecycle": source["rayJobLifecycle"],
        "flushDuration": 0,
        "collectorLogs": None,
        "storageDiffs": None,
        "storage": {},
        "historyServer": {
            "listClusters": {},
            "enterColdLatency": 80_000_000_000,
            "enterMeasured": True,
            "enterStatus": 200,
            "enterAttempts": 1,
            "warmEndpoints": None,
            "notes": [],
        },
        "hsPodEvidence": {
            "executionNamespace": namespace,
            "podName": f"historyserver-{index:02d}",
            "podUID": pod_uid,
            "containerName": "historyserver",
            "containerID": container_id,
            "image": IMAGE,
            "imageID": "docker.io/library/historyserver@" + IMAGE_ID,
            "ready": True,
            "running": True,
            "restartCount": 0,
            "cpuRequest": config["HSCPURequest"],
            "cpuLimit": config["HSCPULimit"],
            "memoryRequest": config["HSMemoryRequest"],
            "memoryLimit": config["HSMemoryLimit"],
            "oomKilled": False,
            "terminationReasons": None,
            "cgroupObserved": True,
            "cgroupMemoryMax": str(memory_bytes),
            "cgroupMemoryMaxBytes": memory_bytes,
            "memoryEventsOOM": 0,
            "memoryEventsOOMKill": 0,
            "cgroupReadErrors": 0,
            "cgroupReadErrorFields": None,
            "valid": True,
            "problems": [],
        },
        "hsValidation": {
            "scope": dict(validate_hs_sweep.MEASUREMENT_CLAIM),
            "expectedTaskAttempts": task_count,
            "taskCountQuery": valid_task_query(source, warm=False),
            "warmTaskQuery": valid_task_query(source, warm=True),
            "fullReplay": {
                "status": "processed",
                "expectedAttempts": task_count,
                "observedAttempts": task_count,
                "distinctTaskIDs": task_count,
                "attemptZero": task_count,
                "finished": task_count,
                "taskLogMetadata": json.loads(
                    json.dumps(source["taskLogMetadata"])
                ),
                "errorCounts": zero_errors(),
                "totalErrors": 0,
                "valid": True,
                "problems": [],
            },
            "logs": {
                "errorCounts": zero_errors(),
                "totalErrors": 0,
                "valid": True,
                "problems": [],
            },
            "lifetimeMemoryPeakBytes": peak_bytes,
            "measurementValid": True,
            "meetsColdSLO": True,
            "valid": True,
            "problems": [],
        },
        "sourceSessionFingerprint": {
            "algorithm": validate_hs_sweep.SOURCE_FINGERPRINT_ALGORITHM,
            "bucket": validate_hs_sweep.SOURCE_BUCKET,
            "start": fingerprint,
            "end": fingerprint,
            "objectCount": source["expectedObjectCount"],
            "totalBytes": source["expectedTotalBytes"],
        },
        "historyServerSessions": None,
        "resources": None,
        "cgroupSampler": {
            "startAttempted": True,
            "started": True,
            "stopRequested": True,
            "streamEnded": True,
            "streamEndedBeforeStop": False,
            "streamComplete": True,
            "startError": "",
            "streamError": "",
        },
        "cgroups": [
            {
                "container": f"historyserver-{index:02d}/historyserver",
                "phase": "historyserver",
                "samples": 40,
                "peakAnonMiB": 700.0,
                "peakCurrentMiB": 740.0,
                "avgCores": 0.9,
                "peakCores": 1.0,
                "lifetimePeakMiB": 0,
                "lifetimePeakBytes": 0,
            },
            {
                "container": f"historyserver-{index:02d}/historyserver",
                "phase": "lifetime",
                "samples": 0,
                "peakAnonMiB": 700.0,
                "peakCurrentMiB": 740.0,
                "avgCores": 0.9,
                "peakCores": 1.0,
                "lifetimePeakMiB": peak_bytes / (1024 * 1024),
                "lifetimePeakBytes": peak_bytes,
            }
        ],
        "collectorWindows": None,
        "collectorIngressGates": None,
        "timeline": None,
        "podTerminations": None,
        "completed": True,
    }


def write_campaign(
    root: pathlib.Path,
    matrix: dict,
    *,
    parent_cpu_root: pathlib.Path | None = None,
    peaks_by_cpu: dict[str, int] | None = None,
    image: str = IMAGE,
) -> None:
    root.mkdir()
    matrix_path = root / "expected-matrix.json"
    matrix_path.write_text(json.dumps(matrix, indent=2, sort_keys=True) + "\n")
    provenance = initial_provenance(
        root, matrix, parent_cpu_root=parent_cpu_root, image=image
    )
    write_kv(root / "provenance.txt", provenance)
    provenance_sha = sha256(root / "provenance.txt")
    matrix_sha = sha256(matrix_path)

    status_lines = []
    for index, arm in enumerate(matrix["arms"]):
        name = arm["name"]
        arm_dir = root / name
        run_dir = arm_dir / "20260808-120000"
        run_dir.mkdir(parents=True)
        peak = 768 * 1024 * 1024
        if peaks_by_cpu is not None:
            peak = peaks_by_cpu[arm["config"]["HSCPURequest"]]
        report = valid_report(arm, index, matrix["source"], peak_bytes=peak)
        identity_path = arm_dir / "execution-namespace.json"
        report["config"]["ExecutionIdentityFile"] = str(identity_path.resolve())
        identity = {
            "name": report["executionNamespace"],
            "uid": report["executionNamespaceUID"],
        }
        identity_path.write_text(json.dumps(identity, indent=2, sort_keys=True) + "\n")
        (arm_dir / "namespace-cleanup-sentinel.txt").write_text(
            f"NAMESPACE-DELETED name={identity['name']} uid={identity['uid']} pods=0\n"
        )
        (arm_dir / "local-port-cleanup-sentinel.txt").write_text(
            "LOCAL-PORTS-FREE ports=19003,30080 consecutive=2\n"
        )
        (run_dir / "bench-report.json").write_text(
            json.dumps(
                report,
                indent=2,
                sort_keys=True,
            )
        )
        (arm_dir / "arm-sentinel.txt").write_text(
            f"ARM-SUCCEEDED name={name} rc=0\n"
        )
        arm_provenance = {
            "arm": name,
            "expected_matrix_sha256": matrix_sha,
            "initial_provenance_sha256": provenance_sha,
            "historyserver_runtime_id": IMAGE_ID,
            "historyserver_runtime_build_source_sha256": "8" * 64,
            "source_collector_runtime_id": matrix["source"]["collectorRuntimeID"],
            "source_rayjob_owned": "true",
            "source_shutdown_after_job": "true",
            "source_job_ttl_seconds": "30",
        }
        if matrix["source"].get("sourceGeneration") is not None:
            arm_provenance.update(
                {
                    "source_completion_sha256": "c" * 64,
                    "source_rayjob_backoff_limit": "0",
                    "source_submitter_backoff_limit": "0",
                }
            )
        arm_provenance.update(task_log_provenance(matrix["source"]))
        write_kv(
            arm_dir / "arm-provenance.txt",
            arm_provenance,
        )
        status_lines.append(f"{name} rc=0 duration=90s")
    status_lines.append(f"SWEEP-SUCCEEDED arms={len(matrix['arms'])}")
    (root / "status.txt").write_text("\n".join(status_lines) + "\n")
    final = dict(provenance)
    final["revalidation_status"] = "valid"
    final["source_fingerprint_sha256"] = SHA_A
    write_kv(root / "provenance-final.txt", final)


def write_go_serialized_fixture(output: pathlib.Path) -> None:
    """Marshal the fixture with the real package-local Go structs."""
    historyserver_root = pathlib.Path(__file__).resolve().parents[3]
    # Overlay only removes unrelated tests from this focused contract check; the
    # fixture itself is produced by TestWriteHSFormalReportFixture using the real
    # Report and nested Go types in hs_validation_test.go.
    with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
        temp = pathlib.Path(tmp)
        stub = temp / "hsbench_query_test.go"
        stub.write_text("package benchmark\n")
        replaced = historyserver_root / "test/benchmark/hsbench_query_test.go"
        overlay = temp / "overlay.json"
        overlay.write_text(
            json.dumps({"Replace": {str(replaced.resolve()): str(stub.resolve())}})
        )
        env = dict(os.environ)
        # A stale explicit GOROOT may not match the selected `go` binary. Keep
        # the caller's module cache, but isolate the writable build cache in the
        # test temporary directory.
        env.pop("GOROOT", None)
        env["GOCACHE"] = str(temp / "gocache")
        env["BENCH_HS_VALIDATOR_FIXTURE_OUT"] = str(output.resolve())
        result = subprocess.run(
            [
                "go",
                "test",
                f"-overlay={overlay}",
                "./test/benchmark",
                "-run",
                "^TestWriteHSFormalReportFixture$",
                "-count=1",
            ],
            cwd=historyserver_root,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    if result.returncode != 0:
        raise AssertionError(
            "Go fixture serialization failed:\n"
            + result.stdout
            + "\n"
            + result.stderr
        )
    if not output.is_file():
        raise AssertionError("Go fixture test did not produce JSON")


class HSSweepTest(unittest.TestCase):
    def make_cpu(self, base: pathlib.Path, task_count: int = 50_000) -> pathlib.Path:
        report = source_report(base, task_count)
        matrix = write_hs_expected_matrix.cpu_document(report, "bench-control-plane")
        root = base / f"cpu-{task_count}"
        write_campaign(
            root,
            matrix,
            peaks_by_cpu={
                "500m": 640 * 1024 * 1024,
                "1": 600 * 1024 * 1024,
                "2": 580 * 1024 * 1024,
                "4": 560 * 1024 * 1024,
            },
        )
        return root

    def test_source_writer_requires_full_collector_and_task_validity(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp)
            for task_count in (1_000, 5_000, 10_000, 50_000):
                with self.subTest(task_count=task_count):
                    report_path = source_report(base, task_count)
                    document = write_hs_expected_matrix.source_document(report_path)
                    self.assertEqual(document["kind"], "hs-source")
                    self.assertEqual(
                        document["source"]["expectedBenchmarkAttempts"], task_count
                    )
                    self.assertEqual(
                        document["source"]["bucket"], validate_hs_sweep.SOURCE_BUCKET
                    )
                    self.assertEqual(
                        document["source"]["storageIsolation"]["label"],
                        "full-lifecycle (T2-pre-start)",
                    )
                    self.assertEqual(
                        document["source"]["taskLogMetadata"],
                        task_log_metadata(task_count),
                    )
                    self.assertEqual(
                        document["claim"], validate_hs_sweep.MEASUREMENT_CLAIM
                    )
                    (
                        expected_wave,
                        _expected_rate,
                        expected_variant,
                        expected_rejected,
                        _expected_rejected_attempts,
                        _expected_collector_request,
                        _expected_collector_limit,
                    ) = (
                        write_hs_expected_matrix.expected_source_generation_controls(
                            task_count
                        )
                    )
                    generation = document["source"]["sourceGeneration"]
                    self.assertEqual(generation["waveSize"], expected_wave)
                    self.assertEqual(generation["pacingVariant"], expected_variant)
                    self.assertEqual(
                        generation["rejectedBaselineReportSHA256"],
                        expected_rejected,
                    )
                    self.assertEqual(
                        generation["lineageSHA256"],
                        sha256(report_path.parent / "lineage.json"),
                    )

            report_path = source_report(base, 5_000)
            report = json.loads(report_path.read_text())
            report["collectorIngressGates"][0]["valid"] = False
            report["collectorIngressGates"][0]["problems"] = ["incomplete stream"]
            report_path.write_text(json.dumps(report))
            with self.assertRaisesRegex(SystemExit, "Collector validity gate failed"):
                write_hs_expected_matrix.source_document(report_path)

            report_path = source_report(base, 5_000)
            report = json.loads(report_path.read_text())
            report["collectorLogs"][0]["imageID"] = "garbage-" + COLLECTOR_IMAGE_ID
            report_path.write_text(json.dumps(report))
            with self.assertRaisesRegex(SystemExit, "imageID differs from provenance"):
                write_hs_expected_matrix.source_document(report_path)

            report_path = source_report(base, 50_000)
            report = json.loads(report_path.read_text())
            report["collectorLogs"][0]["cpuLimit"] = "1"
            report_path.write_text(json.dumps(report))
            with self.assertRaisesRegex(SystemExit, "Collector cpuLimit differs"):
                write_hs_expected_matrix.source_document(report_path)

            report_path = source_report(base, 5_000)
            report = json.loads(report_path.read_text())
            report["rayJobLifecycle"]["ttlSecondsAfterFinished"] = 0
            report_path.write_text(json.dumps(report))
            with self.assertRaisesRegex(SystemExit, "rayJobLifecycle differs"):
                write_hs_expected_matrix.source_document(report_path)

            for field, value in (
                ("rayJobBackoffLimit", None),
                ("rayJobBackoffLimit", 1),
                ("rayJobBackoffLimit", True),
                ("submitterBackoffLimit", None),
                ("submitterBackoffLimit", 2),
                ("submitterBackoffLimit", False),
            ):
                with self.subTest(retry_field=field, retry_value=value):
                    report_path = source_report(base, 5_000)
                    report = json.loads(report_path.read_text())
                    report["rayJobLifecycle"][field] = value
                    report_path.write_text(json.dumps(report))
                    with self.assertRaisesRegex(SystemExit, "rayJobLifecycle differs"):
                        write_hs_expected_matrix.source_document(report_path)
            for field in ("rayJobBackoffLimit", "submitterBackoffLimit"):
                with self.subTest(missing_retry_field=field):
                    report_path = source_report(base, 5_000)
                    report = json.loads(report_path.read_text())
                    report["rayJobLifecycle"].pop(field)
                    report_path.write_text(json.dumps(report))
                    with self.assertRaisesRegex(SystemExit, "rayJobLifecycle differs"):
                        write_hs_expected_matrix.source_document(report_path)

            exact_type_attacks = {
                "completed integer": lambda report: report.__setitem__(
                    "completed", 1
                ),
                "compression integer": lambda report: report["config"].__setitem__(
                    "Compression", 1
                ),
                "drain boolean": lambda report: report["config"].__setitem__(
                    "DrainSleepSec", False
                ),
                "ttl float": lambda report: report["config"].__setitem__(
                    "JobTTLSeconds", 30.0
                ),
                "skip cleanup integer": lambda report: report["config"].__setitem__(
                    "SkipCleanup", 1
                ),
                "validity integer": lambda report: report[
                    "storage"
                ]["events"]["benchTaskValidity"].__setitem__("valid", 1),
                "observed attempts float": lambda report: report[
                    "storage"
                ]["events"]["benchTaskValidity"].__setitem__(
                    "observedAttempts", 5_000.0
                ),
                "attempt zero float": lambda report: report[
                    "storage"
                ]["events"]["benchTaskValidity"].__setitem__(
                    "attemptZero", 5_000.0
                ),
                "finished attempts float": lambda report: report[
                    "storage"
                ]["events"]["benchTaskValidity"].__setitem__(
                    "finishedAttempts", 5_000.0
                ),
                "bench task IDs short": lambda report: report["storage"][
                    "events"
                ].__setitem__("benchTaskIDs", 4_999),
                "observed task IDs short": lambda report: report["storage"][
                    "events"
                ]["benchTaskValidity"].__setitem__("observedTaskIDs", 4_999),
                "submitted attempts short": lambda report: report["storage"][
                    "events"
                ]["benchTaskValidity"].__setitem__(
                    "submittedToWorkerAttempts", 4_999
                ),
                "finished transitions short": lambda report: report["storage"][
                    "events"
                ]["benchTaskValidity"].__setitem__(
                    "finishedTransitionAttempts", 4_999
                ),
                "non-finished attempt": lambda report: report["storage"][
                    "events"
                ]["benchTaskValidity"].__setitem__("nonFinishedAttempts", 1),
                "definition mix short": lambda report: report["storage"][
                    "events"
                ]["countByType"].__setitem__("TASK_DEFINITION_EVENT", 5_004),
                "profile mix outside envelope": lambda report: report["storage"][
                    "events"
                ]["countByType"].__setitem__("TASK_PROFILE_EVENT", 1),
                "raw bytes outside envelope": lambda report: report["storage"][
                    "events"
                ].__setitem__("rawJSONLBytes", 1),
            }
            for name, mutate in exact_type_attacks.items():
                with self.subTest(exact_type_attack=name):
                    report_path = source_report(base, 5_000)
                    report = json.loads(report_path.read_text())
                    mutate(report)
                    report_path.write_text(json.dumps(report))
                    with self.assertRaisesRegex(SystemExit, "differs"):
                        write_hs_expected_matrix.source_document(report_path)

    def test_source_contract_runtime_values_are_typed_and_fixed_arity(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp)
            for task_count in (10_000, 50_000):
                with self.subTest(task_count=task_count):
                    report = source_report(base, task_count)
                    contract_path = report.parent / "source-contract.json"
                    contract = json.loads(contract_path.read_text())
                    source = contract["source"]
                    generation = source["sourceGeneration"]
                    lifecycle = source["rayJobLifecycle"]
                    values = write_hs_expected_matrix.source_contract_runtime_values(
                        contract_path
                    )
                    self.assertEqual(len(values), 15)
                    self.assertEqual(
                        values,
                        (
                            "hs-source",
                            source["spec"],
                            str(task_count),
                            validate_hs_sweep.SOURCE_BUCKET,
                            str(2_000),
                            "1",
                            str(500 if task_count == 50_000 else 0),
                            (
                                "single-driver-rate500-wave2000-v2"
                                if task_count == 50_000
                                else "baseline-wave2000-v1"
                            ),
                            str(task_count),
                            str(generation["driverWallSec"]),
                            str(generation["driverRateTPS"]),
                            generation["lineageSHA256"],
                            (
                                write_hs_expected_matrix.SOURCE_N50_REJECTED_BASELINE_REPORT_SHA256
                                if task_count == 50_000
                                else "none"
                            ),
                            str(lifecycle["rayJobBackoffLimit"]),
                            str(lifecycle["submitterBackoffLimit"]),
                        ),
                    )

            baseline_path = source_report(base, 50_000).parent / "source-contract.json"
            baseline = json.loads(baseline_path.read_text())
            attacks = {
                "float schema": lambda doc: doc.__setitem__("schemaVersion", 5.0),
                "boolean schema": lambda doc: doc.__setitem__(
                    "schemaVersion", True
                ),
                "missing top field": lambda doc: doc.pop("claim"),
                "extra top field": lambda doc: doc.__setitem__("unexpected", 1),
                "missing source field": lambda doc: doc["source"].pop(
                    "sourceGeneration"
                ),
                "extra source field": lambda doc: doc["source"].__setitem__(
                    "unexpected", 1
                ),
                "float attempts": lambda doc: doc["source"].__setitem__(
                    "expectedBenchmarkAttempts", 50_000.0
                ),
                "boolean retry": lambda doc: doc["source"][
                    "rayJobLifecycle"
                ].__setitem__("rayJobBackoffLimit", False),
            }
            for name, mutate in attacks.items():
                with self.subTest(contract_attack=name):
                    attack_path = base.resolve() / (
                        "contract-" + name.replace(" ", "-") + ".json"
                    )
                    document = json.loads(json.dumps(baseline))
                    mutate(document)
                    attack_path.write_text(json.dumps(document))
                    with self.assertRaises(SystemExit):
                        write_hs_expected_matrix.source_contract_runtime_values(
                            attack_path
                        )

            malformed = base.resolve() / "contract-malformed.json"
            malformed.write_text("{")
            with self.assertRaisesRegex(SystemExit, "cannot read source contract"):
                write_hs_expected_matrix.source_contract_runtime_values(malformed)

            matrix = write_hs_expected_matrix.cpu_document(
                source_report(base, 50_000),
                "bench-control-plane",
            )
            for value in (5.0, True):
                with self.subTest(matrix_schema_version=value):
                    attacked = json.loads(json.dumps(matrix))
                    attacked["schemaVersion"] = value
                    with self.assertRaisesRegex(
                        validate_hs_sweep.ValidationError,
                        "schemaVersion",
                    ):
                        validate_hs_sweep.validate_matrix(attacked, "hs-cpu")

    def test_source_bundle_completion_is_atomic_and_fail_closed(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp).resolve()
            stage = source_staging_bundle(base)
            accepted = stage.parent / "accepted"
            stage_identity = write_hs_expected_matrix._directory_identity(
                os.lstat(stage)
            )
            write_hs_expected_matrix.publish_source_bundle(stage, accepted)
            self.assertFalse(stage.exists())
            self.assertEqual(
                write_hs_expected_matrix._directory_identity(os.lstat(accepted)),
                stage_identity,
            )
            completion = write_hs_expected_matrix.verify_source_completion(
                accepted,
                require_published=True,
            )
            self.assertEqual(completion["status"], "accepted")
            self.assertEqual(completion["taskCount"], 50_000)
            self.assertEqual(
                completion["retryControls"],
                {"rayJobBackoffLimit": 0, "submitterBackoffLimit": 0},
            )

        attacks = {
            "missing status": lambda stage: (stage / "status.txt").unlink(),
            "truncated provenance": lambda stage: (
                stage / "provenance-final.txt"
            ).write_text("revalidation_status=valid\n"),
            "extra file": lambda stage: (stage / "unexpected.txt").write_text("x"),
        }
        for name, mutate in attacks.items():
            with self.subTest(attack=name), tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
                stage = source_staging_bundle(pathlib.Path(tmp))
                mutate(stage)
                with self.assertRaises(SystemExit):
                    write_hs_expected_matrix.verify_source_completion(
                        stage,
                        require_published=False,
                    )

        bundle_type_attacks = {
            "completed integer": lambda report: report.__setitem__(
                "completed", 1
            ),
            "skip cleanup integer": lambda report: report["config"].__setitem__(
                "SkipCleanup", 1
            ),
            "observed attempts float": lambda report: report[
                "storage"
            ]["events"]["benchTaskValidity"].__setitem__(
                "observedAttempts", 50_000.0
            ),
        }
        for name, mutate in bundle_type_attacks.items():
            with self.subTest(
                bundle_type_attack=name
            ), tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
                stage = source_staging_bundle(pathlib.Path(tmp))
                report_path = stage / "source-report.json"
                report = json.loads(report_path.read_text())
                mutate(report)
                report_path.write_text(json.dumps(report))
                completion_path = stage / "source-completion.json"
                completion = json.loads(completion_path.read_text())
                completion["artifacts"]["sourceReport"]["sha256"] = sha256(
                    report_path
                )
                completion_path.write_text(json.dumps(completion))
                with self.assertRaisesRegex(SystemExit, "differs"):
                    write_hs_expected_matrix.verify_source_completion(
                        stage,
                        require_published=False,
                    )

        for value in (5.0, True):
            with self.subTest(
                bundle_contract_schema_version=value
            ), tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
                stage = source_staging_bundle(pathlib.Path(tmp))
                contract_path = stage / "source-contract.json"
                contract = json.loads(contract_path.read_text())
                contract["schemaVersion"] = value
                contract_path.write_text(json.dumps(contract))

                final_path = stage / "provenance-final.txt"
                final = write_hs_expected_matrix.read_provenance(
                    final_path,
                    require_revalidated=True,
                )
                final["source_contract_sha256"] = sha256(contract_path)
                write_kv(final_path, final)

                matrix_path = stage / "expected-hs-cpu-matrix.json"
                matrix = json.loads(matrix_path.read_text())
                matrix["source"]["sourceProvenanceSHA256"] = sha256(final_path)
                matrix_path.write_text(json.dumps(matrix))

                completion_path = stage / "source-completion.json"
                completion = json.loads(completion_path.read_text())
                completion["artifacts"]["sourceContract"]["sha256"] = sha256(
                    contract_path
                )
                completion["artifacts"]["finalProvenance"]["sha256"] = sha256(
                    final_path
                )
                completion["artifacts"]["expectedCPUMatrix"]["sha256"] = sha256(
                    matrix_path
                )
                completion_path.write_text(json.dumps(completion))
                with self.assertRaisesRegex(SystemExit, "source contract differs"):
                    write_hs_expected_matrix.verify_source_completion(
                        stage,
                        require_published=False,
                    )

        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            stage = source_staging_bundle(pathlib.Path(tmp))
            external = stage.parent / "external-status.txt"
            external.write_text((stage / "status.txt").read_text())
            (stage / "status.txt").unlink()
            (stage / "status.txt").symlink_to(external)
            with self.assertRaisesRegex(SystemExit, "symlink"):
                write_hs_expected_matrix.verify_source_completion(
                    stage,
                    require_published=False,
                )

        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            stage = source_staging_bundle(pathlib.Path(tmp))
            child = stage.parent / "child"
            child.mkdir()
            traversed = child / ".." / stage.name
            with self.assertRaisesRegex(SystemExit, "canonical absolute path"):
                write_hs_expected_matrix.verify_source_completion(
                    traversed,
                    require_published=False,
                )

        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            stage = source_staging_bundle(pathlib.Path(tmp))
            path = stage / "source-completion.json"
            completion = json.loads(path.read_text())
            completion["artifacts"]["status"]["path"] = "../status.txt"
            path.write_text(json.dumps(completion))
            with self.assertRaisesRegex(SystemExit, "completion differs"):
                write_hs_expected_matrix.verify_source_completion(
                    stage,
                    require_published=False,
                )

        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            stage = source_staging_bundle(pathlib.Path(tmp))
            accepted = stage.parent / "accepted"
            accepted.mkdir()
            with self.assertRaisesRegex(SystemExit, "refusing to replace"):
                write_hs_expected_matrix.publish_source_bundle(stage, accepted)
            self.assertTrue(stage.is_dir())

        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp).resolve()
            real_parent = base / "real-parent"
            real_parent.mkdir()
            stage = source_staging_bundle(real_parent)
            alias = base / "parent-link"
            alias.symlink_to(stage.parent, target_is_directory=True)
            linked_stage = alias / stage.name
            with self.assertRaisesRegex(SystemExit, "symlink-free"):
                write_hs_expected_matrix.verify_source_completion(
                    linked_stage,
                    require_published=False,
                )
            with self.assertRaisesRegex(SystemExit, "symlink-free"):
                write_hs_expected_matrix.completion_document(linked_stage)
            with self.assertRaisesRegex(SystemExit, "symlink-free"):
                write_hs_expected_matrix.source_bundle_paths(
                    linked_stage / "source-report.json",
                    linked_stage / "provenance-final.txt",
                )

        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp).resolve()
            real_root = base / "real-root"
            real_root.mkdir()
            stage = source_staging_bundle(real_root)
            nested = base / "outer" / "nested-link"
            nested.parent.mkdir()
            nested.symlink_to(stage.parent.parent, target_is_directory=True)
            linked_stage = nested / stage.parent.name / stage.name
            with self.assertRaisesRegex(SystemExit, "symlink-free"):
                write_hs_expected_matrix.verify_source_completion(
                    linked_stage,
                    require_published=False,
                )

        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp).resolve()
            stage = source_staging_bundle(base)
            accepted = stage.parent / "accepted"
            external = base / "external-accepted"
            external.mkdir()
            accepted.symlink_to(external, target_is_directory=True)
            with self.assertRaisesRegex(SystemExit, "symlink-free"):
                write_hs_expected_matrix.verify_source_completion(
                    accepted,
                    require_published=True,
                )
            with self.assertRaisesRegex(SystemExit, "symlink-free"):
                write_hs_expected_matrix.publish_source_bundle(stage, accepted)
            self.assertTrue(stage.is_dir())
            self.assertTrue(accepted.is_symlink())

        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp).resolve()
            stage = source_staging_bundle(base)
            accepted = stage.parent / "accepted"
            original_verify = write_hs_expected_matrix.verify_source_completion

            def retarget_parent(*args, **kwargs):
                completion = original_verify(*args, **kwargs)
                original_parent = stage.parent.with_name(stage.parent.name + "-original")
                attacker = base / "attacker-parent"
                attacker.mkdir()
                os.rename(stage.parent, original_parent)
                stage.parent.symlink_to(attacker, target_is_directory=True)
                return completion

            with mock.patch.object(
                write_hs_expected_matrix,
                "verify_source_completion",
                side_effect=retarget_parent,
            ):
                with self.assertRaisesRegex(SystemExit, "symlink-free"):
                    write_hs_expected_matrix.publish_source_bundle(stage, accepted)
            self.assertFalse((base / "attacker-parent" / "accepted").exists())
            self.assertTrue(
                (stage.parent.with_name(stage.parent.name + "-original") / stage.name).is_dir()
            )

        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp).resolve()
            stage = source_staging_bundle(base)
            accepted = stage.parent / "accepted"
            original_verify = write_hs_expected_matrix.verify_source_completion

            def replace_stage(*args, **kwargs):
                completion = original_verify(*args, **kwargs)
                displaced = stage.parent / ".accepted-staging-original"
                os.rename(stage, displaced)
                stage.mkdir()
                return completion

            with mock.patch.object(
                write_hs_expected_matrix,
                "verify_source_completion",
                side_effect=replace_stage,
            ):
                with self.assertRaisesRegex(SystemExit, "identity changed"):
                    write_hs_expected_matrix.publish_source_bundle(stage, accepted)
            self.assertFalse(accepted.exists())
            self.assertTrue((stage.parent / ".accepted-staging-original").is_dir())

        completion_retry_attacks = (
            ("rayJobBackoffLimit", None),
            ("rayJobBackoffLimit", 1),
            ("rayJobBackoffLimit", True),
            ("submitterBackoffLimit", None),
            ("submitterBackoffLimit", 2),
            ("submitterBackoffLimit", False),
        )
        for field, value in completion_retry_attacks:
            with self.subTest(
                completion_retry_field=field,
                completion_retry_value=value,
            ), tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
                stage = source_staging_bundle(pathlib.Path(tmp))
                path = stage / "source-completion.json"
                completion = json.loads(path.read_text())
                completion["retryControls"][field] = value
                path.write_text(json.dumps(completion))
                with self.assertRaisesRegex(SystemExit, "completion differs"):
                    write_hs_expected_matrix.verify_source_completion(
                        stage,
                        require_published=False,
                    )

    def test_atomic_publication_rejects_destination_creation_at_rename_boundary(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            stage = source_staging_bundle(pathlib.Path(tmp))
            accepted = stage.parent / "accepted"
            original_rename = write_hs_expected_matrix._rename_directory_exclusive

            def create_destination(source_name, destination_name, parent_fd, destination):
                os.mkdir(destination_name, dir_fd=parent_fd)
                original_rename(
                    source_name,
                    destination_name,
                    parent_fd,
                    destination,
                )

            with mock.patch.object(
                write_hs_expected_matrix,
                "_rename_directory_exclusive",
                side_effect=create_destination,
            ):
                with self.assertRaisesRegex(SystemExit, "refusing to replace"):
                    write_hs_expected_matrix.publish_source_bundle(stage, accepted)
            self.assertTrue(stage.is_dir())
            self.assertTrue(accepted.is_dir())
            self.assertEqual(list(accepted.iterdir()), [])

    def test_atomic_publication_detects_source_swap_at_rename_boundary(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            stage = source_staging_bundle(pathlib.Path(tmp))
            accepted = stage.parent / "accepted"
            displaced_name = ".accepted-staging-original"
            original_rename = write_hs_expected_matrix._rename_directory_exclusive

            def replace_source(source_name, destination_name, parent_fd, destination):
                os.rename(
                    source_name,
                    displaced_name,
                    src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd,
                )
                os.mkdir(source_name, dir_fd=parent_fd)
                original_rename(
                    source_name,
                    destination_name,
                    parent_fd,
                    destination,
                )

            with mock.patch.object(
                write_hs_expected_matrix,
                "_rename_directory_exclusive",
                side_effect=replace_source,
            ):
                with self.assertRaisesRegex(
                    SystemExit,
                    "published source identity differs",
                ):
                    write_hs_expected_matrix.publish_source_bundle(stage, accepted)
            self.assertTrue((stage.parent / displaced_name).is_dir())
            self.assertTrue(accepted.is_dir())
            self.assertEqual(list(accepted.iterdir()), [])

    def test_atomic_publication_fails_closed_when_syscall_is_unavailable(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            stage = source_staging_bundle(pathlib.Path(tmp))
            accepted = stage.parent / "accepted"
            with mock.patch.object(
                write_hs_expected_matrix.ctypes,
                "CDLL",
                side_effect=OSError("missing renameatx_np"),
            ):
                with self.assertRaisesRegex(SystemExit, "requires available"):
                    write_hs_expected_matrix.publish_source_bundle(stage, accepted)
            self.assertTrue(stage.is_dir())
            self.assertFalse(accepted.exists())

    def test_atomic_publication_closes_pinned_fds_on_failure(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            stage = source_staging_bundle(pathlib.Path(tmp))
            accepted = stage.parent / "accepted"
            captured_parent_fds = []
            captured_stage_fds = []
            original_open = os.open

            def track_open(path, flags, *args, **kwargs):
                descriptor = original_open(path, flags, *args, **kwargs)
                if path == stage.name and kwargs.get("dir_fd") is not None:
                    captured_stage_fds.append(descriptor)
                return descriptor

            def fail_rename(source_name, destination_name, parent_fd, destination):
                captured_parent_fds.append(parent_fd)
                raise SystemExit("injected atomic publication failure")

            with mock.patch.object(os, "open", side_effect=track_open), mock.patch.object(
                write_hs_expected_matrix,
                "_rename_directory_exclusive",
                side_effect=fail_rename,
            ):
                with self.assertRaisesRegex(SystemExit, "injected"):
                    write_hs_expected_matrix.publish_source_bundle(stage, accepted)
            self.assertTrue(captured_parent_fds)
            self.assertTrue(captured_stage_fds)
            for descriptor in captured_parent_fds + captured_stage_fds:
                with self.assertRaises(OSError) as closed:
                    os.fstat(descriptor)
                self.assertEqual(closed.exception.errno, errno.EBADF)

    def test_canonical_directory_guard_supports_private_tmp_and_rejects_missing(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            target = pathlib.Path(tmp).resolve()
            descriptor = write_hs_expected_matrix._open_directory_without_symlinks(
                target,
                "test directory",
            )
            try:
                self.assertEqual(
                    write_hs_expected_matrix._directory_identity(os.fstat(descriptor)),
                    write_hs_expected_matrix._directory_identity(os.lstat(target)),
                )
            finally:
                os.close(descriptor)

            with self.assertRaisesRegex(SystemExit, "existing symlink-free"):
                write_hs_expected_matrix._open_directory_without_symlinks(
                    target / "missing",
                    "test directory",
                )

    def test_canonical_directory_guard_rejects_tmp_alias(self):
        with self.assertRaisesRegex(SystemExit, "canonical absolute path"):
            write_hs_expected_matrix.require_canonical_absolute_path(
                pathlib.Path("//private/tmp"),
                "test directory",
            )
        if not pathlib.Path("/tmp").is_symlink():
            self.skipTest("/tmp is not a symlink on this host")
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            canonical = pathlib.Path(tmp).resolve()
            alias = pathlib.Path("/tmp") / canonical.relative_to("/private/tmp")
            with self.assertRaisesRegex(SystemExit, "canonical bundle root"):
                write_hs_expected_matrix.require_canonical_absolute_path(
                    alias,
                    "test directory",
                )

    def test_canonical_directory_guard_rejects_in_walk_ancestor_replacement(self):
        for replacement in ("symlink", "directory"):
            with self.subTest(replacement=replacement), tempfile.TemporaryDirectory(
                dir="/private/tmp"
            ) as tmp:
                base = pathlib.Path(tmp).resolve()
                ancestor = base / "ancestor"
                target = ancestor / "target"
                target.mkdir(parents=True)
                attacker = base / "attacker"
                (attacker / "target").mkdir(parents=True)
                original_ancestor = base / "ancestor-original"
                original_snapshot = write_hs_expected_matrix._lstat_directory_chain
                calls = 0

                def replace_after_snapshot(path, context):
                    nonlocal calls
                    result = original_snapshot(path, context)
                    calls += 1
                    if calls == 1:
                        os.rename(ancestor, original_ancestor)
                        if replacement == "symlink":
                            ancestor.symlink_to(attacker, target_is_directory=True)
                        else:
                            (ancestor / "target").mkdir(parents=True)
                    return result

                with mock.patch.object(
                    write_hs_expected_matrix,
                    "_lstat_directory_chain",
                    side_effect=replace_after_snapshot,
                ):
                    with self.assertRaisesRegex(
                        SystemExit,
                        "symlink-free|identity changed",
                    ):
                        write_hs_expected_matrix._open_directory_without_symlinks(
                            target,
                            "test directory",
                        )

    def test_canonical_directory_guard_checks_root_and_private_ancestors(self):
        original_lstat = os.lstat
        for component in (pathlib.Path("/"), pathlib.Path("/private")):
            for replacement_mode in (stat.S_IFLNK | 0o777, stat.S_IFREG | 0o600):
                with self.subTest(
                    component=component,
                    replacement_mode=replacement_mode,
                ), tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
                    resolved_tmp = pathlib.Path(tmp).resolve()

                    def replace_mode(path):
                        observed = original_lstat(path)
                        if pathlib.Path(path) == component:
                            fields = list(observed)
                            fields[0] = replacement_mode
                            return os.stat_result(fields)
                        return observed

                    with mock.patch.object(os, "lstat", side_effect=replace_mode):
                        with self.assertRaisesRegex(SystemExit, "symlink-free"):
                            write_hs_expected_matrix._open_directory_without_symlinks(
                                resolved_tmp,
                                "test directory",
                            )

        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            private_calls = 0

            def replace_private_identity(path):
                nonlocal private_calls
                observed = original_lstat(path)
                if pathlib.Path(path) == pathlib.Path("/private"):
                    private_calls += 1
                    if private_calls == 2:
                        fields = list(observed)
                        fields[1] += 1
                        return os.stat_result(fields)
                return observed

            with mock.patch.object(
                os,
                "lstat",
                side_effect=replace_private_identity,
            ):
                with self.assertRaisesRegex(SystemExit, "identity changed"):
                    write_hs_expected_matrix._open_directory_without_symlinks(
                        pathlib.Path(tmp).resolve(),
                        "test directory",
                    )
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            stage = source_staging_bundle(pathlib.Path(tmp))
            path = stage / "source-completion.json"
            completion = json.loads(path.read_text())
            completion["retryControls"].pop("submitterBackoffLimit")
            path.write_text(json.dumps(completion))
            with self.assertRaisesRegex(SystemExit, "completion differs"):
                write_hs_expected_matrix.verify_source_completion(
                    stage,
                    require_published=False,
                )

    def test_source_bundle_rejects_report_provenance_and_completion_swaps(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp)
            report_a = source_report(
                base,
                session_id="session_a",
                driver_wall_sec=100.0,
                compact_lineage=False,
            )
            report_b = source_report(
                base,
                session_id="session_b",
                driver_wall_sec=101.0,
                compact_lineage=True,
            )
            self.assertNotEqual(
                sha256(report_a.parent / "lineage.json"),
                sha256(report_b.parent / "lineage.json"),
            )
            (report_a.parent / "provenance-final.txt").write_bytes(
                (report_b.parent / "provenance-final.txt").read_bytes()
            )
            with self.assertRaises(SystemExit):
                write_hs_expected_matrix.cpu_document(
                    report_a,
                    "bench-control-plane",
                )

        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp)
            stage_a = source_staging_bundle(
                base,
                session_id="session_a",
                driver_wall_sec=100.0,
                compact_lineage=False,
            )
            stage_b = source_staging_bundle(
                base,
                session_id="session_b",
                driver_wall_sec=101.0,
                compact_lineage=True,
            )
            (stage_a / "source-completion.json").write_bytes(
                (stage_b / "source-completion.json").read_bytes()
            )
            with self.assertRaisesRegex(SystemExit, "completion differs"):
                write_hs_expected_matrix.verify_source_completion(
                    stage_a,
                    require_published=False,
                )

    def test_source_generation_writer_rejects_control_and_provenance_drift(self):
        def write_provenance(path: pathlib.Path, values: dict[str, str]) -> None:
            path.write_text(
                "".join(f"{key}={value}\n" for key, value in values.items())
            )

        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp)
            n50_path = source_report(base, 50_000)
            baseline_report = json.loads(n50_path.read_text())
            provenance_path = n50_path.parent / "provenance-final.txt"
            baseline_provenance = write_hs_expected_matrix.read_provenance(
                provenance_path,
                require_revalidated=False,
            )
            report_attacks = {
                "rejected wave 100": lambda doc: doc["config"].__setitem__(
                    "WaveSize", 100
                ),
                "wave 500": lambda doc: doc["config"].__setitem__("WaveSize", 500),
                "wave 250": lambda doc: doc["config"].__setitem__("WaveSize", 250),
                "multiple drivers": lambda doc: doc["config"].__setitem__(
                    "Drivers", 2
                ),
                "boolean drivers": lambda doc: doc["config"].__setitem__(
                    "Drivers", True
                ),
                "target rate": lambda doc: doc["config"].__setitem__(
                    "TargetTaskRate", 1
                ),
                "missing collector request": lambda doc: doc["config"].__setitem__(
                    "CollectorCPURequest", ""
                ),
                "wrong collector limit": lambda doc: doc["config"].__setitem__(
                    "CollectorCPU", "1"
                ),
                "driver task loss": lambda doc: doc["job"].__setitem__(
                    "driverTasks", 49_999
                ),
                "zero wall": lambda doc: doc["job"].__setitem__(
                    "driverWallSec", 0
                ),
                "nonfinite wall": lambda doc: doc["job"].__setitem__(
                    "driverWallSec", float("inf")
                ),
                "nonfinite rate": lambda doc: doc["job"].__setitem__(
                    "driverRateTPS", float("nan")
                ),
                "rate outside pacing band": lambda doc: doc["job"].__setitem__(
                    "driverRateTPS", 600.0
                ),
            }
            for name, mutate in report_attacks.items():
                with self.subTest(report_attack=name):
                    report = json.loads(json.dumps(baseline_report))
                    mutate(report)
                    n50_path.write_text(json.dumps(report))
                    write_provenance(provenance_path, baseline_provenance)
                    with self.assertRaises(SystemExit):
                        write_hs_expected_matrix.cpu_document(
                            n50_path, "bench-control-plane"
                        )

            provenance_attacks = {
                "wrong task count": ("source_task_count", "10000"),
                "wrong wave": ("source_wave_size", "100"),
                "numeric alias": ("source_wave_size", "0100"),
                "wrong driver count": ("source_drivers", "2"),
                "ambient target rate": ("source_target_task_rate", "1000"),
                "wrong collector request": (
                    "source_collector_cpu_request",
                    "250m",
                ),
                "wrong collector limit": (
                    "source_collector_cpu_limit",
                    "1",
                ),
                "missing rejected attempts": (
                    "source_rejected_attempt_report_sha256s",
                    "none",
                ),
                "wrong variant": (
                    "source_pacing_variant",
                    write_hs_expected_matrix.SOURCE_BASELINE_PACING_VARIANT,
                ),
                "missing rejected baseline": (
                    "source_rejected_baseline_report_sha256",
                    "none",
                ),
                "other rejected baseline": (
                    "source_rejected_baseline_report_sha256",
                    "e" * 64,
                ),
                "malformed lineage": ("source_lineage_sha256", "not-a-sha256"),
            }
            for name, (key, value) in provenance_attacks.items():
                with self.subTest(provenance_attack=name):
                    n50_path.write_text(json.dumps(baseline_report))
                    provenance = dict(baseline_provenance)
                    provenance[key] = value
                    write_provenance(provenance_path, provenance)
                    with self.assertRaises(SystemExit):
                        write_hs_expected_matrix.cpu_document(
                            n50_path, "bench-control-plane"
                        )

            lower_path = source_report(base, 10_000)
            lower_report = json.loads(lower_path.read_text())
            lower_report["config"]["WaveSize"] = 100
            lower_path.write_text(json.dumps(lower_report))
            with self.assertRaisesRegex(SystemExit, "config.WaveSize differs"):
                write_hs_expected_matrix.cpu_document(
                    lower_path, "bench-control-plane"
                )

    def test_source_generation_matrix_gate_and_lower_n_legacy_compatibility(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp)
            baseline = write_hs_expected_matrix.cpu_document(
                source_report(base, 50_000), "bench-control-plane"
            )
            attacks = {
                "missing": lambda generation, source: source.pop(
                    "sourceGeneration"
                ),
                "wrong wave": lambda generation, source: generation.__setitem__(
                    "waveSize", 100
                ),
                "wrong driver count": lambda generation, source: generation.__setitem__(
                    "drivers", 2
                ),
                "boolean driver count": lambda generation, source: generation.__setitem__(
                    "drivers", True
                ),
                "target rate": lambda generation, source: generation.__setitem__(
                    "targetTaskRate", 1
                ),
                "wrong collector request": lambda generation, source: generation.__setitem__(
                    "collectorCPURequest", "250m"
                ),
                "wrong collector limit": lambda generation, source: generation.__setitem__(
                    "collectorCPULimit", "1"
                ),
                "missing rejected attempts": lambda generation, source: generation.__setitem__(
                    "rejectedAttemptReportSHA256s", []
                ),
                "wrong variant": lambda generation, source: generation.__setitem__(
                    "pacingVariant", "single-driver-wave250-v1"
                ),
                "driver loss": lambda generation, source: generation.__setitem__(
                    "driverTasks", 49_999
                ),
                "zero wall": lambda generation, source: generation.__setitem__(
                    "driverWallSec", 0
                ),
                "infinite rate": lambda generation, source: generation.__setitem__(
                    "driverRateTPS", float("inf")
                ),
                "rate outside pacing band": lambda generation, source: generation.__setitem__(
                    "driverRateTPS", 600.0
                ),
                "bad lineage": lambda generation, source: generation.__setitem__(
                    "lineageSHA256", "f" * 63
                ),
                "integer lineage": lambda generation, source: generation.__setitem__(
                    "lineageSHA256", int("9" * 64)
                ),
                "baseline swap": lambda generation, source: generation.__setitem__(
                    "rejectedBaselineReportSHA256", "e" * 64
                ),
                "extra field": lambda generation, source: generation.__setitem__(
                    "ambientWave", 500
                ),
                "float attempts": lambda generation, source: source.__setitem__(
                    "expectedBenchmarkAttempts", 50_000.0
                ),
                "boolean object count": lambda generation, source: source.__setitem__(
                    "expectedObjectCount", True
                ),
                "boolean total bytes": lambda generation, source: source.__setitem__(
                    "expectedTotalBytes", True
                ),
                "boolean RayJob retry": lambda generation, source: source[
                    "rayJobLifecycle"
                ].__setitem__("rayJobBackoffLimit", False),
                "missing submitter retry": lambda generation, source: source[
                    "rayJobLifecycle"
                ].pop("submitterBackoffLimit"),
            }
            for name, mutate in attacks.items():
                with self.subTest(attack=name):
                    matrix = json.loads(json.dumps(baseline))
                    source = matrix["source"]
                    generation = source.get("sourceGeneration")
                    mutate(generation, source)
                    with self.assertRaises(validate_hs_sweep.ValidationError):
                        validate_hs_sweep.validate_matrix(matrix, "hs-cpu")

            for task_count in (1_000, 5_000, 10_000):
                with self.subTest(legacy_task_count=task_count):
                    matrix = write_hs_expected_matrix.cpu_document(
                        source_report(base, task_count), "bench-control-plane"
                    )
                    matrix["source"].pop("sourceGeneration")
                    matrix["source"]["rayJobLifecycle"].pop(
                        "rayJobBackoffLimit"
                    )
                    matrix["source"]["rayJobLifecycle"].pop(
                        "submitterBackoffLimit"
                    )
                    for arm in matrix["arms"]:
                        arm["config"].pop("HSSourceRayJobBackoffLimit")
                        arm["config"].pop("HSSourceSubmitterBackoffLimit")
                    validate_hs_sweep.validate_matrix(matrix, "hs-cpu")

    def test_source_writer_rejects_storage_isolation_attacks(self):
        def missing_diffs(report: dict) -> None:
            report.pop("storageDiffs")

        def missing_window(report: dict) -> None:
            report["storageDiffs"].pop()

        def duplicate_window(report: dict) -> None:
            report["storageDiffs"][1]["label"] = "during-job (T1-T0)"

        def deletion(report: dict) -> None:
            report["storageDiffs"][0]["deletedObjects"] = 1
            report["storageDiffs"][0]["deletedKeys"] = ["immutable/session"]

        def unexpected_add(report: dict) -> None:
            report["storageDiffs"][0]["unexpectedKeys"] = ["immutable/new"]

        def same_size_etag_overwrite(report: dict) -> None:
            report["storageDiffs"][0]["changedObjects"] = 1
            report["storageDiffs"][0]["changedBytes"] = 0
            report["storageDiffs"][0]["unexpectedChangedKeys"] = [
                "immutable/same-size-overwrite"
            ]

        def missing_changed_evidence(report: dict) -> None:
            report["storageDiffs"][0].pop("unexpectedChangedKeys")

        def missing_lifecycle_gate(report: dict) -> None:
            report.pop("storageIsolation")

        def pre_t0_foreign_add_with_clean_phases(report: dict) -> None:
            report["storageIsolation"]["unexpectedKeys"] = ["immutable/pre-t0-add"]

        def pre_t0_same_size_overwrite_with_clean_phases(report: dict) -> None:
            report["storageIsolation"]["changedObjects"] = 1
            report["storageIsolation"]["changedBytes"] = 0
            report["storageIsolation"]["unexpectedChangedKeys"] = [
                "immutable/pre-t0-same-size-overwrite"
            ]

        def pre_t0_delete_with_clean_phases(report: dict) -> None:
            report["storageIsolation"]["deletedObjects"] = 1
            report["storageIsolation"]["deletedKeys"] = ["immutable/pre-t0-delete"]

        attacks = {
            "missing storageDiffs": missing_diffs,
            "missing window": missing_window,
            "duplicate window": duplicate_window,
            "deleted object": deletion,
            "unexpected addition": unexpected_add,
            "same-size ETag overwrite": same_size_etag_overwrite,
            "missing changed evidence": missing_changed_evidence,
            "missing lifecycle gate": missing_lifecycle_gate,
            "pre-T0 foreign add with clean phases": pre_t0_foreign_add_with_clean_phases,
            "pre-T0 same-size overwrite with clean phases": pre_t0_same_size_overwrite_with_clean_phases,
            "pre-T0 delete with clean phases": pre_t0_delete_with_clean_phases,
        }
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp)
            report_path = source_report(base)
            baseline = json.loads(report_path.read_text())
            for name, mutate in attacks.items():
                with self.subTest(attack=name):
                    report = json.loads(json.dumps(baseline))
                    mutate(report)
                    report_path.write_text(json.dumps(report))
                    with self.assertRaises(SystemExit):
                        write_hs_expected_matrix.source_document(report_path)
                    with self.assertRaises(SystemExit):
                        write_hs_expected_matrix.cpu_document(
                            report_path, "bench-control-plane"
                        )

    def test_cpu_writer_emits_exact_interleaved_matrix(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp)
            for task_count in (1_000, 5_000, 10_000, 50_000):
                with self.subTest(task_count=task_count):
                    matrix = write_hs_expected_matrix.cpu_document(
                        source_report(base, task_count), "bench-control-plane"
                    )
                    self.assertEqual(matrix["kind"], "hs-cpu")
                    self.assertEqual(len(matrix["arms"]), 12)
                    self.assertEqual(
                        [arm["name"] for arm in matrix["arms"]],
                        [
                            "cpu-500m-r1", "cpu-1-r1", "cpu-4-r1", "cpu-2-r1",
                            "cpu-1-r2", "cpu-2-r2", "cpu-500m-r2", "cpu-4-r2",
                            "cpu-2-r3", "cpu-4-r3", "cpu-1-r3", "cpu-500m-r3",
                        ],
                    )
                    self.assertEqual(
                        matrix["policy"]["warmTaskLimit"], min(task_count, 10_000)
                    )
                    self.assertEqual(matrix["policy"]["coldSLOSeconds"], 120)
                    self.assertEqual(matrix["policy"]["serverTimeoutSeconds"], 600)
                    self.assertEqual(matrix["policy"]["clientTimeoutSeconds"], 720)
                    self.assertEqual(
                        matrix["claim"], validate_hs_sweep.MEASUREMENT_CLAIM
                    )
                    metadata = matrix["source"]["taskLogMetadata"]
                    for arm in matrix["arms"]:
                        self.assertEqual(arm["config"]["TaskCount"], task_count)
                        self.assertEqual(
                            arm["config"]["HSSourceTaskLogMetadataSHA256"],
                            metadata["sha256"],
                        )
                        self.assertEqual(
                            arm["config"]["HSSourceTaskLogMetadataAttempts"],
                            task_count,
                        )
                        self.assertEqual(
                            arm["config"]["HSSourceTaskLogMetadataIncompleteNonNil"],
                            task_count,
                        )
                    validate_hs_sweep.validate_matrix(matrix, "hs-cpu")

    def test_matrix_validator_requires_full_storage_isolation_contract(self):
        def missing_lifecycle(source: dict) -> None:
            source.pop("storageIsolation")

        def unsafe_lifecycle(source: dict) -> None:
            source["storageIsolation"]["unexpectedChangedKeys"] = [
                "foreign/pre-t0-overwrite"
            ]

        def missing_phase_diffs(source: dict) -> None:
            source.pop("storageDiffs")

        attacks = {
            "missing lifecycle": missing_lifecycle,
            "unsafe lifecycle": unsafe_lifecycle,
            "missing phase diffs": missing_phase_diffs,
        }
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp)
            baseline = write_hs_expected_matrix.cpu_document(
                source_report(base), "bench-control-plane"
            )
            for name, mutate in attacks.items():
                with self.subTest(attack=name):
                    matrix = json.loads(json.dumps(baseline))
                    mutate(matrix["source"])
                    with self.assertRaises(validate_hs_sweep.ValidationError):
                        validate_hs_sweep.validate_matrix(matrix, "hs-cpu")

    def test_cpu_writer_rejects_invalid_fixed_source_report(self):
        mutations = {
            "incomplete": lambda doc: doc.__setitem__("completed", False),
            "wrong task count": lambda doc: doc["config"].__setitem__("TaskCount", 49_999),
            "float task count": lambda doc: doc["config"].__setitem__("TaskCount", 50_000.0),
            "wrong Ray image": lambda doc: doc["config"].__setitem__("RayImage", "rayproject/ray:2.54.0"),
            "empty object inventory": lambda doc: doc["storage"].__setitem__("objectCount", 0),
            "empty byte inventory": lambda doc: doc["storage"].__setitem__("totalBytes", 0),
            "boolean object inventory": lambda doc: doc["storage"].__setitem__("objectCount", True),
            "boolean byte inventory": lambda doc: doc["storage"].__setitem__("totalBytes", False),
            "missing marker": lambda doc: doc["storage"].__setitem__("markerPresent", False),
            "invalid attempts": lambda doc: doc["storage"]["events"]["benchTaskValidity"].__setitem__("valid", False),
            "wrong observed attempts": lambda doc: doc["storage"]["events"]["benchTaskValidity"].__setitem__("observedAttempts", 49_999),
            "wrong attempt zero": lambda doc: doc["storage"]["events"]["benchTaskValidity"].__setitem__("attemptZero", 49_999),
            "wrong finished attempts": lambda doc: doc["storage"]["events"]["benchTaskValidity"].__setitem__("finishedAttempts", 49_999),
            "validity problems": lambda doc: doc["storage"]["events"]["benchTaskValidity"].__setitem__("problems", ["bad"]),
            "compression disabled": lambda doc: doc["config"].__setitem__("Compression", False),
            "wrong wave size": lambda doc: doc["config"].__setitem__("WaveSize", 5_000),
            "wrong task CPU": lambda doc: doc["config"].__setitem__("TaskNumCPUs", "0.2"),
            "driver drain": lambda doc: doc["config"].__setitem__("DrainSleepSec", 25),
            "not owned": lambda doc: doc["config"].__setitem__("ShutdownAfterJob", False),
            "wrong TTL": lambda doc: doc["config"].__setitem__("JobTTLSeconds", 0),
            "multiple drivers": lambda doc: doc["config"].__setitem__("Drivers", 2),
            "wrong S3 bucket": lambda doc: doc["config"].__setitem__("S3Bucket", "ray-historyserver"),
            "cleanup enabled": lambda doc: doc["config"].__setitem__("SkipCleanup", False),
            "missing task log metadata": lambda doc: doc["storage"]["events"].pop(
                "taskLogMetadata"
            ),
            "malformed task log hash": lambda doc: doc["storage"]["events"][
                "taskLogMetadata"
            ].__setitem__("sha256", "not-a-sha256"),
            "task log count drift": lambda doc: doc["storage"]["events"][
                "taskLogMetadata"
            ]["counts"].__setitem__("present", 49_999),
            "structurally invalid task log": lambda doc: doc["storage"]["events"][
                "taskLogMetadata"
            ]["counts"].__setitem__("structurallyInvalid", 1),
            "invalid task log summary": lambda doc: doc["storage"]["events"][
                "taskLogMetadata"
            ].__setitem__("valid", False),
            "task log problems": lambda doc: doc["storage"]["events"][
                "taskLogMetadata"
            ].__setitem__("problems", ["conflicting offsets"]),
        }
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp)
            valid_path = source_report(base)
            baseline = json.loads(valid_path.read_text())
            for name, mutate in mutations.items():
                with self.subTest(name=name):
                    doc = json.loads(json.dumps(baseline))
                    mutate(doc)
                    valid_path.write_text(json.dumps(doc))
                    with self.assertRaises(SystemExit):
                        write_hs_expected_matrix.cpu_document(
                            valid_path, "bench-control-plane"
                        )

    def test_source_identity_rejects_path_normalization_attacks(self):
        attacks = {
            "dot namespace": ("namespace", "."),
            "parent namespace": ("namespace", ".."),
            "uppercase namespace": ("namespace", "Test-Ns"),
            "slash namespace": ("namespace", "test/escape"),
            "dot cluster": ("clusterName", "."),
            "parent cluster": ("clusterName", ".."),
            "slash cluster": ("clusterName", "ray/escape"),
            "dot session": ("sessionID", "."),
            "parent session": ("sessionID", ".."),
            "slash session": ("sessionID", "session_escape/child"),
            "non-Ray session": ("sessionID", "safe-looking-but-wrong"),
            "overlong session": ("sessionID", "session_" + "a" * 248),
        }
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp)
            report_path = source_report(base)
            baseline_report = json.loads(report_path.read_text())
            baseline_matrix = write_hs_expected_matrix.cpu_document(
                report_path, "bench-control-plane"
            )
            for name, (field, value) in attacks.items():
                with self.subTest(layer="writer", attack=name):
                    report = json.loads(json.dumps(baseline_report))
                    report[field] = value
                    report_path.write_text(json.dumps(report))
                    with self.assertRaises(SystemExit):
                        write_hs_expected_matrix.cpu_document(
                            report_path, "bench-control-plane"
                        )
                with self.subTest(layer="validator", attack=name):
                    matrix = json.loads(json.dumps(baseline_matrix))
                    matrix["source"][field] = value
                    with self.assertRaises(validate_hs_sweep.ValidationError):
                        validate_hs_sweep.validate_matrix(matrix, "hs-cpu")

    def test_valid_cpu_campaign_and_selection(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            result = validate_hs_sweep.validate_campaign(
                root, expected_kind="hs-cpu"
            )
            selection = validate_hs_sweep.select_cpu(result)
            self.assertEqual(selection.cpu, "500m")
            self.assertEqual(
                selection.discovery_max_lifetime_memory_peak_bytes,
                640 * 1024 * 1024,
            )

    def test_local_port_cleanup_sentinel_is_required_exactly(self):
        for mutation in ("missing", "changed"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
                root = self.make_cpu(pathlib.Path(tmp))
                sentinel = root / "cpu-500m-r1" / "local-port-cleanup-sentinel.txt"
                if mutation == "missing":
                    sentinel.unlink()
                else:
                    sentinel.write_text(
                        "LOCAL-PORTS-FREE ports=19003,30080 consecutive=1\n"
                    )
                with self.assertRaises(validate_hs_sweep.ValidationError):
                    validate_hs_sweep.validate_single_arm(root, "cpu-500m-r1")

    def test_real_go_serialized_fixture_matches_validator_contract(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp)
            go_task_log = task_log_metadata(50_000)
            go_task_log["sha256"] = "e" * 64
            matrix = write_hs_expected_matrix.cpu_document(
                source_report(base, task_log=go_task_log), "kind-control-plane"
            )
            root = base / "cpu"
            write_campaign(
                root,
                matrix,
                image="kuberay/history-server:ray-2.56.0",
            )
            report_path = next((root / "cpu-1-r1").glob("*/bench-report.json"))
            write_go_serialized_fixture(report_path)
            report = json.loads(report_path.read_text())
            arm_dir = root / "cpu-1-r1"
            identity_path = arm_dir / "execution-namespace.json"
            report["hsPodEvidence"]["imageID"] = IMAGE_ID
            report["config"]["ExecutionIdentityFile"] = str(identity_path.resolve())
            report_path.write_text(json.dumps(report, indent=2, sort_keys=True))
            identity = {
                "name": report["executionNamespace"],
                "uid": report["executionNamespaceUID"],
            }
            identity_path.write_text(json.dumps(identity))
            (arm_dir / "namespace-cleanup-sentinel.txt").write_text(
                f"NAMESPACE-DELETED name={identity['name']} uid={identity['uid']} pods=0\n"
            )
            arm = validate_hs_sweep.validate_single_arm(root, "cpu-1-r1")
            self.assertEqual(arm.cpu, "1")
            self.assertEqual(arm.lifetime_memory_peak_bytes, 1024 * 1024 * 1024)

    def test_image_id_normalization_rejects_embedded_digest_garbage(self):
        valid = (
            IMAGE_ID,
            "docker.io/library/historyserver@" + IMAGE_ID,
            "docker-pullable://registry.example/team/historyserver@" + IMAGE_ID,
            "containerd://" + IMAGE_ID,
        )
        for value in valid:
            with self.subTest(valid=value):
                self.assertEqual(
                    validate_hs_sweep.normalize_container_image_id(value),
                    IMAGE_ID,
                )
        invalid = (
            "garbage-" + IMAGE_ID,
            IMAGE_ID + "-suffix",
            "repo@@" + IMAGE_ID,
            "repo@" + IMAGE_ID + " extra",
            "repo@sha256:" + "A" * 64,
        )
        for value in invalid:
            with self.subTest(invalid=value):
                self.assertIsNone(
                    validate_hs_sweep.normalize_container_image_id(value)
                )

    def test_rejects_historyserver_image_id_with_digest_in_garbage(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            report_path = next((root / "cpu-500m-r1").glob("*/bench-report.json"))
            report = json.loads(report_path.read_text())
            report["hsPodEvidence"]["imageID"] = "garbage-" + IMAGE_ID
            report_path.write_text(json.dumps(report))
            with self.assertRaisesRegex(
                validate_hs_sweep.ValidationError,
                "invalid CRI form",
            ):
                validate_hs_sweep.validate_campaign(root, expected_kind="hs-cpu")

    def test_rejects_full_replay_task_log_hash_drift(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            report_path = next((root / "cpu-500m-r1").glob("*/bench-report.json"))
            report = json.loads(report_path.read_text())
            report["hsValidation"]["fullReplay"]["taskLogMetadata"][
                "sha256"
            ] = SHA_B
            report_path.write_text(json.dumps(report))
            with self.assertRaisesRegex(
                validate_hs_sweep.ValidationError,
                "full replay TaskLog metadata differs from source",
            ):
                validate_hs_sweep.validate_campaign(root, expected_kind="hs-cpu")

    def test_rejects_full_replay_task_log_count_drift(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            report_path = next((root / "cpu-500m-r1").glob("*/bench-report.json"))
            report = json.loads(report_path.read_text())
            report["hsValidation"]["fullReplay"]["taskLogMetadata"]["counts"][
                "present"
            ] -= 1
            report_path.write_text(json.dumps(report))
            with self.assertRaisesRegex(
                validate_hs_sweep.ValidationError,
            r"nil\+present does not equal attempts",
            ):
                validate_hs_sweep.validate_campaign(root, expected_kind="hs-cpu")

    def test_rejects_warm_task_log_full_summary(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            report_path = next((root / "cpu-500m-r1").glob("*/bench-report.json"))
            report = json.loads(report_path.read_text())
            report["hsValidation"]["warmTaskQuery"]["taskLogMetadata"] = json.loads(
                json.dumps(report["hsValidation"]["fullReplay"]["taskLogMetadata"])
            )
            report_path.write_text(json.dumps(report))
            with self.assertRaisesRegex(
                validate_hs_sweep.ValidationError,
                "warmTaskQuery.taskLogMetadata.attempts differs",
            ):
                validate_hs_sweep.validate_campaign(root, expected_kind="hs-cpu")

    def test_rejects_warm_task_log_projection_hash_mismatch(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            report_path = next((root / "cpu-500m-r1").glob("*/bench-report.json"))
            report = json.loads(report_path.read_text())
            report["hsValidation"]["warmTaskQuery"][
                "expectedProjectionSHA256"
            ] = SHA_B
            report_path.write_text(json.dumps(report))
            with self.assertRaisesRegex(
                validate_hs_sweep.ValidationError,
                "warm expected projection hash differs",
            ):
                validate_hs_sweep.validate_campaign(root, expected_kind="hs-cpu")

    def test_rejects_history_server_scope_overclaim(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            report_path = next((root / "cpu-500m-r1").glob("*/bench-report.json"))
            report = json.loads(report_path.read_text())
            report["hsValidation"]["scope"]["logsFile"] = True
            report_path.write_text(json.dumps(report))
            with self.assertRaisesRegex(
                validate_hs_sweep.ValidationError,
                "hsValidation.scope differs from matrix claim",
            ):
                validate_hs_sweep.validate_campaign(root, expected_kind="hs-cpu")

    def test_rejects_source_fingerprint_drift(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            report_path = next((root / "cpu-1-r1").glob("*/bench-report.json"))
            report = json.loads(report_path.read_text())
            report["sourceSessionFingerprint"]["start"] = SHA_B
            report["sourceSessionFingerprint"]["end"] = SHA_B
            report_path.write_text(json.dumps(report))
            with self.assertRaisesRegex(validate_hs_sweep.ValidationError, "across arms"):
                validate_hs_sweep.validate_campaign(root, expected_kind="hs-cpu")

    def test_rejects_cross_bucket_fingerprint(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            report_path = next((root / "cpu-1-r1").glob("*/bench-report.json"))
            report = json.loads(report_path.read_text())
            report["sourceSessionFingerprint"]["bucket"] = "ray-historyserver"
            report_path.write_text(json.dumps(report))
            with self.assertRaisesRegex(validate_hs_sweep.ValidationError, "fingerprint bucket"):
                validate_hs_sweep.validate_campaign(root, expected_kind="hs-cpu")

    def test_rejects_cross_bucket_provenance(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            provenance = initial_provenance(root, json.loads((root / "expected-matrix.json").read_text()))
            provenance["source_bucket"] = "ray-historyserver"
            write_kv(root / "provenance.txt", provenance)
            with self.assertRaisesRegex(validate_hs_sweep.ValidationError, "provenance source bucket"):
                validate_hs_sweep.validate_campaign(root, expected_kind="hs-cpu")

    def test_rejects_initial_task_log_metadata_provenance_drift(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            provenance = validate_hs_sweep.read_key_value(
                root / "provenance.txt", "fixture initial provenance"
            )
            provenance["source_task_log_metadata_sha256"] = SHA_B
            write_kv(root / "provenance.txt", provenance)
            with self.assertRaisesRegex(
                validate_hs_sweep.ValidationError,
                "provenance source_task_log_metadata_sha256 differs",
            ):
                validate_hs_sweep.validate_campaign(root, expected_kind="hs-cpu")

    def test_rejects_arm_task_log_metadata_provenance_drift(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            arm_path = root / "cpu-500m-r1" / "arm-provenance.txt"
            provenance = validate_hs_sweep.read_key_value(
                arm_path, "fixture arm provenance"
            )
            provenance["source_task_log_metadata_present"] = "49999"
            write_kv(arm_path, provenance)
            with self.assertRaisesRegex(
                validate_hs_sweep.ValidationError,
                "source_task_log_metadata_present differs",
            ):
                validate_hs_sweep.validate_campaign(root, expected_kind="hs-cpu")

    def test_rejects_reused_pod_lifecycle(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            first = next((root / "cpu-500m-r1").glob("*/bench-report.json"))
            second = next((root / "cpu-1-r1").glob("*/bench-report.json"))
            first_report = json.loads(first.read_text())
            second_report = json.loads(second.read_text())
            second_report["hsPodEvidence"]["podUID"] = first_report["hsPodEvidence"]["podUID"]
            second.write_text(json.dumps(second_report))
            with self.assertRaisesRegex(validate_hs_sweep.ValidationError, "pod UID was reused"):
                validate_hs_sweep.validate_campaign(root, expected_kind="hs-cpu")

    def test_cpu_discovery_keeps_measurement_valid_slo_misses(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            for repeat in range(1, 4):
                report_path = next(
                    (root / f"cpu-500m-r{repeat}").glob("*/bench-report.json")
                )
                report = json.loads(report_path.read_text())
                report["historyServer"].update(
                    {
                        "enterColdLatency": 121_000_000_000,
                        "enterMeasured": True,
                        "enterStatus": 200,
                        "notes": [],
                    }
                )
                report["hsValidation"].update(
                    {
                        "measurementValid": True,
                        "meetsColdSLO": False,
                    }
                )
                report_path.write_text(json.dumps(report))
            result = validate_hs_sweep.validate_campaign(root, expected_kind="hs-cpu")
            self.assertEqual(validate_hs_sweep.select_cpu(result).cpu, "1")

    def test_cpu_discovery_rejects_incomplete_cold_request(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            report_path = next((root / "cpu-500m-r1").glob("*/bench-report.json"))
            report = json.loads(report_path.read_text())
            report["historyServer"].update(
                {
                    "enterColdLatency": 600_000_000_000,
                    "enterMeasured": False,
                    "enterStatus": 500,
                    "notes": ["processing timeout"],
                }
            )
            report["hsValidation"].update(
                {"measurementValid": True, "meetsColdSLO": False}
            )
            report_path.write_text(json.dumps(report))
            with self.assertRaisesRegex(
                validate_hs_sweep.ValidationError, "measured HTTP 200"
            ):
                validate_hs_sweep.validate_campaign(root, expected_kind="hs-cpu")

    def test_rejects_collector_artifacts(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = self.make_cpu(pathlib.Path(tmp))
            report_path = next((root / "cpu-500m-r1").glob("*/bench-report.json"))
            report = json.loads(report_path.read_text())
            report["collectorLogs"] = [{"pod": "unexpected"}]
            report_path.write_text(json.dumps(report))
            with self.assertRaisesRegex(validate_hs_sweep.ValidationError, "Collector artifact"):
                validate_hs_sweep.validate_campaign(root, expected_kind="hs-cpu")

    def test_memory_matrix_is_derived_and_parent_bound(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp)
            cpu_root = self.make_cpu(base)
            matrix = write_hs_expected_matrix.memory_document(
                cpu_root, "bench-control-plane"
            )
            evidence = matrix["selectionEvidence"]
            self.assertEqual(
                matrix["source"]["sourceGeneration"],
                json.loads(
                    (cpu_root / "expected-matrix.json").read_text()
                )["source"]["sourceGeneration"],
            )
            self.assertEqual(evidence["selectedCPU"], "500m")
            self.assertEqual(evidence["candidateMemoryQuantity"], "800Mi")
            self.assertEqual(len(matrix["arms"]), 5)
            memory_root = base / "memory"
            write_campaign(memory_root, matrix, parent_cpu_root=cpu_root)
            validate_hs_sweep.validate_campaign(
                memory_root,
                expected_kind="hs-memory",
                parent_cpu_root=cpu_root,
            )

    def test_memory_validation_rejects_parent_drift(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            base = pathlib.Path(tmp)
            cpu_root = self.make_cpu(base)
            matrix = write_hs_expected_matrix.memory_document(
                cpu_root, "bench-control-plane"
            )
            memory_root = base / "memory"
            write_campaign(memory_root, matrix, parent_cpu_root=cpu_root)
            with (cpu_root / "provenance-final.txt").open("a") as stream:
                stream.write("drift=true\n")
            with self.assertRaisesRegex(validate_hs_sweep.ValidationError, "parent final provenance"):
                validate_hs_sweep.validate_campaign(
                    memory_root,
                    expected_kind="hs-memory",
                    parent_cpu_root=cpu_root,
                )


if __name__ == "__main__":
    unittest.main()
