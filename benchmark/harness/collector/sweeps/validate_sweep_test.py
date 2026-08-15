import hashlib
import json
import pathlib
import socket
import subprocess
import sys
import tempfile
import unittest

import validate_sweep

SHA = "a" * 64
IMAGE_ID = "sha256:" + "b" * 64
TASK_COUNTS = (1000, 10000, 50000, 100000)
TARGET_TASK_RATES = (250, 500, 1000, 2000, 3000, 0)


def valid_config(
    task_count: int,
    target_task_rate: int = 0,
    *,
    skip_history_server: bool = False,
) -> dict:
    return {
        "TaskCount": task_count,
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
        "SkipHistoryServer": skip_history_server,
        "Drivers": 1,
        "TargetTaskRate": target_task_rate,
        "S3Bucket": validate_sweep.BENCHMARK_S3_BUCKET,
    }


def valid_matrix() -> dict:
    return {
        "schemaVersion": 4,
        "kind": "by-task",
        "arms": [
            {
                "name": f"n{task_count}-r{repeat}",
                "repeat": repeat,
                "config": valid_config(task_count),
            }
            for repeat in (1, 2, 3)
            for task_count in TASK_COUNTS
        ],
    }


def valid_rate_matrix() -> dict:
    return {
        "schemaVersion": 4,
        "kind": "by-rate",
        "arms": [
            {
                "name": (
                    f"rate{target_task_rate}-r{repeat}"
                    if target_task_rate > 0
                    else f"unpaced-r{repeat}"
                ),
                "repeat": repeat,
                "config": valid_config(
                    50000,
                    target_task_rate,
                    skip_history_server=True,
                ),
            }
            for repeat in (1, 2, 3)
            for target_task_rate in TARGET_TASK_RATES
        ],
    }


def valid_cap_config() -> dict:
    config = valid_config(50000, 3000, skip_history_server=True)
    config.update(
        {
            "CollectorCPURequest": "150m",
            "CollectorCPU": "1200m",
            "CollectorMemoryRequest": "160Mi",
            "CollectorMemoryLimit": "192Mi",
        }
    )
    return config


def valid_cap_matrix() -> dict:
    return {
        "schemaVersion": 4,
        "kind": "collector-cap",
        "arms": [
            {
                "name": f"rate3000-r{repeat}",
                "repeat": repeat,
                "config": valid_cap_config(),
            }
            for repeat in (1, 2, 3)
        ],
    }


def valid_legacy_v1_matrix() -> dict:
    matrix = valid_matrix()
    matrix["schemaVersion"] = 1
    for arm in matrix["arms"]:
        arm["config"].pop("SkipHistoryServer")
        arm["config"].pop("S3Bucket")
    return matrix


def valid_legacy_v1_rate_matrix() -> dict:
    matrix = valid_rate_matrix()
    matrix["schemaVersion"] = 1
    for arm in matrix["arms"]:
        arm["config"].pop("SkipHistoryServer")
        arm["config"].pop("S3Bucket")
    return matrix


def valid_legacy_v2_matrix() -> dict:
    matrix = valid_matrix()
    matrix["schemaVersion"] = 2
    for arm in matrix["arms"]:
        arm["config"].pop("S3Bucket")
    return matrix


def valid_legacy_v3_cap_matrix() -> dict:
    matrix = valid_cap_matrix()
    matrix["schemaVersion"] = 3
    for arm in matrix["arms"]:
        arm["config"].pop("S3Bucket")
    return matrix


def valid_provenance(matrix_bytes: bytes) -> str:
    values = {key: SHA for key in validate_sweep.REQUIRED_PROVENANCE}
    values.update(
        {
            "repo_head": "c" * 40,
            "ray_image_requested": "rayproject/ray:2.56.0",
            "ray_runtime_id": IMAGE_ID,
            "ray_runtime_repo_digests": "<none>",
            "ray_runtime_version": "2.56.0",
            "ray_runtime_commit": "c" * 40,
            "collector_image_requested": "collector:v0.1.0",
            "collector_runtime_id": IMAGE_ID,
            "collector_runtime_repo_digests": "<none>",
            "historyserver_image_requested": "historyserver:v0.1.0",
            "historyserver_runtime_id": IMAGE_ID,
            "historyserver_runtime_repo_digests": "<none>",
            "expected_matrix_sha256": hashlib.sha256(matrix_bytes).hexdigest(),
        }
    )
    return "".join(f"{key}={value}\n" for key, value in sorted(values.items()))


def valid_final_provenance(initial_text: str) -> str:
    initial = dict(
        line.split("=", 1) for line in initial_text.splitlines() if "=" in line
    )
    values = {
        "revalidation_status": "valid",
        **{
            key: initial[key]
            for key in validate_sweep.FINAL_PROVENANCE_FIELDS
            if key != "revalidation_status"
        },
    }
    return "".join(f"{key}={value}\n" for key, value in sorted(values.items()))


def valid_report(
    expected=1000,
    observed=None,
    target_task_rate=0,
    *,
    skip_history_server=False,
):
    if observed is None:
        observed = expected
    return {
        "completed": True,
        "config": valid_config(
            expected,
            target_task_rate,
            skip_history_server=skip_history_server,
        ),
        "rayJobLifecycle": {
            "ownedCluster": True,
            "shutdownAfterJobFinishes": True,
            "ttlSecondsAfterFinished": 30,
        },
        "storage": {
            "markerPresent": True,
            "events": {
                "totalEvents": 1800,
                "distinctEventIDs": 1800,
                "missingEventIDs": 0,
                "duplicateEventIDs": 0,
                "expectedTasks": expected,
                "benchTaskIDs": observed,
                "benchTaskValidity": {
                    "expectedTaskIDs": expected,
                    "observedTaskIDs": observed,
                    "observedAttempts": observed,
                    "attemptZero": observed,
                    "finishedAttempts": observed,
                    "submittedToWorkerAttempts": observed,
                    "finishedTransitionAttempts": observed,
                    "malformedDefinitions": 0,
                    "missingDefinitionAttemptFields": 0,
                    "missingLifecycleAttemptFields": 0,
                    "invalidLifecycleTransitions": 0,
                    "outOfRangeLifecycleTransitions": 0,
                    "ambiguousLifecycleTransitions": 0,
                    "missingLifecycleAttempts": 0,
                    "nonFinishedAttempts": 0,
                    "valid": observed == expected,
                    "problems": [],
                },
                "taskLifecycleWindows": [
                    {
                        "windowStartUnixNano": 10_000_000_000,
                        "windowEndUnixNano": 20_000_000_000,
                        "submittedToWorkerAttempts": observed,
                        "finishedAttempts": observed,
                        "backlogDelta": 0,
                    }
                ],
                "perNode": [
                    {
                        "nodeID": "node-head",
                        "events": 1000,
                        "distinctEventIDs": 1000,
                        "missingEventIDs": 0,
                        "duplicateEventIDs": 0,
                        "rawBytes": 2000,
                    },
                    {
                        "nodeID": "node-worker",
                        "events": 800,
                        "distinctEventIDs": 800,
                        "missingEventIDs": 0,
                        "duplicateEventIDs": 0,
                        "rawBytes": 1600,
                    },
                ],
            },
        },
        "historyServer": (
            {}
            if skip_history_server
            else {
                "enterMeasured": True,
                "enterStatus": 200,
                "warmEndpoints": [
                    {
                        "endpoint": f"/api/v0/tasks?limit={min(expected, 10000)}",
                        "errors": 0,
                    },
                    {"endpoint": "/api/v0/tasks/summarize", "errors": 0},
                    {"endpoint": "/api/jobs/", "errors": 0},
                    {"endpoint": "/nodes?view=summary", "errors": 0},
                    {"endpoint": "/events", "errors": 0},
                ],
            }
        ),
        "collectorLogs": [
            {
                "pod": "head",
                "role": "head",
                "image": "collector:v0.1.0",
                "imageID": IMAGE_ID,
                "containerID": "container-head",
                "restartCount": 0,
                "uploadFailures": 0,
                "rotationQueueFull": 0,
                "logStreamComplete": True,
                "logStreamTimedOut": False,
                "gracefulShutdownComplete": True,
                "uploadedBytes": 2000,
                "ingressWindows": [
                    {"nodeID": "node-head", "events": 1000, "bytes": 2100}
                ],
            },
            {
                "pod": "worker",
                "role": "worker",
                "image": "collector:v0.1.0",
                "imageID": IMAGE_ID,
                "containerID": "container-worker",
                "restartCount": 0,
                "uploadFailures": 0,
                "rotationQueueFull": 0,
                "logStreamComplete": True,
                "logStreamTimedOut": False,
                "gracefulShutdownComplete": True,
                "uploadedBytes": 1600,
                "ingressWindows": [
                    {"nodeID": "node-worker", "events": 800, "bytes": 1700}
                ],
            },
        ],
        "collectorWindows": [
            {
                "windowStartUnixNano": 10_000_000_000,
                "windowEndUnixNano": 20_000_000_000,
                "submittedToWorkerAttempts": expected,
                "finishedAttempts": expected,
                "backlogDelta": 0,
                "pod": "head",
                "nodeID": "node-head",
                "validForSizing": True,
            },
            {
                "windowStartUnixNano": 10_000_000_000,
                "windowEndUnixNano": 20_000_000_000,
                "submittedToWorkerAttempts": expected,
                "finishedAttempts": expected,
                "backlogDelta": 0,
                "pod": "worker",
                "nodeID": "node-worker",
                "validForSizing": True,
            },
        ],
        "collectorIngressGates": [
            {
                "pod": "head",
                "role": "head",
                "nodeIDs": ["node-head"],
                "rejectedRequests": 0,
                "rotationQueueFull": 0,
                "logStreamComplete": True,
                "gracefulShutdownComplete": True,
                "valid": True,
                "problems": [],
            },
            {
                "pod": "worker",
                "role": "worker",
                "nodeIDs": ["node-worker"],
                "rejectedRequests": 0,
                "rotationQueueFull": 0,
                "logStreamComplete": True,
                "gracefulShutdownComplete": True,
                "valid": True,
                "problems": [],
            },
        ],
        "podTerminations": [
            {
                "pod": "head",
                "container": "collector",
                "containerID": "container-head",
                "restartCount": 0,
                "observed": True,
                "source": "current",
                "exitCode": 0,
                "reason": "Completed",
            },
            {
                "pod": "worker",
                "container": "collector",
                "containerID": "container-worker",
                "restartCount": 0,
                "observed": True,
                "source": "current",
                "exitCode": 0,
                "reason": "Completed",
            },
        ],
    }


def apply_cap_evidence(report: dict) -> dict:
    report["config"].update(
        {
            "CollectorCPURequest": "150m",
            "CollectorCPU": "1200m",
            "CollectorMemoryRequest": "160Mi",
            "CollectorMemoryLimit": "192Mi",
        }
    )
    for collector in report["collectorLogs"]:
        collector.update(
            {
                "cpuRequest": "150m",
                "cpuLimit": "1200m",
                "memoryRequest": "160Mi",
                "memoryLimit": "192Mi",
                "cgroupMemoryObserved": True,
                "cgroupMemoryMax": str(192 * 1024 * 1024),
                "cgroupMemoryMaxBytes": 192 * 1024 * 1024,
                "memoryEventsOOM": 0,
                "memoryEventsOOMKill": 0,
                "cgroupMemoryReadErrors": 0,
                "cgroupMemoryErrorFields": [],
            }
        )
    for gate in report["collectorIngressGates"]:
        gate["peakEventsPerSecond"] = 4500.0
    return report


class ValidateSweepTest(unittest.TestCase):
    def test_local_tcp_port_preflight_rejects_occupied_port(self):
        library = pathlib.Path(__file__).with_name("sweep_lib.sh")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            port = listener.getsockname()[1]
            completed = subprocess.run(
                [
                    "bash",
                    "-c",
                    'source "$1"; require_local_tcp_port_free "$2"',
                    "port-preflight-test",
                    str(library),
                    str(port),
                ],
                capture_output=True,
                text=True,
            )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn(f"localhost:{port} is not free", completed.stderr)

    def test_collector_runner_preflights_and_passes_dedicated_s3_port(self):
        script = pathlib.Path(__file__).with_name("ray256_collector.sh").read_text()

        self.assertIn("S3_LOCAL_PORT=19002", script)
        preflight = 'require_local_tcp_port_free "$S3_LOCAL_PORT" || exit 1'
        self.assertIn(preflight, script)
        self.assertLess(script.index(preflight), script.index("go build -o"))
        self.assertLess(script.index(preflight), script.index('"$OPERATOR_BIN" --metrics-addr'))
        self.assertRegex(
            script,
            r'env "\$\{BENCH_ENV_UNSET\[@\]\}" \\\n'
            r'\s+BENCH_RUN=1 BENCH_KIND_NODE="\$BENCH_KIND_NODE" '
            r'BENCH_OUT_DIR="\$OUT/\$name" \\\n'
            r'\s+BENCH_RAY_IMAGE="\$RAY_IMAGE" '
            r'BENCH_S3_LOCAL_PORT="\$S3_LOCAL_PORT" "\$@"',
        )
        for fragment in (
            "TARGET_TASK_RATES=(3000)",
            "BENCH_COLLECTOR_CPU_REQUEST=150m",
            "BENCH_COLLECTOR_CPU_LIMIT=1200m",
            "BENCH_COLLECTOR_MEMORY_REQUEST=160Mi",
            "BENCH_COLLECTOR_MEMORY_LIMIT=192Mi",
        ):
            self.assertIn(fragment, script)

    def test_expected_matrix_writer_emits_formal_matrix_once(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            output = root / "expected-matrix.json"
            command = [
                sys.executable,
                str(pathlib.Path(__file__).with_name("write_expected_matrix.py")),
                "--output",
                str(output),
                "--task-count",
                "1000",
                "--task-count",
                "10000",
                "--task-count",
                "50000",
                "--task-count",
                "100000",
                "--repeats",
                "3",
                "--wave-size",
                "2000",
                "--task-num-cpus",
                "0.5",
                "--ray-image",
                "rayproject/ray:2.56.0",
                "--compression",
                "true",
                "--shutdown-after-job",
                "true",
                "--job-ttl-seconds",
                "30",
                "--drain-sleep-seconds",
                "0",
                "--warm-iterations",
                "3",
                "--hs-cpu-request",
                "4",
                "--hs-cpu-limit",
                "4",
                "--hs-args=--session-process-timeout=30m",
                "--skip-history-server",
                "false",
                "--drivers",
                "1",
                "--target-task-rate",
                "0",
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)
            matrix = validate_sweep.read_expected_matrix(root, 12)
            self.assertEqual(matrix["n1000-r1"]["config"]["TaskNumCPUs"], "0.5")
            self.assertEqual(matrix["n1000-r1"]["_schemaVersion"], 4)
            self.assertEqual(
                matrix["n1000-r1"]["config"]["S3Bucket"],
                validate_sweep.BENCHMARK_S3_BUCKET,
            )
            self.assertIs(
                matrix["n1000-r1"]["config"]["SkipHistoryServer"], False
            )

            second = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(second.returncode, 0)
            self.assertIn("refusing to overwrite", second.stderr)

            document = json.loads(output.read_text())
            document["arms"][0]["config"]["S3Bucket"] = "ray-historyserver"
            output.write_text(json.dumps(document))
            with self.assertRaisesRegex(validate_sweep.ValidationError, "S3Bucket"):
                validate_sweep.read_expected_matrix(root, 12)

    def test_expected_matrix_writer_emits_formal_rate_matrix(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            output = root / "expected-matrix.json"
            command = [
                sys.executable,
                str(pathlib.Path(__file__).with_name("write_expected_matrix.py")),
                "--output",
                str(output),
                "--kind",
                "by-rate",
                "--task-count",
                "50000",
                "--repeats",
                "3",
                "--wave-size",
                "2000",
                "--task-num-cpus",
                "0.5",
                "--ray-image",
                "rayproject/ray:2.56.0",
                "--compression",
                "true",
                "--shutdown-after-job",
                "true",
                "--job-ttl-seconds",
                "30",
                "--drain-sleep-seconds",
                "0",
                "--warm-iterations",
                "3",
                "--hs-cpu-request",
                "4",
                "--hs-cpu-limit",
                "4",
                "--hs-args=--session-process-timeout=30m",
                "--skip-history-server",
                "true",
                "--drivers",
                "1",
            ]
            for target_task_rate in TARGET_TASK_RATES:
                command.extend(("--target-task-rate", str(target_task_rate)))

            subprocess.run(command, check=True, capture_output=True, text=True)
            matrix = validate_sweep.read_expected_matrix(root, 18)

            self.assertEqual(len(matrix), 18)
            self.assertEqual(matrix["rate250-r1"]["config"]["TaskCount"], 50000)
            self.assertEqual(matrix["unpaced-r3"]["config"]["TargetTaskRate"], 0)
            self.assertIs(
                matrix["rate250-r1"]["config"]["SkipHistoryServer"], True
            )
            self.assertEqual(matrix["rate250-r1"]["_schemaVersion"], 4)
            self.assertEqual(
                matrix["rate250-r1"]["config"]["S3Bucket"],
                validate_sweep.BENCHMARK_S3_BUCKET,
            )

    def test_expected_matrix_writer_emits_collector_cap_matrix(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            output = root / "expected-matrix.json"
            command = [
                sys.executable,
                str(pathlib.Path(__file__).with_name("write_expected_matrix.py")),
                "--output",
                str(output),
                "--kind",
                "collector-cap",
                "--task-count",
                "50000",
                "--repeats",
                "3",
                "--wave-size",
                "2000",
                "--task-num-cpus",
                "0.5",
                "--ray-image",
                "rayproject/ray:2.56.0",
                "--compression",
                "true",
                "--shutdown-after-job",
                "true",
                "--job-ttl-seconds",
                "30",
                "--drain-sleep-seconds",
                "0",
                "--warm-iterations",
                "3",
                "--hs-cpu-request",
                "4",
                "--hs-cpu-limit",
                "4",
                "--hs-args=--session-process-timeout=30m",
                "--skip-history-server",
                "true",
                "--drivers",
                "1",
                "--collector-cpu-request",
                "150m",
                "--collector-cpu-limit",
                "1200m",
                "--collector-memory-request",
                "160Mi",
                "--collector-memory-limit",
                "192Mi",
                "--target-task-rate",
                "3000",
            ]

            subprocess.run(command, check=True, capture_output=True, text=True)
            matrix = validate_sweep.read_expected_matrix(root, 3)

            self.assertEqual(set(matrix), {"rate3000-r1", "rate3000-r2", "rate3000-r3"})
            self.assertEqual(matrix["rate3000-r1"]["_schemaVersion"], 4)
            self.assertEqual(
                matrix["rate3000-r1"]["config"]["S3Bucket"],
                validate_sweep.BENCHMARK_S3_BUCKET,
            )
            self.assertEqual(matrix["rate3000-r1"]["config"]["CollectorMemoryLimit"], "192Mi")

    def test_explicitly_accepts_legacy_v1_matrix_and_normalizes_skip_false(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            (root / "expected-matrix.json").write_text(
                json.dumps(valid_legacy_v1_matrix())
            )
            matrix = validate_sweep.read_expected_matrix(
                root, 12, allow_legacy_schema=True
            )

            self.assertEqual(matrix["n1000-r1"]["_schemaVersion"], 1)
            self.assertIs(
                matrix["n1000-r1"]["config"]["SkipHistoryServer"], False
            )

    def test_explicitly_accepts_legacy_v1_rate_matrix_as_full_history_server_run(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            (root / "expected-matrix.json").write_text(
                json.dumps(valid_legacy_v1_rate_matrix())
            )
            matrix = validate_sweep.read_expected_matrix(
                root, 18, allow_legacy_schema=True
            )

            self.assertEqual(matrix["rate250-r1"]["_schemaVersion"], 1)
            self.assertIs(
                matrix["rate250-r1"]["config"]["SkipHistoryServer"], False
            )

    def test_rejects_v1_matrix_with_v2_extra_field(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            matrix = valid_legacy_v1_matrix()
            matrix["arms"][0]["config"]["SkipHistoryServer"] = False
            (root / "expected-matrix.json").write_text(json.dumps(matrix))

            with self.assertRaisesRegex(validate_sweep.ValidationError, "extra"):
                validate_sweep.read_expected_matrix(
                    root, 12, allow_legacy_schema=True
                )

    def test_rejects_every_legacy_schema_by_default(self):
        cases = (
            (valid_legacy_v1_matrix(), 12),
            (valid_legacy_v2_matrix(), 12),
            (valid_legacy_v3_cap_matrix(), 3),
        )
        for matrix, expected_arms in cases:
            with self.subTest(schema=matrix["schemaVersion"]), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp)
                (root / "expected-matrix.json").write_text(json.dumps(matrix))
                with self.assertRaisesRegex(
                    validate_sweep.ValidationError, "requires explicit"
                ):
                    validate_sweep.read_expected_matrix(root, expected_arms)
                accepted = validate_sweep.read_expected_matrix(
                    root, expected_arms, allow_legacy_schema=True
                )
                self.assertEqual(len(accepted), expected_arms)

    def test_legacy_cli_flag_is_explicit_and_offline_only(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            matrix_text = json.dumps(valid_legacy_v1_matrix(), sort_keys=True)
            (root / "expected-matrix.json").write_text(matrix_text)
            (root / "provenance.txt").write_text(
                valid_provenance(matrix_text.encode())
            )
            command = [
                sys.executable,
                str(pathlib.Path(__file__).with_name("validate_sweep.py")),
                str(root),
                "--provenance-only",
            ]
            rejected = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("requires explicit", rejected.stderr)
            accepted = subprocess.run(
                command + ["--allow-legacy-schema"], capture_output=True, text=True
            )
            self.assertEqual(accepted.returncode, 0, accepted.stderr)
            self.assertIn("PROVENANCE-VALID", accepted.stdout)

    def test_formal_runners_never_enable_legacy_schema(self):
        sweep_dir = pathlib.Path(__file__).resolve().parent
        for runner in ("sweep_lib.sh", "ray256_collector.sh"):
            with self.subTest(runner=runner):
                self.assertNotIn(
                    "--allow-legacy-schema", (sweep_dir / runner).read_text()
                )

    def test_rejects_v4_matrix_missing_skip_history_server(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            matrix = valid_matrix()
            matrix["arms"][0]["config"].pop("SkipHistoryServer")
            (root / "expected-matrix.json").write_text(json.dumps(matrix))

            with self.assertRaisesRegex(validate_sweep.ValidationError, "missing"):
                validate_sweep.read_expected_matrix(root, 12)

    def test_rejects_v4_skip_history_server_wrong_type_or_value(self):
        mutations = (
            (valid_matrix, True),
            (valid_matrix, "false"),
            (valid_rate_matrix, False),
            (valid_rate_matrix, "true"),
        )
        for make_matrix, value in mutations:
            with self.subTest(kind=make_matrix()["kind"], value=value):
                with tempfile.TemporaryDirectory() as temp:
                    root = pathlib.Path(temp)
                    matrix = make_matrix()
                    matrix["arms"][0]["config"]["SkipHistoryServer"] = value
                    (root / "expected-matrix.json").write_text(json.dumps(matrix))

                    with self.assertRaises(validate_sweep.ValidationError):
                        validate_sweep.read_expected_matrix(
                            root, len(matrix["arms"])
                        )

    def make_sweep(
        self, report=None, *, write_ingress_artifacts=True, matrix=None
    ):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = pathlib.Path(temp.name)
        matrix_document = valid_matrix() if matrix is None else matrix
        matrix_text = json.dumps(matrix_document, indent=2, sort_keys=True) + "\n"
        (root / "expected-matrix.json").write_text(matrix_text)
        provenance = valid_provenance(matrix_text.encode())
        (root / "provenance.txt").write_text(provenance)
        (root / "provenance-final.txt").write_text(valid_final_provenance(provenance))
        status_lines = []
        first_arm_name = matrix_document["arms"][0]["name"]
        for arm in matrix_document["arms"]:
            name = arm["name"]
            task_count = arm["config"]["TaskCount"]
            target_task_rate = arm["config"]["TargetTaskRate"]
            skip_history_server = arm["config"].get("SkipHistoryServer", False)
            status_lines.append(f"{name} rc=0 duration=1s\n")
            run = root / name / "20260808-000000"
            run.mkdir(parents=True)
            arm_report = (
                report
                if name == first_arm_name and report is not None
                else valid_report(
                    task_count,
                    target_task_rate=target_task_rate,
                    skip_history_server=skip_history_server,
                )
            )
            if matrix_document["kind"] == "collector-cap":
                apply_cap_evidence(arm_report)
            (run / "bench-report.json").write_text(json.dumps(arm_report))
            driver_lines = [
                f"T = {task_count}",
                "WAVE = 2000",
                f"TARGET = {target_task_rate}",
                "DRIVERS = 1",
                "@ray.remote(num_cpus=0.5, max_retries=0)",
            ]
            if target_task_rate > 0:
                driver_lines.extend(
                    validate_sweep.TARGET_RATE_PACING_BLOCK.splitlines()
                )
            (run / "driver.py").write_text("\n".join(driver_lines))
            if not skip_history_server:
                (run / "historyserver.log").write_text("clean\n")
            if write_ingress_artifacts:
                (run / "collector_ingress_cgroup_10s.csv").write_text(
                    "window_start_unix_nano,window_end_unix_nano,"
                    "submitted_to_worker_attempts,finished_attempts,backlog_delta,"
                    "pod,ray_node_id,events,cpu_coverage_ratio,valid_for_sizing\n"
                    f"10000000000,20000000000,{task_count},{task_count},0,"
                    "head,node-head,1000,0.9,true\n"
                    f"10000000000,20000000000,{task_count},{task_count},0,"
                    "worker,node-worker,800,0.9,true\n"
                )
                (run / "collector_ingress_gate.csv").write_text(
                    "pod,role,node_ids,log_stream_complete,"
                    "graceful_shutdown_complete,valid,problems\n"
                    'head,head,node-head,true,true,true,""\n'
                    'worker,worker,node-worker,true,true,true,""\n'
                )
                (run / "task_lifecycle_10s.csv").write_text(
                    "window_start_unix_nano,window_end_unix_nano,"
                    "submitted_to_worker_attempts,finished_attempts,backlog_delta\n"
                    f"10000000000,20000000000,{task_count},{task_count},0\n"
                )
        (root / "status.txt").write_text("".join(status_lines))
        return root

    def test_accepts_valid_sweep(self):
        validate_sweep.validate_sweep(self.make_sweep(), 12)

    def test_accepts_legacy_v1_sweep_without_report_skip_field(self):
        report = valid_report()
        report["config"].pop("SkipHistoryServer")
        root = self.make_sweep(report, matrix=valid_legacy_v1_matrix())

        validate_sweep.validate_sweep(root, 12, allow_legacy_schema=True)

    def test_accepts_valid_formal_rate_sweep(self):
        root = self.make_sweep(matrix=valid_rate_matrix())
        validate_sweep.validate_sweep(root, 18)

        self.assertFalse(any(root.glob("*/*/historyserver.log")))

    def test_accepts_valid_collector_cap_sweep(self):
        root = self.make_sweep(matrix=valid_cap_matrix())
        validate_sweep.validate_sweep(root, 3)

        self.assertFalse(any(root.glob("*/*/historyserver.log")))

    def test_rejects_collector_cap_resource_or_memory_evidence_drift(self):
        mutations = (
            ("cpuLimit", "1", "Pod resources"),
            ("cgroupMemoryObserved", False, "no cgroup"),
            ("cgroupMemoryMaxBytes", 256 * 1024 * 1024, "not 192Mi"),
            ("memoryEventsOOM", 1, "memory pressure events"),
            ("cgroupMemoryReadErrors", 1, "read errors"),
        )
        for field, value, message in mutations:
            with self.subTest(field=field):
                root = self.make_sweep(matrix=valid_cap_matrix())
                path = next((root / "rate3000-r1").glob("*/bench-report.json"))
                report = json.loads(path.read_text())
                report["collectorLogs"][0][field] = value
                path.write_text(json.dumps(report))
                with self.assertRaisesRegex(validate_sweep.ValidationError, message):
                    validate_sweep.validate_sweep(root, 3)

    def test_rejects_collector_cap_below_empirical_high_load_floor(self):
        root = self.make_sweep(matrix=valid_cap_matrix())
        path = next((root / "rate3000-r1").glob("*/bench-report.json"))
        report = json.loads(path.read_text())
        report["collectorIngressGates"][0]["peakEventsPerSecond"] = 3999.9
        path.write_text(json.dumps(report))

        with self.assertRaisesRegex(validate_sweep.ValidationError, "want at least 4000.0"):
            validate_sweep.validate_sweep(root, 3)

    def test_rejects_collector_cap_matrix_resource_drift(self):
        for field, value in {
            "CollectorCPURequest": "100m",
            "CollectorCPU": "1",
            "CollectorMemoryRequest": "128Mi",
            "CollectorMemoryLimit": "256Mi",
        }.items():
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp)
                matrix = valid_cap_matrix()
                matrix["arms"][0]["config"][field] = value
                (root / "expected-matrix.json").write_text(json.dumps(matrix))
                with self.assertRaises(validate_sweep.ValidationError):
                    validate_sweep.read_expected_matrix(root, 3)

    def test_accepts_legacy_sweep_without_event_id_counts(self):
        report = valid_report()
        event_fields = (
            "totalEvents",
            "distinctEventIDs",
            "missingEventIDs",
            "duplicateEventIDs",
        )
        for field in event_fields:
            report["storage"]["events"].pop(field)
        for node in report["storage"]["events"]["perNode"]:
            for field in event_fields[1:]:
                node.pop(field)
        root = self.make_sweep(report, matrix=valid_legacy_v1_matrix())

        validate_sweep.validate_sweep(root, 12, allow_legacy_schema=True)

    def test_rejects_formal_rate_missing_event_id_field(self):
        report = valid_report(
            50000,
            target_task_rate=250,
            skip_history_server=True,
        )
        report["storage"]["events"].pop("distinctEventIDs")
        root = self.make_sweep(report, matrix=valid_rate_matrix())

        with self.assertRaisesRegex(
            validate_sweep.ValidationError,
            r"storage\.events\.distinctEventIDs must be a non-negative integer",
        ):
            validate_sweep.validate_sweep(root, 18)

    def test_rejects_formal_rate_negative_event_id_count(self):
        report = valid_report(
            50000,
            target_task_rate=250,
            skip_history_server=True,
        )
        report["storage"]["events"]["duplicateEventIDs"] = -1
        root = self.make_sweep(report, matrix=valid_rate_matrix())

        with self.assertRaisesRegex(
            validate_sweep.ValidationError,
            r"storage\.events\.duplicateEventIDs must be a non-negative integer",
        ):
            validate_sweep.validate_sweep(root, 18)

    def test_rejects_formal_rate_inconsistent_event_id_counts(self):
        report = valid_report(
            50000,
            target_task_rate=250,
            skip_history_server=True,
        )
        report["storage"]["events"]["distinctEventIDs"] = 1799
        root = self.make_sweep(report, matrix=valid_rate_matrix())

        with self.assertRaisesRegex(
            validate_sweep.ValidationError,
            "event-ID counts are inconsistent",
        ):
            validate_sweep.validate_sweep(root, 18)

    def test_rejects_formal_rate_missing_event_id(self):
        report = valid_report(
            50000,
            target_task_rate=250,
            skip_history_server=True,
        )
        events = report["storage"]["events"]
        events["distinctEventIDs"] = 1799
        events["missingEventIDs"] = 1
        head = events["perNode"][0]
        head["distinctEventIDs"] = 999
        head["missingEventIDs"] = 1
        root = self.make_sweep(report, matrix=valid_rate_matrix())

        with self.assertRaisesRegex(
            validate_sweep.ValidationError,
            r"storage\.events\.missingEventIDs=1, expected 0",
        ):
            validate_sweep.validate_sweep(root, 18)

    def test_rejects_formal_rate_cross_node_duplicate_event_id(self):
        report = valid_report(
            50000,
            target_task_rate=250,
            skip_history_server=True,
        )
        events = report["storage"]["events"]
        events["distinctEventIDs"] = 1799
        events["duplicateEventIDs"] = 1
        root = self.make_sweep(report, matrix=valid_rate_matrix())

        with self.assertRaisesRegex(
            validate_sweep.ValidationError,
            r"storage\.events\.duplicateEventIDs=1, expected 0",
        ):
            validate_sweep.validate_sweep(root, 18)

    def test_rejects_loss_duplicate_total_cancellation(self):
        report = valid_report(
            50000,
            target_task_rate=250,
            skip_history_server=True,
        )
        events = report["storage"]["events"]
        events["distinctEventIDs"] = 1799
        events["duplicateEventIDs"] = 1
        head = events["perNode"][0]
        head["distinctEventIDs"] = 999
        head["duplicateEventIDs"] = 1
        self.assertEqual(
            sum(node["events"] for node in events["perNode"]),
            events["totalEvents"],
        )
        self.assertEqual(
            sum(
                window["events"]
                for collector in report["collectorLogs"]
                for window in collector["ingressWindows"]
            ),
            events["totalEvents"],
        )
        root = self.make_sweep(report, matrix=valid_rate_matrix())

        with self.assertRaisesRegex(
            validate_sweep.ValidationError,
            r"storage\.events\.duplicateEventIDs=1, expected 0",
        ):
            validate_sweep.validate_sweep(root, 18)

    def test_success_sentinel_write_is_fail_closed(self):
        script = pathlib.Path(__file__).with_name("ray256_collector.sh").read_text()
        self.assertIn(
            'if ! echo "SWEEP-SUCCEEDED arms=$EXPECTED_ARMS" '
            '| tee -a "$OUT/status.txt"; then',
            script,
        )
        self.assertRegex(
            script,
            r'if ! echo "SWEEP-SUCCEEDED arms=\$EXPECTED_ARMS" '
            r'\| tee -a "\$OUT/status\.txt"; then\n'
            r'\s+echo "SWEEP-FAILED: could not persist success sentinel" >&2\n'
            r"\s+exit 1\n"
            r"fi",
        )

    def test_single_arm_validation_ignores_incomplete_campaign_state(self):
        root = self.make_sweep(matrix=valid_rate_matrix())
        (root / "status.txt").unlink()
        (root / "provenance-final.txt").unlink()

        validate_sweep.validate_single_arm(root, "rate250-r1")

        completed = subprocess.run(
            [
                sys.executable,
                str(pathlib.Path(__file__).with_name("validate_sweep.py")),
                str(root),
                "--single-arm",
                "rate250-r1",
            ],
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("ARM-VALID rate250-r1", completed.stdout)

    def test_single_arm_validation_rejects_semantic_failure(self):
        root = self.make_sweep(matrix=valid_rate_matrix())
        report_path = next((root / "rate250-r1").glob("*/bench-report.json"))
        report = json.loads(report_path.read_text())
        report["storage"]["events"]["distinctEventIDs"] = 1799
        report["storage"]["events"]["duplicateEventIDs"] = 1
        report_path.write_text(json.dumps(report))
        (root / "status.txt").unlink()
        (root / "provenance-final.txt").unlink()

        with self.assertRaisesRegex(
            validate_sweep.ValidationError,
            r"storage\.events\.duplicateEventIDs=1, expected 0",
        ):
            validate_sweep.validate_single_arm(root, "rate250-r1")

    def test_collector_runner_fails_fast_on_single_arm_validation(self):
        script = pathlib.Path(__file__).with_name("ray256_collector.sh").read_text()
        self.assertRegex(
            script,
            r'echo "\$name rc=\$rc duration=\$\(\( .+ \)\)s" '
            r'\| tee -a "\$OUT/status\.txt"\n'
            r'\s+if \[ "\$rc" -ne 0 \]; then\n'
            r'\s+echo "ARM-FAILED \$name" >&2\n'
            r"\s+exit 1\n"
            r"\s+fi\n"
            r'\s+if ! python3 "\$SCRIPT_DIR/validate_sweep\.py" '
            r'"\$OUT" --single-arm "\$name"; then\n'
            r'\s+echo "SWEEP-FAILED: semantic validation rejected \$name" '
            r'\| tee -a "\$OUT/status\.txt"\n'
            r"\s+exit 1\n"
            r"\s+fi",
        )

    def test_rejects_history_server_measurement_in_collector_only_sweep(self):
        report = valid_report(
            50000,
            target_task_rate=250,
            skip_history_server=True,
        )
        report["historyServer"] = {
            "enterMeasured": True,
            "enterStatus": 200,
            "warmEndpoints": [],
        }
        root = self.make_sweep(report, matrix=valid_rate_matrix())

        with self.assertRaisesRegex(
            validate_sweep.ValidationError,
            "unexpectedly measured a History Server",
        ):
            validate_sweep.validate_sweep(root, 18)

    def test_rejects_history_server_log_in_collector_only_sweep(self):
        root = self.make_sweep(matrix=valid_rate_matrix())
        run = next((root / "rate250-r1").iterdir())
        (run / "historyserver.log").write_text("unexpected\n")

        with self.assertRaisesRegex(
            validate_sweep.ValidationError,
            "historyserver.log must be absent",
        ):
            validate_sweep.validate_sweep(root, 18)

    def test_rejects_formal_rate_matrix_without_unpaced_arm(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            matrix = valid_rate_matrix()
            matrix["arms"] = [
                arm
                for arm in matrix["arms"]
                if arm["config"]["TargetTaskRate"] != 0
            ]
            (root / "expected-matrix.json").write_text(json.dumps(matrix))
            with self.assertRaisesRegex(
                validate_sweep.ValidationError, "target task rates"
            ):
                validate_sweep.read_expected_matrix(root, 15)

    def test_rejects_unexpected_formal_target_rate(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            matrix = valid_rate_matrix()
            matrix["arms"][0]["name"] = "rate251-r1"
            matrix["arms"][0]["config"]["TargetTaskRate"] = 251
            (root / "expected-matrix.json").write_text(json.dumps(matrix))
            with self.assertRaisesRegex(
                validate_sweep.ValidationError, "target task rates"
            ):
                validate_sweep.read_expected_matrix(root, 18)

    def test_rejects_formal_rate_matrix_fixed_config_drift(self):
        mutations = {
            "TaskCount": 49999,
            "WaveSize": 1000,
            "TaskNumCPUs": "0.2",
            "RayImage": "rayproject/ray:2.54.0",
            "Compression": False,
            "ShutdownAfterJob": False,
            "JobTTLSeconds": 0,
            "DrainSleepSec": 25,
            "WarmIterations": 1,
            "HSCPURequest": "2",
            "HSCPULimit": "2",
            "HSArgs": "--session-process-timeout=2m",
            "Drivers": 2,
        }
        for field, value in mutations.items():
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp)
                matrix = valid_rate_matrix()
                matrix["arms"][0]["config"][field] = value
                (root / "expected-matrix.json").write_text(json.dumps(matrix))
                with self.assertRaises(validate_sweep.ValidationError):
                    validate_sweep.read_expected_matrix(root, 18)

    def test_rejects_post_job_sleep_in_formal_paced_driver(self):
        root = self.make_sweep(matrix=valid_rate_matrix())
        driver = next((root / "rate250-r1").glob("*/driver.py"))
        with driver.open("a") as stream:
            stream.write("\ntime.sleep(25)\n")
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "only the exact target-rate pacing sleep"
        ):
            validate_sweep.validate_sweep(root, 18)

    def test_rejects_missing_runtime_image(self):
        root = self.make_sweep()
        text = (root / "provenance.txt").read_text()
        (root / "provenance.txt").write_text(
            text.replace(f"ray_runtime_id={IMAGE_ID}", "ray_runtime_id=")
        )
        with self.assertRaisesRegex(validate_sweep.ValidationError, "empty"):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_incomplete_tasks(self):
        root = self.make_sweep(valid_report(expected=1000, observed=999))
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "benchTaskIDs mismatch"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_storage_task_count_that_differs_from_matrix(self):
        report = valid_report(expected=999)
        report["config"] = valid_config(1000)
        root = self.make_sweep(report)
        with self.assertRaisesRegex(
            validate_sweep.ValidationError,
            "expectedTasks does not match expected matrix",
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_task_decode_error(self):
        root = self.make_sweep()
        log = next(root.glob("*/*/historyserver.log"))
        log.write_text("failed to unmarshal task lifecycle event\n")
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "historyserver.log"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_warm_query_error(self):
        report = valid_report()
        report["historyServer"]["warmEndpoints"][0]["errors"] = 1
        root = self.make_sweep(report)
        with self.assertRaisesRegex(validate_sweep.ValidationError, "warm endpoint"):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_missing_ingress_artifact(self):
        root = self.make_sweep(write_ingress_artifacts=False)
        with self.assertRaisesRegex(validate_sweep.ValidationError, "is missing"):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_missing_shutdown_marker_in_gate_csv(self):
        root = self.make_sweep()
        path = next(root.glob("*/*/collector_ingress_gate.csv"))
        path.write_text(path.read_text().replace("true,true,true", "true,false,true", 1))
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "missing graceful shutdown-complete marker"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_shared_head_worker_node_id(self):
        report = valid_report()
        report["collectorIngressGates"][1]["nodeIDs"] = ["node-head"]
        root = self.make_sweep(report)
        with self.assertRaisesRegex(validate_sweep.ValidationError, "share one NodeID"):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_incomplete_collector_log_stream(self):
        report = valid_report()
        report["collectorLogs"][0]["logStreamComplete"] = False
        root = self.make_sweep(report)
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "log stream is incomplete"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_missing_collector_shutdown_marker(self):
        report = valid_report()
        report["collectorLogs"][0]["gracefulShutdownComplete"] = False
        root = self.make_sweep(report)
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "shutdown-complete marker"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_accepts_repository_qualified_collector_image_identity(self):
        report = valid_report(
            50000,
            target_task_rate=250,
            skip_history_server=True,
        )
        for collector in report["collectorLogs"]:
            collector["image"] = "docker.io/library/collector:v0.1.0"
            collector["imageID"] = "docker.io/library/collector@" + IMAGE_ID
        root = self.make_sweep(report, matrix=valid_rate_matrix())

        validate_sweep.validate_sweep(root, 18)

    def test_rejects_unbound_collector_runtime_image(self):
        mutations = (
            ("image", "other:v0.1.0", "image does not match"),
            ("image", "", "image does not match"),
            ("imageID", "sha256:" + "d" * 64, "imageID does not match"),
            ("imageID", "sha256:short", "invalid imageID"),
            ("imageID", "", "invalid imageID"),
        )
        for role_index in (0, 1):
            for field, value, message in mutations:
                with self.subTest(role_index=role_index, field=field, value=value):
                    report = valid_report(
                        50000,
                        target_task_rate=250,
                        skip_history_server=True,
                    )
                    report["collectorLogs"][role_index][field] = value
                    root = self.make_sweep(report, matrix=valid_rate_matrix())
                    with self.assertRaisesRegex(
                        validate_sweep.ValidationError, message
                    ):
                        validate_sweep.validate_sweep(root, 18)

    def test_legacy_v1_does_not_require_collector_runtime_image_fields(self):
        report = valid_report()
        report["config"].pop("SkipHistoryServer")
        for collector in report["collectorLogs"]:
            collector.pop("image")
            collector.pop("imageID")
        root = self.make_sweep(report, matrix=valid_legacy_v1_matrix())

        validate_sweep.validate_sweep(root, 12, allow_legacy_schema=True)

    def test_rejects_missing_collector_gate_shutdown_marker(self):
        report = valid_report()
        report["collectorIngressGates"][0]["gracefulShutdownComplete"] = False
        root = self.make_sweep(report)
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "shutdown-complete marker"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_accepts_missed_k8s_termination_with_explicit_shutdown_marker(self):
        report = valid_report()
        report["podTerminations"] = []
        root = self.make_sweep(report)
        validate_sweep.validate_sweep(root, 12)

    def test_accepts_unobserved_k8s_termination_with_explicit_shutdown_marker(self):
        report = valid_report()
        for termination in report["podTerminations"]:
            termination.update(
                {"observed": False, "source": "", "exitCode": 0, "reason": ""}
            )
        root = self.make_sweep(report)
        validate_sweep.validate_sweep(root, 12)

    def test_rejects_missed_termination_after_collector_restart(self):
        report = valid_report()
        report["podTerminations"] = []
        report["collectorLogs"][0]["restartCount"] = 1
        root = self.make_sweep(report)
        with self.assertRaisesRegex(validate_sweep.ValidationError, "restartCount"):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_non_graceful_collector_termination(self):
        report = valid_report()
        report["podTerminations"][0]["exitCode"] = 137
        report["podTerminations"][0]["reason"] = "Error"
        root = self.make_sweep(report)
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "non-current, mismatched, or non-zero"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_mismatched_collector_termination(self):
        report = valid_report()
        report["podTerminations"][0]["containerID"] = "stale-container"
        root = self.make_sweep(report)
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "non-current, mismatched, or non-zero"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_collector_ingress_and_storage_conservation_drift(self):
        mutations = (
            ("events", 1, "ingress event total"),
            ("events", -1, "ingress event total"),
            ("uploadedBytes", 1, "uploadedBytes does not match"),
            ("uploadedBytes", -1, "uploadedBytes does not match"),
        )
        for field, delta, message in mutations:
            with self.subTest(field=field, delta=delta):
                report = valid_report()
                if field == "uploadedBytes":
                    report["collectorLogs"][0][field] += delta
                else:
                    report["collectorLogs"][0]["ingressWindows"][0][field] += delta
                root = self.make_sweep(report)
                with self.assertRaisesRegex(
                    validate_sweep.ValidationError, message
                ):
                    validate_sweep.validate_sweep(root, 12)

    def test_allows_request_bytes_to_differ_from_stored_raw_bytes(self):
        report = valid_report()
        self.assertNotEqual(
            report["collectorLogs"][0]["ingressWindows"][0]["bytes"],
            report["storage"]["events"]["perNode"][0]["rawBytes"],
        )
        root = self.make_sweep(report)
        validate_sweep.validate_sweep(root, 12)

    def test_rejects_collector_ingress_spanning_multiple_node_ids(self):
        report = valid_report()
        report["collectorLogs"][0]["ingressWindows"].append(
            {"nodeID": "node-worker", "events": 0, "bytes": 0}
        )
        root = self.make_sweep(report)
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "exactly one NodeID"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_missing_final_provenance(self):
        root = self.make_sweep()
        (root / "provenance-final.txt").unlink()
        with self.assertRaisesRegex(validate_sweep.ValidationError, "provenance-final"):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_duplicate_provenance_key(self):
        root = self.make_sweep()
        with (root / "provenance.txt").open("a") as stream:
            stream.write(f"ray_runtime_id={IMAGE_ID}\n")
        with self.assertRaisesRegex(validate_sweep.ValidationError, "duplicate key"):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_short_runtime_image_id(self):
        root = self.make_sweep()
        path = root / "provenance.txt"
        path.write_text(path.read_text().replace(IMAGE_ID, "sha256:abc", 1))
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "full sha256 image ID"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_unbound_runtime_source(self):
        root = self.make_sweep()
        path = root / "provenance.txt"
        path.write_text(
            path.read_text().replace(
                f"collector_runtime_build_source_sha256={SHA}",
                f"collector_runtime_build_source_sha256={'d' * 64}",
            )
        )
        with self.assertRaisesRegex(validate_sweep.ValidationError, "not bound"):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_report_config_that_differs_from_matrix(self):
        report = valid_report()
        report["config"]["WaveSize"] = 1
        root = self.make_sweep(report)
        with self.assertRaisesRegex(validate_sweep.ValidationError, "config.WaveSize"):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_wrong_warm_task_limit(self):
        report = valid_report()
        report["historyServer"]["warmEndpoints"][0][
            "endpoint"
        ] = "/api/v0/tasks?limit=999"
        root = self.make_sweep(report)
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "exact expected query set"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_zero_warm_iterations(self):
        report = valid_report()
        report["config"]["WarmIterations"] = 0
        root = self.make_sweep(report)
        with self.assertRaisesRegex(validate_sweep.ValidationError, "WarmIterations"):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_sleep_in_formal_rendered_driver(self):
        root = self.make_sweep()
        driver = next(root.glob("*/*/driver.py"))
        with driver.open("a") as stream:
            stream.write("\ntime.sleep(10)\n")
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "driver contains time.sleep"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_created_rayjob_without_formal_lifecycle(self):
        report = valid_report()
        report["rayJobLifecycle"]["ownedCluster"] = False
        root = self.make_sweep(report)
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "rayJobLifecycle.ownedCluster"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_missing_submitted_attempt(self):
        report = valid_report()
        report["storage"]["events"]["benchTaskValidity"][
            "submittedToWorkerAttempts"
        ] = 999
        root = self.make_sweep(report)
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "submittedToWorkerAttempts"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_task_lifecycle_window_total(self):
        report = valid_report()
        report["storage"]["events"]["taskLifecycleWindows"][0]["finishedAttempts"] = 999
        report["storage"]["events"]["taskLifecycleWindows"][0]["backlogDelta"] = 1
        root = self.make_sweep(report)
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "totals do not match"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_task_lifecycle_csv_total(self):
        root = self.make_sweep()
        path = next(root.glob("*/*/task_lifecycle_10s.csv"))
        path.write_text(
            "window_start_unix_nano,window_end_unix_nano,"
            "submitted_to_worker_attempts,finished_attempts,backlog_delta\n"
            "10000000000,20000000000,999,999,0\n"
        )
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "task_lifecycle_10s.csv totals"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_collector_joined_task_count_mismatch(self):
        report = valid_report()
        report["collectorWindows"][0]["submittedToWorkerAttempts"] = 999
        root = self.make_sweep(report)
        with self.assertRaisesRegex(
            validate_sweep.ValidationError,
            "task counts do not match task_lifecycle_10s.csv",
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_collector_csv_joined_task_count_mismatch(self):
        root = self.make_sweep()
        path = next((root / "n1000-r1").glob("*/collector_ingress_cgroup_10s.csv"))
        path.write_text(
            path.read_text().replace(
                "10000000000,20000000000,1000,1000,0,worker",
                "10000000000,20000000000,999,999,0,worker",
            )
        )
        with self.assertRaisesRegex(
            validate_sweep.ValidationError,
            "task counts do not match task_lifecycle_10s.csv",
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_stale_task_lifecycle_range(self):
        report = valid_report()
        stale_start = 10_000_000_000 + 100 * validate_sweep.TEN_SECONDS_NANO
        stale_end = stale_start + validate_sweep.TEN_SECONDS_NANO
        task_window = report["storage"]["events"]["taskLifecycleWindows"][0]
        task_window["windowStartUnixNano"] = stale_start
        task_window["windowEndUnixNano"] = stale_end
        root = self.make_sweep(report)
        lifecycle = next((root / "n1000-r1").glob("*/task_lifecycle_10s.csv"))
        lifecycle.write_text(
            "window_start_unix_nano,window_end_unix_nano,"
            "submitted_to_worker_attempts,finished_attempts,backlog_delta\n"
            f"{stale_start},{stale_end},1000,1000,0\n"
        )
        with self.assertRaisesRegex(
            validate_sweep.ValidationError, "more than one 10-second bucket apart"
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_task_bucket_missing_from_collector_table(self):
        report = valid_report()
        for row in report["collectorWindows"]:
            row.update(
                {
                    "windowStartUnixNano": 20_000_000_000,
                    "windowEndUnixNano": 30_000_000_000,
                    "submittedToWorkerAttempts": 0,
                    "finishedAttempts": 0,
                    "backlogDelta": 0,
                }
            )
        root = self.make_sweep(report)
        joined = next((root / "n1000-r1").glob("*/collector_ingress_cgroup_10s.csv"))
        joined.write_text(
            "window_start_unix_nano,window_end_unix_nano,"
            "submitted_to_worker_attempts,finished_attempts,backlog_delta,"
            "pod,ray_node_id,events,cpu_coverage_ratio,valid_for_sizing\n"
            "20000000000,30000000000,0,0,0,head,node-head,1000,0.9,true\n"
            "20000000000,30000000000,0,0,0,worker,node-worker,800,0.9,true\n"
        )
        with self.assertRaisesRegex(
            validate_sweep.ValidationError,
            "Collector windows do not contain every task lifecycle bucket",
        ):
            validate_sweep.validate_sweep(root, 12)

    def test_task_window_sequence_rejects_negative_prefix_and_misalignment(self):
        errors = []
        validate_sweep.validate_task_window_sequence(
            "fixture",
            {
                10_000_000_000: (20_000_000_000, 0, 1, -1),
                20_000_000_000: (30_000_000_000, 1, 0, 1),
                30_000_000_001: (40_000_000_001, 0, 0, 0),
            },
            errors,
        )
        self.assertTrue(any("cumulative task backlog is negative" in e for e in errors))
        self.assertTrue(any("not epoch-aligned" in e for e in errors))

    def test_rejects_status_arm_not_in_matrix(self):
        root = self.make_sweep()
        status = root / "status.txt"
        status.write_text(status.read_text().replace("n1000-r1", "wrong-arm", 1))
        with self.assertRaisesRegex(validate_sweep.ValidationError, "status arm set"):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_expected_matrix_drift(self):
        root = self.make_sweep()
        matrix = root / "expected-matrix.json"
        matrix.write_text(
            matrix.read_text().replace('"WaveSize": 2000', '"WaveSize": 1', 1)
        )
        with self.assertRaisesRegex(validate_sweep.ValidationError, "matrix SHA-256"):
            validate_sweep.validate_sweep(root, 12)

    def test_rejects_duplicate_expected_matrix_key(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            text = json.dumps(valid_matrix(), indent=2, sort_keys=True)
            text = text.replace(
                '"schemaVersion": 4', '"schemaVersion": 4, "schemaVersion": 4', 1
            )
            (root / "expected-matrix.json").write_text(text)
            with self.assertRaisesRegex(
                validate_sweep.ValidationError, "duplicate key"
            ):
                validate_sweep.read_expected_matrix(root, 12)

    def test_rejects_campaign_config_drift_between_repeats(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            matrix = valid_matrix()
            matrix["arms"][-1]["config"]["WaveSize"] = 1000
            (root / "expected-matrix.json").write_text(json.dumps(matrix))
            with self.assertRaisesRegex(
                validate_sweep.ValidationError, "campaign-wide config"
            ):
                validate_sweep.read_expected_matrix(root, 12)


if __name__ == "__main__":
    unittest.main()
