#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import errno
import io
import json
import pathlib
import subprocess
import tempfile
import unittest
from unittest import mock

import formal_runner_guard


UID = "11111111-1111-1111-1111-111111111111"


def kubeconfig(*, port: int = 64027, context: str = "kind-bench") -> dict:
    return {
        "current-context": context,
        "contexts": [
            {
                "name": context,
                "context": {"cluster": context, "user": context},
            }
        ],
        "clusters": [
            {
                "name": context,
                "cluster": {"server": f"https://127.0.0.1:{port}"},
            }
        ],
    }


def pod(*, labels: dict[str, str], name: str = "pod", image: str = "rayproject/ray:2.56.0") -> dict:
    return {
        "kind": "Pod",
        "metadata": {"namespace": "test", "name": name, "labels": labels},
        "spec": {"containers": [{"name": "ray-head", "image": image}]},
        "status": {"phase": "Running"},
    }


def workload_inventory(*items: dict) -> dict:
    return {
        "apiVersion": "v1",
        "kind": "List",
        "metadata": {"resourceVersion": ""},
        "items": list(items),
    }


def workload_cr(
    kind: str,
    *,
    name: str,
    namespace: str = "test",
    status: dict | None = None,
    deleting: bool = False,
) -> dict:
    metadata = {"namespace": namespace, "name": name}
    if deleting:
        metadata["deletionTimestamp"] = "2026-08-09T05:00:00Z"
    return {
        "apiVersion": "ray.io/v1",
        "kind": kind,
        "metadata": metadata,
        "status": status or {},
    }


def rayjob_origin(name: str) -> dict[str, str]:
    return {
        "ray.io/originated-from-cr-name": name,
        "ray.io/originated-from-crd": "RayJob",
    }


def submitter_job(
    *,
    rayjob_name: str,
    active: int = 0,
    terminal: bool = True,
    namespace: str = "test",
) -> dict:
    conditions = [{"type": "Complete", "status": "True"}] if terminal else []
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "namespace": namespace,
            "name": rayjob_name,
            "labels": rayjob_origin(rayjob_name),
        },
        "status": {"active": active, "conditions": conditions},
    }


def workload_pod(
    *,
    name: str,
    phase: str,
    labels: dict[str, str],
    namespace: str = "test",
) -> dict:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": namespace,
            "name": name,
            "labels": labels,
        },
        "status": {"phase": phase},
    }


class OperatorGuardTest(unittest.TestCase):
    def test_created_by_label_is_not_operator_identity(self) -> None:
        item = pod(labels={"app.kubernetes.io/created-by": "kuberay-operator"})
        self.assertEqual(formal_runner_guard.active_operator_objects({"items": [item]}), [])

    def test_identity_label_or_process_image_detects_operator(self) -> None:
        labeled = pod(
            labels={"app.kubernetes.io/name": "kuberay-operator"},
            name="controller",
        )
        imaged = pod(
            labels={"app": "unrelated"},
            name="controller-2",
            image="quay.io/kuberay/operator:v1.5.0",
        )
        self.assertEqual(
            formal_runner_guard.active_operator_objects({"items": [labeled, imaged]}),
            ["pod test/controller", "pod test/controller-2"],
        )

    def test_terminating_running_operator_remains_active(self) -> None:
        item = pod(
            labels={"app.kubernetes.io/name": "kuberay-operator"},
            name="terminating-controller",
        )
        item["metadata"]["deletionTimestamp"] = "2026-08-09T05:00:00Z"
        self.assertEqual(
            formal_runner_guard.active_operator_objects({"items": [item]}),
            ["pod test/terminating-controller deleting"],
        )

    def test_malformed_operator_deletion_timestamp_fails_closed(self) -> None:
        item = pod(
            labels={"app.kubernetes.io/name": "kuberay-operator"},
            name="malformed-controller",
        )
        item["metadata"]["deletionTimestamp"] = False
        with self.assertRaises(SystemExit):
            formal_runner_guard.active_operator_objects({"items": [item]})

    def test_malformed_inventory_fails_closed(self) -> None:
        with self.assertRaises(SystemExit):
            formal_runner_guard.active_operator_objects({"items": [{}]})


class ClusterWorkloadGuardTest(unittest.TestCase):
    def assert_check_fails(self, document: dict, context: str = "kind-bench") -> str:
        stderr = io.StringIO()
        with mock.patch.object(
            formal_runner_guard.sys,
            "stdin",
            io.StringIO(json.dumps(document)),
        ), contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            formal_runner_guard.check_cluster_workloads(context)
        return stderr.getvalue()

    def test_stale_ready_raycluster_without_pods_is_rejected(self) -> None:
        message = self.assert_check_fails(
            workload_inventory(
                workload_cr(
                    "RayCluster",
                    name="raycluster-historyserver",
                    status={"state": "ready"},
                )
            )
        )
        self.assertIn("raycluster test/raycluster-historyserver", message)

    def test_deleting_raycluster_is_rejected_until_absent(self) -> None:
        message = self.assert_check_fails(
            workload_inventory(
                workload_cr("RayCluster", name="deleting-cluster", deleting=True)
            )
        )
        self.assertIn("raycluster test/deleting-cluster deleting", message)

    def test_rayservice_is_rejected(self) -> None:
        message = self.assert_check_fails(
            workload_inventory(workload_cr("RayService", name="serve"))
        )
        self.assertIn("rayservice test/serve", message)

    def test_active_rayjob_is_rejected_without_pods(self) -> None:
        message = self.assert_check_fails(
            workload_inventory(
                workload_cr(
                    "RayJob",
                    name="active-job",
                    status={
                        "jobStatus": "RUNNING",
                        "jobDeploymentStatus": "Running",
                        "rayClusterName": "active-job-ab123",
                    },
                )
            )
        )
        self.assertIn("rayjob test/active-job", message)
        self.assertIn("jobStatus=RUNNING", message)

    def test_unsuccessful_terminal_rayjob_is_rejected(self) -> None:
        message = self.assert_check_fails(
            workload_inventory(
                workload_cr(
                    "RayJob",
                    name="failed-job",
                    status={
                        "jobStatus": "FAILED",
                        "jobDeploymentStatus": "Failed",
                    },
                )
            )
        )
        self.assertIn("rayjob test/failed-job", message)
        self.assertIn("jobStatus=FAILED", message)

    def test_terminal_success_residue_with_completed_submitter_is_allowed(self) -> None:
        document = workload_inventory(
            workload_cr(
                "RayJob",
                name="old-job",
                status={
                    "jobStatus": "SUCCEEDED",
                    "jobDeploymentStatus": "Complete",
                    "rayClusterName": "old-job-ab123",
                },
            ),
            submitter_job(rayjob_name="old-job"),
            workload_pod(
                name="old-job-submitter",
                phase="Succeeded",
                labels=rayjob_origin("old-job"),
            ),
        )
        output = io.StringIO()
        with mock.patch.object(
            formal_runner_guard.sys,
            "stdin",
            io.StringIO(json.dumps(document)),
        ), contextlib.redirect_stdout(output):
            formal_runner_guard.check_cluster_workloads("kind-bench")
        self.assertEqual(
            output.getvalue(),
            "KUBERAY-WORKLOADS-CLEAR context=kind-bench terminal-rayjobs=1\n",
        )

    def test_terminal_rayjob_with_live_submitter_is_rejected(self) -> None:
        terminal = workload_cr(
            "RayJob",
            name="old-job",
            status={
                "jobStatus": "SUCCEEDED",
                "jobDeploymentStatus": "Complete",
            },
        )
        message = self.assert_check_fails(
            workload_inventory(
                terminal,
                submitter_job(rayjob_name="old-job", active=1, terminal=False),
            )
        )
        self.assertIn("live submitter job test/old-job", message)

    def test_terminal_rayjob_with_live_owned_pod_is_rejected(self) -> None:
        message = self.assert_check_fails(
            workload_inventory(
                workload_cr(
                    "RayJob",
                    name="old-job",
                    status={
                        "jobStatus": "SUCCEEDED",
                        "jobDeploymentStatus": "Complete",
                    },
                ),
                workload_pod(
                    name="old-job-head",
                    phase="Running",
                    labels={
                        **rayjob_origin("old-job"),
                        "ray.io/cluster": "old-job-ab123",
                    },
                ),
            )
        )
        self.assertIn("live pod test/old-job-head phase=Running", message)

    def test_terminal_rayjob_with_owned_raycluster_is_rejected(self) -> None:
        owned_cluster = workload_cr("RayCluster", name="old-job-ab123")
        owned_cluster["metadata"]["labels"] = rayjob_origin("old-job")
        message = self.assert_check_fails(
            workload_inventory(
                workload_cr(
                    "RayJob",
                    name="old-job",
                    status={
                        "jobStatus": "SUCCEEDED",
                        "jobDeploymentStatus": "Complete",
                        "rayClusterName": "old-job-ab123",
                    },
                ),
                owned_cluster,
            )
        )
        self.assertIn("raycluster test/old-job-ab123", message)

    def test_live_submitter_pod_is_bound_through_job_owner(self) -> None:
        terminal = workload_cr(
            "RayJob",
            name="old-job",
            status={
                "jobStatus": "SUCCEEDED",
                "jobDeploymentStatus": "Complete",
            },
        )
        pod_without_origin_labels = workload_pod(
            name="old-job-ab123",
            phase="Running",
            labels={},
        )
        pod_without_origin_labels["metadata"]["ownerReferences"] = [
            {"apiVersion": "batch/v1", "kind": "Job", "name": "old-job"}
        ]
        message = self.assert_check_fails(
            workload_inventory(
                terminal,
                submitter_job(rayjob_name="old-job"),
                pod_without_origin_labels,
            )
        )
        self.assertIn("live pod test/old-job-ab123 phase=Running", message)

    def test_kuberay_owner_api_mismatch_fails_closed(self) -> None:
        live_pod = workload_pod(name="head", phase="Running", labels={})
        live_pod["metadata"]["ownerReferences"] = [
            {
                "apiVersion": "ray.io/v1alpha1",
                "kind": "RayCluster",
                "name": "stale-cluster",
            }
        ]
        with self.assertRaises(SystemExit):
            formal_runner_guard.reconcilable_workload_objects(
                workload_inventory(live_pod)
            )

        unknown_kuberay_owner = workload_pod(
            name="head-2",
            phase="Running",
            labels={},
        )
        unknown_kuberay_owner["metadata"]["ownerReferences"] = [
            {"apiVersion": "ray.io/v1", "kind": "UnknownRayKind", "name": "owner"}
        ]
        with self.assertRaises(SystemExit):
            formal_runner_guard.reconcilable_workload_objects(
                workload_inventory(unknown_kuberay_owner)
            )

    def test_submitter_job_origin_owner_conflict_fails_closed(self) -> None:
        job = submitter_job(rayjob_name="old-job")
        job["metadata"]["ownerReferences"] = [
            {"apiVersion": "ray.io/v1", "kind": "RayJob", "name": "other-job"}
        ]
        with self.assertRaises(SystemExit):
            formal_runner_guard.reconcilable_workload_objects(
                workload_inventory(
                    workload_cr(
                        "RayJob",
                        name="old-job",
                        status={
                            "jobStatus": "SUCCEEDED",
                            "jobDeploymentStatus": "Complete",
                        },
                    ),
                    job,
                )
            )

    def test_submitter_pod_origin_owner_conflict_fails_closed(self) -> None:
        conflicting_pod = workload_pod(
            name="old-job-submitter",
            phase="Succeeded",
            labels=rayjob_origin("old-job"),
        )
        conflicting_pod["metadata"]["ownerReferences"] = [
            {"apiVersion": "batch/v1", "kind": "Job", "name": "other-job"}
        ]
        with self.assertRaises(SystemExit):
            formal_runner_guard.reconcilable_workload_objects(
                workload_inventory(
                    workload_cr(
                        "RayJob",
                        name="old-job",
                        status={
                            "jobStatus": "SUCCEEDED",
                            "jobDeploymentStatus": "Complete",
                        },
                    ),
                    conflicting_pod,
                )
            )

    def test_wrong_context_and_malformed_types_fail_closed(self) -> None:
        self.assertIn(
            "context='kind-other'",
            self.assert_check_fails(workload_inventory(), context="kind-other"),
        )
        malformed = submitter_job(rayjob_name="old-job")
        malformed["status"]["active"] = True
        with self.assertRaises(SystemExit):
            formal_runner_guard.reconcilable_workload_objects(
                workload_inventory(
                    workload_cr(
                        "RayJob",
                        name="old-job",
                        status={
                            "jobStatus": "SUCCEEDED",
                            "jobDeploymentStatus": "Complete",
                        },
                    ),
                    malformed,
                )
            )

    def test_unknown_inventory_kind_fails_closed(self) -> None:
        with self.assertRaises(SystemExit):
            formal_runner_guard.reconcilable_workload_objects(
                workload_inventory(
                    {
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "metadata": {"namespace": "test", "name": "unexpected"},
                    }
                )
            )


class HostOperatorGuardTest(unittest.TestCase):
    def test_current_api_server_is_bound_to_exact_context(self) -> None:
        endpoint = formal_runner_guard.current_api_server_endpoint(kubeconfig())
        self.assertEqual(endpoint, formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027))

        with self.assertRaises(SystemExit):
            formal_runner_guard.current_api_server_endpoint(
                kubeconfig(context="kind-other")
            )

    def test_renamed_manager_on_same_endpoint_is_operator(self) -> None:
        identity = formal_runner_guard.HostProcessIdentity(
            pid=64255,
            lsof_command="bench-daemon",
            command=(
                "/private/tmp/bench-daemon --metrics-addr=:8083 "
                "--health-probe-bind-address=:8085 "
                "--enable-leader-election=false --use-kubernetes-proxy"
            ),
            cwd="/private/tmp",
            text_paths=("/private/tmp/bench-daemon", "/usr/lib/dyld"),
        )
        self.assertTrue(formal_runner_guard.host_operator_evidence(identity))

        spoofed_name = formal_runner_guard.HostProcessIdentity(
            pid=64256,
            lsof_command="kubectl",
            command="/private/tmp/kubectl --use-kubernetes-proxy",
            cwd="/private/tmp",
            text_paths=("/private/tmp/kubectl",),
        )
        self.assertTrue(formal_runner_guard.host_operator_evidence(spoofed_name))

        relocated_default_binary = formal_runner_guard.HostProcessIdentity(
            pid=64257,
            lsof_command="bench-daemon",
            command="/private/tmp/bench-daemon",
            cwd="/private/tmp",
            text_paths=("/private/tmp/bench-daemon", "/usr/lib/dyld"),
            executable="/private/tmp/bench-daemon",
            go_build_info=(
                "/private/tmp/bench-daemon: go1.26.3\n"
                "\tpath\tcommand-line-arguments\n"
                "\tdep\tgithub.com/ray-project/kuberay/ray-operator\t(devel)\n"
            ),
        )
        self.assertTrue(
            formal_runner_guard.host_operator_evidence(relocated_default_binary)
        )

    def test_kubectl_operator_operand_is_not_process_identity(self) -> None:
        identity = formal_runner_guard.HostProcessIdentity(
            pid=90804,
            lsof_command="kubectl",
            command="/usr/local/bin/kubectl logs pod/kuberay-operator-abc",
            cwd="/Users/example/ray-operator",
            text_paths=("/usr/local/bin/kubectl", "/usr/lib/dyld"),
            executable="/usr/local/bin/kubectl",
            go_build_info=(
                "/usr/local/bin/kubectl: go1.26.3\n"
                "\tpath\tk8s.io/kubernetes/cmd/kubectl\n"
            ),
        )
        self.assertFalse(formal_runner_guard.host_operator_evidence(identity))

    def test_same_command_on_other_api_endpoint_is_not_selected(self) -> None:
        inventory = "\n".join(
            (
                "p911",
                "ccom.docker.backend",
                "f175",
                "n127.0.0.1:64027",
                "p77253",
                "cmanager",
                "f4",
                "n127.0.0.1:64512->127.0.0.1:61990",
            )
        )
        self.assertEqual(
            formal_runner_guard.parse_lsof_api_connections(
                inventory,
                formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027),
            ),
            {},
        )

    def test_short_lived_kubectl_is_explicitly_ignored(self) -> None:
        connection = formal_runner_guard.HostAPIConnection(90804, "kubectl")
        identity = formal_runner_guard.HostProcessIdentity(
            pid=90804,
            lsof_command="kubectl",
            command="/usr/local/bin/kubectl get pods --watch",
            cwd="/Users/example/ray-operator",
            text_paths=("/usr/local/bin/kubectl", "/usr/lib/dyld"),
        )
        output = io.StringIO()
        with mock.patch.object(
            formal_runner_guard,
            "run_lsof_api_inventory",
            return_value={connection.pid: connection},
        ), mock.patch.object(
            formal_runner_guard,
            "inspect_host_process",
            return_value=identity,
        ) as inspect, mock.patch.object(
            formal_runner_guard.sys,
            "stdin",
            io.StringIO(json.dumps(kubeconfig())),
        ), contextlib.redirect_stdout(output):
            formal_runner_guard.check_host_operators()

        inspect.assert_called_once_with(
            connection,
            formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027),
        )
        self.assertIn("HOST-OPERATORS-CLEAR", output.getvalue())

    def test_disappearing_kubectl_is_ignored_after_endpoint_recheck(self) -> None:
        connection = formal_runner_guard.HostAPIConnection(90804, "kubectl")
        ps_result = subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr=""
        )
        with mock.patch.object(
            formal_runner_guard.shutil,
            "which",
            side_effect=lambda name: f"/usr/bin/{name}",
        ), mock.patch.object(
            formal_runner_guard.subprocess,
            "run",
            return_value=ps_result,
        ), mock.patch.object(
            formal_runner_guard,
            "process_still_connected",
            return_value=False,
        ):
            self.assertIsNone(
                formal_runner_guard.inspect_host_process(
                    connection,
                    formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027),
                )
            )

    def test_ps_eperm_uses_exact_k9s_lsof_and_go_identity(self) -> None:
        connection = formal_runner_guard.HostAPIConnection(59304, "k9s")
        files_result = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=(
                "p59304\n"
                "ck9s\n"
                "fcwd\n"
                "n/Users/example/kuberay\n"
                "ftxt\n"
                "n/opt/homebrew/Cellar/k9s/0.32.5/bin/k9s\n"
            ),
            stderr="",
        )
        denied = PermissionError(errno.EPERM, "Operation not permitted", "/bin/ps")
        build_info = (
            "/opt/homebrew/Cellar/k9s/0.32.5/bin/k9s: go1.22.4\n"
            "\tpath\tgithub.com/derailed/k9s\n"
        )
        with mock.patch.object(
            formal_runner_guard.shutil,
            "which",
            side_effect=lambda name: f"/usr/bin/{name}",
        ), mock.patch.object(
            formal_runner_guard.subprocess,
            "run",
            side_effect=(files_result, denied),
        ), mock.patch.object(
            formal_runner_guard,
            "read_go_build_info",
            return_value=build_info,
        ):
            identity = formal_runner_guard.inspect_host_process(
                connection,
                formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027),
            )

        self.assertIsNotNone(identity)
        assert identity is not None
        self.assertEqual(identity.command, "/opt/homebrew/Cellar/k9s/0.32.5/bin/k9s")
        self.assertFalse(formal_runner_guard.host_operator_evidence(identity))

    def test_ps_eperm_rejects_benign_name_with_wrong_go_identity(self) -> None:
        connection = formal_runner_guard.HostAPIConnection(59304, "k9s")
        files_result = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=(
                "p59304\nck9s\nfcwd\nn/private/tmp\n"
                "ftxt\nn/private/tmp/k9s\n"
            ),
            stderr="",
        )
        denied = PermissionError(errno.EPERM, "Operation not permitted", "/bin/ps")
        with mock.patch.object(
            formal_runner_guard.shutil,
            "which",
            side_effect=lambda name: f"/usr/bin/{name}",
        ), mock.patch.object(
            formal_runner_guard.subprocess,
            "run",
            side_effect=(files_result, denied),
        ), mock.patch.object(
            formal_runner_guard,
            "read_go_build_info",
            return_value="/private/tmp/k9s: go1.26.3\n\tpath\tcommand-line-arguments\n",
        ), self.assertRaises(SystemExit):
            formal_runner_guard.inspect_host_process(
                connection,
                formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027),
            )

    def test_ps_eperm_rejects_pid_reuse_with_changed_lsof_command(self) -> None:
        connection = formal_runner_guard.HostAPIConnection(59304, "k9s")
        files_result = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=(
                "p59304\ncmanager\nfcwd\nn/private/tmp\n"
                "ftxt\nn/private/tmp/manager\n"
            ),
            stderr="",
        )
        with mock.patch.object(
            formal_runner_guard.shutil,
            "which",
            side_effect=lambda name: f"/usr/bin/{name}",
        ), mock.patch.object(
            formal_runner_guard.subprocess,
            "run",
            return_value=files_result,
        ), self.assertRaises(SystemExit):
            formal_runner_guard.inspect_host_process(
                connection,
                formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027),
            )

    def test_ps_eperm_rejects_unknown_connected_client(self) -> None:
        connection = formal_runner_guard.HostAPIConnection(64255, "manager")
        files_result = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=(
                "p64255\ncmanager\nfcwd\nn/private/tmp\n"
                "ftxt\nn/private/tmp/manager\n"
            ),
            stderr="",
        )
        denied = PermissionError(errno.EPERM, "Operation not permitted", "/bin/ps")
        with mock.patch.object(
            formal_runner_guard.shutil,
            "which",
            side_effect=lambda name: f"/usr/bin/{name}",
        ), mock.patch.object(
            formal_runner_guard.subprocess,
            "run",
            side_effect=(files_result, denied),
        ), self.assertRaises(SystemExit):
            formal_runner_guard.inspect_host_process(
                connection,
                formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027),
            )

    def test_check_rejects_renamed_operator_on_exact_endpoint(self) -> None:
        connection = formal_runner_guard.HostAPIConnection(64255, "bench-daemon")
        identity = formal_runner_guard.HostProcessIdentity(
            pid=64255,
            lsof_command="bench-daemon",
            command="/private/tmp/bench-daemon --use-kubernetes-proxy",
            cwd="/private/tmp",
            text_paths=("/private/tmp/bench-daemon",),
        )
        stderr = io.StringIO()
        with mock.patch.object(
            formal_runner_guard,
            "run_lsof_api_inventory",
            return_value={connection.pid: connection},
        ), mock.patch.object(
            formal_runner_guard,
            "inspect_host_process",
            return_value=identity,
        ), mock.patch.object(
            formal_runner_guard.sys,
            "stdin",
            io.StringIO(json.dumps(kubeconfig())),
        ), contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            formal_runner_guard.check_host_operators()

        message = stderr.getvalue()
        self.assertIn("active host KubeRay operator", message)
        self.assertIn("127.0.0.1:64027", message)
        self.assertIn("pid=64255", message)

    def test_benign_client_does_not_mask_unknown_connected_client(self) -> None:
        benign = formal_runner_guard.HostAPIConnection(59304, "k9s")
        unknown = formal_runner_guard.HostAPIConnection(64255, "manager")
        benign_identity = formal_runner_guard.HostProcessIdentity(
            pid=59304,
            lsof_command="k9s",
            command="/opt/homebrew/bin/k9s",
            cwd="/Users/example",
            text_paths=("/opt/homebrew/bin/k9s",),
            executable="/opt/homebrew/bin/k9s",
            go_build_info="\tpath\tgithub.com/derailed/k9s\n",
        )

        def inspect(
            connection: formal_runner_guard.HostAPIConnection,
            endpoint: formal_runner_guard.APIServerEndpoint,
        ) -> formal_runner_guard.HostProcessIdentity:
            del endpoint
            if connection == benign:
                return benign_identity
            formal_runner_guard.fail(
                f"cannot inspect non-benign host API client PID {connection.pid} without ps"
            )

        with mock.patch.object(
            formal_runner_guard,
            "run_lsof_api_inventory",
            return_value={benign.pid: benign, unknown.pid: unknown},
        ), mock.patch.object(
            formal_runner_guard,
            "inspect_host_process",
            side_effect=inspect,
        ), mock.patch.object(
            formal_runner_guard.sys,
            "stdin",
            io.StringIO(json.dumps(kubeconfig())),
        ), self.assertRaises(SystemExit):
            formal_runner_guard.check_host_operators()

    def test_missing_lsof_fails_closed(self) -> None:
        with mock.patch.object(
            formal_runner_guard.shutil,
            "which",
            return_value=None,
        ), self.assertRaises(SystemExit):
            formal_runner_guard.run_lsof_api_inventory(
                formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027)
            )

    def test_empty_nonzero_lsof_result_fails_closed(self) -> None:
        result = subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr=""
        )
        with mock.patch.object(
            formal_runner_guard.shutil,
            "which",
            return_value="/usr/sbin/lsof",
        ), mock.patch.object(
            formal_runner_guard.subprocess,
            "run",
            return_value=result,
        ), self.assertRaises(SystemExit):
            formal_runner_guard.run_lsof_api_inventory(
                formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027)
            )

    def test_malformed_lsof_inventory_fails_closed(self) -> None:
        malformed = "\n".join(
            (
                "p911",
                "ccom.docker.backend",
                "f175",
                "n127.0.0.1:64027",
                "p64255",
                "cmanager",
                "f4",
                "nnot-an-endpoint->127.0.0.1:not-a-port",
            )
        )
        with self.assertRaises(SystemExit):
            formal_runner_guard.parse_lsof_api_connections(
                malformed,
                formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027),
            )

    def test_lsof_inventory_without_exact_listener_fails_closed(self) -> None:
        inventory = "\n".join(
            (
                "p77253",
                "cmanager",
                "f4",
                "n127.0.0.1:64512->127.0.0.1:61990",
            )
        )
        with self.assertRaises(SystemExit):
            formal_runner_guard.parse_lsof_api_connections(
                inventory,
                formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027),
            )

    def test_truncated_lsof_process_record_fails_closed(self) -> None:
        inventory = "\n".join(
            (
                "p911",
                "ccom.docker.backend",
                "f175",
                "n127.0.0.1:64027",
                "p64255",
                "cmanager",
            )
        )
        with self.assertRaises(SystemExit):
            formal_runner_guard.parse_lsof_api_connections(
                inventory,
                formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027),
            )

    def test_ambiguous_ps_identity_fails_closed(self) -> None:
        connection = formal_runner_guard.HostAPIConnection(64255, "manager")
        ps_result = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="first\nsecond\n", stderr=""
        )
        with mock.patch.object(
            formal_runner_guard.shutil,
            "which",
            side_effect=lambda name: f"/usr/bin/{name}",
        ), mock.patch.object(
            formal_runner_guard.subprocess,
            "run",
            return_value=ps_result,
        ), self.assertRaises(SystemExit):
            formal_runner_guard.inspect_host_process(
                connection,
                formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027),
            )

    def test_missing_go_for_build_identity_fails_closed(self) -> None:
        with mock.patch.object(
            formal_runner_guard.shutil,
            "which",
            return_value=None,
        ), self.assertRaises(SystemExit):
            formal_runner_guard.read_go_build_info(
                "/private/tmp/bench-daemon",
                64255,
                formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027),
            )

    def test_build_identity_failure_fails_if_process_remains_connected(self) -> None:
        result = subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="stat executable: denied"
        )
        with mock.patch.object(
            formal_runner_guard.shutil,
            "which",
            return_value="/opt/homebrew/bin/go",
        ), mock.patch.object(
            formal_runner_guard.subprocess,
            "run",
            return_value=result,
        ), mock.patch.object(
            formal_runner_guard,
            "process_still_connected",
            return_value=True,
        ), self.assertRaises(SystemExit):
            formal_runner_guard.read_go_build_info(
                "/private/tmp/bench-daemon",
                64255,
                formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027),
            )

    def test_build_identity_failure_allows_process_disappearance(self) -> None:
        result = subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="stat executable: gone"
        )
        with mock.patch.object(
            formal_runner_guard.shutil,
            "which",
            return_value="/opt/homebrew/bin/go",
        ), mock.patch.object(
            formal_runner_guard.subprocess,
            "run",
            return_value=result,
        ), mock.patch.object(
            formal_runner_guard,
            "process_still_connected",
            return_value=False,
        ):
            self.assertIsNone(
                formal_runner_guard.read_go_build_info(
                    "/private/tmp/bench-daemon",
                    64255,
                    formal_runner_guard.APIServerEndpoint("127.0.0.1", 64027),
                )
            )

    def test_ambiguous_executable_text_paths_fail_closed(self) -> None:
        with self.assertRaises(SystemExit):
            formal_runner_guard.select_process_executable(
                "/private/tmp/bench-daemon",
                "bench-daemon",
                ("/private/tmp/bench-daemon", "/other/bench-daemon"),
                64255,
            )

    def test_go_build_info_program_path_requires_exactly_one_path(self) -> None:
        self.assertIsNone(
            formal_runner_guard.go_build_info_program_path(
                "/private/tmp/k9s: go1.26.3\n\tmod\tgithub.com/derailed/k9s\n"
            )
        )
        with self.assertRaises(SystemExit):
            formal_runner_guard.go_build_info_program_path(
                "\tpath\tgithub.com/derailed/k9s\n"
                "\tpath\tk8s.io/kubernetes/cmd/kubectl\n"
            )


class LocalPortReleaseGuardTest(unittest.TestCase):
    def test_delayed_release_requires_two_consecutive_free_checks(self) -> None:
        states = {
            19003: iter(
                [
                    (False, "localhost:19003 is not free"),
                    (True, "localhost:19003 is free"),
                    (True, "localhost:19003 is free"),
                ]
            ),
            30080: iter(
                [
                    (True, "localhost:30080 is free"),
                    (True, "localhost:30080 is free"),
                    (True, "localhost:30080 is free"),
                ]
            ),
        }
        output = io.StringIO()
        with mock.patch.object(
            formal_runner_guard,
            "local_port_status",
            side_effect=lambda port: next(states[port]),
        ) as status, mock.patch.object(
            formal_runner_guard.time,
            "monotonic",
            side_effect=[0.0, 0.1, 0.2],
        ), mock.patch.object(
            formal_runner_guard.time,
            "sleep",
        ) as sleep, contextlib.redirect_stdout(output):
            formal_runner_guard.wait_local_ports_free([19003, 30080], 5, 0.5)

        self.assertEqual(status.call_count, 6)
        self.assertEqual(sleep.call_count, 2)
        self.assertEqual(
            output.getvalue(),
            "LOCAL-PORTS-FREE ports=19003,30080 consecutive=2\n",
        )

    def test_never_free_times_out_with_owner_diagnostics(self) -> None:
        stderr = io.StringIO()
        with mock.patch.object(
            formal_runner_guard,
            "local_port_status",
            side_effect=lambda port: (False, f"localhost:{port} is not free"),
        ), mock.patch.object(
            formal_runner_guard,
            "local_port_owner_diagnostics",
            return_value="localhost:19003 listener: kubectl pid=123",
        ), mock.patch.object(
            formal_runner_guard.time,
            "monotonic",
            side_effect=[0.0, 1.0],
        ), contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            formal_runner_guard.wait_local_ports_free([19003, 30080], 0.5, 0.1)

        message = stderr.getvalue()
        self.assertIn("timed out waiting for formal local ports", message)
        self.assertIn("localhost:19003 listener: kubectl pid=123", message)

    def test_wrong_ports_fail_closed(self) -> None:
        with self.assertRaises(SystemExit):
            formal_runner_guard.wait_local_ports_free([19003, 30081], 5, 0.5)

    def test_nonfinite_wait_values_fail_closed(self) -> None:
        for timeout, poll in (
            (float("nan"), 0.5),
            (float("inf"), 0.5),
            (5, float("nan")),
            (5, float("inf")),
        ):
            with self.subTest(timeout=timeout, poll=poll), self.assertRaises(SystemExit):
                formal_runner_guard.wait_local_ports_free(
                    [19003, 30080], timeout, poll
                )

    def test_poll_sleep_is_capped_by_remaining_deadline(self) -> None:
        with mock.patch.object(
            formal_runner_guard,
            "local_port_status",
            side_effect=lambda port: (False, f"localhost:{port} is not free"),
        ), mock.patch.object(
            formal_runner_guard,
            "local_port_owner_diagnostics",
            return_value="no owner",
        ), mock.patch.object(
            formal_runner_guard.time,
            "monotonic",
            side_effect=[0.0, 1.0, 5.0],
        ), mock.patch.object(
            formal_runner_guard.time,
            "sleep",
        ) as sleep, self.assertRaises(SystemExit):
            formal_runner_guard.wait_local_ports_free([19003, 30080], 5, 100)

        sleep.assert_called_once_with(4.0)


class NamespaceDeletionGuardTest(unittest.TestCase):
    def identity(self, root: pathlib.Path) -> pathlib.Path:
        path = root / "execution-namespace.json"
        path.write_text(json.dumps({"name": "test-ns-arm", "uid": UID}))
        return path

    def test_success_requires_second_not_found_confirmation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            values = [None, {"items": []}, None]
            output = io.StringIO()
            with mock.patch.object(
                formal_runner_guard,
                "kubectl_json",
                side_effect=values,
            ) as call, contextlib.redirect_stdout(output):
                formal_runner_guard.wait_namespace_deleted(
                    self.identity(pathlib.Path(tmp)), 0, 0
                )
            self.assertEqual(call.call_count, 3)
            self.assertEqual(
                output.getvalue(),
                f"NAMESPACE-DELETED name=test-ns-arm uid={UID} pods=0\n",
            )

    def test_namespaced_list_not_found_is_retried(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            namespace = {"metadata": {"name": "test-ns-arm", "uid": UID}}
            values = [namespace, None, None, {"items": []}, None]
            with mock.patch.object(
                formal_runner_guard,
                "kubectl_json",
                side_effect=values,
            ), mock.patch.object(formal_runner_guard.time, "sleep"):
                formal_runner_guard.wait_namespace_deleted(
                    self.identity(pathlib.Path(tmp)), 10, 0
                )

    def test_same_name_recreation_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            recreated = {
                "metadata": {
                    "name": "test-ns-arm",
                    "uid": "22222222-2222-2222-2222-222222222222",
                }
            }
            with mock.patch.object(
                formal_runner_guard,
                "kubectl_json",
                side_effect=[None, {"items": []}, recreated],
            ), self.assertRaises(SystemExit):
                formal_runner_guard.wait_namespace_deleted(
                    self.identity(pathlib.Path(tmp)), 0, 0
                )

    def test_existing_uid_times_out_without_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            namespace = {"metadata": {"name": "test-ns-arm", "uid": UID}}
            with mock.patch.object(
                formal_runner_guard,
                "kubectl_json",
                side_effect=[namespace, {"items": []}],
            ), self.assertRaises(SystemExit):
                formal_runner_guard.wait_namespace_deleted(
                    self.identity(pathlib.Path(tmp)), 0, 0
                )

    def test_cleanup_artifacts_bind_report_name_and_uid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            identity = self.identity(root)
            report = root / "bench-report.json"
            report.write_text(json.dumps({"namespace": "test-ns-arm", "namespaceUID": UID}))
            sentinel = root / "namespace-cleanup-sentinel.txt"
            sentinel.write_text(f"NAMESPACE-DELETED name=test-ns-arm uid={UID} pods=0\n")
            formal_runner_guard.validate_cleanup_artifacts(
                identity, report, sentinel, "namespace", "namespaceUID"
            )
            report.write_text(
                json.dumps({"namespace": "test-ns-arm", "namespaceUID": "wrong-uid"})
            )
            with self.assertRaises(SystemExit):
                formal_runner_guard.validate_cleanup_artifacts(
                    identity, report, sentinel, "namespace", "namespaceUID"
                )


if __name__ == "__main__":
    unittest.main()
