#!/usr/bin/env python3
"""Fail-closed process and namespace guards for formal benchmark runners."""

from __future__ import annotations

import argparse
import dataclasses
import errno
import ipaddress
import json
import math
import os
import pathlib
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from typing import Any
from urllib.parse import urlsplit


DNS1123_LABEL_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
OPERATOR_TOKEN_RE = re.compile(r"(?:^|[/_.-])(?:kuberay|ray)[-_./]?operator(?:$|[:/_.-])")
OPERATOR_IDENTITY_LABELS = {
    "app",
    "app.kubernetes.io/name",
    "app.kubernetes.io/component",
    "component",
    "control-plane",
}
FORMAL_LOCAL_PORTS = (19003, 30080)
FORMAL_PORT_FREE_CONFIRMATIONS = 2
FORMAL_KUBE_CONTEXT = "kind-bench"
BENIGN_KUBERNETES_CLIENTS = {"k9s", "kubectl"}
BENIGN_KUBERNETES_CLIENT_GO_PATHS = {
    "k9s": "github.com/derailed/k9s",
    "kubectl": "k8s.io/kubernetes/cmd/kubectl",
}
MANAGER_TOKEN_RE = re.compile(r"(?:^|[/_.-])manager(?:$|[/_.-])")
KUBERAY_OPERATOR_FLAGS = (
    "--enable-leader-election",
    "--health-probe-bind-address",
    "--metrics-addr",
    "--use-kubernetes-proxy",
)
KUBERAY_OPERATOR_MODULE = "github.com/ray-project/kuberay/ray-operator"
RAY_ORIGIN_NAME_LABEL = "ray.io/originated-from-cr-name"
RAY_ORIGIN_CRD_LABEL = "ray.io/originated-from-crd"
RAY_CLUSTER_LABEL = "ray.io/cluster"
RAY_NODE_LABEL = "ray.io/is-ray-node"
LIVE_POD_PHASES = {"Pending", "Running", "Unknown"}
TERMINAL_POD_PHASES = {"Succeeded", "Failed"}
WORKLOAD_API_VERSIONS = {
    "RayCluster": "ray.io/v1",
    "RayService": "ray.io/v1",
    "RayJob": "ray.io/v1",
    "Job": "batch/v1",
    "Pod": "v1",
}
WORKLOAD_OWNER_API_VERSIONS = {
    "RayCluster": "ray.io/v1",
    "RayService": "ray.io/v1",
    "RayJob": "ray.io/v1",
    "Job": "batch/v1",
}


@dataclasses.dataclass(frozen=True)
class APIServerEndpoint:
    host: str
    port: int

    @property
    def display(self) -> str:
        if ":" in self.host:
            return f"[{self.host}]:{self.port}"
        return f"{self.host}:{self.port}"


@dataclasses.dataclass(frozen=True)
class HostAPIConnection:
    pid: int
    lsof_command: str


@dataclasses.dataclass(frozen=True)
class HostProcessIdentity:
    pid: int
    lsof_command: str
    command: str
    cwd: str
    text_paths: tuple[str, ...]
    executable: str = ""
    go_build_info: str = ""


def fail(message: str) -> None:
    print(f"PREFLIGHT-FAILED: {message}", file=sys.stderr)
    raise SystemExit(1)


def require_object(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{context} is not an object")
    return value


def optional_object(value: Any, context: str) -> dict[str, Any]:
    if value is None:
        return {}
    return require_object(value, context)


def unique_named_entry(items: Any, name: str, context: str) -> dict[str, Any]:
    if not isinstance(items, list):
        fail(f"kubeconfig {context} is not a list")
    matches: list[dict[str, Any]] = []
    for value in items:
        item = require_object(value, f"kubeconfig {context} entry")
        if item.get("name") == name:
            matches.append(item)
    if len(matches) != 1:
        fail(
            f"kubeconfig must contain exactly one {context} entry named {name!r}; "
            f"found {len(matches)}"
        )
    return matches[0]


def current_api_server_endpoint(
    document: dict[str, Any],
    expected_context: str = FORMAL_KUBE_CONTEXT,
) -> APIServerEndpoint:
    current_context = document.get("current-context")
    if current_context != expected_context:
        fail(
            f"kubeconfig current-context={current_context!r}, "
            f"want {expected_context!r}"
        )
    context_entry = unique_named_entry(
        document.get("contexts"), expected_context, "contexts"
    )
    context = require_object(context_entry.get("context"), "kubeconfig context")
    cluster_name = context.get("cluster")
    if not isinstance(cluster_name, str) or not cluster_name:
        fail("kubeconfig current context has no cluster name")
    cluster_entry = unique_named_entry(
        document.get("clusters"), cluster_name, "clusters"
    )
    cluster = require_object(cluster_entry.get("cluster"), "kubeconfig cluster")
    server = cluster.get("server")
    if not isinstance(server, str) or not server:
        fail("kubeconfig current cluster has no API server")
    try:
        parsed = urlsplit(server)
        port = parsed.port
    except ValueError as error:
        fail(f"kubeconfig API server is invalid: {error}")
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.hostname is None
        or port is None
        or not 1 <= port <= 65535
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        fail(f"kubeconfig API server is not an exact HTTPS host:port: {server!r}")
    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError:
        fail(
            f"formal Kind API server must use a loopback IP literal, got "
            f"{parsed.hostname!r}"
        )
    if not address.is_loopback:
        fail(f"formal Kind API server is not loopback: {address}")
    return APIServerEndpoint(str(address), port)


def parse_numeric_endpoint(value: str, context: str) -> APIServerEndpoint:
    if value.startswith("["):
        closing = value.find("]")
        if closing < 0 or closing + 1 >= len(value) or value[closing + 1] != ":":
            fail(f"cannot parse {context} endpoint {value!r}")
        host = value[1:closing]
        port_text = value[closing + 2 :]
    else:
        try:
            host, port_text = value.rsplit(":", 1)
        except ValueError:
            fail(f"cannot parse {context} endpoint {value!r}")
    try:
        address = ipaddress.ip_address(host)
        port = int(port_text)
    except ValueError:
        fail(f"cannot parse {context} endpoint {value!r}")
    if not 1 <= port <= 65535:
        fail(f"cannot parse {context} endpoint {value!r}")
    return APIServerEndpoint(str(address), port)


def parse_lsof_api_connections(
    output: str,
    endpoint: APIServerEndpoint,
) -> dict[int, HostAPIConnection]:
    records: dict[int, dict[str, Any]] = {}
    current_pid: int | None = None
    for raw_line in output.splitlines():
        if not raw_line:
            continue
        field, value = raw_line[0], raw_line[1:]
        if field == "p":
            if not value.isdigit() or int(value) <= 0:
                fail(f"lsof API inventory has invalid PID field {raw_line!r}")
            current_pid = int(value)
            if current_pid in records:
                fail(f"lsof API inventory repeats PID {current_pid}")
            records[current_pid] = {"command": None, "names": []}
        elif field == "c":
            if current_pid is None or not value:
                fail(f"lsof API inventory has orphan or empty command field {raw_line!r}")
            command = records[current_pid]["command"]
            if command is not None and command != value:
                fail(f"lsof API inventory has conflicting commands for PID {current_pid}")
            records[current_pid]["command"] = value
        elif field == "f":
            if current_pid is None or not value:
                fail(f"lsof API inventory has orphan or empty file field {raw_line!r}")
        elif field == "n":
            if current_pid is None or not value:
                fail(f"lsof API inventory has orphan or empty name field {raw_line!r}")
            records[current_pid]["names"].append(value)
        else:
            fail(f"lsof API inventory has unexpected field {raw_line!r}")

    clients: dict[int, HostAPIConnection] = {}
    endpoint_listener_found = False
    for pid, record in records.items():
        command = record["command"]
        names = record["names"]
        if not isinstance(command, str) or not command or not names:
            fail(f"lsof API inventory has incomplete identity for PID {pid}")
        targets: list[APIServerEndpoint] = []
        for name in names:
            if "->" not in name:
                if parse_numeric_endpoint(name, "lsof listener") == endpoint:
                    endpoint_listener_found = True
                continue
            if name.count("->") != 1:
                fail(f"lsof API inventory has malformed TCP connection {name!r}")
            local, remote = name.split("->", 1)
            parse_numeric_endpoint(local, "lsof local")
            targets.append(parse_numeric_endpoint(remote, "lsof remote"))
        if endpoint not in targets:
            continue
        clients[pid] = HostAPIConnection(pid=pid, lsof_command=command)
    if not endpoint_listener_found:
        fail(
            f"lsof API inventory did not prove a listener for exact endpoint "
            f"{endpoint.display}"
        )
    return clients


def run_lsof_api_inventory(endpoint: APIServerEndpoint) -> dict[int, HostAPIConnection]:
    lsof = shutil.which("lsof")
    if lsof is None:
        fail("cannot inspect host API clients: lsof was not found")
    try:
        result = subprocess.run(
            [
                lsof,
                "-nP",
                f"-iTCP@{endpoint.display}",
                "-Fpcn",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        fail(f"cannot inspect host API clients with lsof: {error}")
    if result.returncode != 0 or result.stderr:
        fail(
            "cannot inventory host API clients with lsof: "
            + (result.stderr.strip() or f"exit status {result.returncode}")
        )
    return parse_lsof_api_connections(result.stdout, endpoint)


def parse_lsof_process_files(
    output: str,
    expected_pid: int,
) -> tuple[str, str, tuple[str, ...]]:
    observed_pid: int | None = None
    observed_command: str | None = None
    current_descriptor: str | None = None
    cwd_paths: list[str] = []
    text_paths: list[str] = []
    for raw_line in output.splitlines():
        if not raw_line:
            continue
        field, value = raw_line[0], raw_line[1:]
        if field == "p":
            if not value.isdigit() or int(value) != expected_pid or observed_pid is not None:
                fail(f"lsof process identity has unexpected PID field {raw_line!r}")
            observed_pid = int(value)
            current_descriptor = None
        elif field == "c":
            if observed_pid is None or not value:
                fail(f"lsof process identity has orphan or empty command field {raw_line!r}")
            if observed_command is not None and observed_command != value:
                fail(
                    f"lsof process identity has conflicting commands for PID "
                    f"{expected_pid}"
                )
            observed_command = value
        elif field == "f":
            if observed_pid is None or value not in ("cwd", "txt"):
                fail(f"lsof process identity has unexpected file field {raw_line!r}")
            current_descriptor = value
        elif field == "n":
            if observed_pid is None or current_descriptor is None or not value:
                fail(f"lsof process identity has orphan or empty path field {raw_line!r}")
            if current_descriptor == "cwd":
                cwd_paths.append(value)
            else:
                text_paths.append(value)
        else:
            fail(f"lsof process identity has unexpected field {raw_line!r}")
    if (
        observed_pid is None
        or observed_command is None
        or len(cwd_paths) != 1
        or not text_paths
    ):
        fail(
            f"lsof process identity for PID {expected_pid} must have exactly one cwd "
            f"and at least one text path"
        )
    return observed_command, cwd_paths[0], tuple(text_paths)


def process_still_connected(pid: int, endpoint: APIServerEndpoint) -> bool:
    return pid in run_lsof_api_inventory(endpoint)


def command_arguments(command: str, pid: int) -> list[str]:
    try:
        arguments = shlex.split(command)
    except ValueError as error:
        fail(f"cannot parse host API client PID {pid} command: {error}")
    if not arguments:
        fail(f"host API client PID {pid} command is empty")
    return arguments


def select_process_executable(
    command: str,
    lsof_command: str,
    text_paths: tuple[str, ...],
    pid: int,
) -> str:
    arguments = command_arguments(command, pid)
    preferred_basenames = (
        pathlib.PurePath(arguments[0]).name.lower(),
        lsof_command.lower(),
    )
    for basename in preferred_basenames:
        matches = sorted(
            {
                path
                for path in text_paths
                if pathlib.PurePath(path).name.lower() == basename
            }
        )
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            fail(
                f"host API client PID {pid} has ambiguous executable paths for "
                f"{basename!r}: {matches}"
            )
    fail(
        f"host API client PID {pid} executable does not match ps/lsof command: "
        f"command={arguments[0]!r} comm={lsof_command!r}"
    )


def read_go_build_info(
    executable: str,
    pid: int,
    endpoint: APIServerEndpoint,
) -> str | None:
    go = shutil.which("go")
    if go is None:
        fail("cannot inspect host API client build identity: go was not found")
    try:
        with tempfile.TemporaryDirectory(
            prefix="formal-go-buildinfo-", dir="/private/tmp"
        ) as temporary_home:
            environment = os.environ.copy()
            environment["HOME"] = temporary_home
            result = subprocess.run(
                [go, "version", "-m", executable],
                capture_output=True,
                text=True,
                check=False,
                timeout=5,
                env=environment,
            )
    except (OSError, subprocess.TimeoutExpired) as error:
        fail(f"cannot inspect host API client PID {pid} Go build identity: {error}")
    combined = "\n".join(value for value in (result.stdout, result.stderr) if value)
    if result.returncode == 0 and not result.stderr and result.stdout.strip():
        return result.stdout
    if result.returncode == 1 and "not a Go executable" in combined:
        return ""
    if not process_still_connected(pid, endpoint):
        return None
    fail(
        f"host API client PID {pid} remains connected but Go build identity failed: "
        f"{combined.strip() or f'exit status {result.returncode}'}"
    )


def go_build_info_program_path(build_info: str) -> str | None:
    paths = {
        fields[1]
        for line in build_info.splitlines()
        if len(fields := line.strip().split("\t")) >= 2 and fields[0] == "path"
    }
    if not paths:
        return None
    if len(paths) != 1:
        fail(f"host API client has ambiguous Go program paths: {sorted(paths)}")
    return next(iter(paths))


def lsof_only_benign_identity(
    connection: HostAPIConnection,
    cwd: str,
    text_paths: tuple[str, ...],
    endpoint: APIServerEndpoint,
) -> HostProcessIdentity | None:
    command_name = connection.lsof_command.lower()
    expected_go_path = BENIGN_KUBERNETES_CLIENT_GO_PATHS.get(command_name)
    if expected_go_path is None:
        fail(
            f"cannot inspect non-benign host API client PID {connection.pid} "
            "without ps"
        )
    executable = select_process_executable(
        connection.lsof_command,
        connection.lsof_command,
        text_paths,
        connection.pid,
    )
    go_build_info = read_go_build_info(executable, connection.pid, endpoint)
    if go_build_info is None:
        return None
    observed_go_path = go_build_info_program_path(go_build_info)
    if observed_go_path != expected_go_path:
        fail(
            f"host API client PID {connection.pid} cannot use lsof-only benign "
            f"classification: comm={connection.lsof_command!r} "
            f"goPath={observed_go_path!r} expected={expected_go_path!r}"
        )
    return HostProcessIdentity(
        pid=connection.pid,
        lsof_command=connection.lsof_command,
        command=executable,
        cwd=cwd,
        text_paths=text_paths,
        executable=executable,
        go_build_info=go_build_info,
    )


def inspect_host_process(
    connection: HostAPIConnection,
    endpoint: APIServerEndpoint,
) -> HostProcessIdentity | None:
    ps = shutil.which("ps")
    lsof = shutil.which("lsof")
    if ps is None or lsof is None:
        fail("cannot inspect host API client identity: ps or lsof was not found")
    try:
        files_result = subprocess.run(
            [
                lsof,
                "-nP",
                "-a",
                "-p",
                str(connection.pid),
                "-d",
                "cwd,txt",
                "-Fpcfn",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        fail(f"cannot inspect host API client PID {connection.pid} files: {error}")
    if files_result.returncode != 0:
        if not process_still_connected(connection.pid, endpoint):
            return None
        fail(
            f"host API client PID {connection.pid} remains connected but lsof identity "
            f"failed: {files_result.stderr.strip() or f'exit status {files_result.returncode}'}"
        )
    if files_result.stderr:
        fail(
            f"lsof returned diagnostics for host API client PID {connection.pid}: "
            f"{files_result.stderr.strip()}"
        )
    fresh_lsof_command, cwd, text_paths = parse_lsof_process_files(
        files_result.stdout, connection.pid
    )
    if fresh_lsof_command != connection.lsof_command:
        fail(
            f"host API client PID {connection.pid} changed command between lsof "
            f"inventory and identity reads: inventory={connection.lsof_command!r} "
            f"fresh={fresh_lsof_command!r}"
        )
    fresh_connection = HostAPIConnection(
        pid=connection.pid,
        lsof_command=fresh_lsof_command,
    )

    try:
        ps_result = subprocess.run(
            [ps, "-ww", "-p", str(connection.pid), "-o", "command="],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except OSError as error:
        if error.errno == errno.EPERM:
            return lsof_only_benign_identity(
                fresh_connection, cwd, text_paths, endpoint
            )
        fail(f"cannot inspect host API client PID {connection.pid} with ps: {error}")
    except subprocess.TimeoutExpired as error:
        fail(f"cannot inspect host API client PID {connection.pid} with ps: {error}")
    if ps_result.returncode != 0:
        if not process_still_connected(connection.pid, endpoint):
            return None
        fail(
            f"host API client PID {connection.pid} remains connected but ps failed: "
            f"{ps_result.stderr.strip() or f'exit status {ps_result.returncode}'}"
        )
    if ps_result.stderr:
        fail(
            f"ps returned diagnostics for host API client PID {connection.pid}: "
            f"{ps_result.stderr.strip()}"
        )
    command_lines = [line.strip() for line in ps_result.stdout.splitlines() if line.strip()]
    if len(command_lines) != 1:
        fail(f"ps returned an ambiguous command for host API client PID {connection.pid}")

    executable = select_process_executable(
        command_lines[0], fresh_lsof_command, text_paths, connection.pid
    )
    go_build_info = read_go_build_info(executable, connection.pid, endpoint)
    if go_build_info is None:
        return None
    return HostProcessIdentity(
        pid=connection.pid,
        lsof_command=fresh_lsof_command,
        command=command_lines[0],
        cwd=cwd,
        text_paths=text_paths,
        executable=executable,
        go_build_info=go_build_info,
    )


def command_has_flag(command: str, flag: str) -> bool:
    return re.search(rf"(?:^|\s){re.escape(flag)}(?:=|\s|$)", command) is not None


def go_build_info_has_operator(build_info: str) -> bool:
    for line in build_info.splitlines():
        fields = line.strip().split("\t")
        if (
            len(fields) >= 2
            and fields[0] in ("path", "mod", "dep")
            and fields[1] == KUBERAY_OPERATOR_MODULE
        ):
            return True
    return False


def host_operator_evidence(identity: HostProcessIdentity) -> bool:
    arguments = command_arguments(identity.command, identity.pid)
    executable_evidence = (
        identity.lsof_command,
        arguments[0],
        *identity.text_paths,
    )
    if any(
        OPERATOR_TOKEN_RE.search(value.lower())
        for value in executable_evidence
    ):
        return True
    flags = {
        flag
        for flag in KUBERAY_OPERATOR_FLAGS
        if command_has_flag(identity.command, flag)
    }
    if "--use-kubernetes-proxy" in flags:
        return True
    if go_build_info_has_operator(identity.go_build_info):
        return True
    command_basename = pathlib.PurePath(arguments[0]).name.lower()
    text_basenames = {
        pathlib.PurePath(path).name.lower() for path in identity.text_paths
    }
    if (
        identity.lsof_command.lower() in BENIGN_KUBERNETES_CLIENTS
        and command_basename == identity.lsof_command.lower()
        and command_basename in text_basenames
    ):
        return False
    if OPERATOR_TOKEN_RE.search(identity.cwd.lower()):
        return True
    return (
        MANAGER_TOKEN_RE.search(identity.lsof_command.lower()) is not None
        and len(flags) >= 2
    )


def format_host_process_identity(identity: HostProcessIdentity) -> str:
    executable = identity.executable or identity.text_paths[0]
    return (
        f"pid={identity.pid} comm={identity.lsof_command!r} "
        f"command={identity.command!r} cwd={identity.cwd!r} "
        f"executable={executable!r}"
    )


def check_host_operators() -> None:
    try:
        document = json.load(sys.stdin)
    except (json.JSONDecodeError, OSError) as error:
        fail(f"cannot decode current kubeconfig: {error}")
    endpoint = current_api_server_endpoint(
        require_object(document, "current kubeconfig")
    )
    connections = run_lsof_api_inventory(endpoint)
    operators: list[HostProcessIdentity] = []
    inspected = 0
    for connection in connections.values():
        identity = inspect_host_process(connection, endpoint)
        if identity is None:
            continue
        inspected += 1
        if host_operator_evidence(identity):
            operators.append(identity)
    if operators:
        fail(
            f"active host KubeRay operator connected to {FORMAL_KUBE_CONTEXT} "
            f"API {endpoint.display}: "
            + "; ".join(
                format_host_process_identity(identity)
                for identity in sorted(operators, key=lambda value: value.pid)
            )
        )
    print(
        f"HOST-OPERATORS-CLEAR context={FORMAL_KUBE_CONTEXT} "
        f"api={endpoint.display} inspected={inspected}"
    )


def operator_evidence(item: dict[str, Any]) -> bool:
    metadata = require_object(item.get("metadata"), "Kubernetes object metadata")
    spec = require_object(item.get("spec"), "Kubernetes object spec")
    labels = metadata.get("labels") or {}
    if not isinstance(labels, dict):
        fail("Kubernetes object labels are not an object")

    identity_tokens = [
        str(value)
        for key, value in labels.items()
        if key in OPERATOR_IDENTITY_LABELS
    ]
    template = spec.get("template") if isinstance(spec.get("template"), dict) else {}
    pod_spec = template.get("spec") if isinstance(template.get("spec"), dict) else spec
    if not isinstance(pod_spec, dict):
        fail("Kubernetes pod spec is not an object")
    containers = pod_spec.get("containers") or []
    if not isinstance(containers, list):
        fail("Kubernetes containers field is not a list")
    process_tokens: list[str] = []
    for container in containers:
        if not isinstance(container, dict):
            fail("Kubernetes container is not an object")
        process_tokens.extend(
            [
                str(container.get("name") or ""),
                str(container.get("image") or ""),
            ]
        )
        for field in ("command", "args"):
            values = container.get(field) or []
            if not isinstance(values, list):
                fail(f"Kubernetes container {field} is not a list")
            process_tokens.extend(str(value) for value in values)
    return any(
        OPERATOR_TOKEN_RE.search(token.lower())
        for token in (*identity_tokens, *process_tokens)
    )


def active_operator_objects(document: dict[str, Any]) -> list[str]:
    items = document.get("items")
    if not isinstance(items, list):
        fail("cluster operator inventory has no items list")

    candidates: list[str] = []
    for item_value in items:
        item = require_object(item_value, "Kubernetes inventory item")
        kind = item.get("kind")
        if kind not in ("Deployment", "Pod", "ReplicaSet"):
            fail(f"unexpected Kubernetes inventory kind {kind!r}")
        metadata = require_object(item.get("metadata"), f"{kind} metadata")
        namespace = metadata.get("namespace")
        name = metadata.get("name")
        if not isinstance(namespace, str) or not namespace or not isinstance(name, str) or not name:
            fail(f"{kind} namespace/name is missing")
        if not operator_evidence(item):
            continue

        deletion_timestamp = metadata.get("deletionTimestamp")
        if deletion_timestamp is not None and (
            not isinstance(deletion_timestamp, str) or not deletion_timestamp
        ):
            fail(f"{kind} {namespace}/{name} has invalid deletionTimestamp")
        deleting = deletion_timestamp is not None

        active = False
        if kind in ("Deployment", "ReplicaSet"):
            spec = require_object(item.get("spec"), f"{kind} spec")
            replicas = spec.get("replicas", 1)
            if type(replicas) is not int or replicas < 0:
                fail(f"{kind} {namespace}/{name} has invalid replicas")
            active = replicas > 0
        else:
            status = item.get("status") or {}
            if not isinstance(status, dict):
                fail(f"Pod {namespace}/{name} status is not an object")
            phase = status.get("phase")
            if not isinstance(phase, str):
                fail(f"Pod {namespace}/{name} phase is missing")
            # A deleting Pod can still reconcile until its containers have exited.
            active = phase in ("Pending", "Running", "Unknown")
        if active:
            suffix = " deleting" if deleting else ""
            candidates.append(f"{kind.lower()} {namespace}/{name}{suffix}")
    return candidates


def check_operators() -> None:
    try:
        document = json.load(sys.stdin)
    except (json.JSONDecodeError, OSError) as error:
        fail(f"cannot decode cluster operator inventory: {error}")
    candidates = active_operator_objects(require_object(document, "cluster operator inventory"))
    if candidates:
        fail("active in-cluster KubeRay operator found: " + ", ".join(candidates))


def workload_metadata(
    item: dict[str, Any],
    kind: str,
) -> tuple[str, str, dict[str, str], tuple[tuple[str, str, str], ...], bool]:
    metadata = require_object(item.get("metadata"), f"{kind} metadata")
    namespace = metadata.get("namespace")
    name = metadata.get("name")
    if not isinstance(namespace, str) or not namespace:
        fail(f"{kind} namespace is missing")
    if not isinstance(name, str) or not name:
        fail(f"{kind} {namespace}/<unnamed> name is missing")

    labels_value = optional_object(
        metadata.get("labels"),
        f"{kind} {namespace}/{name} labels",
    )
    labels: dict[str, str] = {}
    for key, value in labels_value.items():
        if not isinstance(key, str) or not isinstance(value, str):
            fail(f"{kind} {namespace}/{name} labels must be strings")
        labels[key] = value

    owners_value = metadata.get("ownerReferences")
    if owners_value is None:
        owners_value = []
    elif not isinstance(owners_value, list):
        fail(f"{kind} {namespace}/{name} ownerReferences are not a list")
    owners: list[tuple[str, str, str]] = []
    for owner_value in owners_value:
        owner = require_object(
            owner_value,
            f"{kind} {namespace}/{name} ownerReference",
        )
        owner_api_version = owner.get("apiVersion")
        owner_kind = owner.get("kind")
        owner_name = owner.get("name")
        if not isinstance(owner_api_version, str) or not owner_api_version:
            fail(f"{kind} {namespace}/{name} ownerReference apiVersion is missing")
        if not isinstance(owner_kind, str) or not owner_kind:
            fail(f"{kind} {namespace}/{name} ownerReference kind is missing")
        if not isinstance(owner_name, str) or not owner_name:
            fail(f"{kind} {namespace}/{name} ownerReference name is missing")
        expected_owner_api_version = WORKLOAD_OWNER_API_VERSIONS.get(owner_kind)
        if owner_api_version.startswith("ray.io/") and expected_owner_api_version is None:
            fail(
                f"{kind} {namespace}/{name} has unsupported KubeRay "
                f"owner kind {owner_kind!r}"
            )
        if (
            expected_owner_api_version is not None
            and owner_api_version != expected_owner_api_version
        ):
            fail(
                f"{kind} {namespace}/{name} ownerReference {owner_kind} "
                f"has apiVersion={owner_api_version!r}, "
                f"want {expected_owner_api_version!r}"
            )
        owners.append((owner_api_version, owner_kind, owner_name))

    deletion_timestamp = metadata.get("deletionTimestamp")
    if deletion_timestamp is not None and (
        not isinstance(deletion_timestamp, str) or not deletion_timestamp
    ):
        fail(f"{kind} {namespace}/{name} deletionTimestamp is invalid")
    return namespace, name, labels, tuple(owners), deletion_timestamp is not None


def workload_origin(
    labels: dict[str, str],
    context: str,
) -> tuple[str, str] | None:
    has_name = RAY_ORIGIN_NAME_LABEL in labels
    has_kind = RAY_ORIGIN_CRD_LABEL in labels
    if has_name != has_kind:
        fail(f"{context} has an incomplete KubeRay origin label pair")
    if not has_name:
        return None
    origin_kind = labels[RAY_ORIGIN_CRD_LABEL]
    origin_name = labels[RAY_ORIGIN_NAME_LABEL]
    if origin_kind not in ("RayJob", "RayService") or not origin_name:
        fail(f"{context} has invalid KubeRay origin labels")
    return origin_kind, origin_name


def kubernetes_job_terminal(status: dict[str, Any], context: str) -> bool:
    active = status.get("active", 0)
    if type(active) is not int or active < 0:
        fail(f"{context} active count is invalid")
    conditions_value = status.get("conditions")
    if conditions_value is None:
        conditions_value = []
    elif not isinstance(conditions_value, list):
        fail(f"{context} conditions are not a list")
    terminal = False
    for condition_value in conditions_value:
        condition = require_object(condition_value, f"{context} condition")
        condition_type = condition.get("type")
        condition_status = condition.get("status")
        if not isinstance(condition_type, str) or not isinstance(condition_status, str):
            fail(f"{context} condition type/status must be strings")
        if condition_type in ("Complete", "Failed") and condition_status == "True":
            terminal = True
    return active == 0 and terminal


def reconcilable_workload_objects(
    document: dict[str, Any],
) -> tuple[list[str], int]:
    if document.get("apiVersion") != "v1" or document.get("kind") != "List":
        fail("cluster workload inventory must be an apiVersion v1 List")
    items = document.get("items")
    if not isinstance(items, list):
        fail("cluster workload inventory has no items list")

    records: list[
        tuple[
            str,
            str,
            str,
            dict[str, str],
            tuple[tuple[str, str, str], ...],
            bool,
            dict[str, Any],
        ]
    ] = []
    identities: set[tuple[str, str, str]] = set()
    for item_value in items:
        item = require_object(item_value, "cluster workload inventory item")
        kind = item.get("kind")
        if kind not in WORKLOAD_API_VERSIONS:
            fail(f"unexpected cluster workload inventory kind {kind!r}")
        if item.get("apiVersion") != WORKLOAD_API_VERSIONS[kind]:
            fail(f"{kind} inventory item has unexpected apiVersion")
        namespace, name, labels, owners, deleting = workload_metadata(item, kind)
        identity = (kind, namespace, name)
        if identity in identities:
            fail(f"cluster workload inventory repeats {kind} {namespace}/{name}")
        identities.add(identity)
        records.append((kind, namespace, name, labels, owners, deleting, item))

    blockers: list[str] = []
    terminal_rayjobs: set[tuple[str, str]] = set()
    for kind, namespace, name, _, _, deleting, item in records:
        deletion = " deleting" if deleting else ""
        if kind in ("RayCluster", "RayService"):
            blockers.append(f"{kind.lower()} {namespace}/{name}{deletion}")
            continue
        if kind != "RayJob":
            continue
        status = optional_object(item.get("status"), f"RayJob {namespace}/{name} status")
        job_status = status.get("jobStatus", "")
        deployment_status = status.get("jobDeploymentStatus", "")
        if not isinstance(job_status, str) or not isinstance(deployment_status, str):
            fail(f"RayJob {namespace}/{name} status fields must be strings")
        cluster_name = status.get("rayClusterName", "")
        if not isinstance(cluster_name, str):
            fail(f"RayJob {namespace}/{name} rayClusterName must be a string")
        if job_status == "SUCCEEDED" and deployment_status == "Complete":
            terminal_rayjobs.add((namespace, name))
        else:
            blockers.append(
                f"rayjob {namespace}/{name}{deletion} "
                f"jobStatus={job_status or '<empty>'} "
                f"jobDeploymentStatus={deployment_status or '<empty>'}"
            )

    rayjob_submitter_jobs: dict[tuple[str, str], str] = {}
    for kind, namespace, name, labels, owners, _, _ in records:
        if kind != "Job":
            continue
        context = f"{kind} {namespace}/{name}"
        origin = workload_origin(labels, context)
        rayjob_owners = {
            owner_name
            for owner_api_version, owner_kind, owner_name in owners
            if owner_api_version == "ray.io/v1" and owner_kind == "RayJob"
        }
        if len(rayjob_owners) > 1:
            fail(f"{context} has multiple RayJob owners")
        if origin is None and rayjob_owners:
            fail(f"{context} has a RayJob owner but no KubeRay origin labels")
        if origin is None:
            continue
        if origin[0] != "RayJob":
            fail(f"{context} has non-RayJob origin for a submitter Job")
        if rayjob_owners and rayjob_owners != {origin[1]}:
            fail(f"{context} RayJob owner conflicts with its KubeRay origin labels")
        rayjob_submitter_jobs[(namespace, name)] = origin[1]

    for kind, namespace, name, labels, owners, _, item in records:
        context = f"{kind} {namespace}/{name}"
        origin = workload_origin(labels, context)
        if kind == "Job":
            rayjob_parent = rayjob_submitter_jobs.get((namespace, name))
            if rayjob_parent is None:
                continue
            status = optional_object(item.get("status"), f"{context} status")
            if (namespace, rayjob_parent) not in terminal_rayjobs:
                blockers.append(
                    f"submitter job {namespace}/{name} parent={rayjob_parent} "
                    "is not an allowed terminal RayJob residue"
                )
            elif not kubernetes_job_terminal(status, context):
                blockers.append(
                    f"live submitter job {namespace}/{name} parent={rayjob_parent}"
                )
            continue
        if kind != "Pod":
            continue
        raycluster_owners = {
            owner_name
            for owner_api_version, owner_kind, owner_name in owners
            if owner_api_version == "ray.io/v1" and owner_kind == "RayCluster"
        }
        if len(raycluster_owners) > 1:
            fail(f"{context} has multiple RayCluster owners")
        cluster_label = labels.get(RAY_CLUSTER_LABEL)
        if cluster_label is not None and not cluster_label:
            fail(f"{context} has an empty {RAY_CLUSTER_LABEL} label")
        if (
            cluster_label is not None
            and raycluster_owners
            and raycluster_owners != {cluster_label}
        ):
            fail(f"{context} RayCluster owner conflicts with its cluster label")

        root_owners: set[tuple[str, str]] = set()
        for owner_api_version, owner_kind, owner_name in owners:
            if owner_api_version == "ray.io/v1" and owner_kind in (
                "RayJob",
                "RayService",
            ):
                root_owners.add((owner_kind, owner_name))
        if len(root_owners) > 1:
            fail(f"{context} has conflicting KubeRay root owners")
        if root_owners and (origin is None or root_owners != {origin}):
            fail(f"{context} KubeRay root owner conflicts with its origin labels")

        job_owners = {
            owner_name
            for owner_api_version, owner_kind, owner_name in owners
            if owner_api_version == "batch/v1" and owner_kind == "Job"
        }
        if len(job_owners) > 1:
            fail(f"{context} has multiple Kubernetes Job owners")
        if origin is not None and origin[0] == "RayJob" and job_owners:
            if job_owners != {origin[1]}:
                fail(f"{context} submitter Job owner conflicts with its RayJob origin")
        submitter_owner = any(
            (namespace, owner_name) in rayjob_submitter_jobs
            or (namespace, owner_name) in terminal_rayjobs
            for owner_name in job_owners
        )
        if origin is not None and origin[0] != "RayJob" and submitter_owner:
            fail(f"{context} submitter Job owner conflicts with its KubeRay origin")
        is_kuberay_pod = (
            origin is not None
            or cluster_label is not None
            or RAY_NODE_LABEL in labels
            or any(
                owner_api_version == "ray.io/v1"
                and owner_kind in ("RayCluster", "RayJob")
                for owner_api_version, owner_kind, _ in owners
            )
            or submitter_owner
        )
        if not is_kuberay_pod:
            continue
        status = optional_object(item.get("status"), f"{context} status")
        phase = status.get("phase")
        if not isinstance(phase, str) or phase not in LIVE_POD_PHASES | TERMINAL_POD_PHASES:
            fail(f"{context} has invalid phase {phase!r}")
        if phase in LIVE_POD_PHASES:
            blockers.append(f"live pod {namespace}/{name} phase={phase}")

    return sorted(blockers), len(terminal_rayjobs)


def check_cluster_workloads(context: str) -> None:
    if context != FORMAL_KUBE_CONTEXT:
        fail(
            f"cluster workload inventory context={context!r}, "
            f"want {FORMAL_KUBE_CONTEXT!r}"
        )
    try:
        document = json.load(sys.stdin)
    except (json.JSONDecodeError, OSError) as error:
        fail(f"cannot decode cluster workload inventory: {error}")
    blockers, terminal_rayjobs = reconcilable_workload_objects(
        require_object(document, "cluster workload inventory")
    )
    if blockers:
        fail(
            "existing KubeRay workloads can be reconciled or consume resources: "
            + "; ".join(blockers)
        )
    print(
        f"KUBERAY-WORKLOADS-CLEAR context={context} "
        f"terminal-rayjobs={terminal_rayjobs}"
    )


def local_port_status(port: int) -> tuple[bool, str]:
    try:
        addresses = socket.getaddrinfo("localhost", port, type=socket.SOCK_STREAM)
    except OSError as error:
        return False, f"localhost:{port} address lookup failed: {error}"
    unique_addresses: list[tuple[int, tuple[Any, ...]]] = []
    for family, _, _, _, address in addresses:
        candidate = (family, address)
        if candidate not in unique_addresses:
            unique_addresses.append(candidate)
    if not unique_addresses:
        return False, f"localhost:{port} has no TCP addresses"

    for family, address in unique_addresses:
        probe = socket.socket(family, socket.SOCK_STREAM)
        try:
            if family == socket.AF_INET6:
                probe.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            probe.bind(address)
        except OSError as error:
            return False, f"localhost:{port} is not free: {error}"
        finally:
            probe.close()
    return True, f"localhost:{port} is free"


def local_port_owner_diagnostics(ports: list[int]) -> str:
    lsof = shutil.which("lsof")
    if lsof is None:
        return "listener ownership unavailable: lsof was not found"
    diagnostics: list[str] = []
    for port in ports:
        try:
            result = subprocess.run(
                [lsof, "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"],
                capture_output=True,
                text=True,
                check=False,
                timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            diagnostics.append(f"localhost:{port} lsof failed: {error}")
            continue
        output = " | ".join(line.strip() for line in result.stdout.splitlines() if line.strip())
        if output:
            diagnostics.append(f"localhost:{port} listener: {output}")
        else:
            diagnostics.append(
                f"localhost:{port} listener owner not found; the port may still be closing"
            )
    return "; ".join(diagnostics)


def wait_local_ports_free(
    ports: list[int],
    timeout_seconds: float,
    poll_seconds: float,
) -> None:
    if tuple(ports) != FORMAL_LOCAL_PORTS:
        fail(
            f"formal local ports must be {FORMAL_LOCAL_PORTS[0]},{FORMAL_LOCAL_PORTS[1]}"
        )
    if (
        not math.isfinite(timeout_seconds)
        or not math.isfinite(poll_seconds)
        or timeout_seconds < 0
        or poll_seconds <= 0
    ):
        fail(
            "local port wait timeout must be finite and nonnegative, and poll interval "
            "must be finite and positive"
        )

    deadline = time.monotonic() + timeout_seconds
    consecutive_free = 0
    last_state = "not checked"
    while True:
        statuses = [(port, *local_port_status(port)) for port in ports]
        busy = [detail for _, free, detail in statuses if not free]
        if not busy:
            consecutive_free += 1
            last_state = (
                f"all ports free for {consecutive_free}/"
                f"{FORMAL_PORT_FREE_CONFIRMATIONS} consecutive checks"
            )
            if consecutive_free >= FORMAL_PORT_FREE_CONFIRMATIONS:
                print(
                    f"LOCAL-PORTS-FREE ports={','.join(str(port) for port in ports)} "
                    f"consecutive={consecutive_free}"
                )
                return
        else:
            consecutive_free = 0
            last_state = "; ".join(busy)

        now = time.monotonic()
        if now >= deadline:
            fail(
                f"timed out waiting for formal local ports to be released after "
                f"{timeout_seconds:g}s: {last_state}; "
                f"{local_port_owner_diagnostics(ports)}"
            )
        time.sleep(min(poll_seconds, deadline - now))


def load_identity(path: pathlib.Path) -> tuple[str, str]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read execution namespace identity {path}: {error}")
    if not isinstance(document, dict) or set(document) != {"name", "uid"}:
        fail("execution namespace identity must contain exactly name and uid")
    name, uid = document["name"], document["uid"]
    if not isinstance(name, str) or len(name) > 63 or not DNS1123_LABEL_RE.fullmatch(name):
        fail(f"unsafe execution namespace name {name!r}")
    if not isinstance(uid, str) or not uid or len(uid) > 128:
        fail("execution namespace UID is missing or invalid")
    return name, uid


def kubectl_json(
    arguments: list[str],
    context: str,
    *,
    allow_not_found: bool = False,
) -> dict[str, Any] | None:
    result = subprocess.run(
        ["kubectl", *arguments],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        combined = f"{result.stderr}\n{result.stdout}".lower()
        if allow_not_found and ("notfound" in combined or "not found" in combined):
            return None
        fail(f"kubectl {context} failed: {result.stderr.strip() or result.stdout.strip()}")
    if not result.stdout.strip():
        return None
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        fail(f"kubectl {context} returned invalid JSON: {error}")
    return require_object(value, f"kubectl {context} output")


def wait_namespace_deleted(identity_path: pathlib.Path, timeout_seconds: float, poll_seconds: float) -> None:
    if timeout_seconds < 0 or poll_seconds < 0:
        fail("namespace wait timeout and poll interval must be nonnegative")
    name, expected_uid = load_identity(identity_path)
    deadline = time.monotonic() + timeout_seconds
    last_state = "not checked"
    while True:
        namespace = kubectl_json(
            ["get", "namespace", name, "--ignore-not-found=true", "-o", "json"],
            f"get namespace {name}",
        )
        if namespace is not None:
            metadata = require_object(namespace.get("metadata"), f"namespace {name} metadata")
            observed_name = metadata.get("name")
            observed_uid = metadata.get("uid")
            if observed_name != name:
                fail(f"namespace lookup returned name={observed_name!r}, want {name!r}")
            if observed_uid != expected_uid:
                fail(
                    f"namespace {name} UID changed: observed={observed_uid!r} expected={expected_uid!r}"
                )
            pods = kubectl_json(
                ["get", "pods", "-n", name, "-o", "json"],
                f"list pods in namespace {name}",
                allow_not_found=True,
            )
            if pods is None:
                last_state = "namespace disappeared during namespaced pod inventory"
                if time.monotonic() >= deadline:
                    fail(f"timed out waiting for namespace {name} UID {expected_uid} deletion: {last_state}")
                time.sleep(poll_seconds)
                continue
            if not isinstance(pods.get("items"), list):
                fail(f"pod inventory for existing namespace {name} is incomplete")
            last_state = f"namespace UID still exists with {len(pods['items'])} pod(s)"
        else:
            pods = kubectl_json(
                ["get", "pods", "--all-namespaces", "-o", "json"],
                "list all pods after namespace deletion",
            )
            if pods is None or not isinstance(pods.get("items"), list):
                fail("cluster-wide pod inventory is incomplete")
            remaining = []
            for item_value in pods["items"]:
                item = require_object(item_value, "pod inventory item")
                metadata = require_object(item.get("metadata"), "pod inventory metadata")
                if metadata.get("namespace") == name:
                    remaining.append(str(metadata.get("name") or "<unnamed>"))
            if not remaining:
                confirm = kubectl_json(
                    ["get", "namespace", name, "--ignore-not-found=true", "-o", "json"],
                    f"confirm namespace {name} deletion",
                )
                if confirm is None:
                    print(f"NAMESPACE-DELETED name={name} uid={expected_uid} pods=0")
                    return
                metadata = require_object(
                    confirm.get("metadata"),
                    f"recreated namespace {name} metadata",
                )
                observed_uid = metadata.get("uid")
                if observed_uid != expected_uid:
                    fail(
                        f"namespace {name} was recreated during deletion wait: "
                        f"observed={observed_uid!r} expected={expected_uid!r}"
                    )
                last_state = "namespace reappeared with the original UID during final confirmation"
            last_state = "namespace absent but pods remain: " + ",".join(sorted(remaining))

        if time.monotonic() >= deadline:
            fail(f"timed out waiting for namespace {name} UID {expected_uid} deletion: {last_state}")
        time.sleep(poll_seconds)


def validate_cleanup_artifacts(
    identity_path: pathlib.Path,
    report_path: pathlib.Path,
    sentinel_path: pathlib.Path,
    namespace_field: str,
    uid_field: str,
) -> None:
    name, uid = load_identity(identity_path)
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read benchmark report {report_path}: {error}")
    report = require_object(report, "benchmark report")
    if report.get(namespace_field) != name or report.get(uid_field) != uid:
        fail(
            f"benchmark report namespace identity differs: "
            f"report={report.get(namespace_field)!r}/{report.get(uid_field)!r} "
            f"identity={name!r}/{uid!r}"
        )
    expected = f"NAMESPACE-DELETED name={name} uid={uid} pods=0\n"
    try:
        observed = sentinel_path.read_text(encoding="utf-8")
    except OSError as error:
        fail(f"cannot read namespace cleanup sentinel {sentinel_path}: {error}")
    if observed != expected:
        fail("namespace cleanup sentinel does not match the execution namespace identity")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("check-operators")
    subparsers.add_parser("check-host-operators")
    workloads = subparsers.add_parser("check-cluster-workloads")
    workloads.add_argument("--context", required=True)
    wait = subparsers.add_parser("wait-namespace-deleted")
    wait.add_argument("--identity", required=True, type=pathlib.Path)
    wait.add_argument("--timeout-seconds", required=True, type=float)
    wait.add_argument("--poll-seconds", default=2.0, type=float)
    ports = subparsers.add_parser("wait-local-ports-free")
    ports.add_argument("--ports", nargs=2, required=True, type=int)
    ports.add_argument("--timeout-seconds", required=True, type=float)
    ports.add_argument("--poll-seconds", default=0.5, type=float)
    validate = subparsers.add_parser("validate-cleanup-artifacts")
    validate.add_argument("--identity", required=True, type=pathlib.Path)
    validate.add_argument("--report", required=True, type=pathlib.Path)
    validate.add_argument("--sentinel", required=True, type=pathlib.Path)
    validate.add_argument("--namespace-field", required=True)
    validate.add_argument("--uid-field", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "check-operators":
        check_operators()
    elif args.command == "check-host-operators":
        check_host_operators()
    elif args.command == "check-cluster-workloads":
        check_cluster_workloads(args.context)
    elif args.command == "wait-namespace-deleted":
        wait_namespace_deleted(args.identity, args.timeout_seconds, args.poll_seconds)
    elif args.command == "wait-local-ports-free":
        wait_local_ports_free(args.ports, args.timeout_seconds, args.poll_seconds)
    else:
        validate_cleanup_artifacts(
            args.identity,
            args.report,
            args.sentinel,
            args.namespace_field,
            args.uid_field,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
