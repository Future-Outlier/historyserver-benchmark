#!/usr/bin/env bash

# Shared, fail-closed helpers for benchmark sweeps. The caller must set HS,
# OPERATOR_BIN, OUT, and BENCH_KIND_NODE before invoking these functions.

BUILD_SOURCE_LABEL='io.kuberay.historyserver.build-source-sha256'

require_local_tcp_port_free() {
  local port="$1"
  python3 - "$port" <<'PY'
import socket
import sys

try:
    port = int(sys.argv[1])
except (IndexError, ValueError):
    print("PREFLIGHT-FAILED: local TCP port must be an integer", file=sys.stderr)
    raise SystemExit(1)

if not 1 <= port <= 65535:
    print(f"PREFLIGHT-FAILED: invalid local TCP port {port}", file=sys.stderr)
    raise SystemExit(1)

addresses = []
for family, socktype, proto, _, address in socket.getaddrinfo(
    "localhost", port, type=socket.SOCK_STREAM
):
    key = (family, address)
    if key not in addresses:
        addresses.append(key)

if not addresses:
    print("PREFLIGHT-FAILED: localhost has no TCP addresses", file=sys.stderr)
    raise SystemExit(1)

for family, address in addresses:
    probe = socket.socket(family, socket.SOCK_STREAM)
    try:
        if family == socket.AF_INET6:
            probe.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        probe.bind(address)
    except OSError as error:
        print(
            f"PREFLIGHT-FAILED: localhost:{port} is not free: {error}",
            file=sys.stderr,
        )
        raise SystemExit(1)
    finally:
        probe.close()
PY
}

require_formal_host_ports_free() {
  local s3_port="${1:-19003}"
  local hs_port="${2:-30080}"
  [ "$s3_port" = 19003 ] && [ "$hs_port" = 30080 ] || {
    echo "PREFLIGHT-FAILED: formal localhost ports must be S3=19003 and HistoryServer=30080" >&2
    return 1
  }
  require_local_tcp_port_free "$s3_port" || return 1
  require_local_tcp_port_free "$hs_port" || return 1
}

wait_for_formal_host_ports_free() {
  local s3_port="${1:-19003}"
  local hs_port="${2:-30080}"
  local timeout_seconds="${BENCH_LOCAL_PORT_RELEASE_TIMEOUT_SECONDS:-30}"
  local poll_seconds="${BENCH_LOCAL_PORT_RELEASE_POLL_SECONDS:-0.5}"
  [ "$s3_port" = 19003 ] && [ "$hs_port" = 30080 ] || {
    echo "PREFLIGHT-FAILED: formal localhost ports must be S3=19003 and HistoryServer=30080" >&2
    return 1
  }
  python3 "$HS/test/benchmark/sweeps/formal_runner_guard.py" \
    wait-local-ports-free \
    --ports "$s3_port" "$hs_port" \
    --timeout-seconds "$timeout_seconds" \
    --poll-seconds "$poll_seconds"
}

require_no_active_incluster_kuberay_operator() {
  local inventory
  inventory=$(kubectl get deployments.apps,replicasets.apps,pods \
    --all-namespaces -o json 2>/dev/null) || {
      echo "PREFLIGHT-FAILED: cannot inventory in-cluster KubeRay operators" >&2
      return 1
    }
  printf '%s\n' "$inventory" \
    | python3 "$HS/test/benchmark/sweeps/formal_runner_guard.py" check-operators
}

require_no_active_host_kuberay_operator() {
  local kubeconfig_view
  kubeconfig_view=$(kubectl config view --minify -o json 2>/dev/null) || {
    echo "PREFLIGHT-FAILED: cannot resolve the current Kubernetes API server" >&2
    return 1
  }
  [ -n "$kubeconfig_view" ] || {
    echo "PREFLIGHT-FAILED: current kubeconfig view is empty" >&2
    return 1
  }
  printf '%s\n' "$kubeconfig_view" \
    | python3 "$HS/test/benchmark/sweeps/formal_runner_guard.py" check-host-operators
}

require_no_reconcilable_kuberay_workloads() {
  local current_context inventory
  current_context=$(kubectl config current-context 2>/dev/null) || {
    echo "PREFLIGHT-FAILED: cannot bind the KubeRay workload inventory to the current context" >&2
    return 1
  }
  [ "$current_context" = "kind-bench" ] || {
    echo "PREFLIGHT-FAILED: kubectl current-context=$current_context, want kind-bench" >&2
    return 1
  }
  inventory=$(kubectl --context "$current_context" get \
    rayclusters.ray.io,rayservices.ray.io,rayjobs.ray.io,jobs.batch,pods \
    --all-namespaces -o json 2>/dev/null) || {
      echo "PREFLIGHT-FAILED: cannot inventory cluster-wide KubeRay workloads" >&2
      return 1
    }
  [ -n "$inventory" ] || {
    echo "PREFLIGHT-FAILED: cluster-wide KubeRay workload inventory is empty" >&2
    return 1
  }
  printf '%s\n' "$inventory" \
    | python3 "$HS/test/benchmark/sweeps/formal_runner_guard.py" \
      check-cluster-workloads --context "$current_context"
}

wait_for_execution_namespace_deletion() {
  local identity_file="$1"
  local timeout_seconds="${BENCH_NAMESPACE_DELETE_TIMEOUT_SECONDS:-300}"
  local poll_seconds="${BENCH_NAMESPACE_DELETE_POLL_SECONDS:-2}"
  python3 "$HS/test/benchmark/sweeps/formal_runner_guard.py" \
    wait-namespace-deleted \
    --identity "$identity_file" \
    --timeout-seconds "$timeout_seconds" \
    --poll-seconds "$poll_seconds"
}

require_kubeconfig_files() {
  local config
  local configs=()
  local found=0
  IFS=: read -r -a configs <<<"${KUBECONFIG:-}"
  [ "${#configs[@]}" -gt 0 ] || {
    echo "PREFLIGHT-FAILED: KUBECONFIG is empty" >&2
    return 1
  }
  for config in "${configs[@]}"; do
    [ -s "$config" ] && found=1
  done
  [ "$found" -eq 1 ] || {
    echo "PREFLIGHT-FAILED: no kubeconfig file exists in: ${KUBECONFIG:-}" >&2
    return 1
  }
}

require_formal_kind_target() {
  local current_context
  current_context=$(kubectl config current-context 2>/dev/null) || {
    echo "PREFLIGHT-FAILED: cannot read kubectl current-context" >&2
    return 1
  }
  [ "$current_context" = "kind-bench" ] || {
    echo "PREFLIGHT-FAILED: kubectl current-context=$current_context, want kind-bench" >&2
    return 1
  }
  [ "${BENCH_KIND_NODE:-}" = "bench-control-plane" ] || {
    echo "PREFLIGHT-FAILED: BENCH_KIND_NODE=${BENCH_KIND_NODE:-<empty>}, want bench-control-plane" >&2
    return 1
  }
}

hs_assert_no_parallel_campaign() {
  local pid
  while IFS= read -r pid; do
    [ -z "$pid" ] && continue
    [ "$pid" = "$$" ] && continue
    echo "PREFLIGHT-FAILED: another benchmark campaign is running (pid=$pid)" >&2
    return 1
  done < <(
    pgrep -f 'benchmark\.test|go test( -vet=off)? ./test/benchmark|(ray256_(bytask|collector|nolimit|hs_cpu|hs_memory|hs_source)|gzip_bytask|ratesweep_shutdown2|shutdownsweep2|c1_observe|hs_nolimit)\.sh' \
      2>/dev/null || true
  )
}

benchmark_tree_sha256() {
  local root="$1"
  python3 "$HS/test/benchmark/sweeps/image_source_hash.py" --root "$root" --tree
}

is_full_sha256() {
  printf '%s\n' "$1" | grep -Eq '^[0-9a-f]{64}$'
}

is_full_image_id() {
  case "$1" in
    sha256:*) is_full_sha256 "${1#sha256:}" ;;
    *) return 1 ;;
  esac
}

is_present_provenance_value() {
  [ -n "$1" ] && [ "$1" != '<none>' ]
}

image_build_source_sha256() {
  local component="$1"
  python3 "$HS/test/benchmark/sweeps/image_source_hash.py" \
    --root "$HS" --component "$component"
}

file_sha256() {
  local line
  line=$(LC_ALL=C /sbin/sha256sum "$1") || return 1
  printf '%s\n' "${line%% *}"
}

tracked_workspace_diff_sha256() {
  local repo_root="$1"
  (
    set -o pipefail
    git -C "$repo_root" diff --binary HEAD -- historyserver ray-operator \
      | LC_ALL=C /sbin/sha256sum \
      | awk '{print $1}'
  )
}

provenance_value() {
  local file="$1"
  local key="$2"
  awk -v wanted="$key" '
    index($0, wanted "=") == 1 {
      count++
      value = substr($0, length(wanted) + 2)
    }
    END {
      if (count != 1 || value == "") exit 1
      print value
    }
  ' "$file"
}

require_same_provenance_value() {
  local name="$1"
  local initial="$2"
  local current="$3"
  if [ "$initial" != "$current" ]; then
    echo "PROVENANCE-FAILED: $name changed: initial=$initial current=$current" >&2
    return 1
  fi
}

runtime_image_metadata() {
  local node="$1"
  local repository_suffix="$2"
  local tag="$3"
  local image_inventory short_id inspect_output

  image_inventory=$(docker exec "$node" crictl images --digests) || {
    echo "PROVENANCE-FAILED: cannot inventory runtime images on $node" >&2
    return 1
  }
  short_id=$(awk -v suffix="$repository_suffix" -v wanted_tag="$tag" '
    $1 ~ (suffix "$") && $2 == wanted_tag && $4 != "<none>" {
      print $4
      exit
    }
  ' <<<"$image_inventory")
  [ -n "$short_id" ] || {
    echo "PROVENANCE-FAILED: runtime image $repository_suffix:$tag is absent on $node" >&2
    return 1
  }

  inspect_output=$(docker exec "$node" crictl inspecti "$short_id") || {
    echo "PROVENANCE-FAILED: cannot inspect runtime image $short_id on $node" >&2
    return 1
  }
  python3 -c '
import json
import sys

doc = json.load(sys.stdin)
status = doc.get("status") or {}
info = doc.get("info") or {}
config = ((info.get("imageSpec") or {}).get("config") or {})
labels = config.get("Labels") or config.get("labels") or {}
image_id = status.get("id") or ""
digests = ",".join(status.get("repoDigests") or []) or "<none>"
version = labels.get("io.ray.ray-version", "<none>")
commit = labels.get("io.ray.ray-commit", "<none>")
source = labels.get("io.kuberay.historyserver.build-source-sha256", "<none>")
if not image_id:
    raise SystemExit("runtime image has no status.id")
print("\t".join((image_id, digests, version, commit, source)))
' <<<"$inspect_output"
}

capture_sweep_provenance() {
  local ray_image_requested="$1"
  local collector_image_requested historyserver_image_requested
  local ray_tag collector_tag historyserver_tag
  local ray_meta collector_meta historyserver_meta
  local ray_runtime_id ray_runtime_digests ray_runtime_version ray_runtime_commit unused_source
  local collector_runtime_id collector_runtime_digests unused_version unused_commit collector_runtime_source_sha256
  local historyserver_runtime_id historyserver_runtime_digests historyserver_runtime_source_sha256
  local operator_sha256 repo_root repo_head tracked_diff_sha256 source_sha256 historyserver_source_sha256
  local expected_matrix_sha256
  local collector_build_source_sha256 historyserver_build_source_sha256
  local raycluster_manifest_sha256 historyserver_manifest_sha256

  [ -n "${HS:-}" ] && [ -n "${OPERATOR_BIN:-}" ] && [ -n "${OUT:-}" ] \
    && [ -n "${BENCH_KIND_NODE:-}" ] || {
      echo "PROVENANCE-FAILED: HS, OPERATOR_BIN, OUT, and BENCH_KIND_NODE are required" >&2
      return 1
    }

  [ ! -e "$OUT/provenance.txt" ] || {
    echo "PROVENANCE-FAILED: refusing to overwrite $OUT/provenance.txt" >&2
    return 1
  }

  [ -s "$OUT/expected-matrix.json" ] || {
    echo "PROVENANCE-FAILED: missing expected matrix $OUT/expected-matrix.json" >&2
    return 1
  }
  expected_matrix_sha256=$(file_sha256 "$OUT/expected-matrix.json") || return 1
  is_full_sha256 "$expected_matrix_sha256" || {
    echo "PROVENANCE-FAILED: expected matrix SHA-256 is invalid" >&2
    return 1
  }

  collector_build_source_sha256=$(image_build_source_sha256 collector) || return 1
  historyserver_build_source_sha256=$(image_build_source_sha256 historyserver) || return 1
  is_full_sha256 "$collector_build_source_sha256" \
    && is_full_sha256 "$historyserver_build_source_sha256" || {
      echo "PROVENANCE-FAILED: current image build source hash is invalid" >&2
      return 1
    }

  collector_image_requested=$(awk '$1 == "image:" && $2 ~ /^collector:/ { print $2; exit }' \
    "$HS/config/raycluster.yaml")
  historyserver_image_requested=$(awk '$1 == "image:" && $2 ~ /^historyserver:/ { print $2; exit }' \
    "$HS/config/historyserver.yaml")
  [ -n "$ray_image_requested" ] && [ -n "$collector_image_requested" ] \
    && [ -n "$historyserver_image_requested" ] || {
      echo "PROVENANCE-FAILED: requested image identity is empty" >&2
      return 1
    }

  ray_tag=${ray_image_requested##*:}
  collector_tag=${collector_image_requested##*:}
  historyserver_tag=${historyserver_image_requested##*:}

  ray_meta=$(runtime_image_metadata "$BENCH_KIND_NODE" 'rayproject/ray' "$ray_tag") || {
    echo "PROVENANCE-FAILED: cannot resolve runtime image for $ray_image_requested" >&2
    return 1
  }
  collector_meta=$(runtime_image_metadata "$BENCH_KIND_NODE" 'collector' "$collector_tag") || {
    echo "PROVENANCE-FAILED: cannot resolve runtime image for $collector_image_requested" >&2
    return 1
  }
  historyserver_meta=$(runtime_image_metadata "$BENCH_KIND_NODE" 'historyserver' "$historyserver_tag") || {
    echo "PROVENANCE-FAILED: cannot resolve runtime image for $historyserver_image_requested" >&2
    return 1
  }

  IFS=$'\t' read -r ray_runtime_id ray_runtime_digests ray_runtime_version ray_runtime_commit unused_source <<<"$ray_meta"
  IFS=$'\t' read -r collector_runtime_id collector_runtime_digests unused_version unused_commit collector_runtime_source_sha256 <<<"$collector_meta"
  IFS=$'\t' read -r historyserver_runtime_id historyserver_runtime_digests unused_version unused_commit historyserver_runtime_source_sha256 <<<"$historyserver_meta"

  is_present_provenance_value "$ray_runtime_version" \
    && is_present_provenance_value "$ray_runtime_commit" || {
      echo "PROVENANCE-FAILED: Ray runtime version and commit labels are required" >&2
      return 1
    }
  is_full_image_id "$ray_runtime_id" \
    && is_full_image_id "$collector_runtime_id" \
    && is_full_image_id "$historyserver_runtime_id" || {
      echo "PROVENANCE-FAILED: runtime image ID is not a full sha256 digest" >&2
      return 1
    }
  is_full_sha256 "$collector_runtime_source_sha256" \
    && is_full_sha256 "$historyserver_runtime_source_sha256" || {
      echo "PROVENANCE-FAILED: runtime image build source label $BUILD_SOURCE_LABEL is missing or invalid" >&2
      return 1
    }
  require_same_provenance_value "collector image build source" \
    "$collector_build_source_sha256" "$collector_runtime_source_sha256" || return 1
  require_same_provenance_value "historyserver image build source" \
    "$historyserver_build_source_sha256" "$historyserver_runtime_source_sha256" || return 1

  operator_sha256=$(file_sha256 "$OPERATOR_BIN") || return 1
  repo_root=$(git -C "$HS" rev-parse --show-toplevel) || return 1
  repo_head=$(git -C "$repo_root" rev-parse HEAD) || return 1
  tracked_diff_sha256=$(tracked_workspace_diff_sha256 "$repo_root") || return 1
  source_sha256=$(benchmark_tree_sha256 "$HS/test/benchmark") || return 1
  historyserver_source_sha256=$(benchmark_tree_sha256 "$HS") || return 1
  raycluster_manifest_sha256=$(file_sha256 "$HS/config/raycluster.yaml") || return 1
  historyserver_manifest_sha256=$(file_sha256 "$HS/config/historyserver.yaml") || return 1

  for value in "$operator_sha256" "$repo_head" "$tracked_diff_sha256" "$source_sha256" "$historyserver_source_sha256" \
    "$ray_runtime_id" "$collector_runtime_id" "$historyserver_runtime_id" \
    "$ray_runtime_version" "$ray_runtime_commit" "$collector_build_source_sha256" \
    "$historyserver_build_source_sha256"; do
    [ -n "$value" ] && [ "$value" != "<none>" ] || {
      echo "PROVENANCE-FAILED: a required provenance value is empty" >&2
      return 1
    }
  done

  {
    printf 'repo_head=%s\n' "$repo_head"
    printf 'tracked_diff_sha256=%s\n' "$tracked_diff_sha256"
    printf 'benchmark_source_sha256=%s\n' "$source_sha256"
    printf 'historyserver_source_sha256=%s\n' "$historyserver_source_sha256"
    printf 'operator_sha256=%s\n' "$operator_sha256"
    printf 'raycluster_manifest_sha256=%s\n' "$raycluster_manifest_sha256"
    printf 'historyserver_manifest_sha256=%s\n' "$historyserver_manifest_sha256"
    printf 'expected_matrix_sha256=%s\n' "$expected_matrix_sha256"
    printf 'ray_image_requested=%s\n' "$ray_image_requested"
    printf 'ray_runtime_id=%s\n' "$ray_runtime_id"
    printf 'ray_runtime_repo_digests=%s\n' "$ray_runtime_digests"
    printf 'ray_runtime_version=%s\n' "$ray_runtime_version"
    printf 'ray_runtime_commit=%s\n' "$ray_runtime_commit"
    printf 'collector_image_requested=%s\n' "$collector_image_requested"
    printf 'collector_build_source_sha256=%s\n' "$collector_build_source_sha256"
    printf 'collector_runtime_id=%s\n' "$collector_runtime_id"
    printf 'collector_runtime_repo_digests=%s\n' "$collector_runtime_digests"
    printf 'collector_runtime_build_source_sha256=%s\n' "$collector_runtime_source_sha256"
    printf 'historyserver_image_requested=%s\n' "$historyserver_image_requested"
    printf 'historyserver_build_source_sha256=%s\n' "$historyserver_build_source_sha256"
    printf 'historyserver_runtime_id=%s\n' "$historyserver_runtime_id"
    printf 'historyserver_runtime_repo_digests=%s\n' "$historyserver_runtime_digests"
    printf 'historyserver_runtime_build_source_sha256=%s\n' "$historyserver_runtime_source_sha256"
  } > "$OUT/provenance.txt" || return 1

  python3 "$HS/test/benchmark/sweeps/validate_sweep.py" --provenance-only "$OUT"
}

revalidate_sweep_provenance() {
  local initial_file="$OUT/provenance.txt"
  local final_file="$OUT/provenance-final.txt"
  local ray_image_requested collector_image_requested historyserver_image_requested
  local repo_initial_head tracked_initial_diff benchmark_initial_source historyserver_initial_source_tree
  local operator_initial_sha raycluster_initial_manifest historyserver_initial_manifest expected_matrix_initial_sha
  local ray_initial_id ray_initial_version ray_initial_commit
  local collector_initial_id collector_initial_source collector_initial_runtime_source
  local historyserver_initial_id historyserver_initial_source historyserver_initial_runtime_source
  local collector_current_source historyserver_current_source
  local repo_root repo_current_head tracked_current_diff benchmark_current_source historyserver_current_source_tree
  local operator_current_sha raycluster_current_manifest historyserver_current_manifest expected_matrix_current_sha
  local ray_meta collector_meta historyserver_meta
  local ray_runtime_id ray_runtime_digests ray_runtime_version ray_runtime_commit unused_source
  local collector_runtime_id collector_runtime_digests unused_version unused_commit collector_runtime_source
  local historyserver_runtime_id historyserver_runtime_digests historyserver_runtime_source
  local ray_tag collector_tag historyserver_tag key expected observed

  [ -s "$initial_file" ] || {
    echo "PROVENANCE-FAILED: missing initial provenance file $initial_file" >&2
    return 1
  }

  ray_image_requested=$(provenance_value "$initial_file" ray_image_requested) || return 1
  collector_image_requested=$(provenance_value "$initial_file" collector_image_requested) || return 1
  historyserver_image_requested=$(provenance_value "$initial_file" historyserver_image_requested) || return 1
  repo_initial_head=$(provenance_value "$initial_file" repo_head) || return 1
  tracked_initial_diff=$(provenance_value "$initial_file" tracked_diff_sha256) || return 1
  benchmark_initial_source=$(provenance_value "$initial_file" benchmark_source_sha256) || return 1
  historyserver_initial_source_tree=$(provenance_value "$initial_file" historyserver_source_sha256) || return 1
  operator_initial_sha=$(provenance_value "$initial_file" operator_sha256) || return 1
  raycluster_initial_manifest=$(provenance_value "$initial_file" raycluster_manifest_sha256) || return 1
  historyserver_initial_manifest=$(provenance_value "$initial_file" historyserver_manifest_sha256) || return 1
  expected_matrix_initial_sha=$(provenance_value "$initial_file" expected_matrix_sha256) || return 1
  ray_initial_id=$(provenance_value "$initial_file" ray_runtime_id) || return 1
  ray_initial_version=$(provenance_value "$initial_file" ray_runtime_version) || return 1
  ray_initial_commit=$(provenance_value "$initial_file" ray_runtime_commit) || return 1
  collector_initial_id=$(provenance_value "$initial_file" collector_runtime_id) || return 1
  collector_initial_source=$(provenance_value "$initial_file" collector_build_source_sha256) || return 1
  collector_initial_runtime_source=$(provenance_value "$initial_file" collector_runtime_build_source_sha256) || return 1
  historyserver_initial_id=$(provenance_value "$initial_file" historyserver_runtime_id) || return 1
  historyserver_initial_source=$(provenance_value "$initial_file" historyserver_build_source_sha256) || return 1
  historyserver_initial_runtime_source=$(provenance_value "$initial_file" historyserver_runtime_build_source_sha256) || return 1

  is_present_provenance_value "$ray_initial_version" \
    && is_present_provenance_value "$ray_initial_commit" || {
      echo "PROVENANCE-FAILED: initial Ray runtime version and commit are missing" >&2
      return 1
    }
  is_full_image_id "$ray_initial_id" \
    && is_full_image_id "$collector_initial_id" \
    && is_full_image_id "$historyserver_initial_id" \
    && is_full_sha256 "$collector_initial_source" \
    && is_full_sha256 "$collector_initial_runtime_source" \
    && is_full_sha256 "$historyserver_initial_source" \
    && is_full_sha256 "$historyserver_initial_runtime_source" \
    && is_full_sha256 "$tracked_initial_diff" \
    && is_full_sha256 "$benchmark_initial_source" \
    && is_full_sha256 "$historyserver_initial_source_tree" \
    && is_full_sha256 "$operator_initial_sha" \
    && is_full_sha256 "$raycluster_initial_manifest" \
    && is_full_sha256 "$historyserver_initial_manifest" \
    && is_full_sha256 "$expected_matrix_initial_sha" || {
      echo "PROVENANCE-FAILED: initial source or runtime image identity is invalid" >&2
      return 1
    }

  collector_current_source=$(image_build_source_sha256 collector) || return 1
  historyserver_current_source=$(image_build_source_sha256 historyserver) || return 1
  is_full_sha256 "$collector_current_source" \
    && is_full_sha256 "$historyserver_current_source" || {
      echo "PROVENANCE-FAILED: final image build source hash is invalid" >&2
      return 1
    }

  [ -n "${OPERATOR_BIN:-}" ] || {
    echo "PROVENANCE-FAILED: OPERATOR_BIN is required for final revalidation" >&2
    return 1
  }
  repo_root=$(git -C "$HS" rev-parse --show-toplevel) || return 1
  repo_current_head=$(git -C "$repo_root" rev-parse HEAD) || return 1
  tracked_current_diff=$(tracked_workspace_diff_sha256 "$repo_root") || return 1
  benchmark_current_source=$(benchmark_tree_sha256 "$HS/test/benchmark") || return 1
  historyserver_current_source_tree=$(benchmark_tree_sha256 "$HS") || return 1
  operator_current_sha=$(file_sha256 "$OPERATOR_BIN") || return 1
  raycluster_current_manifest=$(file_sha256 "$HS/config/raycluster.yaml") || return 1
  historyserver_current_manifest=$(file_sha256 "$HS/config/historyserver.yaml") || return 1
  expected_matrix_current_sha=$(file_sha256 "$OUT/expected-matrix.json") || return 1

  is_full_sha256 "$tracked_current_diff" \
    && is_full_sha256 "$benchmark_current_source" \
    && is_full_sha256 "$historyserver_current_source_tree" \
    && is_full_sha256 "$operator_current_sha" \
    && is_full_sha256 "$raycluster_current_manifest" \
    && is_full_sha256 "$historyserver_current_manifest" \
    && is_full_sha256 "$expected_matrix_current_sha" || {
      echo "PROVENANCE-FAILED: final harness provenance is invalid" >&2
      return 1
    }

  require_same_provenance_value "repository HEAD" "$repo_initial_head" "$repo_current_head" || return 1
  require_same_provenance_value "tracked workspace diff" "$tracked_initial_diff" "$tracked_current_diff" || return 1
  require_same_provenance_value "benchmark source" "$benchmark_initial_source" "$benchmark_current_source" || return 1
  require_same_provenance_value "historyserver source tree" "$historyserver_initial_source_tree" "$historyserver_current_source_tree" || return 1
  require_same_provenance_value "operator binary" "$operator_initial_sha" "$operator_current_sha" || return 1
  require_same_provenance_value "RayCluster manifest" "$raycluster_initial_manifest" "$raycluster_current_manifest" || return 1
  require_same_provenance_value "History Server manifest" "$historyserver_initial_manifest" "$historyserver_current_manifest" || return 1
  require_same_provenance_value "expected matrix" "$expected_matrix_initial_sha" "$expected_matrix_current_sha" || return 1

  ray_tag=${ray_image_requested##*:}
  collector_tag=${collector_image_requested##*:}
  historyserver_tag=${historyserver_image_requested##*:}
  ray_meta=$(runtime_image_metadata "$BENCH_KIND_NODE" rayproject/ray "$ray_tag") || {
    echo "PROVENANCE-FAILED: cannot re-resolve runtime image for $ray_image_requested" >&2
    return 1
  }
  collector_meta=$(runtime_image_metadata "$BENCH_KIND_NODE" collector "$collector_tag") || {
    echo "PROVENANCE-FAILED: cannot re-resolve runtime image for $collector_image_requested" >&2
    return 1
  }
  historyserver_meta=$(runtime_image_metadata "$BENCH_KIND_NODE" historyserver "$historyserver_tag") || {
    echo "PROVENANCE-FAILED: cannot re-resolve runtime image for $historyserver_image_requested" >&2
    return 1
  }

  IFS=$'\t' read -r ray_runtime_id ray_runtime_digests ray_runtime_version ray_runtime_commit unused_source <<<"$ray_meta"
  IFS=$'\t' read -r collector_runtime_id collector_runtime_digests unused_version unused_commit collector_runtime_source <<<"$collector_meta"
  IFS=$'\t' read -r historyserver_runtime_id historyserver_runtime_digests unused_version unused_commit historyserver_runtime_source <<<"$historyserver_meta"

  is_full_image_id "$ray_runtime_id" \
    && is_full_image_id "$collector_runtime_id" \
    && is_full_image_id "$historyserver_runtime_id" \
    && is_full_sha256 "$collector_runtime_source" \
    && is_full_sha256 "$historyserver_runtime_source" \
    && is_present_provenance_value "$ray_runtime_version" \
    && is_present_provenance_value "$ray_runtime_commit" || {
      echo "PROVENANCE-FAILED: final runtime image metadata is incomplete" >&2
      return 1
    }

  require_same_provenance_value "collector source" "$collector_initial_source" "$collector_current_source" || return 1
  require_same_provenance_value "historyserver source" "$historyserver_initial_source" "$historyserver_current_source" || return 1
  require_same_provenance_value "Ray runtime image ID" "$ray_initial_id" "$ray_runtime_id" || return 1
  require_same_provenance_value "Ray runtime version" "$ray_initial_version" "$ray_runtime_version" || return 1
  require_same_provenance_value "Ray runtime commit" "$ray_initial_commit" "$ray_runtime_commit" || return 1
  require_same_provenance_value "collector runtime image ID" "$collector_initial_id" "$collector_runtime_id" || return 1
  require_same_provenance_value "collector initial image/source binding" "$collector_initial_source" "$collector_initial_runtime_source" || return 1
  require_same_provenance_value "collector current image/source binding" "$collector_current_source" "$collector_runtime_source" || return 1
  require_same_provenance_value "collector runtime source label" "$collector_initial_runtime_source" "$collector_runtime_source" || return 1
  require_same_provenance_value "historyserver runtime image ID" "$historyserver_initial_id" "$historyserver_runtime_id" || return 1
  require_same_provenance_value "historyserver initial image/source binding" "$historyserver_initial_source" "$historyserver_initial_runtime_source" || return 1
  require_same_provenance_value "historyserver current image/source binding" "$historyserver_current_source" "$historyserver_runtime_source" || return 1
  require_same_provenance_value "historyserver runtime source label" "$historyserver_initial_runtime_source" "$historyserver_runtime_source" || return 1

  if [ -e "$final_file" ]; then
    for key in revalidation_status repo_head tracked_diff_sha256 benchmark_source_sha256 \
      historyserver_source_sha256 operator_sha256 raycluster_manifest_sha256 historyserver_manifest_sha256 \
      expected_matrix_sha256 \
      ray_runtime_id ray_runtime_version ray_runtime_commit \
      collector_build_source_sha256 collector_runtime_id collector_runtime_build_source_sha256 \
      historyserver_build_source_sha256 historyserver_runtime_id historyserver_runtime_build_source_sha256; do
      observed=$(provenance_value "$final_file" "$key") || return 1
      case "$key" in
        revalidation_status) expected=valid ;;
        repo_head) expected=$repo_current_head ;;
        tracked_diff_sha256) expected=$tracked_current_diff ;;
        benchmark_source_sha256) expected=$benchmark_current_source ;;
        historyserver_source_sha256) expected=$historyserver_current_source_tree ;;
        operator_sha256) expected=$operator_current_sha ;;
        raycluster_manifest_sha256) expected=$raycluster_current_manifest ;;
        historyserver_manifest_sha256) expected=$historyserver_current_manifest ;;
        expected_matrix_sha256) expected=$expected_matrix_current_sha ;;
        ray_runtime_id) expected=$ray_runtime_id ;;
        ray_runtime_version) expected=$ray_runtime_version ;;
        ray_runtime_commit) expected=$ray_runtime_commit ;;
        collector_build_source_sha256) expected=$collector_current_source ;;
        collector_runtime_id) expected=$collector_runtime_id ;;
        collector_runtime_build_source_sha256) expected=$collector_runtime_source ;;
        historyserver_build_source_sha256) expected=$historyserver_current_source ;;
        historyserver_runtime_id) expected=$historyserver_runtime_id ;;
        historyserver_runtime_build_source_sha256) expected=$historyserver_runtime_source ;;
      esac
      require_same_provenance_value "final record $key" "$expected" "$observed" || return 1
    done
  else
    {
      printf 'revalidation_status=valid\n'
      printf 'repo_head=%s\n' "$repo_current_head"
      printf 'tracked_diff_sha256=%s\n' "$tracked_current_diff"
      printf 'benchmark_source_sha256=%s\n' "$benchmark_current_source"
      printf 'historyserver_source_sha256=%s\n' "$historyserver_current_source_tree"
      printf 'operator_sha256=%s\n' "$operator_current_sha"
      printf 'raycluster_manifest_sha256=%s\n' "$raycluster_current_manifest"
      printf 'historyserver_manifest_sha256=%s\n' "$historyserver_current_manifest"
      printf 'expected_matrix_sha256=%s\n' "$expected_matrix_current_sha"
      printf 'ray_runtime_id=%s\n' "$ray_runtime_id"
      printf 'ray_runtime_version=%s\n' "$ray_runtime_version"
      printf 'ray_runtime_commit=%s\n' "$ray_runtime_commit"
      printf 'collector_build_source_sha256=%s\n' "$collector_current_source"
      printf 'collector_runtime_id=%s\n' "$collector_runtime_id"
      printf 'collector_runtime_build_source_sha256=%s\n' "$collector_runtime_source"
      printf 'historyserver_build_source_sha256=%s\n' "$historyserver_current_source"
      printf 'historyserver_runtime_id=%s\n' "$historyserver_runtime_id"
      printf 'historyserver_runtime_build_source_sha256=%s\n' "$historyserver_runtime_source"
    } > "$final_file" || return 1
  fi

  echo "PROVENANCE-REVALIDATED"
}

validate_completed_sweep() {
  local expected_arms="$1"
  revalidate_sweep_provenance || return 1
  python3 "$HS/test/benchmark/sweeps/validate_sweep.py" \
    --expected-arms "$expected_arms" "$OUT" || return 1
  revalidate_sweep_provenance
}
