#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HS=$(cd "$SCRIPT_DIR/../../.." && pwd)
source "$SCRIPT_DIR/sweep_lib.sh"
source "$SCRIPT_DIR/ray256_hs_cpu.sh"

TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/hs-multi-n-contract.XXXXXX")
MATRIX="$TEST_DIR/expected-matrix.json"
cat > "$MATRIX" <<'JSON'
{
  "source": {
    "spec": "source-ns/source-cluster/session_5k",
    "expectedBenchmarkAttempts": 5000,
    "expectedObjectCount": 143,
    "expectedTotalBytes": 2500000,
    "sourceReportSHA256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "bucket": "ray-historyserver-benchmark",
    "collectorRuntimeID": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "collectorImageRequested": "collector:latest",
    "taskLogMetadata": {
      "algorithm": "task-log-metadata-sha256-v1",
      "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
      "attempts": 5000,
      "counts": {
        "nil": 0,
        "present": 5000,
        "structurallyInvalid": 0,
        "incompleteNonNil": 5000,
        "stdoutExactResolvable": 0,
        "stderrExactResolvable": 0,
        "legacyWholeWorkerFallback": 0
      },
      "valid": true,
      "problems": []
    },
    "rayJobLifecycle": {
      "ownedCluster": true,
      "shutdownAfterJobFinishes": true,
      "ttlSecondsAfterFinished": 30
    }
  }
}
JSON

hs_load_matrix_source "$MATRIX"
[ "$HS_SOURCE_SPEC" = "source-ns/source-cluster/session_5k" ]
[ "$HS_SOURCE_TASK_COUNT" = "5000" ]
[ "$HS_SOURCE_OBJECT_COUNT" = "143" ]
[ "$HS_SOURCE_TOTAL_BYTES" = "2500000" ]
[ "$HS_SOURCE_BUCKET" = "ray-historyserver-benchmark" ]
[ "$HS_SOURCE_COLLECTOR_RUNTIME_ID" = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" ]
[ "$HS_SOURCE_RAYJOB_OWNED" = true ]
[ "$HS_SOURCE_SHUTDOWN_AFTER_JOB" = true ]
[ "$HS_SOURCE_JOB_TTL_SECONDS" = 30 ]
[ "$HS_SOURCE_TASK_LOG_METADATA_ALGORITHM" = task-log-metadata-sha256-v1 ]
[ "$HS_SOURCE_TASK_LOG_METADATA_SHA256" = cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc ]
[ "$HS_SOURCE_TASK_LOG_METADATA_ATTEMPTS" = 5000 ]
[ "$HS_SOURCE_TASK_LOG_METADATA_NIL" = 0 ]
[ "$HS_SOURCE_TASK_LOG_METADATA_PRESENT" = 5000 ]
[ "$HS_SOURCE_TASK_LOG_METADATA_STRUCTURALLY_INVALID" = 0 ]
[ "$HS_SOURCE_TASK_LOG_METADATA_INCOMPLETE_NON_NIL" = 5000 ]
[ "$HS_SOURCE_TASK_LOG_METADATA_STDOUT_EXACT_RESOLVABLE" = 0 ]
[ "$HS_SOURCE_TASK_LOG_METADATA_STDERR_EXACT_RESOLVABLE" = 0 ]
[ "$HS_SOURCE_TASK_LOG_METADATA_LEGACY_WHOLE_WORKER_FALLBACK" = 0 ]

for mutation in hash count; do
  invalid_matrix="$TEST_DIR/invalid-task-log-$mutation.json"
  python3 - "$MATRIX" "$invalid_matrix" "$mutation" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    document = json.load(stream)
metadata = document["source"]["taskLogMetadata"]
if sys.argv[3] == "hash":
    metadata["sha256"] = "not-a-sha256"
else:
    metadata["counts"]["present"] -= 1
with open(sys.argv[2], "w", encoding="utf-8") as stream:
    json.dump(document, stream)
PY
  if (hs_load_matrix_source "$invalid_matrix") >/dev/null 2>&1; then
    echo "matrix loader accepted TaskLog metadata $mutation drift" >&2
    exit 1
  fi
done

N50_MATRIX="$TEST_DIR/expected-matrix-n50000.json"
python3 - "$MATRIX" "$N50_MATRIX" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    document = json.load(stream)
source = document["source"]
source["expectedBenchmarkAttempts"] = 50000
metadata = source["taskLogMetadata"]
metadata["attempts"] = 50000
metadata["counts"]["present"] = 50000
metadata["counts"]["incompleteNonNil"] = 50000
source["sourceGeneration"] = {
    "waveSize": 2000,
    "drivers": 1,
    "targetTaskRate": 500,
    "pacingVariant": "single-driver-rate500-wave2000-v2",
    "rejectedAttemptReportSHA256s": [
        "2c191e0133dce4a95a19fe7835daf0628784276fd421de608ed8fd796d3915ef",
        "01274cdc416f6a6709706389042e5aa8e6ab382677b1886b9bf2b8459d47e677",
    ],
    "collectorCPURequest": "100m",
    "collectorCPULimit": "2",
    "driverTasks": 50000,
    "driverWallSec": 40.5,
    "driverRateTPS": 500.0,
    "lineageSHA256": "d" * 64,
    "rejectedBaselineReportSHA256": (
        "7873ec98d6e1d376a5994a86aaaa18ee41b6f5e1a124f67249589c152ad94264"
    ),
}
source["rayJobLifecycle"]["rayJobBackoffLimit"] = 0
source["rayJobLifecycle"]["submitterBackoffLimit"] = 0
with open(sys.argv[2], "w", encoding="utf-8") as stream:
    json.dump(document, stream)
PY
hs_load_matrix_source "$N50_MATRIX"
[ "$HS_SOURCE_TASK_COUNT" = 50000 ]
[ "$HS_SOURCE_RAYJOB_BACKOFF_LIMIT" = 0 ]
[ "$HS_SOURCE_SUBMITTER_BACKOFF_LIMIT" = 0 ]

for mutation in missing old-wave variant rejected rejected-attempts collector-request \
  collector-limit lineage integer-lineage bool-driver infinite-rate out-of-band-rate \
  float-attempts bool-objects bool-bytes missing-rayjob-retry \
  null-rayjob-retry retry-rayjob bool-submitter-retry retry-submitter; do
  invalid_matrix="$TEST_DIR/invalid-source-generation-$mutation.json"
  python3 - "$N50_MATRIX" "$invalid_matrix" "$mutation" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    document = json.load(stream)
source = document["source"]
generation = source["sourceGeneration"]
mutation = sys.argv[3]
if mutation == "missing":
    source.pop("sourceGeneration")
elif mutation == "old-wave":
    generation["waveSize"] = 100
elif mutation == "variant":
    generation["pacingVariant"] = "single-driver-wave250-v1"
elif mutation == "rejected":
    generation["rejectedBaselineReportSHA256"] = "e" * 64
elif mutation == "rejected-attempts":
    generation["rejectedAttemptReportSHA256s"] = []
elif mutation == "collector-request":
    generation["collectorCPURequest"] = "250m"
elif mutation == "collector-limit":
    generation["collectorCPULimit"] = "1"
elif mutation == "lineage":
    generation["lineageSHA256"] = "not-a-sha256"
elif mutation == "integer-lineage":
    generation["lineageSHA256"] = int("9" * 64)
elif mutation == "bool-driver":
    generation["drivers"] = True
elif mutation == "infinite-rate":
    generation["driverRateTPS"] = float("inf")
elif mutation == "out-of-band-rate":
    generation["driverRateTPS"] = 600.0
elif mutation == "float-attempts":
    source["expectedBenchmarkAttempts"] = 50000.0
elif mutation == "bool-objects":
    source["expectedObjectCount"] = True
elif mutation == "bool-bytes":
    source["expectedTotalBytes"] = True
elif mutation == "missing-rayjob-retry":
    source["rayJobLifecycle"].pop("rayJobBackoffLimit")
elif mutation == "null-rayjob-retry":
    source["rayJobLifecycle"]["rayJobBackoffLimit"] = None
elif mutation == "retry-rayjob":
    source["rayJobLifecycle"]["rayJobBackoffLimit"] = 1
elif mutation == "bool-submitter-retry":
    source["rayJobLifecycle"]["submitterBackoffLimit"] = False
elif mutation == "retry-submitter":
    source["rayJobLifecycle"]["submitterBackoffLimit"] = 2
with open(sys.argv[2], "w", encoding="utf-8") as stream:
    json.dump(document, stream)
PY
  if (hs_load_matrix_source "$invalid_matrix") >/dev/null 2>&1; then
    echo "matrix loader accepted sourceGeneration drift: $mutation" >&2
    exit 1
  fi
done

if BENCH_HS_SOURCE_ACCEPTED_DIR="$TEST_DIR/missing/accepted" \
  bash "$SCRIPT_DIR/ray256_hs_cpu.sh" >/dev/null 2>&1; then
  echo "CPU runner accepted a missing source bundle" >&2
  exit 1
fi
touch "$TEST_DIR/arbitrary-report.json" "$TEST_DIR/arbitrary-provenance.txt"
if BENCH_HS_SOURCE_REPORT="$TEST_DIR/arbitrary-report.json" \
  BENCH_HS_SOURCE_PROVENANCE="$TEST_DIR/arbitrary-provenance.txt" \
  bash "$SCRIPT_DIR/ray256_hs_cpu.sh" >/dev/null 2>&1; then
  echo "CPU runner accepted the legacy raw report/provenance input" >&2
  exit 1
fi
mkdir "$TEST_DIR/truncated-attempt" "$TEST_DIR/truncated-attempt/accepted"
printf '{"status":"accepted"}\n' \
  > "$TEST_DIR/truncated-attempt/accepted/source-completion.json"
if BENCH_HS_SOURCE_ACCEPTED_DIR="$TEST_DIR/truncated-attempt/accepted" \
  bash "$SCRIPT_DIR/ray256_hs_cpu.sh" >/dev/null 2>&1; then
  echo "CPU runner accepted a truncated source bundle" >&2
  exit 1
fi
CANONICAL_TEST_DIR=$(cd "$TEST_DIR" && pwd -P)
mkdir "$CANONICAL_TEST_DIR/symlink-target"
mkdir "$CANONICAL_TEST_DIR/symlink-target/accepted"
printf '{"status":"accepted"}\n' \
  > "$CANONICAL_TEST_DIR/symlink-target/accepted/source-completion.json"
ln -s "$CANONICAL_TEST_DIR/symlink-target" \
  "$CANONICAL_TEST_DIR/accepted-parent-link"
if BENCH_HS_SOURCE_ACCEPTED_DIR="$CANONICAL_TEST_DIR/accepted-parent-link/accepted" \
  bash "$SCRIPT_DIR/ray256_hs_cpu.sh" \
  >"$CANONICAL_TEST_DIR/accepted-parent-link.out" 2>&1; then
  echo "CPU runner accepted a source bundle through a parent symlink" >&2
  exit 1
fi
grep -Eq 'symlink-free|canonical bundle root' \
  "$CANONICAL_TEST_DIR/accepted-parent-link.out"
mkdir "$TEST_DIR/dummy-cpu-root"
if BENCH_HS_CPU_CAMPAIGN="$TEST_DIR/dummy-cpu-root" \
  BENCH_HS_SOURCE_ACCEPTED_DIR="$TEST_DIR/missing/accepted" \
  bash "$SCRIPT_DIR/ray256_hs_memory.sh" >/dev/null 2>&1; then
  echo "memory runner accepted a missing source bundle" >&2
  exit 1
fi
if BENCH_HS_MEMORY_EXECUTION_CPU_OVERRIDE=2 \
  bash "$SCRIPT_DIR/ray256_hs_memory.sh" \
  >"$TEST_DIR/invalid-memory-cpu-override.out" 2>&1; then
  echo "memory runner accepted an unsupported execution CPU override" >&2
  exit 1
fi
grep -q 'must be unset or exactly 1' \
  "$TEST_DIR/invalid-memory-cpu-override.out"

kubectl() {
  if [ "$1 $2" = "config current-context" ]; then
    printf '%s\n' "${TEST_CONTEXT:-kind-bench}"
    return
  fi
  if [ "$1 $2" = "config view" ]; then
    [ "${TEST_CONFIG_VIEW_FAIL:-0}" = 0 ] || return 1
    printf '%s\n' '{"current-context":"kind-bench","contexts":[{"name":"kind-bench","context":{"cluster":"kind-bench"}}],"clusters":[{"name":"kind-bench","cluster":{"server":"https://127.0.0.1:64027"}}]}'
    return
  fi
  if [ "$1 $2" = "get deployments.apps,replicasets.apps,pods" ]; then
    if [ -n "${TEST_KUBE_INVENTORY:-}" ]; then
      printf '%s\n' "$TEST_KUBE_INVENTORY"
    else
      printf '%s\n' '{"items":[]}'
    fi
    return
  fi
  if [ "${1:-} ${2:-} ${3:-}" = "--context kind-bench get" ] \
    && [ "${4:-}" = "rayclusters.ray.io,rayservices.ray.io,rayjobs.ray.io,jobs.batch,pods" ]; then
    [ "${TEST_WORKLOAD_INVENTORY_FAIL:-0}" = 0 ] || return 1
    printf '%s\n' "$*" > "$TEST_DIR/workload-kubectl-args"
    if [ -n "${TEST_WORKLOAD_INVENTORY:-}" ]; then
      printf '%s\n' "$TEST_WORKLOAD_INVENTORY"
    else
      printf '%s\n' '{"apiVersion":"v1","kind":"List","items":[]}'
    fi
    return
  fi
  return 1
}
BENCH_KIND_NODE=bench-control-plane

PORT_CALLS=""
require_local_tcp_port_free() {
  PORT_CALLS="${PORT_CALLS}${PORT_CALLS:+ }$1"
}
require_formal_host_ports_free 19003 30080
[ "$PORT_CALLS" = "19003 30080" ] || {
  echo "formal port preflight order differs: $PORT_CALLS" >&2
  exit 1
}
if require_formal_host_ports_free 19003 30081 2>/dev/null; then
  echo "formal port preflight accepted the wrong History Server port" >&2
  exit 1
fi

PORT_WAIT_ARGS=""
python3() {
  if [ "$1" = "$HS/test/benchmark/sweeps/formal_runner_guard.py" ] \
    && [ "$2" = wait-local-ports-free ]; then
    PORT_WAIT_ARGS="$*"
    return 0
  fi
  command python3 "$@"
}
BENCH_LOCAL_PORT_RELEASE_TIMEOUT_SECONDS=17
BENCH_LOCAL_PORT_RELEASE_POLL_SECONDS=0.25
wait_for_formal_host_ports_free 19003 30080
case "$PORT_WAIT_ARGS" in
  *'wait-local-ports-free --ports 19003 30080 --timeout-seconds 17 --poll-seconds 0.25') ;;
  *)
    echo "formal port release wait arguments differ: $PORT_WAIT_ARGS" >&2
    exit 1
    ;;
esac
if wait_for_formal_host_ports_free 19003 30081 2>/dev/null; then
  echo "formal port release wait accepted the wrong History Server port" >&2
  exit 1
fi
unset BENCH_LOCAL_PORT_RELEASE_TIMEOUT_SECONDS BENCH_LOCAL_PORT_RELEASE_POLL_SECONDS
unset -f python3

python3() {
  if [ "$1" = "$HS/test/benchmark/sweeps/formal_runner_guard.py" ] \
    && [ "$2" = check-host-operators ]; then
    printf '%s\n' "$*" > "$TEST_DIR/host-operator-guard-args"
    command cat > "$TEST_DIR/host-operator-guard-input.json"
    return 0
  fi
  command python3 "$@"
}
require_no_active_host_kuberay_operator
grep -q 'formal_runner_guard.py check-host-operators$' \
  "$TEST_DIR/host-operator-guard-args"
command python3 - "$TEST_DIR/host-operator-guard-input.json" <<'PY'
import json
import sys

document = json.load(open(sys.argv[1], encoding="utf-8"))
if document.get("current-context") != "kind-bench":
    raise SystemExit("host operator guard received the wrong kubeconfig")
PY
TEST_CONFIG_VIEW_FAIL=1
if require_no_active_host_kuberay_operator 2>/dev/null; then
  echo "host operator guard accepted a failed kubeconfig inventory" >&2
  exit 1
fi
unset TEST_CONFIG_VIEW_FAIL
unset -f python3

require_no_reconcilable_kuberay_workloads >/dev/null
grep -q -- '--context kind-bench get rayclusters.ray.io,rayservices.ray.io,rayjobs.ray.io,jobs.batch,pods --all-namespaces -o json' \
  "$TEST_DIR/workload-kubectl-args"
TEST_WORKLOAD_INVENTORY='{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"ray.io/v1","kind":"RayCluster","metadata":{"namespace":"old","name":"stale-ready"},"status":{"state":"ready"}}]}'
if require_no_reconcilable_kuberay_workloads >/dev/null 2>&1; then
  echo "formal workload gate accepted a stale Ready RayCluster with zero pods" >&2
  exit 1
fi
TEST_WORKLOAD_INVENTORY='{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"ray.io/v1","kind":"RayJob","metadata":{"namespace":"old","name":"complete"},"status":{"jobStatus":"SUCCEEDED","jobDeploymentStatus":"Complete"}},{"apiVersion":"batch/v1","kind":"Job","metadata":{"namespace":"old","name":"complete","labels":{"ray.io/originated-from-cr-name":"complete","ray.io/originated-from-crd":"RayJob"}},"status":{"conditions":[{"type":"Complete","status":"True"}]}},{"apiVersion":"v1","kind":"Pod","metadata":{"namespace":"old","name":"complete-submitter","labels":{"ray.io/originated-from-cr-name":"complete","ray.io/originated-from-crd":"RayJob"}},"status":{"phase":"Succeeded"}}]}'
require_no_reconcilable_kuberay_workloads >/dev/null
TEST_WORKLOAD_INVENTORY_FAIL=1
if require_no_reconcilable_kuberay_workloads >/dev/null 2>&1; then
  echo "formal workload gate accepted a failed cluster-wide inventory" >&2
  exit 1
fi
unset TEST_WORKLOAD_INVENTORY TEST_WORKLOAD_INVENTORY_FAIL
TEST_CONTEXT=kind-other
if require_no_reconcilable_kuberay_workloads >/dev/null 2>&1; then
  echo "formal workload gate accepted a different current context" >&2
  exit 1
fi
TEST_CONTEXT=kind-bench

TEST_KUBE_INVENTORY='{"items":[{"kind":"Pod","metadata":{"namespace":"ray","name":"head","labels":{"app.kubernetes.io/created-by":"kuberay-operator"}},"spec":{"containers":[{"name":"ray-head","image":"rayproject/ray:2.56.0"}]},"status":{"phase":"Running"}}]}'
require_no_active_incluster_kuberay_operator
TEST_KUBE_INVENTORY='{"items":[{"kind":"Deployment","metadata":{"namespace":"kuberay","name":"controller","labels":{"app.kubernetes.io/name":"kuberay-operator"}},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"manager","image":"quay.io/kuberay/operator:v1.5.0"}]}}}}]}'
if require_no_active_incluster_kuberay_operator 2>/dev/null; then
  echo "formal operator gate accepted an active in-cluster operator" >&2
  exit 1
fi
TEST_KUBE_INVENTORY='{"items":[]}'
require_no_active_incluster_kuberay_operator
require_formal_kind_target
TEST_CONTEXT=kind-other
if require_formal_kind_target 2>/dev/null; then
  echo "context gate accepted the wrong cluster" >&2
  exit 1
fi
TEST_CONTEXT=kind-bench
BENCH_KIND_NODE=other-control-plane
if require_formal_kind_target 2>/dev/null; then
  echo "node gate accepted the wrong Kind node" >&2
  exit 1
fi
BENCH_KIND_NODE=bench-control-plane

grep -q 'BENCH_TASK_COUNT="$HS_SOURCE_TASK_COUNT"' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'BENCH_HS_SOURCE_OBJECT_COUNT="$HS_SOURCE_OBJECT_COUNT"' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'BENCH_HS_SOURCE_TOTAL_BYTES="$HS_SOURCE_TOTAL_BYTES"' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'BENCH_HS_SOURCE_TASK_LOG_METADATA_SHA256="$HS_SOURCE_TASK_LOG_METADATA_SHA256"' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'BENCH_HS_SOURCE_TASK_LOG_METADATA_ATTEMPTS="$HS_SOURCE_TASK_LOG_METADATA_ATTEMPTS"' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'BENCH_HS_SOURCE_TASK_LOG_METADATA_INCOMPLETE_NON_NIL="$HS_SOURCE_TASK_LOG_METADATA_INCOMPLETE_NON_NIL"' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'BENCH_HS_SOURCE_RAYJOB_BACKOFF_LIMIT="$HS_SOURCE_RAYJOB_BACKOFF_LIMIT"' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'BENCH_HS_SOURCE_SUBMITTER_BACKOFF_LIMIT="$HS_SOURCE_SUBMITTER_BACKOFF_LIMIT"' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'BENCH_HS_SOURCE_ACCEPTED_DIR' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'verify-completion' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'BENCH_HS_SOURCE_ACCEPTED_DIR' "$SCRIPT_DIR/ray256_hs_memory.sh"
grep -q 'verify-completion' "$SCRIPT_DIR/ray256_hs_memory.sh"
grep -q 'CPU/source completion binding' "$SCRIPT_DIR/ray256_hs_memory.sh"
grep -q 'BENCH_HS_MEMORY_EXECUTION_CPU_OVERRIDE' "$SCRIPT_DIR/ray256_hs_memory.sh"
if grep -q 'CPU/source expected matrix binding' "$SCRIPT_DIR/ray256_hs_memory.sh"; then
  echo "memory runner still requires the legacy source matrix to equal the current CPU matrix" >&2
  exit 1
fi
grep -q 'BENCH_S3_LOCAL_PORT="$HS_S3_LOCAL_PORT"' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'BENCH_EXECUTION_IDENTITY_FILE="$arm_dir/execution-namespace.json"' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'wait_for_execution_namespace_deletion "$arm_dir/execution-namespace.json"' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'wait_for_formal_host_ports_free "$HS_S3_LOCAL_PORT" "$HS_API_LOCAL_PORT"' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'require_formal_host_ports_free "$HS_S3_LOCAL_PORT" "$HS_API_LOCAL_PORT"' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'require_no_reconcilable_kuberay_workloads' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'BENCH_SKIP_CLEANUP=1' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q "HS_CLIENT_TIMEOUT='12m'" "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'session-process-timeout=10m' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'session-cache-max-bytes=2147483648' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q 'session-cache-ttl=0s' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q "go test -vet=off ./test/benchmark" "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q '/sbin/sha256sum' "$SCRIPT_DIR/ray256_hs_cpu.sh"
grep -q '/sbin/sha256sum' "$SCRIPT_DIR/sweep_lib.sh"
if grep -Eq '(^|[^[:alnum:]_/])shasum([[:space:]]|$)' \
  "$SCRIPT_DIR/ray256_hs_cpu.sh" "$SCRIPT_DIR/sweep_lib.sh"; then
  echo "formal HS scripts must use /sbin/sha256sum in the Codex sandbox" >&2
  exit 1
fi
grep -q 'write_hs_expected_matrix.py" cpu' "$SCRIPT_DIR/ray256_hs_cpu.sh"
if grep -q 'cp "$source_expected_matrix"' "$SCRIPT_DIR/ray256_hs_cpu.sh"; then
  echo "new HS campaign must not reuse the accepted source bundle's old CPU matrix" >&2
  exit 1
fi
grep -q 'BENCH_TASK_COUNT="$SOURCE_TASK_COUNT"' "$SCRIPT_DIR/ray256_hs_source_5k.sh"
grep -q 'BENCH_WAVE_SIZE="$SOURCE_WAVE_SIZE"' "$SCRIPT_DIR/ray256_hs_source_5k.sh"
grep -q 'BENCH_COMPRESSION=true' "$SCRIPT_DIR/ray256_hs_source_5k.sh"
grep -q 'BENCH_SHUTDOWN_AFTER_JOB=true' "$SCRIPT_DIR/ray256_hs_source_5k.sh"
grep -q 'BENCH_JOB_TTL_SECONDS=30' "$SCRIPT_DIR/ray256_hs_source_5k.sh"
grep -q 'BENCH_DRIVER_DRAIN_SLEEP=0' "$SCRIPT_DIR/ray256_hs_source_5k.sh"
grep -q 'BENCH_S3_LOCAL_PORT="$HS_S3_LOCAL_PORT"' "$SCRIPT_DIR/ray256_hs_source_5k.sh"
grep -q 'BENCH_EXECUTION_IDENTITY_FILE="$OUT/run/execution-namespace.json"' "$SCRIPT_DIR/ray256_hs_source_5k.sh"
grep -q 'wait_for_execution_namespace_deletion "$OUT/run/execution-namespace.json"' "$SCRIPT_DIR/ray256_hs_source_5k.sh"
grep -q 'wait_for_formal_host_ports_free "$HS_S3_LOCAL_PORT" "$HS_API_LOCAL_PORT"' "$SCRIPT_DIR/ray256_hs_source_5k.sh"
grep -q 'require_no_reconcilable_kuberay_workloads' "$SCRIPT_DIR/ray256_hs_source_5k.sh"
grep -q 'require_no_reconcilable_kuberay_workloads' "$SCRIPT_DIR/ray256_hs_memory.sh"
grep -q 'BENCH_SKIP_CLEANUP=1' "$SCRIPT_DIR/ray256_hs_source_5k.sh"
if grep -q 'DeleteS3Bucket' "$SCRIPT_DIR/ray256_hs_source_5k.sh"; then
  echo "formal source generator must not call DeleteS3Bucket" >&2
  exit 1
fi
if grep -R -q 'DeleteS3Bucket' "$HS/test/benchmark" \
	--include='*.go' --exclude-dir=out --exclude-dir=__pycache__; then
  echo "benchmark package must not call the hardcoded shared-e2e DeleteS3Bucket" >&2
  exit 1
fi
if grep -E -q 'kubectl[[:space:]]+delete|Namespaces\(\).*Delete' \
  "$SCRIPT_DIR/sweep_lib.sh" "$SCRIPT_DIR/formal_runner_guard.py"; then
  echo "formal runner namespace wait must never issue deletion" >&2
  exit 1
fi

python3 - "$SCRIPT_DIR/ray256_hs_cpu.sh" "$SCRIPT_DIR/ray256_hs_source_5k.sh" "$SCRIPT_DIR/ray256_hs_memory.sh" <<'PY'
import pathlib
import sys

cpu = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
source = pathlib.Path(sys.argv[2]).read_text(encoding="utf-8")
memory = pathlib.Path(sys.argv[3]).read_text(encoding="utf-8")

for name, runner in (("CPU", cpu), ("source", source), ("memory", memory)):
    main = runner[runner.index("main() {"):]
    campaign_order = (
        "require_formal_kind_target",
        "hs_assert_no_parallel_campaign",
        "require_no_active_host_kuberay_operator",
        "require_no_active_incluster_kuberay_operator",
        "require_no_reconcilable_kuberay_workloads",
        "require_formal_host_ports_free",
    )
    indices = [main.index(fragment) for fragment in campaign_order]
    if indices != sorted(indices):
        raise SystemExit(f"{name} campaign host operator gate order differs")

cpu_arm = cpu[cpu.index("hs_run_arm() {"):cpu.index("\nmain() {")]
cpu_preflight_order = (
    "require_no_active_incluster_kuberay_operator",
    "require_no_reconcilable_kuberay_workloads",
    "require_formal_host_ports_free",
    'mkdir "$arm_dir"',
)
if [cpu_arm.index(fragment) for fragment in cpu_preflight_order] != sorted(
    cpu_arm.index(fragment) for fragment in cpu_preflight_order
):
    raise SystemExit("CPU arm KubeRay workload gate order differs")
cpu_order = (
    'wait_for_execution_namespace_deletion "$arm_dir/execution-namespace.json"',
    'wait_for_formal_host_ports_free "$HS_S3_LOCAL_PORT" "$HS_API_LOCAL_PORT"',
    'if [ "$rc" -ne 0 ]',
)
if [cpu_arm.index(fragment) for fragment in cpu_order] != sorted(
    cpu_arm.index(fragment) for fragment in cpu_order
):
    raise SystemExit("CPU arm namespace/port/semantic gate order differs")

source_main = source[source.index("main() {"):source.index("\n}\n\nif [[", source.index("main() {"))]
source_runtime = source_main[
    source_main.index("source_runner_require_supervisor_healthy PREFLIGHT"):
]
source_runtime_order = (
    "source_runner_require_supervisor_healthy PREFLIGHT",
    'mkdir "$OUT/run"',
    "require_no_active_incluster_kuberay_operator",
    "require_no_reconcilable_kuberay_workloads",
    "require_formal_host_ports_free",
    "go test -vet=off ./test/benchmark",
)
if [source_runtime.index(fragment) for fragment in source_runtime_order] != sorted(
    source_runtime.index(fragment) for fragment in source_runtime_order
):
    raise SystemExit("source runtime KubeRay workload gate order differs")
source_order = (
    'wait_for_execution_namespace_deletion "$OUT/run/execution-namespace.json"',
    'wait_for_formal_host_ports_free "$HS_S3_LOCAL_PORT" "$HS_API_LOCAL_PORT"',
    'source_runner_require_supervisor_healthy SOURCE',
    'if [ "$run_rc" -ne 0 ]',
    'write_hs_expected_matrix.py" source',
    'write_hs_expected_matrix.py" status',
    'write_hs_expected_matrix.py" cpu',
    'write_hs_expected_matrix.py" completion',
    'write_hs_expected_matrix.py" verify-completion',
    'write_hs_expected_matrix.py" publish',
)
if [source_main.index(fragment) for fragment in source_order] != sorted(
    source_main.index(fragment) for fragment in source_order
):
    raise SystemExit("source cleanup/semantic/artifact gate order differs")
if source.count("go test -vet=off ./test/benchmark") != 1:
    raise SystemExit("formal source must have exactly one benchmark execution path")
if source.count('BENCH_WAVE_SIZE="$SOURCE_WAVE_SIZE"') != 1:
    raise SystemExit("formal source runtime wave must have one fixed mapping input")
if not source_main.rstrip().endswith('--accepted-dir "$accepted_dir"'):
    raise SystemExit("source publish is not the final operation in main")
if "mv \"$accepted_stage\"" in source_main:
    raise SystemExit("source publish must use the guarded Python os.rename helper")
PY
for runner in "$SCRIPT_DIR/sweep_lib.sh" "$SCRIPT_DIR/ray256_collector.sh"; do
  if grep -q -- '--allow-legacy-schema' "$runner"; then
    echo "formal Collector runner must not enable legacy matrix schemas: $runner" >&2
    exit 1
  fi
done

for task_count in 1000 5000 10000 50000; do
  planned="$TEST_DIR/planned-n${task_count}.json"
  lineage="$TEST_DIR/lineage-n${task_count}.json"
  selected_out="$TEST_DIR/source-n${task_count}"
  if [ "$task_count" = 50000 ]; then
    expected_wave=2000
    expected_rate=500
    expected_variant=single-driver-rate500-wave2000-v2
    expected_campaign=ray256-hs-source-n50000-rate500-v2
    expected_rejected=7873ec98d6e1d376a5994a86aaaa18ee41b6f5e1a124f67249589c152ad94264
    expected_rejected_attempts=2c191e0133dce4a95a19fe7835daf0628784276fd421de608ed8fd796d3915ef,01274cdc416f6a6709706389042e5aa8e6ab382677b1886b9bf2b8459d47e677
    expected_collector_request=100m
    expected_collector_limit=2
  else
    expected_wave=2000
    expected_rate=0
    expected_variant=baseline-wave2000-v1
    expected_campaign="ray256-hs-source-n${task_count}"
    expected_rejected=none
    expected_rejected_attempts=none
    expected_collector_request=none
    expected_collector_limit=none
  fi
  (
    export BENCH_HS_SOURCE_TASK_COUNT="$task_count"
    export BENCH_SWEEP_OUT="$selected_out"
    # An ambient numeric wave is deliberately poisoned. The formal runner must
    # choose only its predeclared task-count mapping.
    export BENCH_WAVE_SIZE=500
    source "$SCRIPT_DIR/ray256_hs_source_5k.sh"
    [ "$SOURCE_TASK_COUNT" = "$task_count" ]
    [ "$SOURCE_WAVE_SIZE" = "$expected_wave" ]
    [ "$SOURCE_TARGET_TASK_RATE" = "$expected_rate" ]
    [ "$SOURCE_PACING_VARIANT" = "$expected_variant" ]
    [ "$SOURCE_CAMPAIGN_NAME" = "$expected_campaign" ]
    [ "$SOURCE_REJECTED_BASELINE_REPORT_SHA256" = "$expected_rejected" ]
    [ "$SOURCE_REJECTED_ATTEMPT_REPORT_SHA256S" = "$expected_rejected_attempts" ]
    [ "${SOURCE_COLLECTOR_CPU_REQUEST:-none}" = "$expected_collector_request" ]
    [ "${SOURCE_COLLECTOR_CPU_LIMIT:-none}" = "$expected_collector_limit" ]
    [ "$OUT" = "$selected_out" ]
    write_source_lineage "$lineage"
    write_source_planned_config "$planned"
  )
  python3 - "$planned" "$lineage" "$task_count" "$expected_wave" \
    "$expected_rate" "$expected_variant" "$expected_rejected" \
    "$expected_rejected_attempts" "$expected_collector_request" \
    "$expected_collector_limit" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    config = json.load(stream)
with open(sys.argv[2], encoding="utf-8") as stream:
    lineage = json.load(stream)
expected = int(sys.argv[3])
expected_wave = int(sys.argv[4])
expected_rate = int(sys.argv[5])
expected_variant = sys.argv[6]
expected_rejected = None if sys.argv[7] == "none" else sys.argv[7]
expected_rejected_attempts = (
    [] if sys.argv[8] == "none" else sys.argv[8].split(",")
)
expected_collector_request = None if sys.argv[9] == "none" else sys.argv[9]
expected_collector_limit = None if sys.argv[10] == "none" else sys.argv[10]
if config["TaskCount"] != expected:
    raise SystemExit(f"TaskCount={config['TaskCount']}, want {expected}")
if config["WaveSize"] != expected_wave:
    raise SystemExit(f"WaveSize={config['WaveSize']}, want {expected_wave}")
if config["PacingVariant"] != expected_variant:
    raise SystemExit("planned config pacing variant differs")
if config["RejectedBaselineReportSHA256"] != expected_rejected:
    raise SystemExit("planned config rejected baseline differs")
if config["RejectedAttemptReportSHA256s"] != expected_rejected_attempts:
    raise SystemExit("planned config rejected attempts differ")
if config["TargetTaskRate"] != expected_rate:
    raise SystemExit("planned config target rate differs")
if config["CollectorCPURequest"] != expected_collector_request:
    raise SystemExit("planned config Collector request differs")
if config["CollectorCPULimit"] != expected_collector_limit:
    raise SystemExit("planned config Collector limit differs")
if config["LineageFile"] != "lineage.json":
    raise SystemExit("planned config lineage file differs")
if config["RayJobBackoffLimit"] != 0 or config["SubmitterBackoffLimit"] != 0:
    raise SystemExit("planned config retry controls differ")
if config["SkipCleanup"] is not True:
    raise SystemExit("formal source planned config must keep SkipCleanup=true")
if config["SkipHistoryServer"] is not True:
    raise SystemExit("formal source planned config must skip History Server")
if config["S3Bucket"] != "ray-historyserver-benchmark":
    raise SystemExit("formal source planned config must use the dedicated benchmark bucket")
if lineage.get("kind") != "hs-source-generation-lineage":
    raise SystemExit("source lineage kind differs")
current = lineage.get("current")
if current != {
    "taskCount": expected,
    "waveSize": expected_wave,
    "drivers": 1,
    "targetTaskRate": expected_rate,
    "pacingVariant": expected_variant,
    "collectorCPURequest": expected_collector_request,
    "collectorCPULimit": expected_collector_limit,
    "rayJobBackoffLimit": 0,
    "submitterBackoffLimit": 0,
}:
    raise SystemExit(f"source lineage current differs: {current!r}")
rejected = lineage.get("rejectedPredecessor")
if expected_rejected is None:
    if rejected is not None:
        raise SystemExit("lower-N lineage must not invent a rejected predecessor")
else:
    if rejected != {
        "taskCount": 50000,
        "waveSize": 2000,
        "reportSHA256": expected_rejected,
        "verdict": "rejected-incomplete-source",
    }:
        raise SystemExit(f"N=50k rejected predecessor differs: {rejected!r}")
expected_attempt_lineage = [
    {
        "taskCount": 50000,
        "waveSize": 100,
        "targetTaskRate": 0,
        "reportSHA256": digest,
        "verdict": "rejected-incomplete-source",
    }
    for digest in expected_rejected_attempts
]
if lineage.get("rejectedAttempts") != expected_attempt_lineage:
    raise SystemExit("source rejected-attempt lineage differs")
PY
done

make_source_contract_fixture() {
  local task_count="$1"
  python3 - "$TEST_DIR" "$SCRIPT_DIR" "$task_count" <<'PY'
import pathlib
import sys

sys.path.insert(0, sys.argv[2])
import validate_hs_sweep_test

report = validate_hs_sweep_test.source_report(
    pathlib.Path(sys.argv[1]).resolve(),
    int(sys.argv[3]),
)
print(report.parent)
PY
}

n10_source_bundle=$(make_source_contract_fixture 10000)
n50_source_bundle=$(make_source_contract_fixture 50000)
source "$SCRIPT_DIR/ray256_hs_source_5k.sh"

assert_source_contract_runtime_values() {
  local contract="$1"
  local expected_tasks="$2"
  local expected_wave="$3"
  local expected_target="$4"
  local expected_variant="$5"
  local expected_rejected="$6"
  local values kind spec tasks bucket wave drivers target variant driver_tasks
  local wall rate lineage rejected rayjob_retry submitter_retry end
  values=$(source_contract_runtime_values "$contract")
  IFS=$'\t' read -r kind spec tasks bucket wave drivers target variant \
    driver_tasks wall rate lineage rejected rayjob_retry submitter_retry end \
    <<<"$values"
  [ "$end" = SOURCE-CONTRACT-END ]
  [ "$kind" = hs-source ]
  [ "$tasks" = "$expected_tasks" ]
  [ "$bucket" = ray-historyserver-benchmark ]
  [ "$wave" = "$expected_wave" ]
  [ "$drivers" = 1 ]
  [ "$target" = "$expected_target" ]
  [ "$variant" = "$expected_variant" ]
  [ "$driver_tasks" = "$expected_tasks" ]
  [ "$rejected" = "$expected_rejected" ]
  [ "$rayjob_retry" = 0 ]
  [ "$submitter_retry" = 0 ]
  [[ "$spec" == */*/* ]]
  [[ "$wall" =~ ^[0-9]+([.][0-9]+)?$ ]]
  [[ "$rate" =~ ^[0-9]+([.][0-9]+)?$ ]]
  [[ "$lineage" =~ ^[0-9a-f]{64}$ ]]
}

assert_source_contract_runtime_values \
  "$n10_source_bundle/source-contract.json" \
  10000 2000 0 baseline-wave2000-v1 none
assert_source_contract_runtime_values \
  "$n50_source_bundle/source-contract.json" \
  50000 2000 500 single-driver-rate500-wave2000-v2 \
  7873ec98d6e1d376a5994a86aaaa18ee41b6f5e1a124f67249589c152ad94264

for mutation in malformed float-schema bool-schema missing extra \
  float-attempts bool-retry; do
  invalid_contract="$TEST_DIR/source-contract-$mutation.json"
  python3 - "$n50_source_bundle/source-contract.json" \
    "$invalid_contract" "$mutation" <<'PY'
import json
import pathlib
import sys

output = pathlib.Path(sys.argv[2])
if sys.argv[3] == "malformed":
    output.write_text("{", encoding="utf-8")
    raise SystemExit
with open(sys.argv[1], encoding="utf-8") as stream:
    document = json.load(stream)
if sys.argv[3] == "float-schema":
    document["schemaVersion"] = 6.0
elif sys.argv[3] == "bool-schema":
    document["schemaVersion"] = True
elif sys.argv[3] == "missing":
    document["source"].pop("sourceGeneration")
elif sys.argv[3] == "extra":
    document["source"]["unexpected"] = 1
elif sys.argv[3] == "float-attempts":
    document["source"]["expectedBenchmarkAttempts"] = 50000.0
elif sys.argv[3] == "bool-retry":
    document["source"]["rayJobLifecycle"]["rayJobBackoffLimit"] = False
output.write_text(json.dumps(document), encoding="utf-8")
PY
  if source_contract_runtime_values "$invalid_contract" >/dev/null 2>&1; then
    echo "source runtime parser accepted contract mutation: $mutation" >&2
    exit 1
  fi
done

# Exercise the exact revalidation consumer with all external provenance probes
# mocked. This proves the 15 values plus the sentinel retain their positions
# under set -u before the terminal provenance is written.
(
SOURCE_TASK_COUNT=50000
SOURCE_WAVE_SIZE=2000
SOURCE_TARGET_TASK_RATE=500
SOURCE_PACING_VARIANT=single-driver-rate500-wave2000-v2
SOURCE_REJECTED_BASELINE_REPORT_SHA256=7873ec98d6e1d376a5994a86aaaa18ee41b6f5e1a124f67249589c152ad94264
SOURCE_REJECTED_ATTEMPT_REPORT_SHA256S=2c191e0133dce4a95a19fe7835daf0628784276fd421de608ed8fd796d3915ef,01274cdc416f6a6709706389042e5aa8e6ab382677b1886b9bf2b8459d47e677
SOURCE_COLLECTOR_CPU_REQUEST=100m
SOURCE_COLLECTOR_CPU_LIMIT=2
SOURCE_CAMPAIGN_NAME=ray256-hs-source-n50000-rate500-v2
OUT="$n50_source_bundle"
OPERATOR_BIN=/bin/sh
require_source_provenance_unchanged() { return 0; }
source_runtime_values() {
  printf 'sha256:%064d\t<none>\t2.56.0\t%040d\tcollector:latest\tsha256:%064d\t%064d\n' \
    0 0 0 0
}
image_build_source_sha256() { printf '%064d\n' 0; }
revalidate_source_provenance \
  "$n50_source_bundle/source-report.json" "$n50_source_bundle"
n50_runtime_values=$(source_contract_runtime_values \
  "$n50_source_bundle/source-contract.json")
IFS=$'\t' read -r _ _ _ _ _ _ _ _ _ expected_wall expected_rate \
  expected_lineage expected_rejected _ _ expected_end <<<"$n50_runtime_values"
[ "$expected_end" = SOURCE-CONTRACT-END ]
[ "$(provenance_value "$n50_source_bundle/provenance-final.txt" \
  source_driver_wall_sec)" = "$expected_wall" ]
[ "$(provenance_value "$n50_source_bundle/provenance-final.txt" \
  source_driver_rate_tps)" = "$expected_rate" ]
[ "$(provenance_value "$n50_source_bundle/provenance-final.txt" \
  source_lineage_sha256)" = "$expected_lineage" ]
[ "$(provenance_value "$n50_source_bundle/provenance-final.txt" \
  source_rejected_baseline_report_sha256)" = "$expected_rejected" ]
[ "$(provenance_value "$n50_source_bundle/provenance-final.txt" \
  source_rayjob_backoff_limit)" = 0 ]
[ "$(provenance_value "$n50_source_bundle/provenance-final.txt" \
  source_submitter_backoff_limit)" = 0 ]
)

(
  unset BENCH_HS_SOURCE_TASK_COUNT BENCH_SWEEP_OUT
  source "$SCRIPT_DIR/ray256_hs_source_5k.sh"
  [ "$SOURCE_TASK_COUNT" = "5000" ]
  [ "$SOURCE_CAMPAIGN_NAME" = "ray256-hs-source-n5000" ]
  case "$OUT" in
    */ray256-hs-source-n5000-*) ;;
    *)
      echo "default source output name is not the compatible 5k name: $OUT" >&2
      exit 1
      ;;
  esac
)

for invalid_count in 0 999 100000 nope; do
  if (
    export BENCH_HS_SOURCE_TASK_COUNT="$invalid_count"
    source "$SCRIPT_DIR/ray256_hs_source_5k.sh"
  ) >/dev/null 2>&1; then
    echo "source generator accepted invalid task count: $invalid_count" >&2
    exit 1
  fi
done

for script in ray256_hs_cpu.sh ray256_hs_memory.sh ray256_hs_source_5k.sh; do
  bash -n "$SCRIPT_DIR/$script"
done

# A matching image may appear near the start of a large CRI inventory.  The
# helper must consume the Docker command fully instead of closing its pipe early
# and turning a successful lookup into a producer SIGPIPE under pipefail.
mock_runtime_id="sha256:$(printf 'a%.0s' {1..64})"
mock_runtime_commit=$(printf 'b%.0s' {1..40})
MOCK_RUNTIME_MODE=valid
docker() {
  [ "$1" = exec ] && [ "$2" = mock-kind-node ] && [ "$3" = crictl ] || return 64
  case "$4" in
    images)
      [ "$5" = --digests ] || return 65
      [ "$MOCK_RUNTIME_MODE" != inventory-error ] || return 66
      printf 'IMAGE TAG DIGEST IMAGE-ID SIZE\n'
      if [ "$MOCK_RUNTIME_MODE" != missing ]; then
        printf 'docker.io/rayproject/ray 2.56.0 <none> aaaaaaaaaaaaa 1GB\n'
      fi
      local index
      for ((index = 0; index < 20000; index++)); do
        printf 'docker.io/example/filler-%d latest <none> deadbeef%05d 1MB\n' \
          "$index" "$index"
      done
      ;;
    inspecti)
      [ "$5" = aaaaaaaaaaaaa ] || return 67
      [ "$MOCK_RUNTIME_MODE" != inspect-error ] || return 68
      printf '{"status":{"id":"%s","repoDigests":[]},"info":{"imageSpec":{"config":{"Labels":{"io.ray.ray-version":"2.56.0","io.ray.ray-commit":"%s"}}}}}\n' \
        "$mock_runtime_id" "$mock_runtime_commit"
      ;;
    *) return 69 ;;
  esac
}
runtime_metadata=$(runtime_image_metadata mock-kind-node rayproject/ray 2.56.0)
IFS=$'\t' read -r runtime_id runtime_digests runtime_version runtime_commit runtime_source \
  <<<"$runtime_metadata"
[ "$runtime_id" = "$mock_runtime_id" ]
[ "$runtime_digests" = '<none>' ]
[ "$runtime_version" = 2.56.0 ]
[ "$runtime_commit" = "$mock_runtime_commit" ]
[ "$runtime_source" = '<none>' ]
for MOCK_RUNTIME_MODE in missing inventory-error inspect-error; do
  if runtime_image_metadata mock-kind-node rayproject/ray 2.56.0 >/dev/null 2>&1; then
    echo "runtime image metadata accepted mock failure mode: $MOCK_RUNTIME_MODE" >&2
    exit 1
  fi
done
unset -f docker

# EXIT cleanup state must outlive main. It must stop and reap only a matching
# active child, remove only its owned lock, and preserve every original status.
source "$SCRIPT_DIR/ray256_hs_source_5k.sh"
unrelated_pid=""
sleep 300 &
unrelated_pid=$!
cleanup_test_processes() {
  [ -z "$unrelated_pid" ] || kill "$unrelated_pid" 2>/dev/null || true
  [ -z "$unrelated_pid" ] || wait "$unrelated_pid" 2>/dev/null || true
}
trap cleanup_test_processes EXIT

run_source_cleanup_case() {
  local name="$1"
  local wanted_rc="$2"
  local replace_lock="${3:-false}"
  local expected_rc="${4:-$wanted_rc}"
  local case_dir="$TEST_DIR/source-cleanup-$name"
  mkdir "$case_dir"
  set +e
  (
    SOURCE_RUNNER_SHELL_PID=""
    SOURCE_RUNNER_SUPERVISOR_PID=""
    SOURCE_RUNNER_SUPERVISOR_IDENTITY=""
    SOURCE_RUNNER_LOCK_DIR="$case_dir/lock"
    trap source_runner_cleanup EXIT
    source_runner_acquire_lock "$SOURCE_RUNNER_LOCK_DIR"
    source_runner_supervise_operator \
      "$case_dir/operator-exit-status.txt" "$case_dir/operator.log" \
      /bin/sh -c 'exec sleep 300' &
    SOURCE_RUNNER_SUPERVISOR_PID=$!
    SOURCE_RUNNER_SHELL_PID=$$
    SOURCE_RUNNER_SUPERVISOR_IDENTITY=$(source_runner_process_identity \
      "$SOURCE_RUNNER_SUPERVISOR_PID")
    printf '%s\n' "$SOURCE_RUNNER_SUPERVISOR_PID" > "$case_dir/supervisor.pid"
    if [ "$replace_lock" = true ]; then
      rmdir "$SOURCE_RUNNER_LOCK_OWNER_DIR"
      rmdir "$SOURCE_RUNNER_LOCK_DIR"
      mkdir "$SOURCE_RUNNER_LOCK_DIR"
      mkdir "$SOURCE_RUNNER_LOCK_DIR/.owner-foreign-token"
    fi
    exit "$wanted_rc"
  )
  local actual_rc=$?
  set -e
  [ "$actual_rc" -eq "$expected_rc" ] || {
    echo "source cleanup exit status mismatch: got=$actual_rc want=$expected_rc" >&2
    return 1
  }
  local stopped_pid
  stopped_pid=$(<"$case_dir/supervisor.pid")
  if kill -0 "$stopped_pid" 2>/dev/null; then
    echo "source cleanup left its matching active supervisor running: $stopped_pid" >&2
    return 1
  fi
  if [ "$replace_lock" = true ]; then
    [ -d "$case_dir/lock/.owner-foreign-token" ] || {
      echo "source cleanup removed a foreign lock owner" >&2
      return 1
    }
    rmdir "$case_dir/lock/.owner-foreign-token"
    rmdir "$case_dir/lock"
  else
    [ ! -e "$case_dir/lock" ] || {
      echo "source cleanup left its owned campaign lock: $case_dir/lock" >&2
      return 1
    }
  fi
  kill -0 "$unrelated_pid" 2>/dev/null || {
    echo "source cleanup stopped an unrelated process" >&2
    return 1
  }
}
run_source_cleanup_case normal-return 0
run_source_cleanup_case early-failure 23
run_source_cleanup_case foreign-lock-success-status 0 true 70
run_source_cleanup_case foreign-lock-existing-failure 31 true 31

# Normal cleanup must TERM the live supervisor, which forwards TERM to its exact
# active operator child and waits for that child before the supervisor exits.
operator_live_dir="$TEST_DIR/source-cleanup-operator-live"
mkdir "$operator_live_dir"
set +e
(
  SOURCE_RUNNER_SHELL_PID=""
  SOURCE_RUNNER_SUPERVISOR_PID=""
  SOURCE_RUNNER_SUPERVISOR_IDENTITY=""
  SOURCE_RUNNER_LOCK_DIR="$operator_live_dir/lock"
  trap source_runner_cleanup EXIT
  source_runner_acquire_lock "$SOURCE_RUNNER_LOCK_DIR"
  source_runner_supervise_operator \
    "$operator_live_dir/operator-exit-status.txt" \
    "$operator_live_dir/operator.log" \
    /bin/sh -c 'printf "%s\n" "$$" > "$1"; exec sleep 300' \
    operator-mock "$operator_live_dir/operator.pid" &
  SOURCE_RUNNER_SUPERVISOR_PID=$!
  SOURCE_RUNNER_SHELL_PID=$$
  SOURCE_RUNNER_SUPERVISOR_IDENTITY=$(source_runner_process_identity \
    "$SOURCE_RUNNER_SUPERVISOR_PID")
  printf '%s\n' "$SOURCE_RUNNER_SUPERVISOR_PID" > "$operator_live_dir/supervisor.pid"
  for _ in {1..100}; do
    [ ! -e "$operator_live_dir/operator.pid" ] || break
    sleep 0.01
  done
  [ -s "$operator_live_dir/operator.pid" ]
  source_runner_pid_is_active_job "$SOURCE_RUNNER_SUPERVISOR_PID"
  exit 0
)
operator_live_rc=$?
set -e
[ "$operator_live_rc" -eq 0 ]
operator_live_supervisor=$(<"$operator_live_dir/supervisor.pid")
operator_live_child=$(<"$operator_live_dir/operator.pid")
if kill -0 "$operator_live_supervisor" 2>/dev/null \
  || kill -0 "$operator_live_child" 2>/dev/null; then
  echo "normal cleanup left the supervisor or its operator child running" >&2
  exit 1
fi
[ ! -e "$operator_live_dir/lock" ]

# An operator may exit before main. Its supervisor must record that status but
# remain an owned active job until EXIT cleanup stops and reaps the supervisor.
operator_early_dir="$TEST_DIR/source-cleanup-operator-early"
mkdir "$operator_early_dir"
set +e
(
  SOURCE_RUNNER_SHELL_PID=""
  SOURCE_RUNNER_SUPERVISOR_PID=""
  SOURCE_RUNNER_SUPERVISOR_IDENTITY=""
  SOURCE_RUNNER_LOCK_DIR="$operator_early_dir/lock"
  trap source_runner_cleanup EXIT
  source_runner_acquire_lock "$SOURCE_RUNNER_LOCK_DIR"
  source_runner_supervise_operator \
    "$operator_early_dir/operator-exit-status.txt" \
    "$operator_early_dir/operator.log" \
    /bin/sh -c 'exit 17' &
  SOURCE_RUNNER_SUPERVISOR_PID=$!
  SOURCE_RUNNER_SHELL_PID=$$
  SOURCE_RUNNER_SUPERVISOR_IDENTITY=$(source_runner_process_identity \
    "$SOURCE_RUNNER_SUPERVISOR_PID")
  printf '%s\n' "$SOURCE_RUNNER_SUPERVISOR_PID" > "$operator_early_dir/supervisor.pid"
  for _ in {1..100}; do
    [ ! -e "$operator_early_dir/operator-exit-status.txt" ] || break
    sleep 0.01
  done
  [ "$(<"$operator_early_dir/operator-exit-status.txt")" = 17 ]
  source_runner_pid_is_active_job "$SOURCE_RUNNER_SUPERVISOR_PID"
  source_runner_require_operator_running \
    "$operator_early_dir/operator-exit-status.txt" SOURCE >/dev/null 2>&1
  exit $?
)
operator_early_rc=$?
set -e
[ "$operator_early_rc" -eq 1 ]
operator_early_supervisor=$(<"$operator_early_dir/supervisor.pid")
if kill -0 "$operator_early_supervisor" 2>/dev/null; then
  echo "operator-early supervisor survived its exact EXIT cleanup" >&2
  exit 1
fi
[ ! -e "$operator_early_dir/lock" ]

# A reused numeric PID is not this shell's active job and its spawn identity no
# longer matches. Cleanup must never signal that unrelated live process.
pid_reuse_dir="$TEST_DIR/source-cleanup-pid-reuse"
mkdir "$pid_reuse_dir"
set +e
(
  SOURCE_RUNNER_SHELL_PID='stale-parent-pid'
  SOURCE_RUNNER_SUPERVISOR_PID="$unrelated_pid"
  SOURCE_RUNNER_SUPERVISOR_IDENTITY='stale-spawn-identity'
  SOURCE_RUNNER_LOCK_DIR="$pid_reuse_dir/lock"
  trap source_runner_cleanup EXIT
  source_runner_acquire_lock "$SOURCE_RUNNER_LOCK_DIR"
  exit 0
)
pid_reuse_rc=$?
set -e
[ "$pid_reuse_rc" -eq 70 ]
kill -0 "$unrelated_pid" 2>/dev/null || {
  echo "source cleanup signaled a simulated PID-reuse process" >&2
  exit 1
}
[ ! -e "$pid_reuse_dir/lock" ]

cleanup_test_processes
unrelated_pid=""
trap - EXIT

echo "HS-MULTI-N-CONTRACT-TEST-PASSED temp=$TEST_DIR"
