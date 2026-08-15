#!/usr/bin/env bash
# Formal Collector memory campaign: A/B boundedness and C stop-first-pass cap.
#
# A: 90s ingest + 15s observation, 1k/2k/3k/5k events/s, n=3.
# B: 30s ingest + 60s idle, 2k negative control and 5k size rotation, n=3.
# C: 90s ingest + 15s observation at 5k events/s, 192/256/512Mi,
#    n=3 per candidate; stop after the first candidate passes all three runs.
#
# Production rotation controls remain 5m/100MiB/30s.  max-disk is deliberately
# 1024MiB so disk-pressure backpressure cannot masquerade as memory boundedness.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HS=$(cd "$SCRIPT_DIR/../../.." && pwd)
REPO_ROOT=$(git -C "$HS" rev-parse --show-toplevel)
BENCH_KIND_NODE=${BENCH_KIND_NODE:-bench-control-plane}
BENCH_SCRATCH=${BENCH_SCRATCH:-${TMPDIR:-/tmp}/kuberay-benchmark}
OUT=${BENCH_SWEEP_OUT:-$HS/test/benchmark/out/collector-memory-formal-$(date +%Y%m%d-%H%M%S)}
OPERATOR_BIN=$OUT/kuberay-operator
RAY_IMAGE=rayproject/ray:2.56.0
COLLECTOR_IMAGE=collector:v0.1.0
S3_LOCAL_PORT=19004
LOCK_DIR=$BENCH_SCRATCH/hs-formal-campaign.lock
LOCK_TOKEN="$$-$(date +%s)"
OP=''
SMOKE_ARM=${BENCH_COLLECTOR_MEMORY_SMOKE_ARM:-}
if [ -n "$SMOKE_ARM" ] && [ "$SMOKE_ARM" != B-rate5000-r1 ]; then
  echo "PREFLIGHT-FAILED: BENCH_COLLECTOR_MEMORY_SMOKE_ARM must be empty or B-rate5000-r1" >&2
  exit 1
fi

source "$SCRIPT_DIR/sweep_lib.sh"

wait_for_collector_port_free() {
  python3 - "$S3_LOCAL_PORT" <<'PY'
import socket
import sys
import time

port = int(sys.argv[1])
if port != 19004:
    raise SystemExit(f"PREFLIGHT-FAILED: Collector S3 port must be 19004, got {port}")
addresses = []
for family, _, _, _, address in socket.getaddrinfo("localhost", port, type=socket.SOCK_STREAM):
    key = (family, address)
    if key not in addresses:
        addresses.append(key)
deadline = time.monotonic() + 30.0
consecutive = 0
last_error = "not checked"
while True:
    free = True
    for family, address in addresses:
        probe = socket.socket(family, socket.SOCK_STREAM)
        try:
            # A dead kubectl port-forward can leave client connections in
            # TIME_WAIT on Darwin. SO_REUSEADDR lets this ownership probe
            # ignore those closed connections, while bind+listen still fails
            # for an active or merely bound port owner. Do not use
            # enable port sharing: two live listeners must never share it.
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if family == socket.AF_INET6:
                probe.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            probe.bind(address)
            probe.listen(1)
        except OSError as error:
            free = False
            last_error = str(error)
        finally:
            probe.close()
    consecutive = consecutive + 1 if free else 0
    if consecutive >= 2:
        print(f"COLLECTOR-PORT-FREE port={port} consecutive={consecutive}")
        break
    if time.monotonic() >= deadline:
        raise SystemExit(
            f"PREFLIGHT-FAILED: timed out waiting 30s for localhost:{port}: {last_error}"
        )
    time.sleep(0.25)
PY
}

export KUBECONFIG=${BENCH_KUBECONFIG:-${KUBECONFIG:-$BENCH_SCRATCH/kubeconfig-bench}}
export GOMODCACHE=${GOMODCACHE:-$BENCH_SCRATCH/gomodcache}
export GOCACHE=${GOCACHE:-$BENCH_SCRATCH/gocache}

sha256_file() {
  LC_ALL=C /sbin/sha256sum "$1" | awk '{print $1}'
}

tracked_diff_sha256_macos() {
  (set -o pipefail; git -C "$REPO_ROOT" diff --binary HEAD -- historyserver ray-operator \
    | LC_ALL=C /sbin/sha256sum | awk '{print $1}')
}

cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  if [ -n "$OP" ] && kill -0 "$OP" 2>/dev/null; then
    kill "$OP" 2>/dev/null || true
    wait "$OP" 2>/dev/null || true
  fi
  if [ -d "$LOCK_DIR/.owner-$LOCK_TOKEN" ]; then
    rmdir "$LOCK_DIR/.owner-$LOCK_TOKEN" 2>/dev/null || true
    rmdir "$LOCK_DIR" 2>/dev/null || true
  fi
  exit "$rc"
}
trap cleanup EXIT INT TERM

acquire_lock() {
  mkdir -p "$BENCH_SCRATCH"
  if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "PREFLIGHT-FAILED: formal campaign lock exists: $LOCK_DIR" >&2
    return 1
  fi
  mkdir "$LOCK_DIR/.owner-$LOCK_TOKEN"
}

write_provenance() {
  local destination="$1" revalidation="${2:-}"
  local ray_meta collector_meta
  local ray_id ray_digests ray_version ray_commit unused
  local collector_id collector_digests collector_version collector_commit collector_source
  local expected_source repo_head tracked benchmark operator matrix runner generator validator
  [ ! -e "$destination" ] || { echo "PROVENANCE-FAILED: refusing overwrite $destination" >&2; return 1; }
  ray_meta=$(runtime_image_metadata "$BENCH_KIND_NODE" rayproject/ray 2.56.0)
  collector_meta=$(runtime_image_metadata "$BENCH_KIND_NODE" collector v0.1.0)
  IFS=$'\t' read -r ray_id ray_digests ray_version ray_commit unused <<<"$ray_meta"
  IFS=$'\t' read -r collector_id collector_digests collector_version collector_commit collector_source <<<"$collector_meta"
  expected_source=$(image_build_source_sha256 collector)
  [ "$collector_source" = "$expected_source" ] || {
    echo "PROVENANCE-FAILED: Collector runtime image was not built from this checkout" >&2
    return 1
  }
  [ "$ray_version" = 2.56.0 ] || {
    echo "PROVENANCE-FAILED: Ray runtime version=$ray_version, want 2.56.0" >&2
    return 1
  }
  repo_head=$(git -C "$REPO_ROOT" rev-parse HEAD)
  tracked=$(tracked_diff_sha256_macos)
  benchmark=$(benchmark_tree_sha256 "$HS/test/benchmark")
  operator=$(sha256_file "$OPERATOR_BIN")
  matrix=$(sha256_file "$OUT/expected-matrix.json")
  runner=$(sha256_file "$SCRIPT_DIR/ray256_collector_memory.sh")
  generator=$(sha256_file "$SCRIPT_DIR/collector_event_replay.py")
  validator=$(sha256_file "$SCRIPT_DIR/validate_collector_memory.py")
  {
    printf 'repo_head=%s\n' "$repo_head"
    printf 'tracked_diff_sha256=%s\n' "$tracked"
    printf 'benchmark_source_sha256=%s\n' "$benchmark"
    printf 'operator_sha256=%s\n' "$operator"
    printf 'expected_matrix_sha256=%s\n' "$matrix"
    printf 'runner_sha256=%s\n' "$runner"
    printf 'generator_sha256=%s\n' "$generator"
    printf 'validator_sha256=%s\n' "$validator"
    printf 'ray_image_requested=%s\n' "$RAY_IMAGE"
    printf 'ray_runtime_id=%s\n' "$ray_id"
    printf 'ray_runtime_version=%s\n' "$ray_version"
    printf 'ray_runtime_commit=%s\n' "$ray_commit"
    printf 'collector_image_requested=%s\n' "$COLLECTOR_IMAGE"
    printf 'collector_build_source_sha256=%s\n' "$expected_source"
    printf 'collector_runtime_id=%s\n' "$collector_id"
    printf 'collector_runtime_build_source_sha256=%s\n' "$collector_source"
    [ -z "$revalidation" ] || printf 'revalidation_status=%s\n' "$revalidation"
  } > "$destination"
}

BENCH_ENV_UNSET=(-u __KUBERAY_BENCH_ENV_SENTINEL__)
while IFS= read -r name; do
  case "$name" in BENCH_*) BENCH_ENV_UNSET+=(-u "$name") ;; esac
done < <(compgen -e)

[ ! -e "$OUT" ] || { echo "PREFLIGHT-FAILED: refusing to overwrite $OUT" >&2; exit 1; }
require_local_tcp_port_free "$S3_LOCAL_PORT"
require_local_tcp_port_free 8083
require_local_tcp_port_free 8085
require_kubeconfig_files
require_formal_kind_target
hs_assert_no_parallel_campaign
require_no_active_host_kuberay_operator
require_no_active_incluster_kuberay_operator
require_no_reconcilable_kuberay_workloads
acquire_lock
mkdir -p "$OUT" "$GOMODCACHE" "$GOCACHE"
python3 "$SCRIPT_DIR/write_collector_memory_matrix.py" --output "$OUT/expected-matrix.json"
(cd "$REPO_ROOT/ray-operator" && go build -o "$OPERATOR_BIN" .)
write_provenance "$OUT/provenance.txt"
python3 "$SCRIPT_DIR/validate_collector_memory.py" "$OUT" --provenance-only
MATRIX_SHA=$(sha256_file "$OUT/expected-matrix.json")

"$OPERATOR_BIN" --metrics-addr=:8083 --health-probe-bind-address=:8085 \
  --enable-leader-election=false --use-kubernetes-proxy > "$OUT/operator.log" 2>&1 &
OP=$!
sleep 6
kill -0 "$OP" 2>/dev/null || { tail -20 "$OUT/operator.log" >&2; exit 1; }

arm_tsv() {
  python3 - "$OUT/expected-matrix.json" "$1" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    matrix = json.load(stream)
matches = [arm for arm in matrix["arms"] if arm["name"] == sys.argv[2]]
if len(matches) != 1:
    raise SystemExit(f"arm lookup count={len(matches)}")
arm = matches[0]
cfg = arm["config"]
print("\t".join(str(value) for value in (
    cfg["experiment"], cfg["targetEventsPerSecond"], arm["repeat"],
    cfg["ingestSeconds"], cfg["idleSeconds"], cfg["cpuRequest"],
    cfg["cpuLimit"], cfg["memoryRequest"], cfg["memoryLimit"],
)))
PY
}

run_one() {
  local name="$1" values experiment rate repeat ingest idle cpu_request cpu_limit memory_request memory_limit kind start rc
  values=$(arm_tsv "$name")
  IFS=$'\t' read -r experiment rate repeat ingest idle cpu_request cpu_limit memory_request memory_limit <<<"$values"
  case "$experiment" in A) kind=continuous ;; B) kind=ingest-idle ;; C) kind=limit ;; *) return 1 ;; esac
  kill -0 "$OP" 2>/dev/null || { echo "OPERATOR-DEAD before $name" >&2; return 1; }
  wait_for_collector_port_free
  start=$(date +%s)
  set +e
  (cd "$HS" && env "${BENCH_ENV_UNSET[@]}" \
    BENCH_COLLECTOR_MEMORY_RUN=1 BENCH_COLLECTOR_MEMORY_ARM="$name" \
    BENCH_COLLECTOR_MEMORY_MATRIX_SHA256="$MATRIX_SHA" \
    BENCH_COLLECTOR_MEMORY_KIND="$kind" BENCH_COLLECTOR_EVENT_RATE="$rate" \
    BENCH_COLLECTOR_INGEST_DURATION="${ingest}s" BENCH_COLLECTOR_IDLE_DURATION="${idle}s" \
    BENCH_COLLECTOR_MEMORY_REPEAT="$repeat" BENCH_COLLECTOR_CPU_REQUEST="$cpu_request" \
    BENCH_COLLECTOR_CPU_LIMIT="$cpu_limit" BENCH_COLLECTOR_MEMORY_REQUEST="$memory_request" \
    BENCH_COLLECTOR_MEMORY_LIMIT="$memory_limit" BENCH_KIND_NODE="$BENCH_KIND_NODE" \
    BENCH_OUT_DIR="$OUT/$name" \
    go test -vet=off ./test/benchmark -run 'TestCollectorMemoryBenchmark$' -count=1 -v -timeout 30m \
    > "$OUT/$name.log" 2>&1)
  rc=$?
  set -e
  printf '%s rc=%s duration=%ss\n' "$name" "$rc" "$(( $(date +%s) - start ))" | tee -a "$OUT/status.txt"
  [ "$rc" -eq 0 ] || { echo "ARM-FAILED $name" >&2; return "$rc"; }
  python3 "$SCRIPT_DIR/validate_collector_memory.py" "$OUT" --single-arm "$name"
}

classify_failed_candidate_arm() {
  local arm="$1" go_rc="$2" classification_rc
  set +e
  python3 "$SCRIPT_DIR/validate_collector_memory.py" "$OUT" \
    --classify-candidate-arm "$arm" --go-test-rc "$go_rc" \
    --verdict-output "$OUT/candidate-arm-verdicts/${arm}.json"
  classification_rc=$?
  set -e
  [ "$classification_rc" -eq 2 ] || {
    echo "CANDIDATE-INTEGRITY-FAILED $arm rc=$classification_rc" >&2
    return 1
  }
}

if [ -n "$SMOKE_ARM" ]; then
  run_one "$SMOKE_ARM"
  printf 'SMOKE-SUCCEEDED arm=%s\n' "$SMOKE_ARM" >> "$OUT/status.txt"
  echo "SMOKE-DONE out=$OUT arm=$SMOKE_ARM"
  exit 0
fi

while IFS= read -r arm; do
  run_one "$arm"
done < <(python3 - "$OUT/expected-matrix.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    print("\n".join(json.load(stream)["executionOrderAB"]))
PY
)

selected=''
for memory_limit in 192Mi 256Mi 512Mi; do
  for repeat in 1 2 3; do
    arm="C-limit${memory_limit}-r${repeat}"
    set +e
    run_one "$arm"
    arm_rc=$?
    set -e
    if [ "$arm_rc" -ne 0 ]; then
      classify_failed_candidate_arm "$arm" "$arm_rc" || exit 1
    fi
  done
  set +e
  python3 "$SCRIPT_DIR/validate_collector_memory.py" "$OUT" --candidate "$memory_limit" \
    --verdict-output "$OUT/candidate-${memory_limit}.json" \
    > "$OUT/candidate-${memory_limit}.txt" 2>&1
  candidate_rc=$?
  set -e
  cat "$OUT/candidate-${memory_limit}.txt"
  case "$candidate_rc" in
    0) selected=$memory_limit; break ;;
    2) ;;
    *) echo "CANDIDATE-HARNESS-FAILED $memory_limit" >&2; exit 1 ;;
  esac
done
[ -n "$selected" ] || { echo "NO-CANDIDATE-PASSED" >&2; exit 1; }

python3 - "$OUT" "$selected" <<'PY'
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
arms = []
for line in (root / "status.txt").read_text().splitlines():
    match = re.fullmatch(r"([^ ]+) rc=[0-9]+ duration=[0-9]+s", line)
    if not match:
        raise SystemExit(f"invalid status before completion: {line!r}")
    arms.append(match[1])
limits = ["192Mi", "256Mi", "512Mi"]
candidate_verdicts = []
for limit in limits[: limits.index(sys.argv[2]) + 1]:
    with (root / f"candidate-{limit}.json").open(encoding="utf-8") as stream:
        candidate_verdicts.append(json.load(stream))
with (root / "completion.json").open("x") as stream:
    json.dump({
        "schemaVersion": 1,
        "selectedMemoryLimit": sys.argv[2],
        "executedArms": sorted(arms),
        "candidateVerdicts": candidate_verdicts,
    }, stream, indent=2, sort_keys=True)
    stream.write("\n")
PY

write_provenance "$OUT/provenance-final.txt" valid
python3 "$SCRIPT_DIR/validate_collector_memory.py" "$OUT" --campaign
printf 'SWEEP-SUCCEEDED selected=%s arms=%s\n' "$selected" "$(wc -l < "$OUT/status.txt" | tr -d ' ')" >> "$OUT/status.txt"
python3 "$SCRIPT_DIR/validate_collector_memory.py" "$OUT" --full
echo "ALL-DONE out=$OUT selected=$selected"
