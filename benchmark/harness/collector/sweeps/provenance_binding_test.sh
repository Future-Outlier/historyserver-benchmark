#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HS=$(cd "$SCRIPT_DIR/../../.." && pwd)
HASH_HELPER="$SCRIPT_DIR/image_source_hash.py"

collector_sha=$(python3 "$HASH_HELPER" --root "$HS" --component collector)
collector_sha_again=$(python3 "$HASH_HELPER" --root "$HS" --component collector)
historyserver_sha=$(python3 "$HASH_HELPER" --root "$HS" --component historyserver)
tree_sha=$(python3 "$HASH_HELPER" --root "$HS" --tree)
tree_sha_relative=$(
  cd "$(dirname "$HS")"
  python3 "$HASH_HELPER" --root "$(basename "$HS")" --tree
)

[[ "$collector_sha" =~ ^[0-9a-f]{64}$ ]]
[[ "$historyserver_sha" =~ ^[0-9a-f]{64}$ ]]
[[ "$collector_sha" == "$collector_sha_again" ]]
[[ "$collector_sha" != "$historyserver_sha" ]]
[[ "$tree_sha" == "$tree_sha_relative" ]]

collector_files=$(python3 "$HASH_HELPER" --root "$HS" --component collector --list-files)
historyserver_files=$(python3 "$HASH_HELPER" --root "$HS" --component historyserver --list-files)
tree_files=$(python3 "$HASH_HELPER" --root "$HS" --tree --list-files)
grep -qx 'Dockerfile.collector' <<<"$collector_files"
grep -qx 'cmd/collector/main.go' <<<"$collector_files"
if grep -q '^html/' <<<"$collector_files"; then
  echo "collector source set unexpectedly contains frontend files" >&2
  exit 1
fi
grep -qx 'Dockerfile.historyserver' <<<"$historyserver_files"
grep -qx 'cmd/historyserver/main.go' <<<"$historyserver_files"
grep -q '^html/' <<<"$historyserver_files"
if grep -Eq '(^|/)(out|__pycache__|\.gocache|\.pytest_cache)(/|$)' <<<"$tree_files"; then
  echo "tree source set unexpectedly contains generated output or cache files" >&2
  exit 1
fi

for dockerfile in Dockerfile.collector Dockerfile.historyserver; do
  grep -q 'ARG KUBERAY_BUILD_SOURCE_SHA256' "$HS/$dockerfile"
  grep -q 'io.kuberay.historyserver.build-source-sha256' "$HS/$dockerfile"
  grep -q '^FROM --platform=\$BUILDPLATFORM golang:' "$HS/$dockerfile"
  grep -q '^ARG TARGETOS$' "$HS/$dockerfile"
  grep -q '^ARG TARGETARCH$' "$HS/$dockerfile"
  grep -q 'TARGETOS/TARGETARCH are required; enable BuildKit' "$HS/$dockerfile"
done
grep -q 'make buildcollector GOOS="\$TARGETOS" GOARCH="\$TARGETARCH"' "$HS/Dockerfile.collector"
grep -q 'make buildhistoryserver GOOS="\$TARGETOS" GOARCH="\$TARGETARCH"' "$HS/Dockerfile.historyserver"

collector_make=$(make -C "$HS" -n localimage-collector ENGINE=echo)
historyserver_make=$(make -C "$HS" -n localimage-historyserver ENGINE=echo)
grep -q -- '--build-arg KUBERAY_BUILD_SOURCE_SHA256=' <<<"$collector_make"
grep -q -- '--build-arg KUBERAY_BUILD_SOURCE_SHA256=' <<<"$historyserver_make"

# Exercise the end-of-sweep binding gate without Docker. The test intentionally
# leaves its uniquely named /tmp fixture in place; it never deletes user data.
OUT=$(mktemp -d /tmp/kuberay-provenance-binding-test.XXXXXX)
BENCH_KIND_NODE=mock-kind-node
source "$SCRIPT_DIR/sweep_lib.sh"

ray_id="sha256:$(printf 'a%.0s' {1..64})"
collector_id="sha256:$(printf 'b%.0s' {1..64})"
historyserver_id="sha256:$(printf 'c%.0s' {1..64})"
ray_commit=$(printf 'd%.0s' {1..40})
mock_collector_source=$collector_sha
mock_historyserver_source=$historyserver_sha
mock_collector_id=$collector_id
mock_repo_head=$(printf '0%.0s' {1..40})
mock_tracked_diff=$(printf '1%.0s' {1..64})
mock_benchmark_source=$(printf '2%.0s' {1..64})
mock_historyserver_source_tree=$(printf '3%.0s' {1..64})
mock_operator_sha=$(printf '4%.0s' {1..64})
mock_raycluster_manifest=$(printf '5%.0s' {1..64})
mock_historyserver_manifest=$(printf '6%.0s' {1..64})
mock_expected_matrix=$(printf '8%.0s' {1..64})
OPERATOR_BIN=/mock/operator

git() {
  case "$*" in
    *'rev-parse --show-toplevel') printf '/mock/repository\n' ;;
    *'rev-parse HEAD') printf '%s\n' "$mock_repo_head" ;;
    *) return 1 ;;
  esac
}

tracked_workspace_diff_sha256() {
  printf '%s\n' "$mock_tracked_diff"
}

benchmark_tree_sha256() {
  case "$1" in
    */test/benchmark) printf '%s\n' "$mock_benchmark_source" ;;
    "$HS") printf '%s\n' "$mock_historyserver_source_tree" ;;
    *) return 1 ;;
  esac
}

file_sha256() {
  case "$1" in
    "$OPERATOR_BIN") printf '%s\n' "$mock_operator_sha" ;;
    "$HS/config/raycluster.yaml") printf '%s\n' "$mock_raycluster_manifest" ;;
    "$HS/config/historyserver.yaml") printf '%s\n' "$mock_historyserver_manifest" ;;
    "$OUT/expected-matrix.json") printf '%s\n' "$mock_expected_matrix" ;;
    *) return 1 ;;
  esac
}

image_build_source_sha256() {
  case "$1" in
    collector) printf '%s\n' "$mock_collector_source" ;;
    historyserver) printf '%s\n' "$mock_historyserver_source" ;;
    *) return 1 ;;
  esac
}

runtime_image_metadata() {
  case "$2" in
    rayproject/ray)
      printf '%s\t<none>\t2.56.0\t%s\t<none>\n' "$ray_id" "$ray_commit"
      ;;
    collector)
      printf '%s\t<none>\t<none>\t<none>\t%s\n' "$mock_collector_id" "$mock_collector_source"
      ;;
    historyserver)
      printf '%s\t<none>\t<none>\t<none>\t%s\n' "$historyserver_id" "$mock_historyserver_source"
      ;;
    *) return 1 ;;
  esac
}

{
  printf 'repo_head=%s\n' "$mock_repo_head"
  printf 'tracked_diff_sha256=%s\n' "$mock_tracked_diff"
  printf 'benchmark_source_sha256=%s\n' "$mock_benchmark_source"
  printf 'historyserver_source_sha256=%s\n' "$mock_historyserver_source_tree"
  printf 'operator_sha256=%s\n' "$mock_operator_sha"
  printf 'raycluster_manifest_sha256=%s\n' "$mock_raycluster_manifest"
  printf 'historyserver_manifest_sha256=%s\n' "$mock_historyserver_manifest"
  printf 'expected_matrix_sha256=%s\n' "$mock_expected_matrix"
  printf 'ray_image_requested=rayproject/ray:2.56.0\n'
  printf 'ray_runtime_id=%s\n' "$ray_id"
  printf 'ray_runtime_version=2.56.0\n'
  printf 'ray_runtime_commit=%s\n' "$ray_commit"
  printf 'collector_image_requested=collector:v0.1.0\n'
  printf 'collector_build_source_sha256=%s\n' "$collector_sha"
  printf 'collector_runtime_id=%s\n' "$collector_id"
  printf 'collector_runtime_build_source_sha256=%s\n' "$collector_sha"
  printf 'historyserver_image_requested=historyserver:v0.1.0\n'
  printf 'historyserver_build_source_sha256=%s\n' "$historyserver_sha"
  printf 'historyserver_runtime_id=%s\n' "$historyserver_id"
  printf 'historyserver_runtime_build_source_sha256=%s\n' "$historyserver_sha"
} > "$OUT/provenance.txt"

revalidate_sweep_provenance >/dev/null
revalidate_sweep_provenance >/dev/null
grep -qx 'revalidation_status=valid' "$OUT/provenance-final.txt"

mock_expected_matrix=$(printf '9%.0s' {1..64})
if revalidate_sweep_provenance >/dev/null 2>&1; then
  echo "expected matrix drift was not rejected" >&2
  exit 1
fi
mock_expected_matrix=$(printf '8%.0s' {1..64})
mock_benchmark_source=$(printf '7%.0s' {1..64})
if revalidate_sweep_provenance >/dev/null 2>&1; then
  echo "benchmark harness drift was not rejected" >&2
  exit 1
fi
mock_benchmark_source=$(printf '2%.0s' {1..64})
mock_collector_source=$(printf 'e%.0s' {1..64})
if revalidate_sweep_provenance >/dev/null 2>&1; then
  echo "source drift was not rejected" >&2
  exit 1
fi
mock_collector_source=$collector_sha
mock_collector_id="sha256:$(printf 'f%.0s' {1..64})"
if revalidate_sweep_provenance >/dev/null 2>&1; then
  echo "runtime image drift was not rejected" >&2
  exit 1
fi

echo "PROVENANCE-BINDING-TEST-PASSED fixture=$OUT"
