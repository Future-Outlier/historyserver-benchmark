#!/usr/bin/env bash
set -euo pipefail
script=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/ray256_hs_isolated.sh

grep -F 'write_hs_expected_matrix.py" isolated' "$script" >/dev/null
[ "$(grep -c 'hs_run_arm "isolated-r${repeat}" 1 1Gi 2 12Gi isolated-request-v1 10s 8s' "$script")" -eq 1 ]
grep -F "'GOMAXPROCS=2,GODEBUG=gctrace=1'" "$script" >/dev/null
grep -F 'for repeat in 1 2 3 4 5' "$script" >/dev/null
grep -F "case \"\$HS_SOURCE_TASK_COUNT\" in 1000|5000|10000|50000)" "$script" >/dev/null
grep -F "printf 'SWEEP-SUCCEEDED arms=5\\n'" "$script" >/dev/null
echo 'HS-ISOLATED-CONTRACT-PASS'
