#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
RUNNER=$SCRIPT_DIR/ray256_collector_memory.sh

bash -n "$RUNNER"
grep -Fq 'set +e' "$RUNNER"
grep -Fq 'classify_failed_candidate_arm "$arm" "$arm_rc" || exit 1' "$RUNNER"
grep -Fq '[ "$classification_rc" -eq 2 ]' "$RUNNER"
grep -Fq '*) echo "CANDIDATE-HARNESS-FAILED $memory_limit" >&2; exit 1 ;;' "$RUNNER"
grep -Fq 'BENCH_COLLECTOR_MEMORY_SMOKE_ARM must be empty or B-rate5000-r1' "$RUNNER"
grep -Fq 'SMOKE-SUCCEEDED arm=%s' "$RUNNER"
grep -Fq 'wait_for_collector_port_free' "$RUNNER"
grep -Fq 'COLLECTOR-PORT-FREE port=' "$RUNNER"
grep -Fq 'consecutive >= 2' "$RUNNER"
grep -Fq 'time.monotonic() + 30.0' "$RUNNER"
grep -Fq 'probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)' "$RUNNER"
grep -Fq 'probe.listen(1)' "$RUNNER"
! grep -Fq 'SO_REUSEPORT' "$RUNNER"

# Exercise the exact socket ownership rule independently of the formal port.
# A live or bound owner must block the probe. Closed-connection TIME_WAIT must
# not block it on Darwin.
python3 - <<'PY'
import socket
import threading


def reusable_probe(port):
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", port))
        probe.listen(1)
        return True
    except OSError:
        return False
    finally:
        probe.close()


listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", 0))
port = listener.getsockname()[1]
listener.listen(1)
if reusable_probe(port):
    raise SystemExit("active listener incorrectly classified as free")

accepted = threading.Event()


def accept_once():
    connection, _ = listener.accept()
    connection.close()
    accepted.set()


thread = threading.Thread(target=accept_once)
thread.start()
client = socket.create_connection(("127.0.0.1", port))
client.close()
accepted.wait(2)
thread.join(2)
listener.close()
if not reusable_probe(port):
    raise SystemExit("closed connection incorrectly blocks reusable probe")

bound = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
bound.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
bound.bind(("127.0.0.1", port))
if reusable_probe(port):
    raise SystemExit("bound non-listening owner incorrectly classified as free")
bound.close()
PY
grep -Fq -- '--verdict-output "$OUT/candidate-${memory_limit}.json"' "$RUNNER"
[ "$(grep -Fc '/sbin/sha256sum' "$RUNNER")" -eq 2 ]
! grep -Fq 'shasum -a 256' "$RUNNER"

echo RUNNER-CONTRACT-VALID
