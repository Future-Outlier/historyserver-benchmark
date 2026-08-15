#!/usr/bin/env python3
"""Deterministic Ray 2.56-shaped event replay for Collector memory benchmarks.

The replay deliberately models one Ray 2.56 HTTP publisher:

* one persistent HTTP connection;
* one in-flight request at a time (no concurrency and no hidden retry);
* exactly 1,000 events per full request;
* camelCase protobuf-JSON shaped ``TASK_LIFECYCLE_EVENT`` objects;
* a single fixed ``jobId``, so every event reaches one Collector category.

Stdout is JSON Lines.  It contains phase boundaries, one row per elapsed
second, and exactly one terminal summary.  A non-200 response or transport
error emits a failed terminal summary and exits non-zero.
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import decimal
import gzip
import hashlib
import http.client
import json
import math
import re
import statistics
import sys
import time
import urllib.parse
from collections import Counter
from typing import Any, Callable, Protocol, Sequence


SCHEMA_VERSION = "collector-event-replay-v1"
RAY_256_BATCH_SIZE = 1_000
TARGET_JSONL_BYTES_PER_EVENT = 895
CALIBRATION_GZIP_RATIO_MIN = 0.08
CALIBRATION_GZIP_RATIO_MAX = 0.12
NANOSECONDS_PER_SECOND = 1_000_000_000
SESSION_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class ReplayError(RuntimeError):
    """A fail-closed replay error."""


class Poster(Protocol):
    """The one-request-at-a-time transport used by :class:`ReplayRunner`."""

    def post(self, body: bytes) -> tuple[int, bytes]:
        """Send one body exactly once and return ``(status, response_body)``."""

    def close(self) -> None:
        """Close the transport."""


@dataclasses.dataclass(frozen=True)
class ReplayConfig:
    endpoint: str
    rate: int
    duration_seconds: decimal.Decimal
    session_name: str
    node_id_hex: str
    job_id_hex: str
    seed: int = 1
    phase_name: str = "ingest"
    connect_timeout_seconds: float = 3.0
    request_timeout_seconds: float = 10.0

    @property
    def duration_ns(self) -> int:
        value = self.duration_seconds * NANOSECONDS_PER_SECOND
        if value != value.to_integral_value():
            raise ReplayError("duration must resolve to an integral number of nanoseconds")
        return int(value)

    @property
    def total_events(self) -> int:
        value = self.duration_seconds * self.rate
        if value != value.to_integral_value():
            raise ReplayError("rate * duration must be an integral number of events")
        return int(value)

    def validate(self) -> None:
        if self.rate <= 0:
            raise ReplayError("rate must be positive")
        if self.duration_seconds <= 0:
            raise ReplayError("duration must be positive")
        if self.total_events <= 0:
            raise ReplayError("replay must contain at least one event")
        _ = self.duration_ns
        if self.total_events % RAY_256_BATCH_SIZE != 0:
            raise ReplayError(
                f"total events must be divisible by the fixed batch size {RAY_256_BATCH_SIZE}"
            )
        if not SESSION_NAME_RE.fullmatch(self.session_name) or self.session_name in {".", ".."}:
            raise ReplayError("session name is not a safe path component")
        _decode_fixed_hex("node ID", self.node_id_hex, 28)
        _decode_fixed_hex("job ID", self.job_id_hex, 4)
        _, _, endpoint_path = _parse_http_endpoint(self.endpoint)
        if endpoint_path.split("?", 1)[0] != "/v1/events":
            raise ReplayError("endpoint path must be /v1/events")
        if self.seed < 0 or self.seed >= 1 << 64:
            raise ReplayError("seed must fit in an unsigned 64-bit integer")
        if self.connect_timeout_seconds <= 0 or self.request_timeout_seconds <= 0:
            raise ReplayError("HTTP timeouts must be positive")


def _decode_fixed_hex(label: str, value: str, wanted_bytes: int) -> bytes:
    try:
        decoded = bytes.fromhex(value)
    except ValueError as exc:
        raise ReplayError(f"{label} must be hexadecimal") from exc
    if len(decoded) != wanted_bytes:
        raise ReplayError(
            f"{label} must encode exactly {wanted_bytes} bytes, got {len(decoded)}"
        )
    return decoded


def _parse_http_endpoint(endpoint: str) -> tuple[str, int, str]:
    parsed = urllib.parse.urlsplit(endpoint)
    if parsed.scheme != "http":
        raise ReplayError("endpoint scheme must be http")
    if not parsed.hostname:
        raise ReplayError("endpoint host is missing")
    if parsed.username is not None or parsed.password is not None:
        raise ReplayError("endpoint credentials are not supported")
    if parsed.fragment:
        raise ReplayError("endpoint fragments are not supported")
    try:
        port = parsed.port or 80
    except ValueError as exc:
        raise ReplayError("endpoint port is invalid") from exc
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    return parsed.hostname, port, path


def _b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _deterministic_bytes(seed: int, event_index: int, label: str, size: int) -> bytes:
    """Return fixed-width pseudorandom bytes without using ambient randomness."""

    prefix = seed.to_bytes(8, "big") + event_index.to_bytes(8, "big") + label.encode("ascii")
    output = bytearray()
    counter = 0
    while len(output) < size:
        output.extend(hashlib.sha256(prefix + counter.to_bytes(4, "big")).digest())
        counter += 1
    return bytes(output[:size])


def _rfc3339_nano(epoch_ns: int) -> str:
    seconds, nanoseconds = divmod(epoch_ns, NANOSECONDS_PER_SECOND)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(seconds)) + f".{nanoseconds:09d}Z"


class EventFactory:
    """Build one fixed-size, one-category Ray event schema."""

    def __init__(self, config: ReplayConfig, base_wall_ns: int) -> None:
        self._config = config
        self._base_wall_ns = base_wall_ns
        self._node_id_b64 = _b64(_decode_fixed_hex("node ID", config.node_id_hex, 28))
        self._job_id_b64 = _b64(_decode_fixed_hex("job ID", config.job_id_hex, 4))
        self._worker_id_b64 = _b64(
            _deterministic_bytes(config.seed, 0, "worker", 28)
        )

    def event_id(self, event_index: int) -> str:
        return _b64(
            _deterministic_bytes(self._config.seed, event_index, "event", 16)
        )

    def _message_padding(self, event_index: int, size: int) -> str:
        """Return unique deterministic base64 text of exactly ``size`` bytes."""

        raw_size = ((size + 3) // 4) * 3
        return _b64(
            _deterministic_bytes(self._config.seed, event_index, "padding", raw_size)
        )[:size]

    def event_bytes(self, event_index: int) -> bytes:
        timestamp_ns = self._base_wall_ns + (
            event_index * NANOSECONDS_PER_SECOND // self._config.rate
        )
        timestamp = _rfc3339_nano(timestamp_ns)
        event: dict[str, Any] = {
            "eventId": self.event_id(event_index),
            "eventType": "TASK_LIFECYCLE_EVENT",
            "message": "",
            "nodeId": self._node_id_b64,
            "sessionName": self._config.session_name,
            "severity": "INFO",
            "sourceType": "CORE_WORKER",
            "taskLifecycleEvent": {
                "jobId": self._job_id_b64,
                "nodeId": self._node_id_b64,
                "stateTransitions": [
                    {"state": "SUBMITTED_TO_WORKER", "timestamp": timestamp},
                    {"state": "FINISHED", "timestamp": timestamp},
                ],
                "taskAttempt": 0,
                "taskId": _b64(
                    _deterministic_bytes(self._config.seed, event_index, "task", 28)
                ),
                "taskLogInfo": {
                    "stderrEnd": "0",
                    "stderrFile": "/tmp/ray/session_latest/logs/worker.err",
                    "stderrStart": "0",
                    "stdoutEnd": "0",
                    "stdoutFile": "/tmp/ray/session_latest/logs/worker.out",
                    "stdoutStart": "0",
                },
                "workerId": self._worker_id_b64,
                "workerPid": 12345,
            },
            "timestamp": timestamp,
        }
        encoded = _compact_json_bytes(event)
        padding = TARGET_JSONL_BYTES_PER_EVENT - 1 - len(encoded)
        if padding < 0:
            raise ReplayError(
                "event schema exceeds target JSONL size: "
                f"encoded={len(encoded) + 1} target={TARGET_JSONL_BYTES_PER_EVENT}"
            )
        event["message"] = self._message_padding(event_index, padding)
        encoded = _compact_json_bytes(event)
        if len(encoded) + 1 != TARGET_JSONL_BYTES_PER_EVENT:
            raise ReplayError("failed to calibrate fixed JSONL event size")
        return encoded

    def batch(self, first_event_index: int, event_count: int) -> tuple[bytes, bytes]:
        events = [
            self.event_bytes(index)
            for index in range(first_event_index, first_event_index + event_count)
        ]
        body = b"[" + b",".join(events) + b"]"
        jsonl = b"\n".join(events) + b"\n"
        return body, jsonl


def _compact_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


class PersistentHTTPPoster:
    """One persistent connection with no library- or application-level retry."""

    def __init__(
        self,
        endpoint: str,
        connect_timeout_seconds: float,
        request_timeout_seconds: float,
    ) -> None:
        host, port, path = _parse_http_endpoint(endpoint)
        self._path = path
        # http.client uses one timeout for connect and socket I/O.  Connect
        # explicitly with its own bound, then switch the connected socket to
        # the request timeout.  Reconnection is deliberately not attempted.
        self._connection = http.client.HTTPConnection(
            host, port, timeout=connect_timeout_seconds
        )
        self._connection.connect()
        if self._connection.sock is None:
            raise ReplayError("HTTP connection has no socket after connect")
        self._connection.sock.settimeout(request_timeout_seconds)

    def post(self, body: bytes) -> tuple[int, bytes]:
        self._connection.request(
            "POST",
            self._path,
            body=body,
            headers={
                "Content-Type": "application/json",
                "Content-Length": str(len(body)),
            },
        )
        response = self._connection.getresponse()
        response_body = response.read()
        return response.status, response_body

    def close(self) -> None:
        self._connection.close()


@dataclasses.dataclass
class SecondWindow:
    second_index: int
    batches: int = 0
    attempted_events: int = 0
    acknowledged_events: int = 0
    request_bytes: int = 0
    raw_jsonl_bytes: int = 0
    latencies_ms: list[float] = dataclasses.field(default_factory=list)
    schedule_lags_ms: list[float] = dataclasses.field(default_factory=list)
    statuses: Counter[int] = dataclasses.field(default_factory=Counter)

    def as_record(self) -> dict[str, Any]:
        return {
            "kind": "second",
            "schemaVersion": SCHEMA_VERSION,
            "secondIndex": self.second_index,
            "startOffsetSeconds": self.second_index,
            "endOffsetSeconds": self.second_index + 1,
            "batches": self.batches,
            "attemptedEvents": self.attempted_events,
            "acknowledgedEvents": self.acknowledged_events,
            "requestBytes": self.request_bytes,
            "rawJSONLBytes": self.raw_jsonl_bytes,
            "meanLatencyMs": _mean_or_zero(self.latencies_ms),
            "maxLatencyMs": max(self.latencies_ms, default=0.0),
            "maxScheduleLagMs": max(self.schedule_lags_ms, default=0.0),
            "statusCounts": {
                str(key): self.statuses[key] for key in sorted(self.statuses)
            },
        }


class ReplayRunner:
    """Execute one paced replay and emit machine-readable evidence."""

    def __init__(
        self,
        config: ReplayConfig,
        poster: Poster,
        *,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        wall_time_ns: Callable[[], int] = time.time_ns,
        sleep: Callable[[float], None] = time.sleep,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        config.validate()
        self._config = config
        self._poster = poster
        self._monotonic_ns = monotonic_ns
        self._wall_time_ns = wall_time_ns
        self._sleep = sleep
        self._emit = emit or emit_json_line

    def run(self) -> dict[str, Any]:
        config = self._config
        start_mono_ns = self._monotonic_ns()
        start_wall_ns = self._wall_time_ns()
        factory = EventFactory(config, start_wall_ns)
        calibration_count = min(RAY_256_BATCH_SIZE, config.total_events)
        calibration_body, calibration_jsonl = factory.batch(0, calibration_count)
        calibration_gzip = gzip.compress(calibration_jsonl, compresslevel=6, mtime=0)
        calibration_gzip_ratio = len(calibration_gzip) / len(calibration_jsonl)
        if not (
            CALIBRATION_GZIP_RATIO_MIN
            <= calibration_gzip_ratio
            <= CALIBRATION_GZIP_RATIO_MAX
        ):
            self._poster.close()
            raise ReplayError(
                "calibration gzip ratio is outside the realistic replay gate: "
                f"ratio={calibration_gzip_ratio:.6f} "
                f"expected=[{CALIBRATION_GZIP_RATIO_MIN:.2f},"
                f"{CALIBRATION_GZIP_RATIO_MAX:.2f}]"
            )
        calibration = {
            "sampleEvents": calibration_count,
            "requestBytes": len(calibration_body),
            "rawJSONLBytes": len(calibration_jsonl),
            "jsonlBytesPerEvent": len(calibration_jsonl) / calibration_count,
            "requestBytesPerEvent": len(calibration_body) / calibration_count,
            "gzipBytes": len(calibration_gzip),
            "gzipRatio": calibration_gzip_ratio,
            "gzipRatioDefinition": "compressed_bytes/raw_jsonl_bytes",
            "gzipRatioGate": {
                "minimum": CALIBRATION_GZIP_RATIO_MIN,
                "maximum": CALIBRATION_GZIP_RATIO_MAX,
                "passed": True,
            },
            "requestSHA256": hashlib.sha256(calibration_body).hexdigest(),
            "messagePaddingBytes": len(json.loads(calibration_body)[0]["message"]),
            "messagePaddingEncoding": "deterministic-sha256-base64",
        }
        self._emit(
            {
                "kind": "phase",
                "schemaVersion": SCHEMA_VERSION,
                "phase": config.phase_name,
                "state": "start",
                "wallTime": _rfc3339_nano(start_wall_ns),
                "monotonicNs": start_mono_ns,
            }
        )

        attempted_events = 0
        acknowledged_events = 0
        request_bytes = 0
        raw_jsonl_bytes = 0
        batches = 0
        latencies_ms: list[float] = []
        schedule_lags_ms: list[float] = []
        status_counts: Counter[int] = Counter()
        windows: dict[int, SecondWindow] = {}
        next_second_to_emit = 0
        failure: str | None = None
        first_event_id: str | None = None
        last_event_id: str | None = None

        try:
            while acknowledged_events < config.total_events:
                first_index = acknowledged_events
                count = min(
                    RAY_256_BATCH_SIZE,
                    config.total_events - acknowledged_events,
                )
                deadline_ns = start_mono_ns + (
                    first_index * NANOSECONDS_PER_SECOND // config.rate
                )
                self._sleep_until(deadline_ns)
                request_start_ns = self._monotonic_ns()
                lag_ms = max(0.0, (request_start_ns - deadline_ns) / 1_000_000)
                if first_index == 0 and count == calibration_count:
                    body, jsonl = calibration_body, calibration_jsonl
                else:
                    body, jsonl = factory.batch(first_index, count)

                first_event_id = first_event_id or factory.event_id(first_index)
                last_event_id = factory.event_id(first_index + count - 1)
                attempted_events += count
                request_bytes += len(body)
                raw_jsonl_bytes += len(jsonl)
                batches += 1
                second_index = max(0, (request_start_ns - start_mono_ns) // NANOSECONDS_PER_SECOND)
                window = windows.setdefault(int(second_index), SecondWindow(int(second_index)))
                window.batches += 1
                window.attempted_events += count
                window.request_bytes += len(body)
                window.raw_jsonl_bytes += len(jsonl)
                window.schedule_lags_ms.append(lag_ms)
                schedule_lags_ms.append(lag_ms)

                try:
                    status, response_body = self._poster.post(body)
                except Exception as exc:  # fail closed; exactly zero retries
                    failure = f"transport error: {type(exc).__name__}: {exc}"
                    raise ReplayError(failure) from exc
                request_end_ns = self._monotonic_ns()
                latency_ms = (request_end_ns - request_start_ns) / 1_000_000
                latencies_ms.append(latency_ms)
                window.latencies_ms.append(latency_ms)
                status_counts[status] += 1
                window.statuses[status] += 1
                if status != 200:
                    preview = response_body[:256].decode("utf-8", errors="replace")
                    failure = f"HTTP {status}: {preview}"
                    raise ReplayError(failure)
                acknowledged_events += count
                window.acknowledged_events += count
                next_second_to_emit = self._emit_completed_seconds(
                    start_mono_ns, windows, next_second_to_emit
                )

            # A rate-R phase is R*duration events over the whole configured
            # interval, not R*duration events ending one batch interval early.
            self._sleep_until(start_mono_ns + config.duration_ns)
        except ReplayError:
            pass
        finally:
            self._poster.close()

        end_mono_ns = self._monotonic_ns()
        end_wall_ns = self._wall_time_ns()
        elapsed_ns = max(1, end_mono_ns - start_mono_ns)
        final_second = max(
            math.ceil(config.duration_ns / NANOSECONDS_PER_SECOND),
            math.ceil(elapsed_ns / NANOSECONDS_PER_SECOND),
        )
        while next_second_to_emit < final_second:
            window = windows.get(next_second_to_emit, SecondWindow(next_second_to_emit))
            self._emit(window.as_record())
            next_second_to_emit += 1

        success = failure is None and acknowledged_events == config.total_events
        summary = {
            "kind": "summary",
            "schemaVersion": SCHEMA_VERSION,
            "success": success,
            "failure": failure,
            "configuration": {
                "endpoint": config.endpoint,
                "rateEventsPerSecond": config.rate,
                "durationSeconds": float(config.duration_seconds),
                "batchSize": RAY_256_BATCH_SIZE,
                "serialPublishers": 1,
                "hiddenRetries": 0,
                "phase": config.phase_name,
                "sessionName": config.session_name,
                "nodeIDHex": config.node_id_hex.lower(),
                "jobIDHex": config.job_id_hex.lower(),
                "seed": config.seed,
            },
            "calibration": calibration,
            "scheduledEvents": config.total_events,
            "attemptedEvents": attempted_events,
            "acknowledgedEvents": acknowledged_events,
            "batches": batches,
            "requestBytes": request_bytes,
            "rawJSONLBytes": raw_jsonl_bytes,
            "firstEventID": first_event_id,
            "lastEventID": last_event_id,
            "statusCounts": {
                str(key): status_counts[key] for key in sorted(status_counts)
            },
            "plannedDurationSeconds": float(config.duration_seconds),
            "actualDurationSeconds": elapsed_ns / NANOSECONDS_PER_SECOND,
            "achievedEventsPerSecond": (
                acknowledged_events * NANOSECONDS_PER_SECOND / elapsed_ns
            ),
            "requestLatencyMs": _distribution(latencies_ms),
            "scheduleLagMs": _distribution(schedule_lags_ms),
            "phaseStartWallTime": _rfc3339_nano(start_wall_ns),
            "phaseEndWallTime": _rfc3339_nano(end_wall_ns),
        }
        self._emit(
            {
                "kind": "phase",
                "schemaVersion": SCHEMA_VERSION,
                "phase": config.phase_name,
                "state": "end",
                "success": success,
                "wallTime": _rfc3339_nano(end_wall_ns),
                "monotonicNs": end_mono_ns,
            }
        )
        self._emit(summary)
        return summary

    def _sleep_until(self, deadline_ns: int) -> None:
        while True:
            remaining_ns = deadline_ns - self._monotonic_ns()
            if remaining_ns <= 0:
                return
            self._sleep(remaining_ns / NANOSECONDS_PER_SECOND)

    def _emit_completed_seconds(
        self,
        start_mono_ns: int,
        windows: dict[int, SecondWindow],
        next_second: int,
    ) -> int:
        elapsed_complete_seconds = (
            self._monotonic_ns() - start_mono_ns
        ) // NANOSECONDS_PER_SECOND
        while next_second < elapsed_complete_seconds:
            window = windows.get(next_second, SecondWindow(next_second))
            self._emit(window.as_record())
            next_second += 1
        return next_second


def _mean_or_zero(values: Sequence[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return float(ordered[rank - 1])


def _distribution(values: Sequence[float]) -> dict[str, float | int]:
    return {
        "count": len(values),
        "p50": _percentile(values, 0.50),
        "p95": _percentile(values, 0.95),
        "p99": _percentile(values, 0.99),
        "max": max(values, default=0.0),
    }


def emit_json_line(record: dict[str, Any]) -> None:
    print(json.dumps(record, sort_keys=True, separators=(",", ":")), flush=True)


def _positive_decimal(value: str) -> decimal.Decimal:
    try:
        parsed = decimal.Decimal(value)
    except decimal.InvalidOperation as exc:
        raise argparse.ArgumentTypeError("must be a decimal number") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> ReplayConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--rate", required=True, type=int, help="events per second")
    parser.add_argument("--duration", required=True, type=_positive_decimal, help="seconds")
    parser.add_argument(
        "--batch-size",
        type=int,
        choices=[RAY_256_BATCH_SIZE],
        default=RAY_256_BATCH_SIZE,
        help=f"fixed Ray 2.56 publisher batch size; must be {RAY_256_BATCH_SIZE}",
    )
    parser.add_argument("--session", required=True)
    parser.add_argument("--node-id-hex", required=True)
    parser.add_argument("--job-id-hex", required=True)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--phase-name", default="ingest")
    parser.add_argument("--connect-timeout", type=float, default=3.0)
    parser.add_argument("--request-timeout", type=float, default=10.0)
    args = parser.parse_args(argv)
    return ReplayConfig(
        endpoint=args.endpoint,
        rate=args.rate,
        duration_seconds=args.duration,
        session_name=args.session,
        node_id_hex=args.node_id_hex,
        job_id_hex=args.job_id_hex,
        seed=args.seed,
        phase_name=args.phase_name,
        connect_timeout_seconds=args.connect_timeout,
        request_timeout_seconds=args.request_timeout,
    )


def main(argv: Sequence[str] | None = None) -> int:
    try:
        config = parse_args(argv)
        config.validate()
        poster = PersistentHTTPPoster(
            config.endpoint,
            config.connect_timeout_seconds,
            config.request_timeout_seconds,
        )
        summary = ReplayRunner(config, poster).run()
        return 0 if summary["success"] else 1
    except (ReplayError, OSError, http.client.HTTPException) as exc:
        emit_json_line(
            {
                "kind": "summary",
                "schemaVersion": SCHEMA_VERSION,
                "success": False,
                "failure": f"startup error: {type(exc).__name__}: {exc}",
            }
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
