#!/usr/bin/env python3

from __future__ import annotations

import base64
import decimal
import gzip
import json
import pathlib
import re
import sys
import unittest


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import collector_event_replay as replay


NODE_ID_HEX = "01" * 28
JOB_ID_HEX = "03000000"


def config(*, rate: int = 2_000, duration: str = "1.5") -> replay.ReplayConfig:
    return replay.ReplayConfig(
        endpoint="http://collector:8084/v1/events",
        rate=rate,
        duration_seconds=decimal.Decimal(duration),
        session_name="session_2026-08-13_12-00-00_000001",
        node_id_hex=NODE_ID_HEX,
        job_id_hex=JOB_ID_HEX,
        seed=7,
    )


class FakeClock:
    def __init__(self) -> None:
        self.monotonic = 100_000_000_000
        self.wall = 1_786_645_296_123_456_789
        self.sleep_deadlines: list[int] = []

    def monotonic_ns(self) -> int:
        return self.monotonic

    def wall_time_ns(self) -> int:
        return self.wall + (self.monotonic - 100_000_000_000)

    def sleep(self, seconds: float) -> None:
        delta = int(round(seconds * replay.NANOSECONDS_PER_SECOND))
        self.monotonic += delta
        self.sleep_deadlines.append(self.monotonic)


class FakePoster:
    def __init__(self, clock: FakeClock, statuses: list[int] | None = None) -> None:
        self.clock = clock
        self.statuses = list(statuses or [])
        self.calls: list[tuple[int, bytes]] = []
        self.closed = False

    def post(self, body: bytes) -> tuple[int, bytes]:
        self.calls.append((self.clock.monotonic_ns(), body))
        self.clock.monotonic += 50_000_000
        status = self.statuses.pop(0) if self.statuses else 200
        return status, b"failure" if status != 200 else b""

    def close(self) -> None:
        self.closed = True


class EventFactoryTest(unittest.TestCase):
    def test_fixed_size_unique_realistic_one_category_events(self) -> None:
        cfg = config()
        factory = replay.EventFactory(cfg, 1_786_645_296_123_456_789)
        first = factory.event_bytes(0)
        second = factory.event_bytes(1)

        self.assertEqual(len(first) + 1, replay.TARGET_JSONL_BYTES_PER_EVENT)
        self.assertEqual(len(second) + 1, replay.TARGET_JSONL_BYTES_PER_EVENT)
        a = json.loads(first)
        b = json.loads(second)
        self.assertEqual(a["eventType"], "TASK_LIFECYCLE_EVENT")
        self.assertEqual(a["taskLifecycleEvent"]["jobId"], base64.b64encode(bytes.fromhex(JOB_ID_HEX)).decode())
        self.assertEqual(a["nodeId"], base64.b64encode(bytes.fromhex(NODE_ID_HEX)).decode())
        self.assertNotEqual(a["eventId"], b["eventId"])
        self.assertNotEqual(a["taskLifecycleEvent"]["taskId"], b["taskLifecycleEvent"]["taskId"])
        self.assertEqual(len(base64.b64decode(a["eventId"])), 16)
        self.assertEqual(len(base64.b64decode(a["taskLifecycleEvent"]["taskId"])), 28)

    def test_batch_calibrates_jsonl_and_gzip(self) -> None:
        factory = replay.EventFactory(config(), 1_786_645_296_123_456_789)
        body, jsonl = factory.batch(0, replay.RAY_256_BATCH_SIZE)
        self.assertEqual(len(json.loads(body)), 1_000)
        self.assertEqual(len(jsonl), 895_000)
        self.assertEqual(len(body), len(jsonl) + 1)
        self.assertEqual(jsonl.count(b"\n"), 1_000)

    def test_high_entropy_padding_matches_realistic_gzip_gate(self) -> None:
        factory = replay.EventFactory(config(), 1_786_645_296_123_456_789)
        body, jsonl = factory.batch(0, replay.RAY_256_BATCH_SIZE)
        events = json.loads(body)
        messages = [event["message"] for event in events]
        gzip_ratio = len(gzip.compress(jsonl, compresslevel=6, mtime=0)) / len(jsonl)

        self.assertEqual(len(set(messages)), replay.RAY_256_BATCH_SIZE)
        self.assertTrue(all(re.fullmatch(r"[A-Za-z0-9+/]+", value) for value in messages))
        self.assertTrue(all(len(value) == len(messages[0]) for value in messages))
        self.assertGreaterEqual(gzip_ratio, replay.CALIBRATION_GZIP_RATIO_MIN)
        self.assertLessEqual(gzip_ratio, replay.CALIBRATION_GZIP_RATIO_MAX)


class ReplayRunnerTest(unittest.TestCase):
    def test_serial_absolute_pacing_and_complete_phase(self) -> None:
        cfg = config(rate=2_000, duration="1.5")
        clock = FakeClock()
        poster = FakePoster(clock)
        records: list[dict] = []

        summary = replay.ReplayRunner(
            cfg,
            poster,
            monotonic_ns=clock.monotonic_ns,
            wall_time_ns=clock.wall_time_ns,
            sleep=clock.sleep,
            emit=records.append,
        ).run()

        self.assertTrue(summary["success"])
        self.assertEqual(summary["scheduledEvents"], 3_000)
        self.assertEqual(summary["acknowledgedEvents"], 3_000)
        self.assertEqual(summary["batches"], 3)
        self.assertEqual([when for when, _ in poster.calls], [
            100_000_000_000,
            100_500_000_000,
            101_000_000_000,
        ])
        self.assertEqual(clock.monotonic_ns(), 101_500_000_000)
        self.assertTrue(poster.closed)
        self.assertEqual(summary["statusCounts"], {"200": 3})
        self.assertEqual(
            summary["calibration"]["gzipRatioDefinition"],
            "compressed_bytes/raw_jsonl_bytes",
        )
        self.assertEqual(sum(r["acknowledgedEvents"] for r in records if r["kind"] == "second"), 3_000)
        self.assertEqual(records[-1], summary)
        self.assertEqual(records[-2]["kind"], "phase")
        self.assertEqual(records[-2]["state"], "end")

    def test_non_200_fails_without_retry(self) -> None:
        cfg = config(rate=1_000, duration="2")
        clock = FakeClock()
        poster = FakePoster(clock, statuses=[503, 200, 200])
        records: list[dict] = []

        summary = replay.ReplayRunner(
            cfg,
            poster,
            monotonic_ns=clock.monotonic_ns,
            wall_time_ns=clock.wall_time_ns,
            sleep=clock.sleep,
            emit=records.append,
        ).run()

        self.assertFalse(summary["success"])
        self.assertEqual(len(poster.calls), 1)
        self.assertEqual(summary["attemptedEvents"], 1_000)
        self.assertEqual(summary["acknowledgedEvents"], 0)
        self.assertEqual(summary["statusCounts"], {"503": 1})
        self.assertIn("HTTP 503", summary["failure"])
        self.assertTrue(poster.closed)
    def test_transport_error_fails_without_retry(self) -> None:
        class FailingPoster(FakePoster):
            def post(self, body: bytes) -> tuple[int, bytes]:
                self.calls.append((self.clock.monotonic_ns(), body))
                raise TimeoutError("sentinel")

        cfg = config(rate=1_000, duration="1")
        clock = FakeClock()
        poster = FailingPoster(clock)
        summary = replay.ReplayRunner(
            cfg,
            poster,
            monotonic_ns=clock.monotonic_ns,
            wall_time_ns=clock.wall_time_ns,
            sleep=clock.sleep,
            emit=lambda _: None,
        ).run()
        self.assertFalse(summary["success"])
        self.assertEqual(len(poster.calls), 1)
        self.assertEqual(summary["acknowledgedEvents"], 0)
        self.assertIn("transport error", summary["failure"])


class ParseArgsTest(unittest.TestCase):
    def _argv(self, batch_size: int) -> list[str]:
        return [
            "--endpoint", "http://collector:8084/v1/events",
            "--rate", "2000",
            "--duration", "1",
            "--batch-size", str(batch_size),
            "--session", "session_2026-08-13_12-00-00_000001",
            "--node-id-hex", NODE_ID_HEX,
            "--job-id-hex", JOB_ID_HEX,
        ]

    def test_accepts_only_fixed_ray_256_batch_size(self) -> None:
        parsed = replay.parse_args(self._argv(replay.RAY_256_BATCH_SIZE))
        self.assertEqual(parsed.rate, 2_000)
        with self.assertRaises(SystemExit):
            replay.parse_args(self._argv(replay.RAY_256_BATCH_SIZE - 1))


class ConfigValidationTest(unittest.TestCase):
    def test_rejects_bad_identity_and_fractional_event_count(self) -> None:
        with self.assertRaises(replay.ReplayError):
            dataclass_replace(config(), node_id_hex="01").validate()
        with self.assertRaises(replay.ReplayError):
            dataclass_replace(config(), duration_seconds=decimal.Decimal("0.0001")).validate()
        with self.assertRaises(replay.ReplayError):
            dataclass_replace(config(), rate=1_001, duration_seconds=decimal.Decimal("1")).validate()
        with self.assertRaises(replay.ReplayError):
            dataclass_replace(config(), endpoint="http://collector:8084/wrong").validate()


def dataclass_replace(value: replay.ReplayConfig, **changes: object) -> replay.ReplayConfig:
    data = {
        field.name: getattr(value, field.name)
        for field in value.__dataclass_fields__.values()
    }
    data.update(changes)
    return replay.ReplayConfig(**data)


if __name__ == "__main__":
    unittest.main()
