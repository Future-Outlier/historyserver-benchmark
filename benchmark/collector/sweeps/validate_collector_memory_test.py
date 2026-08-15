#!/usr/bin/env python3

from __future__ import annotations

import csv
import datetime as dt
import json
import pathlib
import tempfile
import unittest

import validate_collector_memory as validator
import write_collector_memory_matrix as matrix_writer

SHA = "a" * 64
IMAGE_ID = "sha256:" + "9" * 64
START = 1_800_000_000_000_000_000


def arm(name: str) -> dict:
    return next(item for item in matrix_writer.build_matrix()["arms"] if item["name"] == name)


def write_csv(path: pathlib.Path, fields: set[str], rows: list[dict]) -> None:
    ordered = sorted(fields)
    with path.open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=ordered)
        writer.writeheader()
        writer.writerows(rows)


class Fixture:
    def __init__(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        matrix_path = self.root / "expected-matrix.json"
        matrix_path.write_text(json.dumps(matrix_writer.build_matrix(), indent=2, sort_keys=True) + "\n")
        self.matrix_sha = validator.sha256(matrix_path)
        values = {
            "repo_head": SHA, "tracked_diff_sha256": SHA,
            "benchmark_source_sha256": SHA, "operator_sha256": SHA,
            "expected_matrix_sha256": self.matrix_sha, "runner_sha256": SHA,
            "generator_sha256": SHA, "validator_sha256": SHA,
            "ray_image_requested": matrix_writer.RAY_IMAGE,
            "ray_runtime_id": IMAGE_ID, "ray_runtime_version": "2.56.0",
            "ray_runtime_commit": "test", "collector_image_requested": matrix_writer.COLLECTOR_IMAGE,
            "collector_build_source_sha256": SHA, "collector_runtime_id": IMAGE_ID,
            "collector_runtime_build_source_sha256": SHA,
        }
        (self.root / "provenance.txt").write_text("".join(f"{key}={value}\n" for key, value in values.items()))

    def add_discovery(self) -> None:
        for repeat in range(1, 4):
            name = f"A-rate5000-r{repeat}"
            if not (self.root / name).exists():
                self.add(name, peak_mib=100)

    def close(self) -> None:
        self.temp.cleanup()

    def add(
        self,
        name: str,
        *,
        peak_mib: int = 100,
        bad_config: bool = False,
        shutdown_upload_offset_seconds: int = 1,
        sample_through_shutdown: bool = True,
        sample_end_before_upload_ns: int | None = None,
    ) -> dict:
        expected = arm(name)
        cfg = expected["config"]
        run = self.root / name / "run"
        run.mkdir(parents=True)
        baseline = START
        ingest = baseline + 10_000_000_000
        idle = ingest + cfg["ingestSeconds"] * 1_000_000_000
        shutdown = idle + cfg["idleSeconds"] * 1_000_000_000
        complete = shutdown + 2_000_000_000
        report_cfg = validator.expected_report_config(expected, self.matrix_sha, self.root / name)
        if bad_config:
            report_cfg["rateEventsPerSecond"] += 1
        planned = cfg["plannedEvents"]
        raw = planned * 895
        high = cfg["idleGate"] == "reclaim-after-delete"
        upload_time = dt.datetime.fromtimestamp((idle + 5_000_000_000) / 1e9, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")
        final_cgroup_nano = shutdown + shutdown_upload_offset_seconds * 1_000_000_000
        final_cgroup_time = dt.datetime.fromtimestamp(
            final_cgroup_nano / 1e9, tz=dt.timezone.utc
        ).isoformat().replace("+00:00", "Z")
        report = {
            "schemaVersion": 1, "completed": True, "config": report_cfg,
            "phases": [{"name": n, "timeNano": t} for n, t in (
                ("baseline", baseline), ("ingest", ingest), ("idle", idle),
                ("shutdown", shutdown), ("complete", complete))],
            "clockSkew": {
                "before": {"hostBeforeUnixNano": START - 2_000_000_000,
                           "nodeUnixNano": START - 1_950_000_000,
                           "hostAfterUnixNano": START - 1_900_000_000,
                           "roundTripNano": 100_000_000, "skewNano": 0},
                "after": {"hostBeforeUnixNano": complete + 1_000_000_000,
                          "nodeUnixNano": complete + 1_050_000_000,
                          "hostAfterUnixNano": complete + 1_100_000_000,
                          "roundTripNano": 100_000_000, "skewNano": 0},
            },
            "replay": {
                "schema_version": 1, "target_events_per_second": cfg["targetEventsPerSecond"],
                "batch_size": 1000, "planned_events": planned, "sent_events": planned,
                "accepted_events": planned, "expected_jsonl_bytes": raw,
                "achieved_events_per_second": cfg["targetEventsPerSecond"],
                "latency_p99_ms": 10.0, "non_200_responses": 0, "retries": 0,
                "jsonl_bytes_per_event": 895, "fixture_gzip_ratio": 0.095,
                "fixture_gzip_ratio_definition": "compressed_bytes/raw_jsonl_bytes",
            },
            "remote": {"lines": planned, "uniqueEventIDs": planned, "duplicateEventIDs": 0,
                       "malformedLines": 0, "unexpectedEventIDs": 0, "rawJSONLBytes": raw},
            "duringIdle": {"addedObjects": 1 if high else 0},
            "runtime": {"podUID": "pod-uid", "image": "docker.io/library/collector:v0.1.0",
                        "imageID": IMAGE_ID, "containerID": "container",
                        "restartCount": 0, "cpuRequest": "100m", "cpuLimit": "2",
                        "memoryRequest": "128Mi", "memoryLimit": cfg["memoryLimit"],
                        "terminationGracePeriodSeconds": 120},
            "rayRuntime": {"podUID": "pod-uid", "image": "docker.io/rayproject/ray:2.56.0",
                           "imageID": IMAGE_ID, "containerID": "ray-container", "restartCount": 0},
            "collectorLogs": [{
                "containerID": "container", "restartCount": 0, "cpuRequest": "100m",
                "cpuLimit": "2", "memoryRequest": "128Mi", "memoryLimit": cfg["memoryLimit"],
                "gracefulShutdownComplete": True, "logStreamComplete": True,
                "diskPressure503s": 0, "rotationQueueFull": 0, "uploadFailures": 0,
                "memoryEventsOOM": 0, "memoryEventsOOMKill": 0, "cgroupMemoryReadErrors": 0,
                "cgroupMemoryMaxBytes": validator.parse_quantity_bytes(cfg["memoryLimit"]),
                "uploadedBytes": raw,
                "uploadTimeline": ([{"time": upload_time, "unixNano": idle + 5_000_000_000, "bytes": 100 * validator.MIB}] if high else [])
                              + [{"time": dt.datetime.fromtimestamp((shutdown + shutdown_upload_offset_seconds * 1_000_000_000) / 1e9, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z"), "unixNano": shutdown + shutdown_upload_offset_seconds * 1_000_000_000, "bytes": raw}],
                "finalCgroupMemory": {
                    "time": final_cgroup_time,
                    "unixNano": final_cgroup_nano,
                    "currentBytes": 80 * validator.MIB,
                    "peakBytes": peak_mib * validator.MIB,
                    "eventsMax": 0,
                    "eventsOOM": 0,
                    "eventsOOMKill": 0,
                    "psiFullTotalUsec": 0,
                },
                "mainReturn": {
                    "time": dt.datetime.fromtimestamp(
                        (final_cgroup_nano + 50_000_000) / 1e9,
                        tz=dt.timezone.utc,
                    ).isoformat().replace("+00:00", "Z"),
                    "unixNano": final_cgroup_nano + 50_000_000,
                    "exitCode": 0,
                    "reason": "Completed",
                },
            }],
            "remotePreflight": {"prefix": "fresh/job_events/01000000/", "objects": 0, "bytes": 0, "empty": True},
            "cgroupSampler": {"streamComplete": True},
            "memoryDetail": {"observed": True, "readErrors": 0, "samples": 100},
            "eventSpool": {"invalidSamples": 0, "samples": 100},
        }
        (run / "collector-memory-report.json").write_text(json.dumps(report))
        memory_rows: list[dict] = []
        spool_rows: list[dict] = []
        timestamp = baseline
        memory_max = str(validator.parse_quantity_bytes(cfg["memoryLimit"]))
        if sample_end_before_upload_ns is not None:
            sample_end = (
                shutdown
                + shutdown_upload_offset_seconds * 1_000_000_000
                - sample_end_before_upload_ns
            )
        else:
            sample_end = final_cgroup_nano if sample_through_shutdown else shutdown
        while timestamp <= sample_end:
            file_bytes = 50 * validator.MIB
            raw_spool = min(120 * validator.MIB, max(0, int((timestamp - ingest) / max(1, idle - ingest) * 120 * validator.MIB)))
            if high and timestamp >= idle + 6_000_000_000:
                file_bytes = 5 * validator.MIB
                raw_spool = 0
            memory_rows.append({
                "time_nano": timestamp, "container_id": "container", "container": "pod/collector",
                "current_bytes": 80 * validator.MIB, "peak_bytes": peak_mib * validator.MIB,
                "anon_bytes": 25 * validator.MIB, "file_bytes": file_bytes,
                "file_dirty_bytes": 0, "file_writeback_bytes": 0, "kernel_bytes": 5 * validator.MIB,
                "slab_bytes": validator.MIB, "memory_max": memory_max,
                "memory_events_low": 0, "memory_events_high": 0, "memory_events_max": 0,
                "memory_events_oom": 0, "memory_events_oom_kill": 0,
                "psi_some_total_usec": 0, "psi_full_total_usec": 0,
            })
            spool_rows.append({
                "time_nano": timestamp, "pod_uid": "pod-uid", "pod": "pod",
                "total_bytes": raw_spool, "raw_jsonl_bytes": raw_spool, "gzip_bytes": 0,
                "tmp_bytes": 0, "other_bytes": 0, "file_count": int(raw_spool > 0),
                "valid": "true", "error": "",
            })
            timestamp += 500_000_000
        write_csv(run / "collector_memory_samples.csv", validator.MEMORY_FIELDS, memory_rows)
        write_csv(run / "event_spool_samples.csv", validator.SPOOL_FIELDS, spool_rows)
        return report


class ValidatorContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fx = Fixture()

    def tearDown(self) -> None:
        self.fx.close()

    def validate(self, name: str) -> dict:
        provenance = validator.validate_provenance(self.fx.root)
        return validator.validate_arm(self.fx.root, arm(name), provenance)

    def test_actual_go_schema_accepts_a_b_negative_b_reclaim_and_c(self) -> None:
        for name in ("A-rate1000-r1", "B-rate2000-r1", "B-rate5000-r1", "C-limit192Mi-r1"):
            with self.subTest(name=name):
                self.fx.add(name)
                self.validate(name)

    def test_matrix_binding_fails_closed(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name, bad_config=True)
        with self.assertRaisesRegex(validator.ValidationError, "config/matrix binding"):
            self.validate(name)

    def test_remote_preflight_nonempty_fails_closed(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        report["remotePreflight"] = {"prefix": "fresh/", "objects": 1, "bytes": 895, "empty": False}
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "remote preflight"):
            self.validate(name)

    def test_runtime_image_aliases_are_exact_and_fail_closed(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        self.validate(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        report["runtime"]["image"] = "evil.example/library/collector:v0.1.0"
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "Collector image differs"):
            self.validate(name)
        self.assertFalse(validator.docker_hub_references_equal(None, None))
        self.assertFalse(validator.docker_hub_references_equal("", ""))
        self.assertFalse(validator.docker_hub_references_equal(
            "docker.io/library/rayproject/ray:2.56.0", "rayproject/ray:2.56.0"
        ))

    def test_spool_lifecycle_silence_fails_closed(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        run = self.fx.root / name / "run"
        spool_path = run / "event_spool_samples.csv"
        with spool_path.open() as stream:
            rows = list(csv.DictReader(stream))
        rows = rows[:2] + rows[-2:]
        spool_path.unlink()
        write_csv(spool_path, validator.SPOOL_FIELDS, rows)
        with self.assertRaisesRegex(validator.ValidationError, "spool (timestamp coverage|sample gap)"):
            self.validate(name)

    def test_exact_unix_nano_disambiguates_second_precision_log(self) -> None:
        name = "B-rate2000-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        shutdown = next(
            phase["timeNano"] for phase in report["phases"] if phase["name"] == "shutdown"
        )
        second_bucket = dt.datetime.fromtimestamp(
            shutdown / 1e9, tz=dt.timezone.utc
        ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        report["collectorLogs"][0]["uploadTimeline"] = [
            {"time": second_bucket, "unixNano": shutdown + 100_000_000,
             "bytes": report["replay"]["expected_jsonl_bytes"]}
        ]
        report_path.write_text(json.dumps(report))
        self.validate(name)

    def test_clock_skew_recomputation_fails_closed(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        report["clockSkew"]["after"]["skewNano"] = 1
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "clockSkew.after"):
            self.validate(name)

    def test_pre_post_clock_skew_intervals_must_overlap(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        sample = report["clockSkew"]["after"]
        sample["nodeUnixNano"] += 200_000_000
        midpoint = sample["hostBeforeUnixNano"] + sample["roundTripNano"] // 2
        sample["skewNano"] = sample["nodeUnixNano"] - midpoint
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "clock-skew intervals do not overlap"):
            self.validate(name)

    def test_clock_probes_must_bracket_phase_timeline(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        baseline = min(phase["timeNano"] for phase in report["phases"])
        sample = report["clockSkew"]["before"]
        sample["hostBeforeUnixNano"] = baseline + 100_000_000
        sample["hostAfterUnixNano"] = baseline + 200_000_000
        sample["nodeUnixNano"] = baseline + 150_000_000
        sample["roundTripNano"] = 100_000_000
        sample["skewNano"] = 0
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "do not bracket phase timeline"):
            self.validate(name)

    def test_final_in_process_cgroup_evidence_is_required(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        report["collectorLogs"][0].pop("finalCgroupMemory")
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "final cgroup memory evidence"):
            self.validate(name)

    def test_clean_main_return_evidence_is_required(self) -> None:
        name = "C-limit192Mi-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        report["collectorLogs"][0]["mainReturn"]["exitCode"] = 1
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "not clean completion"):
            self.validate(name)

    def test_final_cgroup_peak_cannot_be_below_sampled_kernel_peak(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        report["collectorLogs"][0]["finalCgroupMemory"]["peakBytes"] = 99 * validator.MIB
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "below sampled memory.peak"):
            self.validate(name)

    def test_clock_skew_rtt_above_250ms_fails_closed(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        sample = report["clockSkew"]["after"]
        sample["hostAfterUnixNano"] = sample["hostBeforeUnixNano"] + 250_000_001
        sample["roundTripNano"] = 250_000_001
        sample["skewNano"] = sample["nodeUnixNano"] - (
            sample["hostBeforeUnixNano"] + 125_000_000
        )
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "250ms RTT"):
            self.validate(name)

    def test_clock_skew_negative_rtt_fails_closed(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        sample = report["clockSkew"]["after"]
        sample["hostAfterUnixNano"] = sample["hostBeforeUnixNano"] - 1
        sample["roundTripNano"] = -1
        sample["skewNano"] = sample["nodeUnixNano"] - sample["hostBeforeUnixNano"]
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "250ms RTT"):
            self.validate(name)

    def test_gzip_ratio_definition_fails_closed(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        report["replay"]["fixture_gzip_ratio_definition"] = "savings_fraction"
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "gzip_ratio_definition"):
            self.validate(name)

    def test_clock_uncertainty_interval_rejects_midpoint_false_pass(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        sample = report["clockSkew"]["after"]
        sample["hostAfterUnixNano"] = sample["hostBeforeUnixNano"] + 200_000_000
        sample["roundTripNano"] = 200_000_000
        # Midpoint skew is only +950ms, but the uncertainty upper bound is
        # +1.05s. The old midpoint-only gate would have accepted this sample.
        sample["nodeUnixNano"] = sample["hostBeforeUnixNano"] + 1_050_000_000
        sample["skewNano"] = 950_000_000
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "uncertainty gate"):
            self.validate(name)

    def test_termination_grace_period_fails_closed(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        report["runtime"]["terminationGracePeriodSeconds"] = 30
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "termination grace"):
            self.validate(name)

    def test_timestamp_gap_fails_closed(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        csv_path = self.fx.root / name / "run" / "collector_memory_samples.csv"
        with csv_path.open() as stream:
            rows = list(csv.DictReader(stream))
        rows = [row for row in rows if not (START + 20_000_000_000 < int(row["time_nano"]) < START + 23_000_000_000)]
        csv_path.unlink()
        write_csv(csv_path, validator.MEMORY_FIELDS, rows)
        with self.assertRaisesRegex(validator.ValidationError, "gap exceeds"):
            self.validate(name)

    def test_candidate_uses_lifetime_peak_not_sampled_current(self) -> None:
        for repeat in range(1, 4):
            self.fx.add(f"A-rate5000-r{repeat}", peak_mib=100)
            self.fx.add(f"C-limit192Mi-r{repeat}", peak_mib=170)
        errors = validator.candidate_errors(self.fx.root, "192Mi")
        self.assertEqual(len(errors), 3)
        self.assertTrue(all("headroom-including-reclaimable-cache" in error for error in errors), errors)

    def test_candidate_records_composition_and_rejects_peak_spread(self) -> None:
        peaks = (100, 100, 120)
        for repeat, peak in enumerate(peaks, 1):
            self.fx.add(f"A-rate5000-r{repeat}", peak_mib=100)
            self.fx.add(f"C-limit192Mi-r{repeat}", peak_mib=peak)
        evaluation = validator.candidate_evaluation(self.fx.root, "192Mi")
        self.assertTrue(any("three-run memory.peak spread" in error for error in evaluation["errors"]), evaluation)
        valid = [item for item in evaluation["arms"] if item["classification"] == "valid-run"]
        self.assertEqual(len(valid), 3)
        self.assertEqual(valid[0]["peakComposition"]["anonBytes"], 25 * validator.MIB)
        self.assertIn("fileDirtyBytes", valid[0]["peakComposition"])

    def test_candidate_evaluation_accepts_recorded_hard_failure(self) -> None:
        self.fx.add_discovery()
        provenance = validator.validate_provenance(self.fx.root)
        for repeat in range(1, 4):
            name = f"C-limit192Mi-r{repeat}"
            report = self.fx.add(name)
            if repeat == 1:
                report_path = self.fx.root / name / "run" / "collector-memory-report.json"
                report["completed"] = False
                report["error"] = "rate sag"
                report["replay"]["achieved_events_per_second"] = 4000
                report_path.write_text(json.dumps(report))
                verdict = validator.classify_candidate_hard_failure(
                    self.fx.root, arm(name), provenance, 1
                )
                verdict_path = validator.candidate_arm_verdict_path(self.fx.root, name)
                validator.write_json_exclusive(verdict_path, verdict)
        evaluation = validator.candidate_evaluation(self.fx.root, "192Mi")
        self.assertEqual(evaluation["verdict"], "failed")
        self.assertEqual(evaluation["arms"][0]["classification"], "resource-failure")
        self.assertIn("hard resource failure", evaluation["errors"][0])

    def test_candidate_hard_failure_verdict_tampering_is_rejected(self) -> None:
        self.fx.add_discovery()
        provenance = validator.validate_provenance(self.fx.root)
        for repeat in range(1, 4):
            name = f"C-limit192Mi-r{repeat}"
            report = self.fx.add(name)
            if repeat == 1:
                report_path = self.fx.root / name / "run" / "collector-memory-report.json"
                report["completed"] = False
                report["error"] = "rate sag"
                report["replay"]["achieved_events_per_second"] = 4000
                report_path.write_text(json.dumps(report))
                verdict = validator.classify_candidate_hard_failure(
                    self.fx.root, arm(name), provenance, 1
                )
                verdict["reasons"] = ["remote-loss"]
                validator.write_json_exclusive(
                    validator.candidate_arm_verdict_path(self.fx.root, name), verdict
                )
        with self.assertRaisesRegex(validator.ValidationError, "verdict schema"):
            validator.candidate_evaluation(self.fx.root, "192Mi")

    def test_memory_must_reach_within_one_second_of_final_shutdown_upload(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name, shutdown_upload_offset_seconds=3, sample_through_shutdown=False)
        with self.assertRaisesRegex(validator.ValidationError, "ends more than 1s before final cgroup evidence"):
            self.validate(name)

    def test_terminal_pod_exit_may_end_sampling_just_before_final_upload_log(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(
            name,
            shutdown_upload_offset_seconds=1,
            sample_end_before_upload_ns=20_000_000,
        )
        self.validate(name)

    def test_upload_inside_shutdown_clock_uncertainty_is_rejected(self) -> None:
        name = "B-rate2000-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        shutdown = next(
            phase["timeNano"] for phase in report["phases"] if phase["name"] == "shutdown"
        )
        ambiguous = shutdown
        report["collectorLogs"][0]["uploadTimeline"] = [{
            "time": dt.datetime.fromtimestamp(
                ambiguous / 1e9, tz=dt.timezone.utc
            ).isoformat().replace("+00:00", "Z"),
            "unixNano": ambiguous,
            "bytes": report["replay"]["expected_jsonl_bytes"],
        }]
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "ambiguous across shutdown"):
            self.validate(name)

    def test_terminal_sample_in_shutdown_uncertainty_is_closed_by_final_evidence(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name, shutdown_upload_offset_seconds=1, sample_through_shutdown=False)
        self.validate(name)

    def test_final_evidence_must_reach_conservative_shutdown_boundary(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name, shutdown_upload_offset_seconds=1, sample_through_shutdown=False)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        shutdown = next(item["timeNano"] for item in report["phases"] if item["name"] == "shutdown")
        final = report["collectorLogs"][0]["finalCgroupMemory"]
        final["unixNano"] = shutdown - 1
        final["time"] = dt.datetime.fromtimestamp((shutdown - 1) / 1e9, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "precedes conservative shutdown boundary"):
            self.validate(name)

    def test_memory_series_must_reach_within_one_second_of_final_evidence(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name, shutdown_upload_offset_seconds=2, sample_through_shutdown=False)
        with self.assertRaisesRegex(validator.ValidationError, "ends more than 1s before final cgroup evidence"):
            self.validate(name)

    def test_final_psi_counter_cannot_precede_sampled_counter(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        run = self.fx.root / name / "run"
        with (run / "collector_memory_samples.csv").open() as stream:
            rows = list(csv.DictReader(stream))
        rows[-1]["psi_full_total_usec"] = "1"
        with (run / "collector_memory_samples.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        with self.assertRaisesRegex(validator.ValidationError, "PSI full total precedes"):
            self.validate(name)

    def test_final_psi_counter_must_be_absolute_zero(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        report["collectorLogs"][0]["finalCgroupMemory"]["psiFullTotalUsec"] = 1
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "PSI full pressure is nonzero"):
            self.validate(name)

    def test_c_candidate_final_pressure_is_an_outcome_not_corrupt_evidence(self) -> None:
        name = "C-limit192Mi-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        final = report["collectorLogs"][0]["finalCgroupMemory"]
        final["eventsMax"] = 1
        final["psiFullTotalUsec"] = 1
        report_path.write_text(json.dumps(report))
        validated = self.validate(name)
        self.assertEqual(validated["_events_max_delta"], 1)
        self.assertEqual(validated["_psi_full_delta"], 1)

    def test_c_candidate_final_pressure_fails_candidate_verdict(self) -> None:
        self.fx.add_discovery()
        for repeat in range(1, 4):
            self.fx.add(f"C-limit192Mi-r{repeat}")
        report_path = self.fx.root / "C-limit192Mi-r1" / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        final = report["collectorLogs"][0]["finalCgroupMemory"]
        final["eventsMax"] = 1
        final["psiFullTotalUsec"] = 1
        report_path.write_text(json.dumps(report))
        errors = validator.candidate_errors(self.fx.root, "192Mi")
        self.assertTrue(any("memory.events.max" in item for item in errors))
        self.assertTrue(any("PSI full pressure" in item for item in errors))

    def test_non_candidate_final_memory_max_must_be_absolute_zero(self) -> None:
        name = "A-rate1000-r1"
        self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        report["collectorLogs"][0]["finalCgroupMemory"]["eventsMax"] = 1
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "memory.events.max is nonzero"):
            self.validate(name)

    def test_hard_resource_failure_is_recoverable_but_integrity_loss_is_not(self) -> None:
        name = "C-limit192Mi-r1"
        report = self.fx.add(name)
        report_path = self.fx.root / name / "run" / "collector-memory-report.json"
        report["completed"] = False
        report["error"] = "achieved rate below target"
        report["replay"]["achieved_events_per_second"] = 4000
        report_path.write_text(json.dumps(report))
        provenance = validator.validate_provenance(self.fx.root)
        verdict = validator.classify_candidate_hard_failure(self.fx.root, arm(name), provenance, 1)
        self.assertEqual(verdict["classification"], "resource-failure")
        self.assertIn("rate-sag", verdict["reasons"])
        report["remote"]["lines"] -= 1
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "accepted event lines were lost"):
            validator.classify_candidate_hard_failure(self.fx.root, arm(name), provenance, 1)

    def test_campaign_records_lower_hard_failure_and_selects_next_limit(self) -> None:
        matrix = matrix_writer.build_matrix()
        for name in matrix["executionOrderAB"]:
            self.fx.add(name)
        provenance = validator.validate_provenance(self.fx.root)
        statuses: dict[str, int] = {name: 0 for name in matrix["executionOrderAB"]}
        for limit in ("192Mi", "256Mi"):
            for repeat in range(1, 4):
                name = f"C-limit{limit}-r{repeat}"
                report = self.fx.add(name)
                statuses[name] = 0
                if limit == "192Mi" and repeat == 1:
                    report_path = self.fx.root / name / "run" / "collector-memory-report.json"
                    report["completed"] = False
                    report["error"] = "rate sag"
                    report["replay"]["achieved_events_per_second"] = 4000
                    report_path.write_text(json.dumps(report))
                    hard = validator.classify_candidate_hard_failure(
                        self.fx.root, arm(name), provenance, 1
                    )
                    validator.write_json_exclusive(
                        validator.candidate_arm_verdict_path(self.fx.root, name), hard
                    )
                    statuses[name] = 1
        candidate_verdicts = []
        for limit in ("192Mi", "256Mi"):
            verdict = validator.candidate_evaluation(self.fx.root, limit)
            validator.write_json_exclusive(
                validator.candidate_verdict_path(self.fx.root, limit), verdict
            )
            candidate_verdicts.append(verdict)
        self.assertEqual(candidate_verdicts[0]["verdict"], "failed")
        self.assertEqual(candidate_verdicts[1]["verdict"], "passed")
        (self.fx.root / "status.txt").write_text("".join(
            f"{name} rc={rc} duration=1s\n" for name, rc in statuses.items()
        ))
        initial = (self.fx.root / "provenance.txt").read_text()
        (self.fx.root / "provenance-final.txt").write_text(initial + "revalidation_status=valid\n")
        completion = {
            "schemaVersion": 1,
            "selectedMemoryLimit": "256Mi",
            "executedArms": sorted(statuses),
            "candidateVerdicts": candidate_verdicts,
        }
        (self.fx.root / "completion.json").write_text(json.dumps(completion))
        self.assertEqual(
            validator.validate_campaign(self.fx.root, full=False), completion
        )

    def test_b_gates_are_conditional(self) -> None:
        low = "B-rate2000-r1"
        self.fx.add(low)
        report_path = self.fx.root / low / "run" / "collector-memory-report.json"
        report = json.loads(report_path.read_text())
        early = START + 1_000_000_000
        early_time = dt.datetime.fromtimestamp(
            early / 1e9, tz=dt.timezone.utc
        ).isoformat().replace("+00:00", "Z")
        report["collectorLogs"][0]["uploadTimeline"].insert(
            0, {"time": early_time, "unixNano": early, "bytes": 1}
        )
        report_path.write_text(json.dumps(report))
        with self.assertRaisesRegex(validator.ValidationError, "uploaded before shutdown"):
            self.validate(low)


if __name__ == "__main__":
    unittest.main()
