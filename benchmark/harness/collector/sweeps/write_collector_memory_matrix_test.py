#!/usr/bin/env python3

from __future__ import annotations

import pathlib
import tempfile
import unittest

import write_collector_memory_matrix as matrix_writer


class CollectorMemoryMatrixTest(unittest.TestCase):
    def test_exact_abc_matrix(self) -> None:
        matrix = matrix_writer.build_matrix()
        self.assertEqual(matrix["schemaVersion"], 1)
        self.assertEqual(matrix["kind"], "collector-memory-abc")
        self.assertEqual(len(matrix["arms"]), 27)
        self.assertEqual(len(matrix["executionOrderAB"]), 18)
        self.assertEqual(
            [group["memoryLimit"] for group in matrix["candidateGroupsC"]],
            ["192Mi", "256Mi", "512Mi"],
        )
        self.assertEqual(matrix["acceptance"]["maximumMemoryPeakToLimitRatio"], 0.80)
        self.assertEqual(matrix["acceptance"]["maximumThreeRunPeakSpreadFraction"], 0.10)

        by_name = {item["name"]: item for item in matrix["arms"]}
        self.assertEqual(set(matrix["executionOrderAB"]), {
            f"{experiment}-rate{rate}-r{repeat}"
            for experiment, rates in (("A", (1000, 2000, 3000, 5000)), ("B", (2000, 5000)))
            for rate in rates
            for repeat in range(1, 4)
        })
        self.assertEqual(by_name["A-rate5000-r2"]["config"]["plannedEvents"], 450_000)
        self.assertEqual(by_name["B-rate2000-r3"]["config"]["plannedEvents"], 60_000)
        self.assertEqual(by_name["B-rate2000-r3"]["config"]["idleSeconds"], 60)
        self.assertEqual(by_name["C-limit192Mi-r1"]["config"]["memoryLimit"], "192Mi")

        for item in matrix["arms"]:
            config = item["config"]
            self.assertEqual(config["publisherMode"], "serial")
            self.assertEqual(config["batchEvents"], 1000)
            self.assertEqual(config["rotationIntervalSeconds"], 300)
            self.assertEqual(config["rotationCheckSeconds"], 30)
            self.assertEqual(config["maxFileSizeMiB"], 100)
            self.assertEqual(config["maxDiskMiB"], 1024)
            self.assertEqual(config["sampleIntervalMilliseconds"], 250)
            self.assertEqual(config["cpuRequest"], "100m")
            self.assertEqual(config["cpuLimit"], "2")
            self.assertEqual(config["memoryRequest"], "128Mi")
            self.assertEqual(config["fixtureSchema"], "single-task-lifecycle-category-v1")
            if config["experiment"] in {"A", "C"}:
                self.assertEqual(config["idleSeconds"], 15)
        self.assertEqual(
            by_name["B-rate2000-r1"]["config"]["idleGate"],
            "retained-file-plateau-negative-control",
        )
        self.assertEqual(
            by_name["B-rate5000-r1"]["config"]["idleGate"],
            "reclaim-after-delete",
        )

    def test_order_is_deterministic_and_interleaved(self) -> None:
        first = matrix_writer.build_matrix()["executionOrderAB"]
        second = matrix_writer.build_matrix()["executionOrderAB"]
        self.assertEqual(first, second)
        self.assertNotEqual(first[:6], sorted(first[:6]))
        for offset in range(0, 18, 6):
            repeats = {name.rsplit("-r", 1)[1] for name in first[offset : offset + 6]}
            self.assertEqual(repeats, {str(offset // 6 + 1)})

    def test_main_refuses_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "matrix.json"
            path.write_text("occupied")
            with self.assertRaises(SystemExit):
                old = matrix_writer.parse_args
                try:
                    matrix_writer.parse_args = lambda: type("Args", (), {"output": path})()
                    matrix_writer.main()
                finally:
                    matrix_writer.parse_args = old


if __name__ == "__main__":
    unittest.main()
