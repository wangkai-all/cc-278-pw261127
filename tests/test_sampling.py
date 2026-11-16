"""Sampling correctness: allocation, weights, coverage and read budget."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from warehouse import config
from warehouse.catalog import Catalog
from warehouse.generate import bootstrap
from warehouse.sampling import (
    SamplingError,
    _largest_remainder,
    _sample_indices,
    build_plan,
    execute_plan_file,
)


class AllocationTests(unittest.TestCase):
    def test_largest_remainder_respects_caps_and_total(self):
        alloc = _largest_remainder(10, [1, 1, 1], [2, 100, 100])
        self.assertEqual(sum(alloc), 10)
        self.assertLessEqual(alloc[0], 2)
        self.assertTrue(all(a >= 0 for a in alloc))

    def test_largest_remainder_impossible_target_does_not_overshoot(self):
        alloc = _largest_remainder(50, [5, 5], [7, 3])
        self.assertEqual(alloc, [7, 3])

    def test_sample_indices_are_unique_and_sorted(self):
        for count in (0, 1, 5, 50):
            picked = _sample_indices(50, count, __import__("random").Random(7))
            self.assertEqual(picked, sorted(set(picked)))
            self.assertEqual(len(picked), count)

    def test_sample_indices_deterministic_by_seed(self):
        import random

        a = _sample_indices(1000, 60, random.Random(42))
        b = _sample_indices(1000, 60, random.Random(42))
        self.assertEqual(a, b)


class SamplingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls.root = Path(cls._tmp.name) / "data"
        bootstrap(cls.root, records_per_partition=240, progress=lambda _m: None)
        cls.catalog = Catalog(cls.root)
        files = cls.catalog.ensure_indexes(cls.catalog.scan(use_cache=False))
        cls.snapshot = cls.catalog.ensure_current(files, note="test")
        cls.snapshot, cls.files = cls.catalog.resolve()

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def plan(self, method: str, budget: int, seed: str = "unit"):
        return build_plan(self.catalog, self.snapshot, self.files, method, seed, budget)

    def execute(self, plan):
        by_path = {f.path: f for f in self.files}
        rows = []
        stats = {}
        for file_plan in plan.files:
            got, got_stats = execute_plan_file(
                self.catalog, plan, by_path[file_plan.path], file_plan
            )
            rows.extend(got)
            stats[file_plan.path] = got_stats
        return rows, stats

    def test_all_methods_stay_within_budget_and_are_unique(self):
        for method in config.PREVIEW_METHODS:
            plan = self.plan(method, 150)
            rows, _ = self.execute(plan)
            keys = [(r["source_path"], r["source_line"]) for r in rows]
            self.assertEqual(len(keys), len(set(keys)), method)
            self.assertLessEqual(len(rows), 150, method)
            self.assertLessEqual(plan.planned, 150, method)

    def test_head_method_visits_the_physical_prefix(self):
        plan = self.plan("head", 90)
        rows, _ = self.execute(plan)
        by_path = {}
        for row in rows:
            by_path.setdefault(row["source_path"], []).append(row["source_line"])
        for path, lines in by_path.items():
            self.assertEqual(lines, list(range(len(lines))), path)
        self.assertTrue(all(r["weight"] is None for r in rows))

    def test_stratified_covers_every_partition_and_weights_sum_to_population(self):
        plan = self.plan("stratified", 300)
        self.assertEqual(plan.channels["representative"], 300)
        rows, _ = self.execute(plan)
        partitions = {r["partition"] for r in rows}
        self.assertEqual(len(partitions), len({(f.date, f.region) for f in self.files}))
        total = 0.0
        for row in rows:
            self.assertAlmostEqual(row["weight"], row["population"] / row["sample_n"], places=5)
            total += row["weight"] / row["sample_n"] * row["sample_n"]
        # Every file contributes its sampled mass; compare to file populations.
        by_file = {}
        for row in rows:
            by_file.setdefault(row["source_path"], 0)
            by_file[row["source_path"]] += 1
        expected = {f.path: f.line_count for f in self.files}
        for path, n in by_file.items():
            self.assertAlmostEqual(n * (expected[path] / n), expected[path])

    def test_rare_supplement_reaches_every_rare_kind(self):
        plan = self.plan("stratified_rare", 400)
        self.assertIn("black_friday_doorbuster", plan.rare_kinds)
        rows, _ = self.execute(plan)
        rare_rows = [r for r in rows if r["channel"] == "rare-supplement"]
        rare_kinds = {r["kind"] for r in rare_rows}
        self.assertEqual(rare_kinds, set(plan.rare_kinds))
        self.assertTrue(all(r["weight"] and r["weight"] > 0 for r in rare_rows))

    def test_uniform_is_proportional_and_can_starve_small_partitions(self):
        # Fewer sampled records than files: strictly proportional allocation
        # must leave some partitions with zero.
        plan = self.plan("uniform", 20)
        self.assertEqual(plan.channels["uniform"], 20)
        allocation = {f.path: f.rep_n for f in plan.files}
        self.assertEqual(sum(allocation.values()), 20)
        self.assertTrue(any(n == 0 for n in allocation.values()))
        coverage = plan.coverage["uncovered_partitions"]
        self.assertTrue(coverage)
        self.assertGreater(coverage[0]["population"], 0)

    def test_kind_method_uses_kind_frames(self):
        plan = self.plan("kind", 120)
        self.assertEqual(plan.channels["kind-stratum"], 120)
        rows, _ = self.execute(plan)
        self.assertTrue(all(r["channel"] == "kind-stratum" for r in rows))
        self.assertGreater(len({r["kind"] for r in rows}), 8)

    def test_increasing_budget_is_monotonic_in_plan_declared_coverage(self):
        small = self.plan("stratified", 60)
        large = self.plan("stratified", 600)
        self.assertGreaterEqual(large.planned, small.planned)
        self.assertLessEqual(
            len(large.coverage["uncovered_partitions"]),
            len(small.coverage["uncovered_partitions"]),
        )

    def test_invalid_method_and_budget_are_rejected(self):
        with self.assertRaises(SamplingError):
            self.plan("nonsense", 100)

    def test_same_seed_same_rows_different_seed_different_rows(self):
        a, _ = self.execute(self.plan("stratified_rare", 200, seed="one"))
        b, _ = self.execute(self.plan("stratified_rare", 200, seed="one"))
        c, _ = self.execute(self.plan("stratified_rare", 200, seed="two"))
        key = lambda rows: [(r["source_path"], r["source_line"]) for r in rows]
        self.assertEqual(key(a), key(b))
        self.assertNotEqual(key(a), key(c))

    def test_read_budget_is_far_below_full_scan(self):
        plan = self.plan("stratified_rare", 400)
        rows, stats = self.execute(plan)
        lines = sum(s["lines_read"] for s in stats.values())
        population = plan.population
        self.assertGreater(len(rows), 0)
        # Each sampled row can cost at most CHECKPOINT_STRIDE physical lines
        # plus one extra line for the requested position.
        self.assertLessEqual(lines, len(rows) * (config.CHECKPOINT_STRIDE + 2))
        self.assertLess(lines, population / 2)

    def test_head_can_miss_kinds_that_stratified_rare_finds(self):
        head_rows, _ = self.execute(self.plan("head", 300))
        rare_rows, _ = self.execute(self.plan("stratified_rare", 300))
        head_kinds = {r["kind"] for r in head_rows}
        rare_kinds = {r["kind"] for r in rare_rows}
        self.assertTrue(rare_kinds - head_kinds)


if __name__ == "__main__":
    unittest.main()
