"""Data generation, indexing and reproducibility tests."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from warehouse import config
from warehouse.catalog import Catalog
from warehouse.generate import bootstrap, generate_records
from warehouse.util import canonical_json, sha256_text


def dataset_digest(files) -> str:
    return sha256_text(
        canonical_json([[f.path, f.content_sha256, f.line_count] for f in files])
    )


class GeneratorTests(unittest.TestCase):
    def test_records_are_deterministic_for_a_seed(self):
        first = generate_records("us", "2026-11-15", 50, seed="abc")
        second = generate_records("us", "2026-11-15", 50, seed="abc")
        other = generate_records("us", "2026-11-15", 50, seed="xyz")
        self.assertEqual(first, second)
        self.assertNotEqual(first, other)
        ids = [r["event_id"] for r in first]
        self.assertEqual(len(ids), len(set(ids)))

    def test_campaign_partition_introduces_a_new_kind_and_schema(self):
        records = generate_records("eu", "2026-11-15", 400, seed="abc")
        kinds = {r["kind"] for r in records}
        self.assertIn("black_friday_doorbuster", kinds)
        self.assertTrue(all(r["schema_version"] == 2 for r in records))
        self.assertTrue(any(r["payload"].get("campaign") == "black_friday" for r in records))

    def test_bootstrap_rebuild_is_byte_identical(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            bootstrap(Path(a) / "data", records_per_partition=240, progress=lambda _m: None)
            bootstrap(Path(b) / "data", records_per_partition=240, progress=lambda _m: None)
            cat_a = Catalog(Path(a) / "data")
            cat_b = Catalog(Path(b) / "data")
            files_a = cat_a.ensure_indexes(cat_a.scan(use_cache=False))
            files_b = cat_b.ensure_indexes(cat_b.scan(use_cache=False))
            expected = (len(range(1, 15)) + 2) * len(config.REGIONS) * config.FILES_PER_PARTITION
            self.assertEqual(len(files_a), expected)
            self.assertEqual(dataset_digest(files_a), dataset_digest(files_b))
            self.assertEqual(
                [(f.path, f.content_sha256) for f in files_a],
                [(f.path, f.content_sha256) for f in files_b],
            )

    def test_index_counts_match_actual_lines_and_kinds(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "data"
            bootstrap(root, records_per_partition=120, progress=lambda _m: None)
            catalog = Catalog(root)
            files = catalog.ensure_indexes(catalog.scan(use_cache=False))
            for data_file in files[::17]:
                path = root / data_file.path
                raw_lines = path.read_bytes().splitlines()
                self.assertEqual(data_file.line_count, len(raw_lines))
                kinds = {}
                for line in raw_lines:
                    import json

                    kind = json.loads(line)["kind"]
                    kinds[kind] = kinds.get(kind, 0) + 1
                self.assertEqual(data_file.kinds, kinds)
                for kind, offsets in data_file.stats["kind_line_offsets"].items():
                    for index, offset in offsets:
                        self.assertTrue(0 <= index < len(raw_lines))
                        self.assertLess(offset, path.stat().st_size)

    def test_sparse_offsets_are_capped_for_common_kinds(self):
        from unittest import mock

        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            config, "INDEX_OFFSETS_PER_KIND", 5
        ):
            root = Path(tmp) / "data"
            bootstrap(root, records_per_partition=120, progress=lambda _m: None)
            catalog = Catalog(root)
            files = catalog.ensure_indexes(catalog.scan(use_cache=False))
            page_views = [f for f in files if "page_view" in f.kinds][0]
            self.assertGreater(page_views.kinds["page_view"], 5)
            self.assertEqual(
                len(page_views.stats["kind_line_offsets"]["page_view"]), 5
            )
            self.assertIn("page_view", page_views.stats["kind_offsets_capped"])
            # Every retained offset must still point at a real page_view line.
            path = root / page_views.path
            data = path.read_bytes()
            for index, offset in page_views.stats["kind_line_offsets"]["page_view"]:
                line = data[offset:].split(b"\n", 1)[0]
                import json

                self.assertEqual(json.loads(line)["kind"], "page_view")
                self.assertEqual(index, data[:offset].count(b"\n"))


if __name__ == "__main__":
    unittest.main()
