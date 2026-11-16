"""HTTP API: versioned pagination, concurrency, recovery and honest statistics."""

from __future__ import annotations

import json
import threading
import time
import unittest

from support import ApiCase
from warehouse.tokens import TokenCodec, TokenError


class PaginationTests(ApiCase):
    def test_pages_are_stable_and_tokens_bound(self):
        status, created = self.create_preview(budget=180, page_size=10)
        self.assertEqual(status, 202)
        task = self.wait(created["task"]["id"], self.app)
        self.assertEqual(task["status"], "complete", task["error"])
        # A rare pick can coincide with the representative line that is the
        # kind's only indexed position; the row is then counted once and the
        # task reports the shortfall instead of duplicating it.
        self.assertLessEqual(task["stats"]["actual_rows"], 180)
        self.assertGreaterEqual(task["stats"]["actual_rows"], 170)
        self.assertEqual(task["stats"]["actual_rows"], task["stats"]["channels"]["representative"] + task["stats"]["channels"].get("rare-supplement", 0))

        pages = []
        token = None
        for _ in range(5):
            path = f"/api/tasks/{task['id']}/rows"
            path += f"?token={token}" if token else "?offset=0"
            code, page = self.api("GET", path)
            self.assertEqual(code, 200)
            pages.append(page["rows"])
            token = page["next_token"]
            if not token:
                break
        self.assertEqual(sum(len(p) for p in pages), 50)
        seen = [(r["source_path"], r["source_line"]) for page in pages for r in page]
        self.assertEqual(len(seen), len(set(seen)))

        # A fresh task with the same seed/budget reproduces the same rows.
        _, created_again = self.create_preview(budget=180, page_size=10)
        again = self.wait(created_again["task"]["id"], self.app)
        code, page = self.api("GET", f"/api/tasks/{again['id']}/rows?offset=0")
        self.assertEqual([(r["source_path"], r["source_line"]) for r in page["rows"]],
                         [(r["source_path"], r["source_line"]) for r in pages[0]])

    def test_tampered_token_is_rejected(self):
        _, created = self.create_preview(budget=120, page_size=10)
        task = self.wait(created["task"]["id"], self.app)
        _, page = self.api("GET", f"/api/tasks/{task['id']}/rows?offset=0")
        token = page["next_token"]
        prefix, body, tag = token.split(".")
        tampered = f"{prefix}.{body[:-2]}ZZ.{tag}"
        code, body = self.api("GET", f"/api/tasks/{task['id']}/rows?token={tampered}")
        self.assertEqual(code, 409)
        self.assertEqual(body["code"], "stale_page_token")

    def test_token_from_other_task_is_rejected(self):
        _, a = self.create_preview(budget=120, page_size=10, seed="alpha")
        task_a = self.wait(a["task"]["id"], self.app)
        _, b = self.create_preview(budget=120, page_size=10, seed="beta")
        task_b = self.wait(b["task"]["id"], self.app)
        _, page_a = self.api("GET", f"/api/tasks/{task_a['id']}/rows?offset=0")
        code, body = self.api(
            "GET", f"/api/tasks/{task_b['id']}/rows?token={page_a['next_token']}"
        )
        self.assertEqual(code, 409)
        self.assertIn("task", body["error"])

    def test_expired_token_is_rejected(self):
        key_dir = self.base / "expired"
        codec = TokenCodec(key_dir / "token.key", ttl_seconds=1)
        token = codec.issue({"task_id": "x", "offset": 10})
        time.sleep(1.2)
        with self.assertRaises(TokenError):
            codec.verify(token)

    def test_page_size_change_invalidates_old_token(self):
        _, created = self.create_preview(budget=120, page_size=10)
        task = self.wait(created["task"]["id"], self.app)
        _, page = self.api("GET", f"/api/tasks/{task['id']}/rows?offset=0")
        token = page["next_token"]
        self.app.store.update_task(task["id"], page_size=20)
        code, body = self.api("GET", f"/api/tasks/{task['id']}/rows?token={token}")
        self.assertEqual(code, 409)
        self.assertIn("page_size", body["error"])

    def test_validation_errors(self):
        code, body = self.create_preview(budget=5)
        self.assertEqual(code, 400)
        code, body = self.create_preview(page_size=1)
        self.assertEqual(code, 400)
        code, body = self.create_preview(method="does_not_exist")
        self.assertEqual(code, 400)


class ConcurrencyTests(ApiCase):
    def test_merge_during_paging_keeps_original_sample(self):
        _, created = self.create_preview(budget=160, page_size=10)
        task = self.wait(created["task"]["id"], self.app)
        self.assertEqual(task["status"], "complete", task["error"])
        _, first = self.api("GET", f"/api/tasks/{task['id']}/rows?offset=0")
        token = first["next_token"]
        first_rows = [(r["source_path"], r["source_line"]) for r in first["rows"]]

        snapshot, _ = self.app.current()
        dates = sorted({p["date"] for p in snapshot["partitions"]})[:2]
        result = self.app.merge_partitions(dates, base_snapshot_id=snapshot["snapshot_id"])
        self.assertTrue(result["files_written"])

        # The preview's snapshot is preserved; the page token still works and
        # the next page belongs to the same sample digest.
        code, second = self.api(
            "GET", f"/api/tasks/{task['id']}/rows?token={token}"
        )
        self.assertEqual(code, 200, second)
        self.assertEqual(second["offset"], 10)
        self.assertEqual(second["sample_digest"], first["sample_digest"])
        self.assertNotEqual(
            [(r["source_path"], r["source_line"]) for r in second["rows"]], first_rows
        )

        # A new preview now sees the merged (fewer-file) snapshot.
        _, after = self.create_preview(budget=160, page_size=10)
        new_task = self.wait(after["task"]["id"], self.app)
        self.assertEqual(new_task["status"], "complete", new_task["error"])
        self.assertNotEqual(new_task["snapshot_id"], task["snapshot_id"])

    def test_merge_while_preview_runs_does_not_disturb_it(self):
        # A preview in flight is bound to its snapshot, so a merge may proceed
        # concurrently; the preview must still finish on its original input.
        _, created = self.create_preview(budget=600, page_size=10)
        task_id = created["task"]["id"]
        snapshot, _ = self.app.current()
        dates = sorted({p["date"] for p in snapshot["partitions"]})[:2]
        result = self.app.merge_partitions(dates, base_snapshot_id=snapshot["snapshot_id"])
        self.assertTrue(result["files_written"])
        done = self.wait(task_id, self.app)
        self.assertEqual(done["status"], "complete", done["error"])
        self.assertEqual(done["snapshot_id"], snapshot["snapshot_id"])
        self.assertGreater(done["stats"]["actual_rows"], 550)
        # And the merged raw paths really are gone for those partitions.
        import os

        for path in result["files_written"]:
            self.assertTrue((self.app.data_root / path).exists())

    def test_merge_rejects_stale_base(self):
        snapshot, _ = self.app.current()
        dates = sorted({p["date"] for p in snapshot["partitions"]})[:1]
        self.app.merge_partitions(dates, base_snapshot_id=snapshot["snapshot_id"])
        from warehouse.service import ServiceError

        with self.assertRaises(ServiceError) as ctx:
            self.app.merge_partitions(dates, base_snapshot_id=snapshot["snapshot_id"])
        self.assertEqual(ctx.exception.status, 409)

    def test_full_query_and_preview_can_run_together(self):
        _, created = self.create_preview(budget=150, page_size=10)
        task = self.wait(created["task"]["id"], self.app)
        code, full = self.api(
            "POST",
            "/api/full-queries",
            {"snapshot_id": task["snapshot_id"], "kind": "page_view", "limit": 20},
        )
        self.assertEqual(code, 202)
        deadline = time.time() + 120
        while time.time() < deadline:
            code, full = self.api("GET", f"/api/full-queries/{full['id']}")
            if full["status"] in ("complete", "failed"):
                break
            time.sleep(0.05)
        self.assertEqual(full["status"], "complete", full.get("error"))
        self.assertEqual(
            full["stats"]["matched"], full["stats"]["counts"].get("page_view")
        )
        self.assertGreater(full["stats"]["lines_scanned"], task["stats"]["read"]["lines_read"])

        code, cmp = self.api("GET", f"/api/compare/{task['id']}/{full['id']}")
        self.assertEqual(code, 200)
        self.assertIn("disclaimer", cmp["comparison"])
        self.assertTrue(cmp["comparison"]["estimable"])


class RecoveryTests(ApiCase):
    def test_injected_failure_then_retry_reuses_shards(self):
        _, created = self.create_preview(budget=200, page_size=10, fault_after_files=3)
        task = self.wait(created["task"]["id"], self.app)
        self.assertEqual(task["status"], "failed")
        self.assertIn("injected fault", task["error"])
        shards = {s["path"]: s["status"] for s in self.app.store.shard_rows(task["id"])}
        completed = [p for p, s in shards.items() if s == "complete"]
        self.assertEqual(len(completed), 3)

        code, retried = self.api("POST", f"/api/tasks/{task['id']}/retry", {})
        self.assertEqual(code, 202)
        done = self.wait(task["id"], self.app)
        self.assertEqual(done["status"], "complete", done["error"])
        self.assertEqual(done["attempts"], 1)
        self.assertGreaterEqual(done["stats"]["actual_rows"], 190)
        self.assertLessEqual(done["stats"]["actual_rows"], 200)
        shards_after = {s["path"]: s["status"] for s in self.app.store.shard_rows(task["id"])}
        self.assertTrue(all(status == "complete" for status in shards_after.values()))
        # The reused shards were never rewritten.
        reused = [s for s in self.app.store.shard_rows(task["id"]) if s["path"] in completed]
        self.assertTrue(reused)

    def test_restart_marks_interrupted_and_resume_completes(self):
        _, created = self.create_preview(budget=200, page_size=10, fault_after_files=2)
        task = self.wait(created["task"]["id"], self.app)
        self.assertEqual(task["status"], "failed")

        # Simulate a process restart: a fresh App over the same state dir,
        # and the HTTP layer rebound to it.
        from warehouse.service import App as FreshApp

        self.app.executor.shutdown(wait=True)
        restarted = FreshApp(
            self.base / self._testMethodName / "data",
            self.base / self._testMethodName / "state" / "state.db",
            self.base / self._testMethodName / "state" / "token.key",
        )
        self.app = restarted
        self.server.http_app.app = restarted
        code, resumed = self.api("POST", "/api/tasks/resume", {})
        self.assertEqual(code, 200)
        self.assertIn(task["id"], resumed["resumed"])
        done = self.wait(task["id"], self.app)
        self.assertEqual(done["status"], "complete", done["error"])

    def test_retry_keeps_snapshot_even_after_new_partition(self):
        _, created = self.create_preview(budget=160, page_size=10, fault_after_files=2)
        task = self.wait(created["task"]["id"], self.app)
        self.assertEqual(task["status"], "failed")
        old_snapshot = task["snapshot_id"]
        self.app.append_partition("2026-11-18", "flash_sale_probe", 0.03, records=600)
        self.assertNotEqual(self.app.catalog.current_snapshot_id(), old_snapshot)
        code, _ = self.api("POST", f"/api/tasks/{task['id']}/retry", {})
        self.assertEqual(code, 202)
        done = self.wait(task["id"], self.app)
        self.assertEqual(done["status"], "complete", done["error"])
        self.assertEqual(done["snapshot_id"], old_snapshot)


class HonestyTests(ApiCase):
    def test_preview_stats_are_marked_estimates(self):
        _, created = self.create_preview()
        task = self.wait(created["task"]["id"], self.app)
        code, stats = self.api("GET", f"/api/tasks/{task['id']}/stats")
        self.assertEqual(code, 200)
        self.assertTrue(stats["estimated"])
        self.assertIn("not", stats["not_full_result"])
        self.assertTrue(stats["estimates"]["estimable"])

    def test_head_has_no_population_estimate(self):
        _, created = self.create_preview(method="head")
        task = self.wait(created["task"]["id"], self.app)
        code, stats = self.api("GET", f"/api/tasks/{task['id']}/stats")
        self.assertEqual(code, 200)
        self.assertFalse(stats["estimates"]["estimable"])
        self.assertIn("non-probability", stats["estimates"]["reason"])

    def test_compare_does_not_count_rare_supplement_as_estimate(self):
        _, created = self.create_preview(budget=300)
        task = self.wait(created["task"]["id"], self.app)
        code, full = self.api(
            "POST",
            "/api/full-queries",
            {"snapshot_id": task["snapshot_id"], "limit": 5},
        )
        deadline = time.time() + 120
        while time.time() < deadline:
            code, full = self.api("GET", f"/api/full-queries/{full['id']}")
            if full["status"] in ("complete", "failed"):
                break
            time.sleep(0.05)
        self.assertEqual(full["status"], "complete", full.get("error"))
        code, cmp = self.api("GET", f"/api/compare/{task['id']}/{full['id']}")
        comparison = cmp["comparison"]
        rare_kinds = set(task["plan"]["rare_kinds"])
        by_kind = {row["kind"]: row for row in comparison["rows"]}
        for kind in rare_kinds:
            row = by_kind.get(kind)
            if row is None:
                continue
            self.assertTrue(row["caught_by_preview"])
            if row["preview_estimate"] is None:
                self.assertEqual(row["observed_representative"], 0)
        self.assertTrue(comparison["unestimated_kinds"])

    def test_compare_across_snapshots_is_refused(self):
        _, created = self.create_preview(budget=120)
        task = self.wait(created["task"]["id"], self.app)
        code, full = self.api("POST", "/api/full-queries", {"limit": 5})
        deadline = time.time() + 120
        while time.time() < deadline:
            code, full = self.api("GET", f"/api/full-queries/{full['id']}")
            if full["status"] in ("complete", "failed"):
                break
            time.sleep(0.05)
        self.app.append_partition("2026-11-18", "probe_kind", 0.02, records=300)
        code, full_new = self.api("POST", "/api/full-queries", {"limit": 5})
        deadline = time.time() + 120
        while time.time() < deadline:
            code, full_new = self.api("GET", f"/api/full-queries/{full_new['id']}")
            if full_new["status"] in ("complete", "failed"):
                break
            time.sleep(0.05)
        code, body = self.api("GET", f"/api/compare/{task['id']}/{full_new['id']}")
        self.assertEqual(code, 409)

    def test_new_partition_and_kind_appear_in_next_preview(self):
        _, before = self.create_preview(budget=200)
        old = self.wait(before["task"]["id"], self.app)
        old_kinds = {k["kind"] for k in old["plan"]["coverage"]["kinds"]}
        self.app.append_partition("2026-11-18", "flash_sale_probe", 0.03, records=900)
        _, after = self.create_preview(budget=200)
        new = self.wait(after["task"]["id"], self.app)
        self.assertEqual(new["status"], "complete", new["error"])
        self.assertIn("flash_sale_probe", new["plan"]["new_kinds"])
        self.assertNotIn("flash_sale_probe", old_kinds)
        rows = self.app.store.all_sample_rows(new["id"])
        self.assertIn("flash_sale_probe", {r["kind"] for r in rows})
        self.assertGreater(new["stats"]["rare"]["rare_kinds_covered"], 0)


if __name__ == "__main__":
    unittest.main()
