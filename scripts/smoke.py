#!/usr/bin/env python3
"""End-to-end smoke test against a real HTTP server on a throwaway base dir.

Run from the repository root:

    python3 scripts/smoke.py [--records 600] [--keep]

It bootstraps a synthetic warehouse, starts the service, then walks the flow a
browser would: preview -> token pages -> stats -> full query -> compare ->
retry a task that was failed on purpose -> append a partition -> merge.  Exits
non-zero on the first broken expectation.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.catalog import Catalog  # noqa: E402
from warehouse.generate import bootstrap_campaign, bootstrap_v1  # noqa: E402
from warehouse.service import App  # noqa: E402
from warehouse.web import serve  # noqa: E402


class Client:
    def __init__(self, base: str):
        self.base = base

    def call(self, method: str, path: str, body=None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.base + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.status, json.loads(response.read() or b"null")
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read() or b"null")

    def wait_task(self, task_id: str, timeout: float = 120.0) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            _, task = self.call("GET", f"/api/tasks/{task_id}")
            if task["status"] in ("complete", "failed", "interrupted"):
                return task
            time.sleep(0.02)
        raise SystemExit(f"task {task_id} did not finish")


def check(condition, message: str) -> None:
    if not condition:
        raise SystemExit("FAIL: " + message)
    print("ok   " + message)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=600)
    parser.add_argument("--keep", action="store_true", help="keep the temp base dir")
    args = parser.parse_args(argv)

    tmp = tempfile.TemporaryDirectory()
    base = Path(tmp.name)
    data_root = base / "data"
    # Same two-phase bootstrap as the CLI: a v1 baseline snapshot first, then
    # the campaign partitions, so "new since reference" is factual.
    bootstrap_v1(data_root, records_per_partition=args.records, progress=lambda _m: None)
    catalog = Catalog(data_root)
    baseline = catalog.ensure_indexes(catalog.scan(use_cache=False))
    catalog.commit_snapshot(baseline, note="bootstrap v1 baseline", author="bootstrap")
    bootstrap_campaign(data_root, records_per_partition=args.records, progress=lambda _m: None)
    indexed = catalog.ensure_indexes(catalog.scan(use_cache=False))
    catalog.ensure_current(indexed, note="campaign partitions")

    app = App(data_root, base / "state" / "state.db", base / "state" / "token.key")
    snapshot = app.bootstrap_if_empty()
    print(f"base={base} snapshot={snapshot['snapshot_id']} records={snapshot['record_count']}")

    server = serve(app, "127.0.0.1", 0)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client = Client(f"http://127.0.0.1:{port}")
    try:
        status, _ = client.call("GET", "/api/health")
        check(status == 200, "health endpoint")

        code, created = client.call(
            "POST",
            "/api/previews",
            {"method": "stratified_rare", "seed": "smoke", "budget": 160, "page_size": 10},
        )
        check(code == 202, "preview accepted")
        task = client.wait_task(created["task"]["id"])
        check(task["status"] == "complete", "preview completed")
        check(task["stats"]["actual_rows"] <= 160, "sample stays inside budget")
        check(task["stats"]["read"]["lines_read"] > 0, "physical reads are accounted")

        _, first = client.call("GET", f"/api/tasks/{task['id']}/rows?offset=0")
        _, second = client.call(
            "GET", f"/api/tasks/{task['id']}/rows?token={first['next_token']}"
        )
        check(second["paged_by_token"], "continuation page used the signed token")
        seen_a = {(r["source_path"], r["source_line"]) for r in first["rows"]}
        seen_b = {(r["source_path"], r["source_line"]) for r in second["rows"]}
        check(not (seen_a & seen_b), "pages do not overlap")
        check(
            second["sample_digest"] == first["sample_digest"],
            "pages belong to the same sample digest",
        )
        bad = first["next_token"].split(".")
        bad[1] = bad[1][:-2] + "ZZ"
        code, body = client.call("GET", f"/api/tasks/{task['id']}/rows?token={'.'.join(bad)}")
        check(code == 409 and body["code"] == "stale_page_token", "tampered token is refused")

        _, stats = client.call("GET", f"/api/tasks/{task['id']}/stats")
        check(stats["estimated"] is True and "not a full-query" in stats["not_full_result"],
              "stats are labelled as estimates")
        check(stats["coverage"]["uncovered_partitions"] is not None, "coverage gap is reported")
        check(stats["rare"]["rare_kinds"], "rare-kind coverage is reported")

        code, full = client.call("POST", "/api/full-queries", {"limit": 20})
        check(code == 202, "full query accepted")
        full = client.wait_task(full["id"])
        check(full["status"] == "complete", "full query completed")
        check(full["stats"]["exact"] is True, "full query is an exact scan")

        code, cmp = client.call("GET", f"/api/compare/{task['id']}/{full['id']}")
        check(code == 200 and cmp["comparison"]["estimable"], "preview vs full comparison")
        check(all(r["full_count"] >= 0 for r in cmp["comparison"]["rows"]), "compare rows are sane")

        # Deliberate failure, then retry reusing finished shards.
        _, broken = client.call(
            "POST",
            "/api/previews",
            {
                "method": "stratified",
                "seed": "smoke-retry",
                "budget": 160,
                "page_size": 10,
                "fault_after_files": 2,
            },
        )
        failed = client.wait_task(broken["task"]["id"])
        check(failed["status"] == "failed", "injected failure is surfaced")
        done_shards = [s for s in failed["shards"] if s["status"] == "complete"]
        code, _ = client.call("POST", f"/api/tasks/{failed['id']}/retry", {})
        check(code == 202, "retry accepted")
        retried = client.wait_task(failed["id"])
        check(retried["status"] == "complete" and retried["attempts"] == 1, "retry completed")
        check(len(done_shards) == 2, "two shards were already checkpointed before retry")

        code, added = client.call(
            "POST", "/api/partitions",
            {"date": "2026-11-18", "kind": "smoke_kind", "weight": 0.02, "records": 400},
        )
        check(code == 201 and added["kind"] == "smoke_kind", "new partition ingested")

        code, merged = client.call(
            "POST",
            "/api/merge",
            {"dates": ["2026-11-13", "2026-11-14"], "base_snapshot_id": added["snapshot_id"]},
        )
        check(code == 200 and merged["merged_partitions"], "merge published a snapshot")

        # The preview created before the merge still pages from its own snapshot.
        code, later = client.call(
            "GET", f"/api/tasks/{task['id']}/rows?token={second['next_token']}"
        )
        check(code == 200 and later["sample_digest"] == second["sample_digest"],
              "old token still pages the original sample after a merge")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        app.executor.shutdown(wait=True)
        if args.keep:
            print(f"kept {base}")
            tmp._finalizer.detach()  # type: ignore[attr-defined]
        else:
            tmp.cleanup()
    print("SMOKE PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
