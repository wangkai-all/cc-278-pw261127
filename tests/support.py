"""Test helpers: isolated temp warehouse with a small, fast corpus."""

from __future__ import annotations

import json
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from warehouse.generate import bootstrap_campaign, bootstrap_v1
from warehouse.service import App
from warehouse.web import serve


def seed_corpus(data_root: Path, records_per_partition: int) -> None:
    bootstrap_v1(data_root, records_per_partition=records_per_partition, progress=lambda _m: None)
    bootstrap_campaign(
        data_root, records_per_partition=records_per_partition, progress=lambda _m: None
    )


class WarehouseCase(unittest.TestCase):
    records_per_partition = 240

    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls.base = Path(cls._tmp.name)
        cls.data_root = cls.base / "data"
        seed_corpus(cls.data_root, cls.records_per_partition)

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def make_app(self, name: str) -> App:
        root = self.base / name
        if not (root / "data").exists():
            shutil.copytree(self.data_root, root / "data")
        app = App(root / "data", root / "state" / "state.db", root / "state" / "token.key")
        app.bootstrap_if_empty()
        return app

    def wait(self, task_id: str, app: App, timeout: float = 90.0) -> dict:
        import time

        deadline = time.time() + timeout
        while time.time() < deadline:
            task = app.store.get_task(task_id)
            if task["status"] in ("complete", "failed", "interrupted"):
                return task
            time.sleep(0.01)
        raise AssertionError(f"task {task_id} did not finish in {timeout}s")


class ApiCase(WarehouseCase):
    def setUp(self):
        self.app = self.make_app(self._testMethodName)
        self.server = serve(self.app, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.app.executor.shutdown(wait=True)

    def api(self, method: str, path: str, body=None) -> tuple[int, dict]:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status, json.loads(response.read() or b"null")
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            return exc.code, json.loads(raw or b"null")

    def create_preview(self, **kwargs) -> tuple[int, dict]:
        payload = {"method": "stratified_rare", "seed": "test-seed", "budget": 200, "page_size": 10}
        payload.update(kwargs)
        return self.api("POST", "/api/previews", payload)
