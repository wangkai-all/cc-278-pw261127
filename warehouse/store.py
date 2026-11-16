"""SQLite state store for tasks, shards, sample rows and full-query results.

One writer connection at a time (guarded by a lock), WAL mode for concurrent
readers, and short transactions.  Shard rows are checkpointed, so a failed
preview task resumes from completed files instead of resampling them.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('preview', 'full_query')),
    snapshot_id TEXT NOT NULL,
    method TEXT,
    seed TEXT,
    budget INTEGER,
    page_size INTEGER,
    filter_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL,
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    plan_json TEXT,
    stats_json TEXT,
    sample_digest TEXT,
    fault_after_files INTEGER,
    attempts INTEGER NOT NULL DEFAULT 0,
    input_hash TEXT
);
CREATE INDEX IF NOT EXISTS tasks_created ON tasks (created_at);
CREATE INDEX IF NOT EXISTS tasks_kind ON tasks (kind, status);

CREATE TABLE IF NOT EXISTS task_shards (
    task_id TEXT NOT NULL,
    path TEXT NOT NULL,
    seq INTEGER NOT NULL,
    status TEXT NOT NULL,
    rows INTEGER NOT NULL DEFAULT 0,
    stats_json TEXT,
    started_at TEXT,
    finished_at TEXT,
    error TEXT,
    PRIMARY KEY (task_id, path)
);

CREATE TABLE IF NOT EXISTS shard_rows (
    task_id TEXT NOT NULL,
    path TEXT NOT NULL,
    seq INTEGER NOT NULL,
    row_json TEXT NOT NULL,
    PRIMARY KEY (task_id, path, seq)
);

CREATE TABLE IF NOT EXISTS sample_rows (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    channel TEXT NOT NULL,
    kind TEXT,
    partition TEXT,
    row_json TEXT NOT NULL,
    PRIMARY KEY (task_id, seq)
);
CREATE INDEX IF NOT EXISTS sample_rows_kind ON sample_rows (task_id, kind);
CREATE INDEX IF NOT EXISTS sample_rows_channel ON sample_rows (task_id, channel);

CREATE TABLE IF NOT EXISTS full_query_rows (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    row_json TEXT NOT NULL,
    PRIMARY KEY (task_id, seq)
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at TEXT NOT NULL,
    level TEXT NOT NULL,
    task_id TEXT,
    message TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_at ON events (at);
"""


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Store:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.RLock()
        self._local = threading.local()
        with self._connect() as conn:
            conn.executescript(SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._connect()
            self._local.conn = conn
        return conn

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        conn = self.conn()
        with self._write_lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    # ---------------------------------------------------------------- events
    def log(self, level: str, message: str, task_id: str | None = None) -> None:
        with self.write() as conn:
            conn.execute(
                "INSERT INTO events (at, level, task_id, message) VALUES (?,?,?,?)",
                (now(), level, task_id, message),
            )

    def events(self, limit: int = 100, task_id: str | None = None) -> list[dict]:
        if task_id:
            rows = self.conn().execute(
                "SELECT * FROM events WHERE task_id = ? ORDER BY id DESC LIMIT ?",
                (task_id, limit),
            ).fetchall()
        else:
            rows = self.conn().execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    # ----------------------------------------------------------------- tasks
    def create_task(
        self,
        kind: str,
        snapshot_id: str,
        method: str | None,
        seed: str | None,
        budget: int | None,
        page_size: int | None,
        filter_obj: dict | None = None,
        fault_after_files: int | None = None,
    ) -> dict:
        task_id = uuid.uuid4().hex
        stamp = now()
        payload = {
            "kind": kind,
            "snapshot_id": snapshot_id,
            "method": method,
            "seed": seed,
            "budget": budget,
            "page_size": page_size,
            "filter": filter_obj or {},
        }
        from .util import canonical_json, sha256_text

        input_hash = sha256_text(canonical_json(payload))
        with self.write() as conn:
            conn.execute(
                """INSERT INTO tasks
                   (id, kind, snapshot_id, method, seed, budget, page_size,
                    filter_json, status, created_at, updated_at,
                    fault_after_files, input_hash)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    task_id,
                    kind,
                    snapshot_id,
                    method,
                    seed,
                    budget,
                    page_size,
                    canonical_json(filter_obj or {}),
                    "queued",
                    stamp,
                    stamp,
                    fault_after_files,
                    input_hash,
                ),
            )
        self.log("info", f"task created: {kind}", task_id)
        return self.get_task(task_id)

    def get_task(self, task_id: str) -> dict | None:
        row = self.conn().execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        return _task_dict(row) if row else None

    def list_tasks(self, limit: int = 50, kind: str | None = None) -> list[dict]:
        if kind:
            rows = self.conn().execute(
                "SELECT * FROM tasks WHERE kind = ? ORDER BY created_at DESC LIMIT ?",
                (kind, limit),
            ).fetchall()
        else:
            rows = self.conn().execute(
                "SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [_task_dict(r) for r in rows]

    def update_task(self, task_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = now()
        columns = ", ".join(f"{key} = ?" for key in fields)
        values = [fields[key] for key in fields]
        values.append(task_id)
        with self.write() as conn:
            conn.execute(f"UPDATE tasks SET {columns} WHERE id = ?", values)

    def mark_interrupted_tasks(self) -> int:
        """On process start, running tasks cannot still be running."""
        with self.write() as conn:
            rows = conn.execute(
                "SELECT id FROM tasks WHERE status IN ('queued','running','assembling')"
            ).fetchall()
            if not rows:
                return 0
            conn.execute(
                "UPDATE tasks SET status='interrupted', error=?, updated_at=? "
                "WHERE status IN ('queued','running','assembling')",
                (
                    "server restarted while the task was in flight; retry resumes "
                    "from completed file shards",
                    now(),
                ),
            )
        for row in rows:
            self.log("warn", "task interrupted by restart", row["id"])
        return len(rows)

    # ---------------------------------------------------------------- shards
    def shard_rows(self, task_id: str) -> list[dict]:
        rows = self.conn().execute(
            "SELECT * FROM task_shards WHERE task_id = ? ORDER BY seq", (task_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def shard_status(self, task_id: str) -> dict[str, dict]:
        return {r["path"]: r for r in self.shard_rows(task_id)}

    def begin_shard(self, task_id: str, path: str, seq: int) -> None:
        with self.write() as conn:
            conn.execute(
                """INSERT INTO task_shards (task_id, path, seq, status, started_at)
                   VALUES (?,?,?,'running',?)
                   ON CONFLICT(task_id, path) DO UPDATE SET status='running',
                   started_at=excluded.started_at, error=NULL""",
                (task_id, path, seq, now()),
            )

    def finish_shard(
        self, task_id: str, path: str, seq: int, rows: list[dict], stats: dict
    ) -> None:
        with self.write() as conn:
            conn.execute(
                "DELETE FROM shard_rows WHERE task_id = ? AND path = ?", (task_id, path)
            )
            conn.executemany(
                "INSERT INTO shard_rows (task_id, path, seq, row_json) VALUES (?,?,?,?)",
                [
                    (task_id, path, index, json.dumps(row, ensure_ascii=False))
                    for index, row in enumerate(rows)
                ],
            )
            conn.execute(
                """UPDATE task_shards SET status='complete', rows=?, stats_json=?,
                   finished_at=?, error=NULL WHERE task_id=? AND path=?""",
                (len(rows), json.dumps(stats), now(), task_id, path),
            )

    def fail_shard(self, task_id: str, path: str, error: str) -> None:
        with self.write() as conn:
            conn.execute(
                "UPDATE task_shards SET status='failed', error=?, finished_at=? "
                "WHERE task_id=? AND path=?",
                (error, now(), task_id, path),
            )

    def assembly_inputs(self, task_id: str) -> list[dict]:
        """Completed shards with their rows, in file order determined by seq."""
        shards = self.conn().execute(
            "SELECT * FROM task_shards WHERE task_id=? AND status='complete' ORDER BY seq",
            (task_id,),
        ).fetchall()
        out = []
        for shard in shards:
            rows = self.conn().execute(
                "SELECT row_json FROM shard_rows WHERE task_id=? AND path=? ORDER BY seq",
                (task_id, shard["path"]),
            ).fetchall()
            out.append(
                {
                    "path": shard["path"],
                    "seq": shard["seq"],
                    "stats": json.loads(shard["stats_json"] or "{}"),
                    "rows": [json.loads(r["row_json"]) for r in rows],
                }
            )
        return out

    # ------------------------------------------------------------- rows/pages
    def replace_sample_rows(self, task_id: str, rows: list[dict], sample_digest: str) -> None:
        with self.write() as conn:
            conn.execute("DELETE FROM sample_rows WHERE task_id=?", (task_id,))
            conn.executemany(
                """INSERT INTO sample_rows (task_id, seq, channel, kind, partition, row_json)
                   VALUES (?,?,?,?,?,?)""",
                [
                    (
                        task_id,
                        index,
                        row.get("channel", ""),
                        row.get("kind"),
                        row.get("partition"),
                        json.dumps(row, ensure_ascii=False),
                    )
                    for index, row in enumerate(rows)
                ],
            )
            conn.execute(
                "UPDATE tasks SET sample_digest=?, updated_at=? WHERE id=?",
                (sample_digest, now(), task_id),
            )

    def sample_count(self, task_id: str) -> int:
        row = self.conn().execute(
            "SELECT COUNT(*) AS n FROM sample_rows WHERE task_id=?", (task_id,)
        ).fetchone()
        return int(row["n"])

    def sample_page(self, task_id: str, offset: int, limit: int) -> list[dict]:
        rows = self.conn().execute(
            "SELECT row_json FROM sample_rows WHERE task_id=? ORDER BY seq LIMIT ? OFFSET ?",
            (task_id, limit, offset),
        ).fetchall()
        return [json.loads(r["row_json"]) for r in rows]

    def all_sample_rows(self, task_id: str) -> list[dict]:
        rows = self.conn().execute(
            "SELECT row_json FROM sample_rows WHERE task_id=? ORDER BY seq", (task_id,)
        ).fetchall()
        return [json.loads(r["row_json"]) for r in rows]

    def store_full_query(
        self, task_id: str, rows: list[dict], stats: dict
    ) -> None:
        with self.write() as conn:
            conn.execute("DELETE FROM full_query_rows WHERE task_id=?", (task_id,))
            conn.executemany(
                "INSERT INTO full_query_rows (task_id, seq, row_json) VALUES (?,?,?)",
                [
                    (task_id, index, json.dumps(row, ensure_ascii=False))
                    for index, row in enumerate(rows)
                ],
            )
            conn.execute(
                "UPDATE tasks SET stats_json=?, updated_at=? WHERE id=?",
                (json.dumps(stats), now(), task_id),
            )

    def full_query_rows(self, task_id: str, limit: int = 100) -> list[dict]:
        rows = self.conn().execute(
            "SELECT row_json FROM full_query_rows WHERE task_id=? ORDER BY seq LIMIT ?",
            (task_id, limit),
        ).fetchall()
        return [json.loads(r["row_json"]) for r in rows]

    def stats(self) -> dict:
        def count(table: str) -> int:
            return int(self.conn().execute(f"SELECT COUNT(*) n FROM {table}").fetchone()["n"])

        by_status = {
            r["status"]: r["n"]
            for r in self.conn().execute(
                "SELECT status, COUNT(*) n FROM tasks GROUP BY status"
            ).fetchall()
        }
        return {
            "tasks": count("tasks"),
            "tasks_by_status": by_status,
            "sample_rows": count("sample_rows"),
            "db_bytes": os.path.getsize(self.db_path) if self.db_path.exists() else 0,
        }


def _task_dict(row: sqlite3.Row) -> dict:
    data = dict(row)
    data["filter"] = json.loads(data.pop("filter_json") or "{}")
    plan = data.pop("plan_json", None)
    data["plan"] = json.loads(plan) if plan else None
    stats = data.pop("stats_json", None)
    data["stats"] = json.loads(stats) if stats else None
    if data.get("attempts") is None:
        data["attempts"] = 0
    return data
