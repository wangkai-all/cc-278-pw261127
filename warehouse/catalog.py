"""Catalog of raw NDJSON files and immutable, content-addressed snapshots.

A snapshot is the set of files plus their hashes and line counts at a moment
in time.  Preview tasks bind to a snapshot id, so ingest, a new partition, or
a merge never silently changes what a continued page shows.

Files are additionally pinned in a content-addressed blob store (`_blobs/`)
with hard links.  A merge may remove or replace raw paths; snapshots that
referenced those paths still resolve to the exact bytes they were committed
with.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import os
import re
import shutil
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from . import config
from .util import atomic_write_json, canonical_json, file_sha256, sha256_text

FILE_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})/(?P<region>[a-z0-9_-]+)/events-(?P<index>\d{5})(?:-merged)?\.ndjson$"
)


class SnapshotIntegrityError(RuntimeError):
    """Committed bytes are no longer available or no longer match."""


class SnapshotConflict(RuntimeError):
    """A write was based on a snapshot that is no longer current."""


@dataclass(frozen=True)
class DataFile:
    path: str          # logical path relative to the data root, POSIX separators
    date: str
    region: str
    file_index: int
    size: int
    mtime_ns: int
    content_sha256: str
    stats: dict = field(default_factory=dict)

    @property
    def line_count(self) -> int:
        return int(self.stats.get("line_count", 0))

    @property
    def kinds(self) -> dict:
        return dict(self.stats.get("kinds", {}))

    def with_stats(self, stats: dict) -> "DataFile":
        return DataFile(
            path=self.path,
            date=self.date,
            region=self.region,
            file_index=self.file_index,
            size=self.size,
            mtime_ns=self.mtime_ns,
            content_sha256=self.content_sha256,
            stats=stats,
        )


def fingerprint_files(files: Iterable[DataFile]) -> str:
    payload = [
        {
            "path": f.path,
            "sha256": f.content_sha256,
            "size": f.size,
            "line_count": f.line_count,
        }
        for f in sorted(files, key=lambda f: (f.date, f.region, f.file_index))
    ]
    return sha256_text(canonical_json(payload))


class Catalog:
    def __init__(self, data_root: Path):
        self.data_root = Path(data_root)
        self.catalog_root = self.data_root / "_catalog"
        self.snapshots_root = self.catalog_root / "snapshots"
        self.index_root = self.catalog_root / "index"
        self.blobs_root = self.data_root / "_blobs"
        self.staging_root = self.catalog_root / "staging"
        for path in (self.snapshots_root, self.index_root, self.blobs_root, self.staging_root):
            path.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.scan_lock = threading.RLock()
        self._current_file = self.catalog_root / "CURRENT"
        self._scan_cache: dict[str, tuple[int, int, str]] = {}

    # ------------------------------------------------------------- scanning
    def scan(self, use_cache: bool = True) -> list[DataFile]:
        with self.scan_lock:
            return self._scan_unlocked(use_cache)

    def _scan_unlocked(self, use_cache: bool) -> list[DataFile]:
        files: list[DataFile] = []
        if not self.data_root.exists():
            return files
        for dirpath, dirnames, filenames in os.walk(self.data_root):
            base = Path(dirpath)
            dirnames[:] = sorted(
                d for d in dirnames if not d.startswith("_") and not d.startswith(".")
            )
            for name in sorted(filenames):
                if not name.endswith(".ndjson") or name.startswith("."):
                    continue
                full = base / name
                rel = full.relative_to(self.data_root).as_posix()
                match = FILE_RE.match(rel)
                if not match:
                    continue
                stat = full.stat()
                digest = ""
                cached = self._scan_cache.get(rel)
                if use_cache and cached and cached[0] == stat.st_size and cached[1] == stat.st_mtime_ns:
                    digest = cached[2]
                else:
                    digest = file_sha256(full)
                    self._scan_cache[rel] = (stat.st_size, stat.st_mtime_ns, digest)
                stats = self._load_index(digest) if digest else {}
                files.append(
                    DataFile(
                        path=rel,
                        date=match.group("date"),
                        region=match.group("region"),
                        file_index=int(match.group("index")),
                        size=stat.st_size,
                        mtime_ns=stat.st_mtime_ns,
                        content_sha256=digest,
                        stats=stats,
                    )
                )
        files.sort(key=lambda f: (f.date, f.region, f.file_index))
        return files

    # --------------------------------------------------------------- blobs
    def blob_path(self, digest: str) -> Path:
        return self.blobs_root / digest[:2] / digest

    def pin(self, data_file: DataFile, source: Path | None = None) -> Path:
        """Make the committed bytes durable and independent of the raw path."""
        blob = self.blob_path(data_file.content_sha256)
        if blob.exists():
            return blob
        source = source or (self.data_root / data_file.path)
        if not source.exists():
            raise SnapshotIntegrityError(
                f"cannot pin {data_file.path}: raw file and blob are both missing"
            )
        blob.parent.mkdir(parents=True, exist_ok=True)
        tmp = blob.with_name(f".tmp-{os.getpid()}-{threading.get_ident()}")
        try:
            try:
                os.link(source, tmp)
            except OSError:
                shutil.copyfile(source, tmp)
                with tmp.open("rb") as handle:
                    os.fsync(handle.fileno())
            os.replace(tmp, blob)
        except BaseException:
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass
            raise
        return blob

    def storage_path(self, data_file: DataFile, verify: bool = False) -> Path:
        """Return a path whose bytes match the committed content hash."""
        raw = self.data_root / data_file.path
        if raw.exists():
            stat = raw.stat()
            size_ok = stat.st_size == data_file.size
            mtime_ok = data_file.mtime_ns == 0 or stat.st_mtime_ns == data_file.mtime_ns
            if size_ok and mtime_ok and not verify:
                return raw
            digest = file_sha256(raw)
            if digest == data_file.content_sha256 and size_ok:
                return raw
        blob = self.blob_path(data_file.content_sha256)
        if blob.exists():
            return blob
        raise SnapshotIntegrityError(
            f"committed bytes for {data_file.path} ({data_file.content_sha256[:12]}) are unavailable"
        )

    def open_data(self, data_file: DataFile):
        path = self.storage_path(data_file)
        return path.open("rb")

    # --------------------------------------------------------------- index
    def index_path(self, digest: str) -> Path:
        return self.index_root / digest[:2] / f"{digest}.json"

    def _load_index(self, digest: str) -> dict:
        if not digest:
            return {}
        try:
            payload = json.loads(self.index_path(digest).read_text("utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
        if payload.get("content_sha256") != digest:
            return {}
        return payload

    def load_index(self, digest: str) -> dict | None:
        stats = self._load_index(digest)
        return stats or None

    def build_index(
        self,
        source: Path,
        digest: str,
        stride: int | None = None,
    ) -> dict:
        """Stream a file once: line count, kind histogram, sparse positions.

        Per-kind positions use a deterministic bottom-k sample keyed by
        (kind, occurrence, content hash): each retained line is a uniform
        sparse sample of that kind's positions, reproducible from the bytes
        alone.  A single 64-line checkpoint array supports line seeks.
        """
        stride = stride or config.CHECKPOINT_STRIDE
        capacity = config.INDEX_OFFSETS_PER_KIND
        checkpoint_cap = 4096
        line_count = 0
        byte_count = 0
        kinds: dict[str, int] = {}
        checkpoints: list[list[int]] = []
        checkpoints_truncated = False
        heaps: dict[str, list[tuple[int, int, int]]] = {}
        with source.open("rb") as handle:
            while True:
                start = handle.tell()
                line = handle.readline()
                if not line:
                    break
                index = line_count
                line_count += 1
                byte_count += len(line)
                if index % stride == 0:
                    checkpoints.append([index, start])
                    if len(checkpoints) > checkpoint_cap:
                        checkpoints = checkpoints[::2]
                        stride *= 2
                        checkpoints_truncated = True
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    kinds["__invalid__"] = kinds.get("__invalid__", 0) + 1
                    continue
                kind = str(record.get("kind", "__missing__"))
                kinds[kind] = kinds.get(kind, 0) + 1
                seen = kinds[kind]
                key = int(
                    hashlib.sha256(f"{kind}\x1f{seen}\x1f{digest}".encode()).hexdigest()[:16],
                    16,
                )
                heap = heaps.setdefault(kind, [])
                if len(heap) < capacity:
                    heapq.heappush(heap, (-key, index, start))
                elif key < -heap[0][0]:
                    heapq.heapreplace(heap, (-key, index, start))
        stats = {
            "content_sha256": digest,
            "line_count": line_count,
            "byte_count": byte_count,
            "kinds": dict(sorted(kinds.items())),
            "kind_line_offsets": {
                kind: [[index, offset] for _, index, offset in sorted(heap, key=lambda t: t[1])]
                for kind, heap in sorted(heaps.items())
            },
            "kind_offsets_capped": sorted(
                kind for kind, count in kinds.items() if count > capacity
            ),
            "checkpoints": checkpoints,
            "checkpoint_stride": stride,
            "checkpoints_truncated": checkpoints_truncated,
            "index_version": 2,
        }
        atomic_write_json(self.index_path(digest), stats)
        return stats

    def ensure_index(self, data_file: DataFile) -> DataFile:
        if not data_file.content_sha256:
            raise ValueError("cannot index a file without a content hash")
        stats = self._load_index(data_file.content_sha256)
        if not stats:
            source = self.storage_path(data_file, verify=True)
            stats = self.build_index(source, data_file.content_sha256)
        if stats == data_file.stats:
            return data_file
        return data_file.with_stats(stats)

    def ensure_indexes(self, files: Iterable[DataFile]) -> list[DataFile]:
        return [self.ensure_index(f) for f in files]

    def index_matches(self, data_file: DataFile) -> bool:
        index = self._load_index(data_file.content_sha256)
        return bool(index) and index.get("line_count") == data_file.line_count

    # ----------------------------------------------------------- snapshots
    def current_snapshot_id(self) -> str | None:
        try:
            value = self._current_file.read_text("utf-8").strip()
        except FileNotFoundError:
            return None
        return value or None

    def snapshot_path(self, snapshot_id: str) -> Path:
        return self.snapshots_root / f"{snapshot_id}.json"

    def load_snapshot(self, snapshot_id: str) -> dict | None:
        try:
            return json.loads(self.snapshot_path(snapshot_id).read_text("utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def snapshot_id_for(self, files: Iterable[DataFile]) -> str:
        return fingerprint_files(files)[:24]

    def _snapshot_rows(self, files: list[DataFile]) -> list[dict]:
        return [
            {
                "path": f.path,
                "date": f.date,
                "region": f.region,
                "file_index": f.file_index,
                "size": f.size,
                "mtime_ns": f.mtime_ns,
                "line_count": f.line_count,
                "sha256": f.content_sha256,
                "kinds": f.kinds,
            }
            for f in files
        ]

    def commit_snapshot(
        self,
        files: list[DataFile],
        note: str,
        author: str = "system",
        sources: dict[str, Path] | None = None,
    ) -> dict:
        with self.lock:
            files = sorted(files, key=lambda f: (f.date, f.region, f.file_index))
            sources = sources or {}
            for data_file in files:
                self.pin(data_file, sources.get(data_file.content_sha256))
            snapshot_id = self.snapshot_id_for(files)
            path = self.snapshot_path(snapshot_id)
            if path.exists():
                return json.loads(path.read_text("utf-8"))
            partitions: dict[str, dict] = {}
            for f in files:
                key = f"{f.date}/{f.region}"
                entry = partitions.setdefault(
                    key, {"date": f.date, "region": f.region, "files": 0, "records": 0}
                )
                entry["files"] += 1
                entry["records"] += f.line_count
            snapshot = {
                "snapshot_id": snapshot_id,
                "created_at": _now(),
                "note": note,
                "author": author,
                "file_count": len(files),
                "record_count": sum(f.line_count for f in files),
                "partitions": [partitions[k] for k in sorted(partitions)],
                "files": self._snapshot_rows(files),
            }
            atomic_write_json(path, snapshot)
            atomic_write_json(self.snapshots_root / "index.json", self._snapshot_index())
            return snapshot

    def _snapshot_index(self) -> list[dict]:
        rows = []
        for path in sorted(self.snapshots_root.glob("*.json")):
            if path.name == "index.json":
                continue
            try:
                data = json.loads(path.read_text("utf-8"))
            except json.JSONDecodeError:
                continue
            rows.append(
                {
                    "snapshot_id": data.get("snapshot_id"),
                    "created_at": data.get("created_at"),
                    "note": data.get("note"),
                    "file_count": data.get("file_count"),
                    "record_count": data.get("record_count"),
                    "author": data.get("author"),
                }
            )
        rows.sort(key=lambda r: (r.get("created_at") or "", r.get("snapshot_id") or ""))
        return rows

    def list_snapshots(self) -> list[dict]:
        return self._snapshot_index()

    def set_current(self, snapshot_id: str) -> None:
        if not self.snapshot_path(snapshot_id).exists():
            raise KeyError(f"unknown snapshot {snapshot_id}")
        tmp = self._current_file.with_name(".CURRENT.tmp")
        tmp.write_text(snapshot_id + "\n", "utf-8")
        os.replace(tmp, self._current_file)

    def ensure_current(self, files: list[DataFile], note: str = "scan") -> dict:
        with self.lock:
            files = self.ensure_indexes(files)
            snapshot_id = self.snapshot_id_for(files)
            current = self.current_snapshot_id()
            if current == snapshot_id:
                snapshot = self.load_snapshot(snapshot_id)
                if snapshot:
                    return snapshot
            snapshot = self.commit_snapshot(files, note=note, author="system")
            self.set_current(snapshot_id)
            return snapshot

    def resolve(self, snapshot_id: str | None = None) -> tuple[dict, list[DataFile]]:
        snapshot_id = snapshot_id or self.current_snapshot_id()
        if not snapshot_id:
            raise KeyError("no snapshot has been committed yet")
        snapshot = self.load_snapshot(snapshot_id)
        if snapshot is None:
            raise KeyError(f"unknown snapshot {snapshot_id}")
        files: list[DataFile] = []
        for row in snapshot["files"]:
            digest = row["sha256"]
            stats = self._load_index(digest)
            if not stats:
                stub = DataFile(
                    path=row["path"],
                    date=row["date"],
                    region=row["region"],
                    file_index=row["file_index"],
                    size=row["size"],
                    mtime_ns=row.get("mtime_ns", 0),
                    content_sha256=digest,
                )
                source = self.storage_path(stub, verify=True)
                stats = self.build_index(source, digest)
            if int(stats.get("line_count", -1)) != int(row["line_count"]):
                raise SnapshotIntegrityError(
                    f"index line count for {row['path']} disagrees with snapshot {snapshot_id}"
                )
            files.append(
                DataFile(
                    path=row["path"],
                    date=row["date"],
                    region=row["region"],
                    file_index=row["file_index"],
                    size=row["size"],
                    mtime_ns=row.get("mtime_ns", 0),
                    content_sha256=digest,
                    stats=stats,
                )
            )
        return snapshot, files


def _now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
