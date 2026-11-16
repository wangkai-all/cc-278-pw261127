"""Task orchestration: previews, exact full queries, and file merges.

Concurrency rules
-----------------
* A preview task reads the immutable snapshot bound at creation. Next-page
  requests only use the task's own stored rows; they never rescan the
  warehouse. Therefore ingest, merges, and full queries can run concurrently
  and a page cannot switch samples mid-task.
* Full queries also bind a snapshot, then stream it exactly.  Their rows are
  stored as a bounded excerpt plus exact aggregate counts.
* A merge produces a new file set.  It refuses to run if the base snapshot is
  no longer current (compare-and-swap), or while any preview task is still
  running on that snapshot.  The catalog pins old bytes, so already-created
  tasks (running or finished) can still resolve their input after the merge.
* Failed preview tasks checkpoint per-file shards; retry reuses completed
  shards and reproduces identical per-file sampling because per-file RNGs are
  derived from (seed, snapshot, method, path) - not from scheduling order.
"""

from __future__ import annotations

import json
import os
import random
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from . import config
from .catalog import Catalog, DataFile, SnapshotConflict, fingerprint_files
from .generate import file_name_for, write_partition_file
from .metrics import channel_breakdown, rare_coverage
from .sampling import (
    Plan,
    SamplingError,
    build_plan,
    execute_plan_file,
)
from .store import Store, now
from .util import canonical_json, sha256_text, stable_uniform

ACTIVE_STATUSES = ("queued", "running", "assembling")


class ServiceError(RuntimeError):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


class App:
    def __init__(self, data_root: Path, db_path: Path, token_key_path: Path):
        self.data_root = Path(data_root)
        self.catalog = Catalog(self.data_root)
        self.store = Store(Path(db_path))
        self.executor = ThreadPoolExecutor(
            max_workers=config.TASK_WORKERS, thread_name_prefix="preview"
        )
        self.merge_lock = threading.Lock()
        self._running: dict[str, threading.Event] = {}
        self._running_lock = threading.Lock()
        self.store.mark_interrupted_tasks()

    # ------------------------------------------------------------------ setup
    def bootstrap_if_empty(self) -> dict:
        files = self.catalog.scan()
        if files:
            return self.catalog.ensure_current(files, note="startup scan")
        from .generate import bootstrap_campaign, bootstrap_v1

        # Commit the pre-campaign baseline first, then the campaign days.  That
        # gives every later preview a reference snapshot against which "new
        # partition / new kind" is factual rather than guessed.
        bootstrap_v1(self.data_root, progress=lambda _msg: None)
        files = self.catalog.scan(use_cache=False)
        files = self.catalog.ensure_indexes(files)
        self.catalog.commit_snapshot(files, note="bootstrap v1 baseline", author="bootstrap")
        bootstrap_campaign(self.data_root, progress=lambda _msg: None)
        files = self.catalog.scan(use_cache=False)
        files = self.catalog.ensure_indexes(files)
        return self.catalog.ensure_current(files, note="campaign partitions")

    def current(self) -> tuple[dict, list[DataFile]]:
        files = self.catalog.ensure_indexes(self.catalog.scan())
        snapshot = self.catalog.ensure_current(files, note="scan")
        return snapshot, files

    # ---------------------------------------------------------------- preview
    def create_preview(
        self,
        method: str,
        seed: str,
        budget: int,
        page_size: int,
        snapshot_id: str | None = None,
        fault_after_files: int | None = None,
    ) -> dict:
        if method not in config.PREVIEW_METHODS:
            raise ServiceError(f"unknown method; choose one of {config.PREVIEW_METHODS}")
        if not (config.MIN_BUDGET_RECORDS <= budget <= config.MAX_BUDGET_RECORDS):
            raise ServiceError(
                f"budget must be between {config.MIN_BUDGET_RECORDS} and {config.MAX_BUDGET_RECORDS}"
            )
        if not (config.MIN_PAGE_SIZE <= page_size <= config.MAX_PAGE_SIZE):
            raise ServiceError(
                f"page size must be between {config.MIN_PAGE_SIZE} and {config.MAX_PAGE_SIZE}"
            )
        from .util import normalise_seed

        seed = normalise_seed(seed)
        try:
            snapshot, files = self.catalog.resolve(snapshot_id) if snapshot_id else self.current()
        except KeyError as exc:
            raise ServiceError(str(exc), 404) from exc
        try:
            plan = build_plan(self.catalog, snapshot, files, method, seed, budget)
        except SamplingError as exc:
            raise ServiceError(str(exc)) from exc
        task = self.store.create_task(
            kind="preview",
            snapshot_id=snapshot["snapshot_id"],
            method=method,
            seed=seed,
            budget=budget,
            page_size=page_size,
            filter_obj={},
            fault_after_files=fault_after_files,
        )
        # The plan (including coverage and channels) is persisted on the task,
        # so paging does not depend on a later warehouse state.
        self.store.update_task(task["id"], plan_json=json.dumps(plan.to_dict()))
        self._submit_preview(task["id"])
        return self.store.get_task(task["id"])

    def retry_preview(self, task_id: str) -> dict:
        task = self.store.get_task(task_id)
        if task is None:
            raise ServiceError("task not found", 404)
        if task["kind"] != "preview":
            raise ServiceError("only preview tasks can be retried")
        if task["status"] == "running":
            raise ServiceError("task is already running", 409)
        # Freeze the same snapshot and seed: retry must not silently resample
        # from a newer warehouse state.
        shards = self.store.shard_status(task_id)
        completed = {p for p, s in shards.items() if s["status"] == "complete"}
        self.store.update_task(
            task_id,
            status="queued",
            error=None,
            finished_at=None,
            fault_after_files=None,
            attempts=int(task.get("attempts") or 0) + 1,
        )
        self.store.log(
            "info",
            f"retry queued; {len(completed)} completed shards will be reused",
            task_id,
        )
        self._submit_preview(task_id)
        return self.store.get_task(task_id)

    def _submit_preview(self, task_id: str) -> None:
        event = threading.Event()
        with self._running_lock:
            if task_id in self._running:
                return
            self._running[task_id] = event
        self.executor.submit(self._run_preview_guarded, task_id, event)

    def _run_preview_guarded(self, task_id: str, event: threading.Event) -> None:
        try:
            self._run_preview(task_id, event)
        except Exception as exc:  # pragma: no cover - defensive
            self.store.update_task(task_id, status="failed", error=str(exc))
            self.store.log("error", f"unhandled task failure: {exc}", task_id)
        finally:
            with self._running_lock:
                self._running.pop(task_id, None)

    def _run_preview(self, task_id: str, cancel: threading.Event) -> None:
        task = self.store.get_task(task_id)
        if task is None:
            return
        plan_dict = task.get("plan") or {}
        try:
            snapshot, files = self.catalog.resolve(task["snapshot_id"])
        except (KeyError, Exception) as exc:
            self.store.update_task(task_id, status="failed", error=f"input unavailable: {exc}")
            self.store.log("error", f"cannot resolve input snapshot: {exc}", task_id)
            return
        files_by_path = {f.path: f for f in files}
        plan_files = {row["path"]: row for row in plan_dict.get("files", [])}
        seq_order = [f.path for f in files if f.path in plan_files]
        self.store.update_task(
            task_id,
            status="running",
            started_at=now(),
            plan_json=json.dumps(plan_dict),
        )
        shards = self.store.shard_status(task_id)
        fault_after = task.get("fault_after_files")
        processed = 0
        try:
            for seq, path in enumerate(seq_order):
                if cancel.is_set():
                    self.store.update_task(task_id, status="interrupted", error="cancelled")
                    return
                existing = shards.get(path)
                if existing and existing["status"] == "complete":
                    continue
                data_file = files_by_path[path]
                row = plan_files[path]
                file_plan = _file_plan_from(row)
                file_plan.kind_counts = dict(data_file.kinds)
                self.store.begin_shard(task_id, path, seq)
                try:
                    rows, stats = execute_plan_file(
                        self.catalog, _plan_from_dict(plan_dict), data_file, file_plan
                    )
                except Exception as exc:
                    self.store.fail_shard(task_id, path, str(exc))
                    raise
                self.store.finish_shard(task_id, path, seq, rows, stats)
                processed += 1
                if fault_after is not None and processed >= int(fault_after):
                    raise RuntimeError(
                        f"injected fault after {processed} file shard(s) (recovery test)"
                    )
            self._assemble(task_id)
        except Exception as exc:
            self.store.update_task(task_id, status="failed", error=str(exc))
            self.store.log("error", f"preview failed: {exc}", task_id)

    def _assemble(self, task_id: str) -> None:
        task = self.store.get_task(task_id)
        self.store.update_task(task_id, status="assembling")
        inputs = self.store.assembly_inputs(task_id)
        expected = [f["path"] for f in (task.get("plan") or {}).get("files", [])]
        got = [i["path"] for i in inputs]
        if sorted(expected) != sorted(got):
            missing = sorted(set(expected) - set(got))
            raise RuntimeError(f"cannot assemble; incomplete shards: {missing}")

        rows: list[dict] = []
        read = {"lines_read": 0, "bytes_read": 0, "seeks": 0, "parse_errors": 0}
        satisfied_by_other = 0
        planned = 0
        for item in inputs:
            rows.extend(item["rows"])
            planned += int(item["stats"].get("requested", 0))
            satisfied_by_other += int(item["stats"].get("satisfied_by_other_channel", 0))
            for key in read:
                read[key] += int(item["stats"].get(key, 0))
        self._order_rows(rows, task)
        digest = sha256_text(
            canonical_json(
                [
                    [r["source_path"], r["source_line"], r["record"].get("event_id")]
                    for r in rows
                ]
            )
        )
        plan = task.get("plan") or {}
        channels = channel_breakdown(rows)
        rare = rare_coverage(rows, plan)
        coverage = plan.get("coverage", {})
        planned_total = int(plan.get("planned_records") or 0)
        warnings = list(plan.get("warnings", []))
        if satisfied_by_other:
            warnings.append(
                f"{satisfied_by_other} planned rare pick(s) pointed at lines the "
                "representative channel already owned and the kind frame had no "
                "other indexed position; the kind is still represented once, so "
                "actual rows are lower than planned by that amount."
            )
        if len(rows) > planned_total:
            warnings.append(
                f"actual rows ({len(rows)}) exceed the plan ({planned_total}); "
                "the plan should be treated as invalid"
            )
        stats = {
            "population": plan.get("population"),
            "planned_records": plan.get("planned_records"),
            "actual_rows": len(rows),
            "planned_vs_actual": len(rows) - int(plan.get("planned_records") or 0),
            "satisfied_by_other_channel": satisfied_by_other,
            "channels": channels,
            "read": {
                **read,
                "budget_records": plan.get("budget"),
                "budget_used_ratio": len(rows) / max(1, int(plan.get("budget") or 1)),
                "physical_lines_per_sampled_row": (
                    read["lines_read"] / len(rows) if rows else None
                ),
            },
            "rare": rare,
            "coverage": {
                "partitions": coverage.get("partitions", []),
                "uncovered_partitions": coverage.get("uncovered_partitions", []),
                "uncovered_population": coverage.get("uncovered_population", 0),
                "uncovered_share": coverage.get("uncovered_share", 0),
                "kind_count": coverage.get("kind_count"),
                "rare_kind_count": coverage.get("rare_kind_count"),
            },
            "new_since_reference": {
                "partitions": plan.get("new_partitions", []),
                "kinds": plan.get("new_kinds", []),
            },
            "method": plan.get("method"),
            "snapshot_id": plan.get("snapshot_id"),
            "warnings": warnings,
            "finished_at": now(),
        }
        self.store.replace_sample_rows(task_id, rows, digest)
        self.store.update_task(
            task_id,
            status="complete",
            stats_json=json.dumps(stats),
            sample_digest=digest,
            finished_at=now(),
        )
        self.store.log(
            "info",
            f"preview complete: {len(rows)} rows, {read['lines_read']} physical lines read",
            task_id,
        )

    def _order_rows(self, rows: list[dict], task: dict) -> None:
        method = task.get("method")
        if method == "head":
            rows.sort(key=lambda r: (r["partition"], r["source_line"]))
            return
        if method == "kind":
            rows.sort(key=lambda r: (r["kind"] or "", r["partition"], r["source_line"]))
            return
        # Interleave channels and partitions so the first screen shows variety:
        # round-robin over (channel, partition) buckets, each sorted by position.
        buckets: dict[tuple[str, str], list[dict]] = {}
        for row in rows:
            buckets.setdefault((row["channel"], row["partition"]), []).append(row)
        for bucket in buckets.values():
            bucket.sort(key=lambda r: r["source_line"])
        ordered: list[dict] = []
        keys = sorted(buckets, key=lambda k: (k[0] != "representative", k[0], k[1]))
        while keys:
            progressed = False
            for key in list(keys):
                bucket = buckets[key]
                if bucket:
                    ordered.append(bucket.pop(0))
                    progressed = True
                else:
                    keys.remove(key)
            if not progressed:
                break
        rows[:] = ordered

    # ------------------------------------------------------------ full query
    def create_full_query(
        self,
        kind: str | None = None,
        limit: int = config.FULL_QUERY_PAGE_LIMIT,
        snapshot_id: str | None = None,
    ) -> dict:
        if not (1 <= limit <= config.FULL_QUERY_PAGE_LIMIT):
            raise ServiceError(f"limit must be between 1 and {config.FULL_QUERY_PAGE_LIMIT}")
        try:
            snapshot, files = self.catalog.resolve(snapshot_id) if snapshot_id else self.current()
        except KeyError as exc:
            raise ServiceError(str(exc), 404) from exc
        task = self.store.create_task(
            kind="full_query",
            snapshot_id=snapshot["snapshot_id"],
            method="exact_scan",
            seed=None,
            budget=None,
            page_size=limit,
            filter_obj={"kind": kind},
        )
        self.store.update_task(task["id"], status="running", started_at=now())
        self.executor.submit(self._run_full_query, task["id"])
        return self.store.get_task(task["id"])

    def _run_full_query(self, task_id: str) -> None:
        task = self.store.get_task(task_id)
        if task is None:
            return
        wanted = task["filter"].get("kind")
        limit = int(task["page_size"] or config.FULL_QUERY_PAGE_LIMIT)
        try:
            snapshot, files = self.catalog.resolve(task["snapshot_id"])
            counts: dict[str, int] = {}
            matched = 0
            excerpt: list[dict] = []
            lines = 0
            for data_file in files:
                if task["status"] == "interrupted":
                    return
                path = self.catalog.storage_path(data_file)
                with path.open("rb") as handle:
                    for index, line in enumerate(handle):
                        lines += 1
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            counts["__invalid__"] = counts.get("__invalid__", 0) + 1
                            continue
                        kind = str(record.get("kind", "__missing__"))
                        if wanted and kind != wanted:
                            continue
                        matched += 1
                        counts[kind] = counts.get(kind, 0) + 1
                        if len(excerpt) < limit:
                            excerpt.append(record)
            stats = {
                "exact": True,
                "snapshot_id": snapshot["snapshot_id"],
                "population": snapshot["record_count"],
                "lines_scanned": lines,
                "matched": matched,
                "counts": dict(sorted(counts.items())),
                "excerpt_limit": limit,
                "excerpt_returned": len(excerpt),
                "scope": f"kind={wanted}" if wanted else "all records",
                "finished_at": now(),
            }
            self.store.store_full_query(task_id, excerpt, stats)
            self.store.update_task(task_id, status="complete", finished_at=now())
            self.store.log(
                "info", f"full query complete: {matched} matched of {lines} lines", task_id
            )
        except Exception as exc:
            self.store.update_task(task_id, status="failed", error=str(exc))
            self.store.log("error", f"full query failed: {exc}", task_id)

    # ------------------------------------------------------------------ merge
    def merge_partitions(
        self,
        dates: list[str],
        base_snapshot_id: str | None = None,
        note: str = "merge partitions",
    ) -> dict:
        """Merge all files of the given date partitions into one file each.

        Compaction is per (date, region): the source lines are concatenated in
        file-index order and line order into a single events-merged.ndjson whose
        content hash differs from any source.  The new snapshot is committed
        atomically, and CURRENT only advances if it still points at the base.
        """
        if not dates:
            raise ServiceError("at least one partition date is required")
        with self.merge_lock, self.catalog.scan_lock:
            base_id = base_snapshot_id or self.catalog.current_snapshot_id()
            base = self.catalog.load_snapshot(base_id) if base_id else None
            if base is None:
                raise ServiceError("base snapshot not found", 404)
            # Concurrency policy: previews and full queries may be in flight.
            # They are bound to an immutable, pinned snapshot, so a merge that
            # lands mid-task cannot change their rows or their page tokens.
            # The only race we must stop is two merges/ingests based on the
            # same CURRENT: that is the compare-and-swap below.
            current = self.catalog.current_snapshot_id()
            if current != base_id:
                raise ServiceError(
                    "base snapshot is no longer current; reload and retry the merge",
                    409,
                )
            # A completed preview built on the current snapshot is a convenient
            # concurrency probe for tests: after a merge, its token must remain
            # valid because the task owns an immutable snapshot.
            groups: dict[str, list[dict]] = {}
            for row in base["files"]:
                if row["date"] in dates:
                    groups.setdefault(f"{row['date']}/{row['region']}", []).append(row)
            if not groups:
                raise ServiceError("no files matched the requested partition dates", 404)
            staging = self.catalog.staging_root / f"merge-{os.getpid()}-{time.time_ns()}"
            staging.mkdir(parents=True, exist_ok=True)
            new_rows: list[DataFile] = []
            sources: dict[str, Path] = {}
            replaced_raw: dict[str, list[str]] = {}
            try:
                for key, rows in sorted(groups.items()):
                    rows.sort(key=lambda r: r["file_index"])
                    date, region = key.split("/")
                    out_path = self.data_root / file_name_for(region, date, 99999)
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    tmp = staging / f"{date}__{region}.ndjson"
                    with tmp.open("wb") as out:
                        for row in rows:
                            stub = _stub_from_row(row)
                            source = self.catalog.storage_path(stub, verify=True)
                            with source.open("rb") as handle:
                                shutil.copyfileobj(handle, out)
                        out.flush()
                        os.fsync(out.fileno())
                    from .util import file_sha256

                    digest = file_sha256(tmp)
                    new_rows.append(
                        DataFile(
                            path=out_path.relative_to(self.data_root).as_posix(),
                            date=date,
                            region=region,
                            file_index=99999,
                            size=tmp.stat().st_size,
                            mtime_ns=tmp.stat().st_mtime_ns,
                            content_sha256=digest,
                        )
                    )
                    sources[digest] = tmp
                    replaced_raw.setdefault(key, []).extend(r["path"] for r in rows)
                    # Pin before replacing raw paths so any task that resolves
                    # the old snapshot still has bytes.
                    self.catalog.pin(new_rows[-1], tmp)
                # Build indexes for merged outputs before publishing.
                indexed: list[DataFile] = []
                for data_file in new_rows:
                    indexed.append(
                        data_file.with_stats(
                            self.catalog.build_index(
                                sources[data_file.content_sha256], data_file.content_sha256
                            )
                        )
                    )
                # Publish: replace raw files, then commit + advance CURRENT.
                for data_file in indexed:
                    target = self.data_root / data_file.path
                    os.replace(sources[data_file.content_sha256], target)
                for group_key, paths in replaced_raw.items():
                    for rel in paths:
                        if rel == file_name_for(*group_key.split("/"), 99999) or rel.endswith(
                            "-merged.ndjson"
                        ):
                            continue
                        try:
                            (self.data_root / rel).unlink()
                        except FileNotFoundError:
                            pass
                kept: list[DataFile] = []
                for row in base["files"]:
                    if row["date"] in dates:
                        continue
                    stats = self.catalog.load_index(row["sha256"]) or {
                        # Never let a merge silently zero a partition's count:
                        # rebuild from the pinned bytes if the index is absent.
                        "line_count": row["line_count"],
                        "kinds": row.get("kinds", {}),
                    }
                    kept.append(_stub_from_row(row).with_stats(stats))
                new_files = kept + indexed
                snapshot = self.catalog.commit_snapshot(
                    new_files,
                    note=f"{note} ({', '.join(sorted(dates))})",
                    author="merge",
                )
                self.catalog.set_current(snapshot["snapshot_id"])
            finally:
                shutil.rmtree(staging, ignore_errors=True)
            self.store.log(
                "info",
                f"merge complete: {len(new_rows)} files written, snapshot {snapshot['snapshot_id'][:8]}",
            )
            return {
                "snapshot_id": snapshot["snapshot_id"],
                "base_snapshot_id": base_id,
                "merged_partitions": sorted(groups),
                "files_written": [f.path for f in indexed],
                "record_count": snapshot["record_count"],
            }

    # -------------------------------------------------------------- new data
    def append_partition(
        self,
        date: str,
        kind: str,
        weight: float,
        campaign: str | None = None,
        records: int = 6_000,
        seed: str = "baseline-v1",
    ) -> dict:
        """Generate and publish a new campaign partition (new kind included)."""
        from datetime import datetime

        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError as exc:
            raise ServiceError("date must be YYYY-MM-DD") from exc
        if not kind or len(kind) > 64 or not all(c.isalnum() or c in "_-" for c in kind):
            raise ServiceError("kind must be 1-64 chars of letters, digits, _ or -")
        if not (0.0 < weight <= 1.0):
            raise ServiceError("weight must be in (0, 1]")
        existing = {f.date for f in self.catalog.scan()}
        if date in existing:
            raise ServiceError(f"partition {date} already exists", 409)
        if not (config.MIN_BUDGET_RECORDS <= records <= 500_000):
            raise ServiceError("records out of range")
        special = {
            "kind": kind,
            "weight": weight,
            "campaign": campaign or kind,
            "schema_version": 3,
        }
        from .generate import counts_for_partition

        counts = counts_for_partition(date, records)
        total = sum(counts)
        with self.merge_lock, self.catalog.scan_lock:
            for region in config.REGIONS:
                for index, count in enumerate(counts):
                    write_partition_file(
                        self.data_root, region, date, count, seed, index, special=special
                    )
            files = self.catalog.scan(use_cache=False)
            files = self.catalog.ensure_indexes(files)
            snapshot = self.catalog.commit_snapshot(
                files, note=f"ingest {date} kind={kind}", author="ingest"
            )
            self.catalog.set_current(snapshot["snapshot_id"])
        self.store.log(
            "info",
            f"ingested partition {date} ({total} records, new kind {kind}), "
            f"snapshot {snapshot['snapshot_id'][:8]}",
        )
        return {
            "date": date,
            "kind": kind,
            "record_count": total,
            "snapshot_id": snapshot["snapshot_id"],
        }

    # ------------------------------------------------------------- recovery
    def resume_incomplete(self) -> dict:
        """Queue retries for interrupted/failed preview tasks."""
        resumed = []
        for task in self.store.list_tasks(limit=500, kind="preview"):
            if task["status"] in ("interrupted", "failed"):
                try:
                    self.retry_preview(task["id"])
                    resumed.append(task["id"])
                except ServiceError:
                    continue
        return {"resumed": resumed}


def _stub_from_row(row: dict) -> DataFile:
    return DataFile(
        path=row["path"],
        date=row["date"],
        region=row["region"],
        file_index=row.get("file_index", 0),
        size=row["size"],
        mtime_ns=row.get("mtime_ns", 0),
        content_sha256=row["sha256"],
    )


def _file_plan_from(row: dict):
    from .sampling import FilePlan

    by_kind = {
        kind: int(n)
        for kind, n in (row.get("kind_strata") or {}).items()
    }
    rare = {kind: int(n) for kind, n in (row.get("rare_supplement") or {}).items()}
    return FilePlan(
        path=row["path"],
        partition=row["partition"],
        population=row["population"],
        rep_n=int(row.get("representative") or 0),
        rare_by_kind=rare,
        kind_n=by_kind,
    )


def _plan_from_dict(plan_dict: dict) -> Plan:
    return Plan(
        method=plan_dict["method"],
        seed=plan_dict["seed"],
        budget=int(plan_dict["budget"]),
        snapshot_id=plan_dict["snapshot_id"],
        files=[],
        population=int(plan_dict["population"]),
        channels=plan_dict.get("channels", {}),
        rare_kinds=plan_dict.get("rare_kinds", []),
        new_kinds=plan_dict.get("new_kinds", []),
        new_partitions=plan_dict.get("new_partitions", []),
        coverage=plan_dict.get("coverage", {}),
        warnings=plan_dict.get("warnings", []),
    )
