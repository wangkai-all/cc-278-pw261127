"""Standard-library HTTP API and static page for the preview service."""

from __future__ import annotations

import json
import mimetypes
import re
import threading
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from . import config
from .metrics import compare_with_full, estimate_counts
from .service import App, ServiceError
from .store import now
from .tokens import TokenCodec, TokenError, bindings_from_task, require_bindings

STATIC_ROOT = Path(__file__).resolve().parent.parent / "static"


class HttpApp:
    def __init__(self, app: App, token_codec: TokenCodec, allowed_hosts: set[str] | None = None):
        self.app = app
        self.tokens = token_codec
        self.allowed_hosts = allowed_hosts or {"127.0.0.1", "localhost", "::1", "[::1]"}

    # --------------------------------------------------------------- helpers
    def task_payload(self, task: dict, include_rows: bool = False) -> dict:
        payload = {
            "id": task["id"],
            "kind": task["kind"],
            "status": task["status"],
            "error": task["error"],
            "snapshot_id": task["snapshot_id"],
            "method": task["method"],
            "seed": task["seed"],
            "budget": task["budget"],
            "page_size": task["page_size"],
            "attempts": task.get("attempts") or 0,
            "created_at": task["created_at"],
            "updated_at": task["updated_at"],
            "started_at": task["started_at"],
            "finished_at": task["finished_at"],
            "sample_digest": task["sample_digest"],
            "stats": task.get("stats"),
            "plan": task.get("plan"),
            "filter": task.get("filter"),
            "shards": [
                {
                    "path": s["path"],
                    "status": s["status"],
                    "rows": s["rows"],
                    "error": s["error"],
                }
                for s in self.app.store.shard_rows(task["id"])
            ],
        }
        if include_rows:
            payload["rows"] = self.app.store.all_sample_rows(task["id"])
        return payload

    def page(self, task: dict, offset: int) -> dict:
        size = int(task["page_size"] or config.DEFAULT_PAGE_SIZE)
        total = self.app.store.sample_count(task["id"])
        offset = max(0, min(offset, max(0, total)))
        if offset > config.MAX_PAGE_OFFSET:
            raise ServiceError("page offset too large", 400)
        rows = self.app.store.sample_page(task["id"], offset, size)
        base = bindings_from_task(task)

        def token_for(at: int) -> str:
            if at < 0 or at >= total:
                return None
            return self.tokens.issue(dict(base, offset=at))

        next_offset = offset + size if offset + size < total else None
        prev_offset = max(0, offset - size) if offset > 0 else None
        return {
            "task_id": task["id"],
            "snapshot_id": task["snapshot_id"],
            "method": task["method"],
            "seed": task["seed"],
            "budget": task["budget"],
            "offset": offset,
            "page_size": size,
            "total_rows": total,
            "has_prev": prev_offset is not None,
            "has_next": next_offset is not None,
            "next_offset": next_offset,
            "prev_offset": prev_offset,
            "next_token": token_for(next_offset) if next_offset is not None else None,
            "prev_token": token_for(prev_offset) if prev_offset is not None else None,
            "rows": rows,
            "page_token": token_for(offset),
            "sample_digest": task["sample_digest"],
        }

    def stats_for(self, task: dict) -> dict:
        stats = dict(task.get("stats") or {})
        rows = self.app.store.all_sample_rows(task["id"])
        stats["estimates"] = estimate_counts(rows, int((task.get("plan") or {}).get("population") or 0))
        stats["estimated"] = True
        stats["not_full_result"] = (
            "Estimates only: a preview over a sample of the committed snapshot. "
            "It is not a full-query result; use a full query for exact counts."
        )
        return stats

    # -------------------------------------------------------------- routing
    def handle(self, method: str, raw_path: str, body: dict | None) -> tuple[int, dict | str, str]:
        parsed = urllib.parse.urlparse(raw_path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        try:
            if method == "GET" and path == "/api/health":
                return 200, {"ok": True, "at": now()}, "application/json"
            if method == "GET" and path == "/api/config":
                return 200, {
                    "methods": list(config.PREVIEW_METHODS),
                    "default_method": config.DEFAULT_METHOD,
                    "default_budget": config.DEFAULT_BUDGET_RECORDS,
                    "default_page_size": config.DEFAULT_PAGE_SIZE,
                    "min_budget": config.MIN_BUDGET_RECORDS,
                    "max_budget": config.MAX_BUDGET_RECORDS,
                    "min_page_size": config.MIN_PAGE_SIZE,
                    "max_page_size": config.MAX_PAGE_SIZE,
                    "rare_share_threshold": config.RARE_SHARE_THRESHOLD,
                }, "application/json"
            if method == "GET" and path == "/api/snapshot":
                snapshot, files = self.app.current()
                return 200, {
                    "snapshot": _public_snapshot(snapshot),
                    "snapshots": self.app.catalog.list_snapshots()[-20:],
                    "storage": self.app.store.stats(),
                }, "application/json"
            if method == "GET" and path == "/api/snapshots":
                return 200, {"snapshots": self.app.catalog.list_snapshots()}, "application/json"
            if method == "GET" and path == "/api/snapshot/files":
                snapshot, files = self.app.current()
                return 200, {
                    "snapshot_id": snapshot["snapshot_id"],
                    "files": [
                        {
                            "path": f.path,
                            "date": f.date,
                            "region": f.region,
                            "line_count": f.line_count,
                            "size": f.size,
                            "sha256": f.content_sha256,
                            "kinds": f.kinds,
                            "index_capped": f.stats.get("kind_offsets_capped", []),
                        }
                        for f in files
                    ],
                }, "application/json"
            if method == "GET" and path == "/api/events":
                limit = _int_param(query, "limit", 60, 1, 200)
                return 200, {"events": self.app.store.events(limit=limit)}, "application/json"
            if method == "POST" and path == "/api/previews":
                data = body or {}
                budget = int(data.get("budget", config.DEFAULT_BUDGET_RECORDS))
                size = int(data.get("page_size", config.DEFAULT_PAGE_SIZE))
                task = self.app.create_preview(
                    method=str(data.get("method", config.DEFAULT_METHOD)),
                    seed=data.get("seed", "preview-v1"),
                    budget=budget,
                    page_size=size,
                    snapshot_id=data.get("snapshot_id"),
                    fault_after_files=data.get("fault_after_files"),
                )
                status = self.page(task, 0)
                return 202, {"task": self.task_payload(task), "page": status}, "application/json"
            if method == "GET" and path == "/api/tasks":
                kind = query.get("kind", [None])[0]
                limit = _int_param(query, "limit", 50, 1, 200)
                return 200, {
                    "tasks": [
                        _task_summary(t)
                        for t in self.app.store.list_tasks(limit=limit, kind=kind)
                    ]
                }, "application/json"
            match = re.fullmatch(r"/api/tasks/([0-9a-f]{32})", path)
            if method == "GET" and match:
                task = self.app.store.get_task(match.group(1))
                if task is None:
                    raise ServiceError("task not found", 404)
                return 200, self.task_payload(task), "application/json"
            match = re.fullmatch(r"/api/tasks/([0-9a-f]{32})/stats", path)
            if method == "GET" and match:
                task = self.app.store.get_task(match.group(1))
                if task is None:
                    raise ServiceError("task not found", 404)
                if task["kind"] != "preview":
                    raise ServiceError("not a preview task", 400)
                return 200, self.stats_for(task), "application/json"
            match = re.fullmatch(r"/api/tasks/([0-9a-f]{32})/retry", path)
            if method == "POST" and match:
                task = self.app.retry_preview(match.group(1))
                return 202, self.task_payload(task), "application/json"
            if method == "POST" and path == "/api/tasks/resume":
                return 200, self.app.resume_incomplete(), "application/json"
            match = re.fullmatch(r"/api/tasks/([0-9a-f]{32})/rows", path)
            if method == "GET" and match:
                task = self.app.store.get_task(match.group(1))
                if task is None:
                    raise ServiceError("task not found", 404)
                if task["status"] != "complete":
                    raise ServiceError(f"task is {task['status']}", 409)
                token = query.get("token", [None])[0]
                if token:
                    payload = self.tokens.verify(token)
                    require_bindings(payload, task)
                    offset = int(payload["offset"])
                else:
                    # Direct offset paging is supported, but it skips the
                    # freshness check a token performs - the page body marks it.
                    offset = _int_param(query, "offset", 0, 0, config.MAX_PAGE_OFFSET)
                page = self.page(task, offset)
                page["paged_by_token"] = bool(token)
                return 200, page, "application/json"
            if method == "POST" and path == "/api/full-queries":
                data = body or {}
                limit = int(data.get("limit", config.FULL_QUERY_PAGE_LIMIT))
                task = self.app.create_full_query(
                    kind=data.get("kind"), limit=limit, snapshot_id=data.get("snapshot_id")
                )
                return 202, self.task_payload(task), "application/json"
            match = re.fullmatch(r"/api/full-queries/([0-9a-f]{32})", path)
            if method == "GET" and match:
                task = self.app.store.get_task(match.group(1))
                if task is None:
                    raise ServiceError("task not found", 404)
                if task["kind"] != "full_query":
                    raise ServiceError("not a full query", 400)
                payload = self.task_payload(task)
                payload["rows"] = self.app.store.full_query_rows(task["id"])
                return 200, payload, "application/json"
            match = re.fullmatch(r"/api/compare/([0-9a-f]{32})/([0-9a-f]{32})", path)
            if method == "GET" and match:
                preview = self.app.store.get_task(match.group(1))
                full = self.app.store.get_task(match.group(2))
                if preview is None or full is None:
                    raise ServiceError("task not found", 404)
                if preview["kind"] != "preview" or full["kind"] != "full_query":
                    raise ServiceError("expected a preview and a full-query task", 400)
                if preview["status"] != "complete":
                    raise ServiceError("preview is not complete", 409)
                if full["status"] != "complete":
                    raise ServiceError("full query is not complete", 409)
                if preview["snapshot_id"] != full["snapshot_id"]:
                    raise ServiceError(
                        "tasks target different snapshots; the full query is not a "
                        "reference for this preview",
                        409,
                    )
                preview_rows = self.app.store.all_sample_rows(preview["id"])
                full_stats = full.get("stats") or {}
                comparison = compare_with_full(
                    preview_rows,
                    {k: int(v) for k, v in (full_stats.get("counts") or {}).items()},
                    preview.get("plan"),
                )
                return 200, {
                    "preview_task_id": preview["id"],
                    "full_task_id": full["id"],
                    "snapshot_id": preview["snapshot_id"],
                    "comparison": comparison,
                }, "application/json"
            if method == "POST" and path == "/api/merge":
                data = body or {}
                result = self.app.merge_partitions(
                    dates=list(data.get("dates") or []),
                    base_snapshot_id=data.get("base_snapshot_id"),
                )
                return 200, result, "application/json"
            if method == "POST" and path == "/api/partitions":
                data = body or {}
                result = self.app.append_partition(
                    date=str(data.get("date", "")),
                    kind=str(data.get("kind", "")),
                    weight=float(data.get("weight", 0.03)),
                    campaign=data.get("campaign"),
                    records=int(data.get("records", 6_000)),
                )
                return 201, result, "application/json"
            raise ServiceError("not found", 404)
        except TokenError as exc:
            return 409, {"error": str(exc), "code": "stale_page_token"}, "application/json"
        except ServiceError as exc:
            return exc.status, {"error": str(exc)}, "application/json"
        except ValueError as exc:
            return 400, {"error": str(exc)}, "application/json"


def _public_snapshot(snapshot: dict) -> dict:
    return {
        "snapshot_id": snapshot["snapshot_id"],
        "created_at": snapshot["created_at"],
        "note": snapshot["note"],
        "file_count": snapshot["file_count"],
        "record_count": snapshot["record_count"],
        "partition_count": len(snapshot["partitions"]),
        "partitions": snapshot["partitions"][:200],
    }


def _task_summary(task: dict) -> dict:
    return {
        "id": task["id"],
        "kind": task["kind"],
        "status": task["status"],
        "method": task["method"],
        "seed": task["seed"],
        "budget": task["budget"],
        "snapshot_id": task["snapshot_id"],
        "attempts": task.get("attempts") or 0,
        "created_at": task["created_at"],
        "finished_at": task["finished_at"],
        "error": task["error"],
        "actual_rows": (task.get("stats") or {}).get("actual_rows"),
    }


def _int_param(query: dict, name: str, default: int, low: int, high: int) -> int:
    raw = query.get(name, [None])[0]
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ServiceError(f"{name} must be an integer") from exc
    if not (low <= value <= high):
        raise ServiceError(f"{name} must be between {low} and {high}")
    return value


def make_handler(http_app: HttpApp):
    class Handler(BaseHTTPRequestHandler):
        server_version = "PreviewService/1.0"

        def log_message(self, fmt: str, *args) -> None:  # quiet default logging
            return

        def _send(self, status: int, payload, content_type: str) -> None:
            if isinstance(payload, (dict, list)):
                data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            else:
                data = payload if isinstance(payload, bytes) else str(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; style-src 'self'; script-src 'self'; img-src 'self' data:; connect-src 'self'",
            )
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(data)

        def _read_body(self):
            raw_len = self.headers.get("Content-Length", "0")
            try:
                length = int(raw_len)
            except ValueError:
                raise ServiceError("invalid Content-Length")
            if length > config.MAX_BODY_BYTES:
                raise ServiceError("request body too large", 413)
            if length <= 0:
                return None
            raw = self.rfile.read(length)
            try:
                body = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ServiceError("request body must be JSON") from exc
            if not isinstance(body, dict):
                raise ServiceError("request body must be a JSON object")
            return body

        def _dispatch(self, method: str) -> None:
            path = urllib.parse.urlparse(self.path).path
            host = (self.headers.get("Host") or "").split(":")[0]
            if host and host not in http_app.allowed_hosts:
                # DNS-rebinding guard: a hostile page resolving its own name to
                # 127.0.0.1 would otherwise be same-origin with this service.
                self._send(421, {"error": "host not allowed"}, "application/json")
                return
            try:
                if path.startswith("/api/"):
                    body = self._read_body() if method in ("POST", "PUT", "PATCH") else None
                    status, payload, ctype = http_app.handle(method, self.path, body)
                    self._send(status, payload, ctype)
                elif method == "GET":
                    self._serve_static(path)
                else:
                    self._send(404, {"error": "not found"}, "application/json")
            except ServiceError as exc:
                self._send(exc.status, {"error": str(exc)}, "application/json")
            except BrokenPipeError:
                return
            except Exception as exc:  # pragma: no cover - last resort
                self._send(500, {"error": f"internal error: {exc}"}, "application/json")

        def _serve_static(self, path: str) -> None:
            if path in ("/", "/index.html"):
                target = STATIC_ROOT / "index.html"
            elif path == "/app.js":
                target = STATIC_ROOT / "app.js"
            elif path == "/styles.css":
                target = STATIC_ROOT / "styles.css"
            else:
                self._send(404, "not found", "text/plain; charset=utf-8")
                return
            if not target.exists():
                self._send(500, "static files missing", "text/plain; charset=utf-8")
                return
            ctype = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
            if target.suffix in (".js", ".css"):
                ctype += "; charset=utf-8"
            self._send(200, target.read_bytes(), ctype)

        def do_GET(self) -> None:
            self._dispatch("GET")

        def do_HEAD(self) -> None:
            self._dispatch("GET")

        def do_POST(self) -> None:
            self._dispatch("POST")

    return Handler


def serve(app: App, host: str, port: int) -> ThreadingHTTPServer:
    codec = TokenCodec(app.data_root.parent / "state" / "token.key", config.TOKEN_TTL_SECONDS)
    http_app = HttpApp(app, codec)
    server = ThreadingHTTPServer((host, port), make_handler(http_app))
    server.daemon_threads = True
    # Exposed so an embedder (tests, notebooks) can rebind the service after a
    # simulated restart without tearing down the listening socket.
    server.http_app = http_app
    return server
