"""Versioned, tamper-evident pagination tokens.

A page token binds: task id, snapshot id, sampling method, normalized seed,
budget, page size, page offset, and a digest of the full ordered sample.  The
MAC key lives next to the database.  If any binding changes - a different
snapshot, seed, budget, method, or a merged file set - the server refuses the
token instead of silently showing a different sample.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from pathlib import Path

from .util import canonical_json

TOKEN_VERSION = 1
PREFIX = "pv1"


class TokenError(ValueError):
    """Token is malformed, tampered with, expired, or inconsistent."""


class TokenCodec:
    def __init__(self, key_path: Path, ttl_seconds: int):
        self.key_path = Path(key_path)
        self.ttl_seconds = int(ttl_seconds)
        if not self.key_path.exists():
            self.key_path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self.key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as handle:
                handle.write(secrets.token_bytes(32))
        self._key = self.key_path.read_bytes()
        if len(self._key) < 32:
            raise RuntimeError("token key is too short")

    def _mac(self, body: bytes) -> bytes:
        return hmac.new(self._key, body, hashlib.sha256).digest()[:24]

    def issue(self, payload: dict) -> str:
        body = dict(payload)
        body["v"] = TOKEN_VERSION
        body["iat"] = int(time.time())
        raw = canonical_json(body).encode("utf-8")
        encoded = base64.urlsafe_b64encode(raw).rstrip(b"=")
        tag = base64.urlsafe_b64encode(self._mac(raw)).rstrip(b"=")
        return f"{PREFIX}.{encoded.decode()}.{tag.decode()}"

    def verify(self, token: str) -> dict:
        if not isinstance(token, str) or not token.startswith(PREFIX + "."):
            raise TokenError("not a page token")
        parts = token.split(".")
        if len(parts) != 3 or parts[0] != PREFIX:
            raise TokenError("malformed page token")
        try:
            raw = _b64decode(parts[1])
            tag = _b64decode(parts[2])
        except (ValueError, TypeError) as exc:
            raise TokenError("malformed page token encoding") from exc
        if not hmac.compare_digest(tag, self._mac(raw)):
            raise TokenError("page token signature does not match")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TokenError("page token body is not JSON") from exc
        if payload.get("v") != TOKEN_VERSION:
            raise TokenError(f"unsupported page token version {payload.get('v')!r}")
        issued = int(payload.get("iat", 0))
        if issued <= 0 or time.time() - issued > self.ttl_seconds:
            raise TokenError("page token has expired")
        return payload


def _b64decode(text: str) -> bytes:
    pad = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + pad)


def bindings_from_task(task: dict, extra: dict | None = None) -> dict:
    plan = task.get("plan") or {}
    bound = {
        "task_id": task["id"],
        "kind": task["kind"],
        "snapshot_id": task["snapshot_id"],
        "method": task.get("method"),
        "seed": task.get("seed"),
        "budget": task.get("budget"),
        "page_size": task.get("page_size"),
        "filter": task.get("filter") or {},
        "sample_digest": task.get("sample_digest"),
        "input_hash": task.get("input_hash"),
        "planned": plan.get("planned_records"),
    }
    if extra:
        bound.update(extra)
    return bound


def require_bindings(token_payload: dict, task: dict) -> dict:
    """Reject a token whose binding no longer describes the stored task."""
    expected = bindings_from_task(task)
    mismatches = []
    for key in ("kind", "snapshot_id", "method", "seed", "budget", "page_size", "filter"):
        if token_payload.get(key) != expected.get(key):
            mismatches.append(key)
    if token_payload.get("task_id") != expected["task_id"]:
        mismatches.append("task_id")
    if token_payload.get("sample_digest") != expected["sample_digest"]:
        mismatches.append("sample_digest")
    if mismatches:
        raise TokenError(
            "page token no longer matches its task (changed: "
            + ", ".join(sorted(set(mismatches)))
            + "); restart the preview instead of mixing samples"
        )
    return expected
