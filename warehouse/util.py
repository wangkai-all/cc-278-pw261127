"""Hashing, atomic writes, canonical JSON and deterministic randomness."""

from __future__ import annotations

import hashlib
import json
import os
import random
import tempfile
from pathlib import Path
from typing import Any


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def file_sha256(path: Path, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(chunk_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp-")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def atomic_write_json(path: Path, value: Any, pretty: bool = True) -> None:
    if pretty:
        text = json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    else:
        text = canonical_json(value)
    atomic_write_bytes(path, text.encode("utf-8"))


def stable_uniform(*parts: Any) -> float:
    """Deterministic uniform in [0, 1) from an arbitrary tuple of values."""
    digest = hashlib.sha256(canonical_json(list(parts)).encode("utf-8")).hexdigest()
    return int(digest[:16], 16) / float(1 << 64)


def seeded_rng(seed: str, *namespace: Any) -> random.Random:
    digest = hashlib.sha256(
        (seed + "\x1f" + canonical_json(list(namespace))).encode("utf-8")
    ).digest()
    return random.Random(int.from_bytes(digest[:16], "big"))


def normalise_seed(seed: Any) -> str:
    if seed is None or seed == "":
        return "preview-v1"
    text = str(seed)
    if len(text) > 128:
        raise ValueError("seed must be at most 128 characters")
    return text
