"""Deterministic synthetic event generator and bootstrap orchestrator.

Everything is derived from a seed string, so deleting .data/ and re-running
`python -m warehouse bootstrap` recreates byte-identical files.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator

from . import config

WORDS = (
    "linen jacket", "running shoe", "cast iron pan", "noise cancelling",
    "desk lamp", "trail backpack", "wool socks", "ceramic mug", "usb hub",
    "mechanical keyboard", "standing mat", "water bottle", "graphic novel",
    "espresso tamper", "garden hose", "bike helmet", "phone case", "sleep mask",
    "sourdough starter", "hiking poles", "yoga block", "led strip", "dog leash",
    "board game", "coffee grinder", "monitor arm", "thermal socks", "hammock",
)
USERS = 50_000
SKU_COUNT = 900
AFFIXES = ("", "", "", "-pro", "-mini", "-refurb", "-gift", "-bundle")

# Deterministic per-kind payload extras.
KIND_FIELDS = {
    "page_view": ("path",),
    "search": ("query", "result_count"),
    "add_to_cart": ("sku", "qty"),
    "checkout": ("cart_value_cents", "payment_method"),
    "purchase": ("order_id", "amount_cents", "currency"),
    "refund": ("order_id", "amount_cents", "reason"),
    "experiment_exposure": ("experiment", "variant"),
    "session_end": ("duration_ms", "page_count"),
    "vendor_timeout": ("vendor", "latency_ms", "error_code"),
    "pii_redaction_audit": ("field", "rule", "records_touched"),
    "fraud_flag": ("risk_score", "signals"),
    "chargeback_trace": ("case_id", "amount_cents", "network"),
    "schema_migration": ("table", "column", "from_type", "to_type"),
    "quarantine_replay": ("queue", "attempts", "replayed"),
    "shadow_replay": ("shadow_id", "compared_fields", "mismatches"),
    "black_friday_doorbuster": ("doorbuster_id", "discount_pct", "starts_at"),
    "holiday_delivery_estimate": ("zip", "estimate_days", "carrier"),
}

VENDORS = ("payment-gw", "tax-svc", "inventory-svc", "ship-quote")
PAYMENTS = ("card", "paypal", "apple_pay", "gift_card")
CURRENCIES = ("USD", "EUR", "GBP")
CARRIERS = ("ups", "fedex", "usps", "dhl")
ERROR_CODES = ("E_TIMEOUT", "E_5XX", "E_CONN_RESET", "E_DEADLINE")


def _iso(day: str, offset_seconds: int) -> str:
    base = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return (base + timedelta(seconds=offset_seconds)).isoformat().replace("+00:00", "Z")


def _hash_bytes(*parts: str) -> bytes:
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).digest()


def _pick(rng: random.Random, weighted: list[tuple[str, float]]) -> str:
    total = sum(weight for _, weight in weighted)
    needle = rng.random() * total
    upto = 0.0
    for value, weight in weighted:
        upto += weight
        if needle < upto:
            return value
    return weighted[-1][0]


def kind_mix(region: str, date: str, special: dict | None = None) -> list[tuple[str, float]]:
    mix: dict[str, float] = {}
    for kind, weight in config.BASE_KINDS:
        mix[kind] = mix.get(kind, 0.0) + weight
    for kind, weight in config.REGION_RARE_KINDS.get(region, ()):
        mix[kind] = mix.get(kind, 0.0) + weight
    for kind, weight in config.GLOBAL_RARE_KINDS:
        mix[kind] = mix.get(kind, 0.0) + weight
    if special is None:
        special = config.SPECIAL_DATES.get(date)
    if special:
        # A campaign inflates its own kind without removing the base mix.
        mix[special["kind"]] = mix.get(special["kind"], 0.0) + special["weight"]
    return sorted(mix.items())


def file_name_for(region: str, date: str, file_index: int) -> str:
    suffix = "-merged" if file_index >= 90000 else ""
    return f"{date}/{region}/events-{file_index:05d}{suffix}.ndjson"


def counts_for_partition(date: str, records_per_partition: int) -> list[int]:
    """Split a partition's records across files with integer, exact totals."""
    total = records_per_partition
    if records_per_partition == config.DEFAULT_RECORDS_PER_PARTITION:
        # The shipped corpus makes campaign days heavier; a caller asking for a
        # specific (e.g. test-sized) corpus gets exactly that size everywhere.
        total = config.PARTITION_RECORDS_BY_DATE.get(date, records_per_partition)
    raw = [total * weight for weight in config.FILE_WEIGHTS]
    counts = [int(value) for value in raw]
    counts[-1] += total - sum(counts)
    return counts


def _payload(
    rng: random.Random,
    kind: str,
    user_id: int,
    date: str,
    schema_version: int,
    campaign: str | None = None,
) -> dict:
    payload: dict = {}
    if kind == "page_view":
        payload["path"] = rng.choice(("/", "/product", "/cart", "/search", "/account"))
    elif kind == "search":
        payload["query"] = rng.choice(WORDS)
        payload["result_count"] = rng.randint(0, 240)
    elif kind == "add_to_cart":
        payload["sku"] = f"SKU-{rng.randrange(SKU_COUNT):04d}{rng.choice(AFFIXES)}"
        payload["qty"] = rng.randint(1, 3)
    elif kind == "checkout":
        payload["cart_value_cents"] = rng.randint(500, 90_000)
        payload["payment_method"] = rng.choice(PAYMENTS)
    elif kind == "purchase":
        payload["order_id"] = f"ord_{rng.getrandbits(48):012x}"
        payload["amount_cents"] = rng.randint(300, 120_000)
        payload["currency"] = rng.choice(CURRENCIES)
    elif kind == "refund":
        payload["order_id"] = f"ord_{rng.getrandbits(48):012x}"
        payload["amount_cents"] = rng.randint(300, 60_000)
        payload["reason"] = rng.choice(("damaged", "late", "changed_mind", "fraud"))
    elif kind == "experiment_exposure":
        payload["experiment"] = rng.choice(("checkout-v3", "search-rank", "pricing"))
        payload["variant"] = rng.choice(("control", "treatment"))
    elif kind == "session_end":
        payload["duration_ms"] = rng.randint(500, 3_600_000)
        payload["page_count"] = rng.randint(1, 60)
    elif kind == "vendor_timeout":
        payload["vendor"] = rng.choice(VENDORS)
        payload["latency_ms"] = rng.randint(2_000, 30_000)
        payload["error_code"] = rng.choice(ERROR_CODES)
    elif kind == "pii_redaction_audit":
        payload["field"] = rng.choice(("email", "phone", "address", "name"))
        payload["rule"] = rng.choice(("mask", "drop", "tokenize"))
        payload["records_touched"] = rng.randint(1, 4)
    elif kind == "fraud_flag":
        payload["risk_score"] = rng.randint(70, 100)
        payload["signals"] = rng.sample(
            ("geo_mismatch", "velocity", "new_card", "blocklist"), rng.randint(1, 3)
        )
    elif kind == "chargeback_trace":
        payload["case_id"] = f"cb_{rng.getrandbits(40):010x}"
        payload["amount_cents"] = rng.randint(1_000, 200_000)
        payload["network"] = rng.choice(("visa", "mastercard", "amex"))
    elif kind == "schema_migration":
        payload["table"] = rng.choice(("orders", "users", "items"))
        payload["column"] = rng.choice(("meta", "geo", "source"))
        payload["from_type"] = "string"
        payload["to_type"] = "jsonb"
    elif kind == "quarantine_replay":
        payload["queue"] = rng.choice(("dlq-orders", "dlq-events"))
        payload["attempts"] = rng.randint(2, 9)
        payload["replayed"] = rng.random() < 0.7
    elif kind == "shadow_replay":
        payload["shadow_id"] = f"sh_{rng.getrandbits(32):08x}"
        payload["compared_fields"] = rng.randint(10, 80)
        payload["mismatches"] = rng.choice((0, 0, 0, 1, 2))
    elif kind == "black_friday_doorbuster":
        payload["doorbuster_id"] = f"bf_{rng.randint(1, 120):03d}"
        payload["discount_pct"] = rng.choice((20, 30, 40, 50, 60))
        payload["starts_at"] = _iso(date, rng.randint(0, 86_400))
    elif kind == "holiday_delivery_estimate":
        payload["zip"] = f"{rng.randint(1_000, 99_999):05d}"
        payload["estimate_days"] = rng.randint(1, 12)
        payload["carrier"] = rng.choice(CARRIERS)
    else:
        # Runtime-added partitions may introduce a brand new kind.
        payload["probe"] = True
        payload["campaign"] = campaign
        payload["value"] = rng.randint(1, 1_000)
    if schema_version >= 2:
        payload["campaign"] = campaign
        payload["ingest_source"] = rng.choice(("web", "mobile", "partner"))
    return payload


def generate_records(
    region: str,
    date: str,
    count: int,
    seed: str = "baseline-v1",
    file_index: int = 0,
    special: dict | None = None,
) -> list[dict]:
    """Return `count` deterministic records for one partition/file."""
    if special is None:
        special = config.SPECIAL_DATES.get(date)
    schema_version = special.get("schema_version", 1) if special else 1
    campaign = special.get("campaign") if special else None
    mix = kind_mix(region, date, special)
    records: list[dict] = []
    for index in range(count):
        digest = _hash_bytes(seed, date, region, str(file_index), str(index))
        rng = random.Random(int.from_bytes(digest[:16], "big"))
        kind = _pick(rng, mix)
        user_id = rng.randrange(1, USERS + 1)
        offset = rng.randrange(0, 86_400)
        record = {
            "event_id": f"{region}-{date}-{file_index}-{index:06d}",
            "ts": _iso(date, offset),
            "region": region,
            "kind": kind,
            "user_id": user_id,
            "schema_version": schema_version,
            "payload": _payload(rng, kind, user_id, date, schema_version, campaign),
        }
        records.append(record)
    return records


def serialise(record: dict) -> bytes:
    return (json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")


def write_partition_file(
    root: Path,
    region: str,
    date: str,
    count: int,
    seed: str = "baseline-v1",
    file_index: int = 0,
    special: dict | None = None,
) -> Path:
    """Atomically write one NDJSON file and return its path."""
    target = root / file_name_for(region, date, file_index)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), prefix=".tmp-", suffix=".ndjson")
    try:
        with os.fdopen(fd, "wb") as handle:
            for record in generate_records(region, date, count, seed, file_index, special):
                handle.write(serialise(record))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, target)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise
    return target


def iter_dates(start: str, end: str) -> Iterator[str]:
    cursor = datetime.strptime(start, "%Y-%m-%d")
    stop = datetime.strptime(end, "%Y-%m-%d")
    while cursor <= stop:
        yield cursor.strftime("%Y-%m-%d")
        cursor += timedelta(days=1)


def bootstrap(
    root: Path,
    records_per_partition: int = config.DEFAULT_RECORDS_PER_PARTITION,
    seed: str = "baseline-v1",
    progress=print,
) -> list[Path]:
    """Write the whole corpus (v1 baseline plus campaign partitions)."""
    written = bootstrap_v1(root, records_per_partition, seed, progress)
    written += bootstrap_campaign(root, records_per_partition, seed, progress)
    return written


def bootstrap_v1(
    root: Path,
    records_per_partition: int = config.DEFAULT_RECORDS_PER_PARTITION,
    seed: str = "baseline-v1",
    progress=print,
) -> list[Path]:
    """Write the pre-campaign baseline days."""
    root.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for day in iter_dates(config.BOOTSTRAP_START_DATE, config.BOOTSTRAP_V1_END_DATE):
        counts = counts_for_partition(day, records_per_partition)
        for region in config.REGIONS:
            for file_index, count in enumerate(counts):
                written.append(
                    write_partition_file(root, region, day, count, seed, file_index)
                )
    progress(f"baseline v1: {len(written)} files")
    return written


def bootstrap_campaign(
    root: Path,
    records_per_partition: int = config.DEFAULT_RECORDS_PER_PARTITION,
    seed: str = "baseline-v1",
    progress=print,
) -> list[Path]:
    """Write the campaign days that carry new kinds and schema v2."""
    root.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for day in (config.BOOTSTRAP_V2_DATE, config.BOOTSTRAP_V2_END_DATE):
        counts = counts_for_partition(day, records_per_partition)
        for region in config.REGIONS:
            for file_index, count in enumerate(counts):
                written.append(
                    write_partition_file(root, region, day, count, seed, file_index)
                )
    progress(f"campaign v2: {len(written)} files")
    return written
