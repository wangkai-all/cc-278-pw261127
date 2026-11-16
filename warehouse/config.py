"""Dataset shape and tunable limits.

The synthetic dataset is intentionally campaign-shaped: a new date partition
arrives with new record kinds and a newer payload schema.  That makes it
possible to see the difference between "the first few hundred lines" and a
partition-stratified sample.
"""

from __future__ import annotations

BOOTSTRAP_START_DATE = "2026-11-01"
BOOTSTRAP_V1_END_DATE = "2026-11-14"
BOOTSTRAP_V2_DATE = "2026-11-15"
BOOTSTRAP_V2_END_DATE = "2026-11-16"

REGIONS = ("us", "eu", "apac")
FILES_PER_PARTITION = 3
FILE_WEIGHTS = (0.42, 0.34, 0.24)
DEFAULT_RECORDS_PER_PARTITION = 6_000
PARTITION_RECORDS_BY_DATE = {
    "2026-11-15": 9_000,
    "2026-11-16": 7_500,
}

# Base traffic mix.  Values need not sum to one; the generator normalises.
BASE_KINDS = (
    ("page_view", 0.500),
    ("search", 0.180),
    ("add_to_cart", 0.095),
    ("checkout", 0.070),
    ("purchase", 0.050),
    ("refund", 0.014),
    ("experiment_exposure", 0.025),
    ("session_end", 0.020),
)

REGION_RARE_KINDS = {
    "us": (("vendor_timeout", 0.0060), ("pii_redaction_audit", 0.0015)),
    "eu": (("fraud_flag", 0.0080), ("chargeback_trace", 0.0015)),
    "apac": (("schema_migration", 0.0050), ("quarantine_replay", 0.0020)),
}

GLOBAL_RARE_KINDS = (("shadow_replay", 0.0001),)

# Campaign partitions.  The first addition is part of the bootstrap v2;
# later additions can be created from the web UI / maintenance API.
SPECIAL_DATES = {
    "2026-11-15": {
        "kind": "black_friday_doorbuster",
        "weight": 0.040,
        "campaign": "black_friday",
        "schema_version": 2,
    },
    "2026-11-16": {
        "kind": "holiday_delivery_estimate",
        "weight": 0.015,
        "campaign": "holiday_eta",
        "schema_version": 2,
    },
    # Runtime-added campaign partitions (e.g. via the UI) may use a new kind.
    "2026-11-17": {
        "kind": "cyber_monday_preview",
        "weight": 0.030,
        "campaign": "cyber_monday",
        "schema_version": 3,
    },
}

PREVIEW_METHODS = (
    "stratified",
    "stratified_rare",
    "kind",
    "uniform",
    "head",
)

DEFAULT_METHOD = "stratified_rare"
DEFAULT_BUDGET_RECORDS = 1_200
DEFAULT_PAGE_SIZE = 25
MIN_BUDGET_RECORDS = 20
MAX_BUDGET_RECORDS = 20_000
MIN_PAGE_SIZE = 5
MAX_PAGE_SIZE = 200
MAX_QUERY_FILES = 10_000

# Ingest-time sparse index: one byte checkpoint per this many lines, and at
# most this many deterministically chosen line offsets per (file, kind).
CHECKPOINT_STRIDE = 8
INDEX_OFFSETS_PER_KIND = 96

# A kind is labelled "rare" in the UI when its estimated share is below this.
RARE_SHARE_THRESHOLD = 0.01
# Fraction of the record budget the rare-and-new-kind supplement may use.
RARE_SUPPLEMENT_FRACTION = 0.40
# Aim for at least this many observations per rare kind (if the budget allows).
RARE_TARGET_OBSERVATIONS = 12

# Web/API limits.
MAX_BODY_BYTES = 64 * 1024
MAX_PAGE_OFFSET = 1_000_000
TASK_WORKERS = 4
FULL_QUERY_PAGE_LIMIT = 100
DEFAULT_FULL_PAGE_SIZE = 25
TOKEN_TTL_SECONDS = 7 * 24 * 3600
