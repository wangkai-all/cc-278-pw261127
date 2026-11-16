"""Sampling plans and execution under a bounded record-read budget.

Frame
-----
The exact population is known from the ingest-time index (line count and kind
histogram of every committed file).  Reading is *not* a full scan: each file
index stores byte checkpoints and sparse per-kind positions, so a plan of n
records costs O(n) seeks and at most O(n * checkpoint_stride) physical lines.

Methods
-------
head              first `budget` lines in file order (non-probability sample)
uniform           global SRSWOR, strictly proportional per file
stratified        SRSWOR within each date/region partition, proportional
                  allocation with a floor of one record per partition
kind              SRSWOR within kind strata, square-root allocation with a
                  floor per kind (single coherent probability sample)
stratified_rare   partition-stratified sample plus a second-phase supplement
                  aimed at rare and newly appeared kinds

Weights
-------
Representative rows carry weight = N_file / n_file (design weight).
Kind-channel rows carry weight = N_file_kind / n_file_kind.
The rare supplement is a second phase; its counts are valid within file-kind
strata but must not be summed with the representative channel.  Every row
declares its channel, stratum and weight; the API never publishes a pooled
"full result".
"""

from __future__ import annotations

import bisect
import json
import math
import random
from dataclasses import dataclass, field
from pathlib import Path

from . import config
from .catalog import Catalog, DataFile
from .util import seeded_rng

CHANNEL_REP = "representative"
CHANNEL_RARE = "rare-supplement"
CHANNEL_KIND = "kind-stratum"
CHANNEL_HEAD = "head"
CHANNEL_UNIFORM = "uniform"


class SamplingError(RuntimeError):
    """The plan cannot be built or a planned read failed."""


# --------------------------------------------------------------------- plans
def _largest_remainder(total: int, weights: list[float], caps: list[int]) -> list[int]:
    """Integer allocation proportional to weights, each bounded by caps."""
    n = len(weights)
    if n == 0 or total <= 0:
        return [0] * n
    caps = [max(0, min(int(c), total)) for c in caps]
    positive = [max(0.0, float(w)) for w in weights]
    if sum(positive) <= 0:
        positive = [1.0] * n

    def capped_quota(scale: float) -> list[float]:
        return [min(positive[i] * scale, caps[i]) for i in range(n)]

    # Find the proportional scale whose capped quotas sum to `total`.
    lo, hi = 0.0, 1.0
    while sum(capped_quota(hi)) < total and hi < 1e18:
        hi *= 2
    for _ in range(80):
        mid = (lo + hi) / 2
        if sum(capped_quota(mid)) < total:
            lo = mid
        else:
            hi = mid
    quotas = capped_quota(hi)
    allocation = [min(caps[i], int(math.floor(quotas[i]))) for i in range(n)]
    leftover = total - sum(allocation)
    if leftover > 0:
        order = sorted(
            range(n),
            key=lambda i: (-(quotas[i] - math.floor(quotas[i])), -positive[i], i),
        )
        for i in order:
            if leftover <= 0:
                break
            if allocation[i] < caps[i]:
                # One unit per pass; large leftovers are handled below.
                allocation[i] += 1
                leftover -= 1
        # If quotas were shaved by caps, distribute remaining mass fairly.
        while leftover > 0:
            progressed = False
            for i in order:
                if leftover <= 0:
                    break
                if allocation[i] < caps[i]:
                    allocation[i] += 1
                    leftover -= 1
                    progressed = True
            if not progressed:
                break
    return allocation


def _allocate_with_floor(
    budget: int,
    weights: list[float],
    floor: int,
    caps: list[int],
) -> list[int]:
    """Per-item floor first, then proportional allocation of the remainder."""
    n = len(weights)
    if n == 0 or budget <= 0:
        return [0] * n
    caps = [max(0, min(int(c), budget)) for c in caps]
    eligible = [i for i in range(n) if caps[i] > 0]
    seeds = [0] * n
    floor = max(0, int(floor))
    if floor and budget >= floor * len(eligible):
        for i in eligible:
            seeds[i] = min(floor, caps[i])
    remaining = budget - sum(seeds)
    if remaining > 0:
        extra = _largest_remainder(
            remaining,
            [max(0.0, weights[i]) if caps[i] > seeds[i] else 0.0 for i in range(n)],
            [caps[i] - seeds[i] for i in range(n)],
        )
        seeds = [seeds[i] + extra[i] for i in range(n)]
    return seeds


def _sample_indices(population: int, count: int, rng: random.Random) -> list[int]:
    """SRSWOR line indices via Floyd's algorithm; sorted for sequential reads."""
    count = max(0, min(count, population))
    if count == 0:
        return []
    if count == population:
        return list(range(population))
    chosen: set[int] = set()
    for j in range(population - count, population):
        t = rng.randrange(0, j + 1)
        if t in chosen:
            chosen.add(j)
        else:
            chosen.add(t)
    return sorted(chosen)


# ----------------------------------------------------------------- structure
@dataclass
class FilePlan:
    path: str
    partition: str
    population: int
    rep_n: int = 0
    rare_by_kind: dict[str, int] = field(default_factory=dict)
    kind_n: dict[str, int] = field(default_factory=dict)
    kind_counts: dict[str, int] = field(default_factory=dict)
    kind_frame: dict[str, int] = field(default_factory=dict)

    @property
    def rare_n(self) -> int:
        return sum(self.rare_by_kind.values())

    @property
    def kind_total(self) -> int:
        return sum(self.kind_n.values())

    @property
    def planned(self) -> int:
        return self.rep_n + self.rare_n + self.kind_total


@dataclass
class Plan:
    method: str
    seed: str
    budget: int
    snapshot_id: str
    files: list[FilePlan]
    population: int
    channels: dict[str, int]
    rare_kinds: list[str]
    new_kinds: list[str]
    new_partitions: list[str]
    coverage: dict
    warnings: list[str]

    @property
    def planned(self) -> int:
        return sum(f.planned for f in self.files)

    def to_dict(self) -> dict:
        return {
            "method": self.method,
            "seed": self.seed,
            "budget": self.budget,
            "snapshot_id": self.snapshot_id,
            "population": self.population,
            "planned_records": self.planned,
            "channels": self.channels,
            "rare_kinds": self.rare_kinds,
            "new_kinds": self.new_kinds,
            "new_partitions": self.new_partitions,
            "coverage": self.coverage,
            "warnings": self.warnings,
            "files": [
                {
                    "path": f.path,
                    "partition": f.partition,
                    "population": f.population,
                    "representative": f.rep_n,
                    "rare_supplement": f.rare_by_kind,
                    "kind_strata": f.kind_n,
                }
                for f in self.files
            ],
        }


def _kind_totals(files) -> dict[str, int]:
    totals: dict[str, int] = {}
    for f in files:
        for kind, count in f.kinds.items():
            totals[kind] = totals.get(kind, 0) + int(count)
    return dict(sorted(totals.items()))


def _file_kind_counts(files) -> dict[str, dict[str, int]]:
    return {f.path: {k: int(v) for k, v in f.kinds.items()} for f in files}


def _reference_snapshot(catalog: Catalog, current_id: str) -> dict | None:
    """Oldest committed snapshot (the v1 baseline), to detect what is new."""
    others = [s for s in catalog.list_snapshots() if s.get("snapshot_id") != current_id]
    if others:
        return catalog.load_snapshot(others[0]["snapshot_id"])
    return catalog.load_snapshot(current_id)


def build_plan(
    catalog: Catalog,
    snapshot: dict,
    files: list[DataFile],
    method: str,
    seed: str,
    budget: int,
) -> Plan:
    if method not in config.PREVIEW_METHODS:
        raise SamplingError(f"unknown method {method!r}")
    if budget < 1:
        raise SamplingError("budget must be positive")
    files = sorted(files, key=lambda f: (f.date, f.region, f.file_index))
    if not files:
        raise SamplingError("snapshot has no files")
    total = sum(f.line_count for f in files)
    if total == 0:
        raise SamplingError("snapshot has no records")
    budget = min(budget, total)
    kind_counts = _kind_totals(files)
    file_kind = _file_kind_counts(files)

    partitions: dict[str, list[DataFile]] = {}
    for f in files:
        partitions.setdefault(f"{f.date}/{f.region}", []).append(f)
    partition_totals = {
        name: sum(f.line_count for f in group) for name, group in partitions.items()
    }

    file_plans = {
        f.path: FilePlan(
            path=f.path,
            partition=f"{f.date}/{f.region}",
            population=f.line_count,
            kind_counts=file_kind[f.path],
            kind_frame={
                kind: len(f.stats.get("kind_line_offsets", {}).get(kind, []))
                for kind in file_kind[f.path]
            },
        )
        for f in files
    }
    warnings: list[str] = []
    rare_kinds: list[str] = []
    new_kinds: list[str] = []
    new_partitions: list[str] = []
    reference = _reference_snapshot(catalog, snapshot["snapshot_id"])
    if reference:
        ref_kinds = {
            kind
            for row in reference.get("files", [])
            for kind in (row.get("kinds") or {})
        }
        new_kinds = sorted(k for k in kind_counts if k not in ref_kinds)
        ref_parts = {f"{row['date']}/{row['region']}" for row in reference.get("files", [])}
        new_partitions = sorted(p for p in partitions if p not in ref_parts)

    if method == "head":
        _plan_head(file_plans, files, budget)
        channels = {CHANNEL_HEAD: budget}
        warnings.append(
            "head sampling is non-probability: the preview is the physical file "
            "prefix and cannot be projected to the full population."
        )
    elif method == "uniform":
        alloc = _largest_remainder(
            budget, [f.line_count for f in files], [f.line_count for f in files]
        )
        for f, n in zip(files, alloc):
            file_plans[f.path].rep_n = n
        channels = {CHANNEL_UNIFORM: sum(alloc)}
        warnings.append(
            "uniform allocation is strictly proportional: a small partition can "
            "receive zero records and disappear from the preview."
        )
    elif method == "stratified":
        _plan_partition_stratified(file_plans, partitions, budget, floor=1)
        channels = {CHANNEL_REP: sum(fp.rep_n for fp in file_plans.values())}
    elif method == "kind":
        used, uncovered = _plan_kind_stratified(
            file_plans, files, kind_counts, file_kind, budget
        )
        channels = {CHANNEL_KIND: used}
        if used < budget:
            warnings.append(
                f"kind frames cap this plan at {used} of {budget} records; "
                "common kinds have more lines than the sparse index retains."
            )
        if uncovered:
            warnings.append(
                "kind coverage gap: "
                + ", ".join(sorted(uncovered)[:5])
                + " could not be reached through the sparse kind frames."
            )
    elif method == "stratified_rare":
        rare_kinds = sorted(
            kind
            for kind, count in kind_counts.items()
            if count / total < config.RARE_SHARE_THRESHOLD or kind in new_kinds
        )
        supplement = _rare_supplement_budget(kind_counts, new_kinds, rare_kinds, budget)
        rep_budget = max(1, budget - supplement)
        _plan_partition_stratified(file_plans, partitions, rep_budget, floor=1)
        used = _plan_rare_supplement(file_plans, files, kind_counts, rare_kinds, new_kinds, supplement)
        if used < supplement:
            warnings.append(
                f"rare supplement planned {supplement} but indexed positions only "
                f"support {used}; this snapshot has fewer rare records than the target."
            )
        actual_rep = sum(fp.rep_n for fp in file_plans.values())
        channels = {CHANNEL_REP: actual_rep, CHANNEL_RARE: used}
        if not rare_kinds:
            warnings.append("no kind falls below the rare share threshold in this snapshot.")
        if new_kinds or new_partitions:
            warnings.append(
                "new since v1: partitions "
                + (", ".join(new_partitions[:4]) or "none")
                + "; kinds "
                + (", ".join(new_kinds[:6]) or "none")
                + ". They receive rare-channel coverage rather than only their "
                "proportional share."
            )
    else:  # pragma: no cover
        raise SamplingError(f"unknown method {method!r}")

    planned = sum(fp.planned for fp in file_plans.values())
    if planned > budget:
        raise SamplingError(
            f"internal allocation error: planned {planned} > budget {budget}"
        )
    if planned < budget:
        warnings.append(
            f"plan uses {planned} of {budget} records: remaining budget cannot "
            "be spent without overlapping samples or empty strata."
        )
    coverage = _coverage(partitions, partition_totals, file_plans, total, kind_counts)
    if coverage["uncovered_partitions"]:
        warnings.append(
            "coverage gap: "
            + ", ".join(p["partition"] for p in coverage["uncovered_partitions"][:5])
            + " receive no records under this method."
        )
    return Plan(
        method=method,
        seed=seed,
        budget=budget,
        snapshot_id=snapshot["snapshot_id"],
        files=list(file_plans.values()),
        population=total,
        channels=channels,
        rare_kinds=rare_kinds,
        new_kinds=new_kinds,
        new_partitions=new_partitions,
        coverage=coverage,
        warnings=warnings,
    )


def _plan_head(file_plans: dict[str, FilePlan], files: list[DataFile], budget: int) -> None:
    remaining = budget
    for f in files:
        if remaining <= 0:
            break
        take = min(f.line_count, remaining)
        file_plans[f.path].rep_n = take
        remaining -= take


def _plan_partition_stratified(
    file_plans: dict[str, FilePlan],
    partitions: dict[str, list[DataFile]],
    budget: int,
    floor: int,
) -> None:
    names = sorted(partitions)
    weights = [sum(f.line_count for f in partitions[n]) for n in names]
    alloc = _allocate_with_floor(budget, weights, floor, weights)
    for name, part_alloc in zip(names, alloc):
        group = sorted(partitions[name], key=lambda f: f.file_index)
        if part_alloc <= 0:
            continue
        per_file = _largest_remainder(
            part_alloc,
            [f.line_count for f in group],
            [f.line_count for f in group],
        )
        for f, n in zip(group, per_file):
            file_plans[f.path].rep_n = n


def _plan_kind_stratified(
    file_plans: dict[str, FilePlan],
    files: list[DataFile],
    kind_counts: dict[str, int],
    file_kind: dict[str, dict[str, int]],
    budget: int,
) -> tuple[int, set[str]]:
    kinds = sorted(kind_counts)
    frame_totals = [
        sum(
            len(f.stats.get("kind_line_offsets", {}).get(kind, []))
            for f in files
            if kind in file_kind[f.path]
        )
        for kind in kinds
    ]
    weights = [math.sqrt(kind_counts[k]) for k in kinds]
    alloc = _allocate_with_floor(budget, weights, 2, frame_totals)
    used = 0
    uncovered: set[str] = set()
    for kind, n_kind in zip(kinds, alloc):
        if n_kind <= 0:
            continue
        group = [f for f in files if file_kind[f.path].get(kind, 0) > 0]
        frames = [
            len(f.stats.get("kind_line_offsets", {}).get(kind, [])) for f in group
        ]
        per_file = _largest_remainder(n_kind, frames, frames)
        granted = 0
        for f, n in zip(group, per_file):
            if n > 0:
                file_plans[f.path].kind_n[kind] = n
                granted += n
        if granted == 0:
            uncovered.add(kind)
        used += granted
    return used, uncovered


def _rare_supplement_budget(
    kind_counts: dict[str, int],
    new_kinds: list[str],
    rare_kinds: list[str],
    budget: int,
) -> int:
    cap = int(budget * config.RARE_SUPPLEMENT_FRACTION)
    demand = 0
    for kind in rare_kinds:
        target = config.RARE_TARGET_OBSERVATIONS
        if kind in new_kinds:
            target = max(2, target // 2)
        demand += min(kind_counts[kind], target)
    return max(0, min(cap, demand))


def _plan_rare_supplement(
    file_plans: dict[str, FilePlan],
    files: list[DataFile],
    kind_counts: dict[str, int],
    rare_kinds: list[str],
    new_kinds: list[str],
    budget: int,
) -> int:
    if budget <= 0 or not rare_kinds:
        return 0
    demand = {}
    for kind in rare_kinds:
        target = config.RARE_TARGET_OBSERVATIONS
        if kind in new_kinds:
            target = max(2, target // 2)
        demand[kind] = min(kind_counts[kind], target)
    allocated = _largest_remainder(
        budget, [demand[k] for k in rare_kinds], [demand[k] for k in rare_kinds]
    )
    used = 0
    for kind, want in zip(rare_kinds, allocated):
        if want <= 0:
            continue
        group = [f for f in files if f.kinds.get(kind, 0) > 0]
        frames = [len(f.stats.get("kind_line_offsets", {}).get(kind, [])) for f in group]
        per_file = _largest_remainder(want, frames, frames)
        for f, n in zip(group, per_file):
            if n > 0:
                file_plans[f.path].rare_by_kind[kind] = n
                used += n
    return used


def _coverage(
    partitions: dict[str, list[DataFile]],
    partition_totals: dict[str, int],
    file_plans: dict[str, FilePlan],
    total: int,
    kind_counts: dict[str, int],
) -> dict:
    planned_total = max(1, sum(fp.planned for fp in file_plans.values()))
    per_partition = {}
    for name, group in partitions.items():
        planned = sum(file_plans[f.path].planned for f in group)
        per_partition[name] = planned
    rows = []
    for name in sorted(partitions):
        population = partition_totals[name]
        planned = per_partition[name]
        rows.append(
            {
                "partition": name,
                "population": population,
                "planned": planned,
                "population_share": population / total,
                "planned_share": planned / planned_total,
                "covered": planned > 0,
            }
        )
    uncovered = [r for r in rows if not r["covered"]]
    uncovered_records = sum(r["population"] for r in uncovered)
    random_total = max(1, sum(fp.rep_n for fp in file_plans.values()))
    kind_rows = []
    for kind, count in kind_counts.items():
        representable = sum(fp.kind_frame.get(kind, 0) for fp in file_plans.values())
        kind_rows.append(
            {
                "kind": kind,
                "population": count,
                "share": count / total,
                "rare": count / total < config.RARE_SHARE_THRESHOLD,
                "index_frame": representable,
                "index_capped": representable < count,
            }
        )
    return {
        "partitions": rows,
        "uncovered_partitions": uncovered,
        "uncovered_population": uncovered_records,
        "uncovered_share": uncovered_records / total,
        "representative_total": random_total,
        "kinds": kind_rows,
        "kind_count": len(kind_rows),
        "rare_kind_count": sum(1 for r in kind_rows if r["rare"]),
    }


# ---------------------------------------------------------------- execution
class LineReader:
    """Seek-based bounded reader with byte accounting and a small line cache."""

    def __init__(self, path: Path, checkpoints: list[list[int]]):
        self.path = path
        self._handle = path.open("rb")
        self._starts = [int(cp[0]) for cp in checkpoints]
        self._offsets = [int(cp[1]) for cp in checkpoints]
        self.lines_read = 0
        self.bytes_read = 0
        self.seeks = 0
        self._last: tuple[int, bytes] | None = None

    def close(self) -> None:
        self._handle.close()

    def __enter__(self) -> "LineReader":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _checkpoint_for(self, index: int) -> tuple[int, int]:
        if not self._starts:
            return 0, 0
        pos = bisect.bisect_right(self._starts, index) - 1
        if pos < 0:
            pos = 0
        return self._starts[pos], self._offsets[pos]

    def read_index(self, index: int, offset_hint: int | None = None) -> tuple[int, bytes] | None:
        """Read exactly the line at `index`; returns (index, bytes) or None."""
        if self._last is not None and self._last[0] == index:
            return self._last
        if offset_hint is not None:
            read_from, seek_to = index, offset_hint
        else:
            read_from, seek_to = self._checkpoint_for(index)
        if self._last is not None and self._last[0] + 1 == index and offset_hint is None:
            line = self._handle.readline()
            if line:
                self.lines_read += 1
                self.bytes_read += len(line)
                self._last = (index, line)
                return self._last
            return None
        self._handle.seek(seek_to)
        self.seeks += 1
        current = read_from
        while current <= index:
            line = self._handle.readline()
            if not line:
                return None
            self.lines_read += 1
            self.bytes_read += len(line)
            if current == index:
                self._last = (current, line)
                return self._last
            current += 1
        return None

    def stats(self) -> dict:
        return {
            "lines_read": self.lines_read,
            "bytes_read": self.bytes_read,
            "seeks": self.seeks,
        }


def _parse(line: bytes) -> dict | None:
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        return None
    return record if isinstance(record, dict) else None


def _row(
    record: dict,
    data_file: DataFile,
    line_index: int,
    channel: str,
    stratum: str,
    population: int,
    sample_n: int,
    weight: float | None,
    note: str,
    extra: dict | None = None,
) -> dict:
    row = {
        "record": record,
        "kind": record.get("kind"),
        "partition": f"{data_file.date}/{data_file.region}",
        "channel": channel,
        "stratum": stratum,
        "weight": round(weight, 6) if weight is not None else None,
        "inclusion_probability": round(1.0 / weight, 8) if weight else None,
        "population": population,
        "sample_n": sample_n,
        "source_path": data_file.path,
        "source_line": line_index,
        "note": note,
    }
    if extra:
        row.update(extra)
    return row


def _read_kind_picks(
    reader: LineReader,
    data_file: DataFile,
    picks: dict[str, int],
    rng_namespace: tuple,
    channel: str,
    data_file_plan: FilePlan,
    selected: set[int],
) -> tuple[list[dict], int, dict]:
    """Read per-kind picks from the sparse frame.

    If a planned position was already selected by another channel, advance
    deterministically to the next unclaimed slot in the same frame.  When the
    frame is exhausted (every indexed position is already in the sample), the
    kind is already represented; the pick is recorded as satisfied rather than
    duplicated or silently dropped.
    """
    rows: list[dict] = []
    parse_errors = 0
    satisfied_by_other = 0
    offsets_by_kind = data_file.stats.get("kind_line_offsets", {})
    capped = set(data_file.stats.get("kind_offsets_capped", []))
    for kind, wanted in sorted(picks.items()):
        positions = offsets_by_kind.get(kind, [])
        frame = len(positions)
        take = min(wanted, frame)
        if take <= 0:
            continue
        rng = seeded_rng(*rng_namespace, data_file.path, kind, channel)
        chosen_slots = _sample_indices(frame, take, rng)
        used_slots = {
            slot for slot in range(frame) if int(positions[slot][0]) in selected
        }
        resolved: list[int] = []
        for slot in chosen_slots:
            if slot not in used_slots:
                resolved.append(slot)
                used_slots.add(slot)
                continue
            for step in range(1, frame + 1):
                candidate = (slot + step) % frame
                if candidate not in used_slots:
                    resolved.append(candidate)
                    used_slots.add(candidate)
                    break
            else:
                # The whole frame is already in the sample via another channel.
                satisfied_by_other += 1
        delivered = 0
        kind_rows: list[dict] = []
        population = data_file_plan.kind_counts.get(kind, 0)
        for slot in resolved:
            line_index, byte_offset = positions[slot]
            line_index = int(line_index)
            if line_index in selected:
                continue
            found = reader.read_index(line_index, offset_hint=int(byte_offset))
            if found is None:
                parse_errors += 1
                continue
            actual, line = found
            record = _parse(line)
            if record is None or str(record.get("kind")) != kind:
                parse_errors += 1
                continue
            selected.add(actual)
            delivered += 1
            kind_rows.append(
                _row(
                    record=record,
                    data_file=data_file,
                    line_index=actual,
                    channel=channel,
                    stratum=f"file-kind:{data_file.path}|{kind}",
                    population=population,
                    sample_n=delivered,  # provisional; fixed up below
                    weight=None,
                    note="",
                    extra={
                        "index_frame": frame,
                        "index_frame_capped": kind in capped,
                        "kind_population": population,
                    },
                )
            )
        # The design weight uses the number actually delivered in this stratum
        # (which can be smaller than planned only if indexed positions ran out).
        n_kind = max(1, delivered)
        for row in kind_rows:
            row["sample_n"] = n_kind
            row["weight"] = round(population / n_kind, 6)
            row["inclusion_probability"] = round(n_kind / population, 8) if population else None
            row["note"] = (
                "design weight N_file_kind / n_file_kind within the file-kind stratum"
                if channel == CHANNEL_KIND
                else "second phase; weight is per file-kind stratum and must not be "
                "pooled with representative rows"
            )
        rows.extend(kind_rows)
    return rows, parse_errors, {"satisfied_by_other_channel": satisfied_by_other}


def execute_plan_file(
    catalog: Catalog,
    plan: Plan,
    data_file: DataFile,
    file_plan: FilePlan,
) -> tuple[list[dict], dict]:
    """Execute one file's share of a plan."""
    path = catalog.storage_path(data_file)
    rows: list[dict] = []
    parse_errors = 0
    requested = file_plan.planned
    if plan.method == "head":
        with path.open("rb") as handle:
            for index in range(min(file_plan.rep_n, data_file.line_count)):
                line = handle.readline()
                if not line:
                    break
                record = _parse(line)
                if record is None:
                    parse_errors += 1
                    continue
                rows.append(
                    _row(
                        record=record,
                        data_file=data_file,
                        line_index=index,
                        channel=CHANNEL_HEAD,
                        stratum=f"file-prefix:{data_file.path}",
                        population=data_file.line_count,
                        sample_n=file_plan.rep_n,
                        weight=None,
                        note="non-probability head sample: no design weight",
                    )
                )
        return rows, {
            "rows": len(rows),
            "requested": requested,
            "parse_errors": parse_errors,
            "lines_read": min(file_plan.rep_n, data_file.line_count),
            "bytes_read": 0,
            "seeks": 0,
        }

    checkpoints = data_file.stats.get("checkpoints", [])
    selected: set[int] = set()
    extra_stats: dict = {}
    with LineReader(path, checkpoints) as reader:
        if plan.method == "kind":
            rows, parse_errors, extra_stats = _read_kind_picks(
                reader,
                data_file,
                file_plan.kind_n,
                (plan.seed, plan.snapshot_id, plan.method),
                CHANNEL_KIND,
                file_plan,
                selected,
            )
        else:
            channel = CHANNEL_UNIFORM if plan.method == "uniform" else CHANNEL_REP
            if file_plan.rep_n:
                rng = seeded_rng(
                    plan.seed, plan.snapshot_id, plan.method, data_file.path, "rep"
                )
                for index in _sample_indices(data_file.line_count, file_plan.rep_n, rng):
                    found = reader.read_index(index)
                    if found is None:
                        parse_errors += 1
                        continue
                    actual, line = found
                    record = _parse(line)
                    if record is None:
                        parse_errors += 1
                        continue
                    selected.add(actual)
                    rows.append(
                        _row(
                            record=record,
                            data_file=data_file,
                            line_index=actual,
                            channel=channel,
                            stratum=f"file:{data_file.path}",
                            population=data_file.line_count,
                            sample_n=file_plan.rep_n,
                            weight=data_file.line_count / max(1, file_plan.rep_n),
                            note="design weight N_file / n_file",
                        )
                    )
            if file_plan.rare_by_kind:
                rare_rows, rare_errors, extra_stats = _read_kind_picks(
                    reader,
                    data_file,
                    file_plan.rare_by_kind,
                    (plan.seed, plan.snapshot_id, plan.method),
                    CHANNEL_RARE,
                    file_plan,
                    selected,
                )
                rows.extend(rare_rows)
                parse_errors += rare_errors
        stats = reader.stats()
    return rows, {
        "rows": len(rows),
        "requested": requested,
        "parse_errors": parse_errors,
        **extra_stats,
        **stats,
    }
