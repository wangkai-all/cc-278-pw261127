"""Quantitative method comparison against an exact full scan.

Produces reports/method-comparison.json + .md from the same synthetic corpus
used by the service.  Run: python3 -m warehouse.report --base .
"""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path

from . import config
from .catalog import Catalog
from .generate import bootstrap
from .sampling import build_plan, execute_plan_file
from .util import atomic_write_json


def exact_counts(files) -> Counter:
    counts: Counter = Counter()
    for f in files:
        for kind, n in f.kinds.items():
            counts[kind] += int(n)
    return counts


def run_method(catalog, snapshot, files, method, seed, budget) -> dict:
    started = time.perf_counter()
    plan = build_plan(catalog, snapshot, files, method, seed, budget)
    by_path = {f.path: f for f in files}
    rows = []
    lines = 0
    seeks = 0
    for file_plan in plan.files:
        got, stats = execute_plan_file(
            catalog, plan, by_path[file_plan.path], file_plan
        )
        rows.extend(got)
        lines += int(stats.get("lines_read", 0))
        seeks += int(stats.get("seeks", 0))
    elapsed = time.perf_counter() - started
    return {
        "method": method,
        "budget": budget,
        "planned": plan.planned,
        "rows": len(rows),
        "lines_read": lines,
        "seeks": seeks,
        "read_amplification": round(lines / max(1, len(rows)), 3),
        "channels": plan.channels,
        "rare_kinds": plan.rare_kinds,
        "new_kinds": plan.new_kinds,
        "uncovered_partitions": len(plan.coverage["uncovered_partitions"]),
        "uncovered_share": round(plan.coverage["uncovered_share"], 6),
        "elapsed_ms": round(elapsed * 1000, 1),
        "warnings": plan.warnings,
        "_rows": rows,
    }


def estimate_errors(rows: list[dict], full: Counter, total: int) -> dict:
    """Hájek estimate per kind from representative rows only."""
    rep = [r for r in rows if r.get("channel") in ("representative", "uniform")]
    if not rep:
        return {"estimable": False, "kinds": {}}
    total_weight = sum(float(r["weight"]) for r in rep)
    observed = Counter(r["kind"] for r in rep)
    observed_all = Counter(r["kind"] for r in rows)
    out = {}
    for kind in full:
        mass = sum(float(r["weight"]) for r in rep if r["kind"] == kind)
        estimate = mass / total_weight * total if total_weight else 0.0
        out[kind] = {
            "full": int(full[kind]),
            "estimate": round(estimate, 2),
            "relative_error": (
                round((estimate - full[kind]) / full[kind], 4) if full[kind] else None
            ),
            "observed_representative": observed.get(kind, 0),
            "observed_any_channel": observed_all.get(kind, 0),
        }
    return {"estimable": True, "kinds": out}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="warehouse.report")
    parser.add_argument("--base", default=".")
    parser.add_argument("--records", type=int, default=config.DEFAULT_RECORDS_PER_PARTITION)
    parser.add_argument("--budgets", default="20,200,1200")
    parser.add_argument("--seed", default="report-v1")
    args = parser.parse_args(argv)
    base = Path(args.base)
    data_root = base / "data"
    if not any(data_root.glob("*/*/*.ndjson")):
        bootstrap(data_root, records_per_partition=args.records, progress=print)
    catalog = Catalog(data_root)
    files = catalog.ensure_indexes(catalog.scan(use_cache=False))
    snapshot = catalog.ensure_current(files, note="report")
    snapshot, files = catalog.resolve()
    full = exact_counts(files)
    total = sum(full.values())
    budgets = [int(b) for b in args.budgets.split(",") if b.strip()]

    results = []
    for budget in budgets:
        for method in config.PREVIEW_METHODS:
            result = run_method(catalog, snapshot, files, method, args.seed, budget)
            rows = result.pop("_rows")
            result["estimates"] = estimate_errors(rows, full, total)
            result["rare_observed"] = sorted(
                {
                    r["kind"]
                    for r in rows
                    if r["channel"] == "rare-supplement"
                }
            )
            result.pop("warnings")
            results.append(result)
            print(
                f"{method:16s} budget={budget:5d} rows={result['rows']:5d} "
                f"lines={result['lines_read']:6d} uncovered_partitions="
                f"{result['uncovered_partitions']}"
            )
    payload = {
        "snapshot_id": snapshot["snapshot_id"],
        "population": total,
        "full_counts": dict(sorted(full.items())),
        "methods": results,
        "notes": {
            "estimates": (
                "Hájek weighted estimates from representative/uniform rows only; "
                "rare-supplement rows are coverage, not population estimates."
            ),
            "head": "non-probability; no estimates column is meaningful.",
            "read_budget": (
                "lines_read counts physical NDJSON lines touched; seeks counts "
                "byte seeks.  Full scan of this snapshot would read "
                f"{total} lines."
            ),
        },
    }
    reports = base / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    atomic_write_json(reports / "method-comparison.json", payload)
    (reports / "method-comparison.md").write_text(render_markdown(payload), "utf-8")
    print(f"wrote {reports / 'method-comparison.md'}")
    return 0


def render_markdown(payload: dict) -> str:
    lines = [
        "# 抽样方法对照（同一快照，全量为真值）",
        "",
        f"- 快照：`{payload['snapshot_id']}`",
        f"- 全量记录：{payload['population']:,}",
        f"- 全量类型数：{len(payload['full_counts'])}",
        "",
        "## 读取预算与代表性",
        "",
        "| 方法 | 预算 | 实际样本 | 读取行数 | 读取放大 | seek | 未覆盖分区 | 未覆盖人口占比 | 稀有通道命中类型数 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for m in payload["methods"]:
        lines.append(
            f"| {m['method']} | {m['budget']} | {m['rows']} | {m['lines_read']} | "
            f"{m['read_amplification']}× | {m['seeks']} | {m['uncovered_partitions']} | "
            f"{m['uncovered_share']*100:.2f}% | {len(m['rare_observed'])} |"
        )
    lines += [
        "",
        "## 稀有与新增类型的可见性（预算 1200）",
        "",
        "| 方法 | estimated kinds | 稀有类型观测 | chargeback_trace | pii_redaction_audit | shadow_replay | black_friday_doorbuster |",
        "|---|---:|---|---:|---:|---:|---:|",
    ]
    rare_of_interest = [
        "chargeback_trace",
        "pii_redaction_audit",
        "shadow_replay",
        "black_friday_doorbuster",
    ]
    for m in payload["methods"]:
        if m["budget"] != 1200:
            continue
        est = m["estimates"]
        cells = []
        for kind in rare_of_interest:
            if est["estimable"]:
                cells.append(str(est["kinds"].get(kind, {}).get("observed_any_channel", 0)))
            else:
                cells.append("—")
        lines.append(
            f"| {m['method']} | {len(est['kinds']) if est['estimable'] else 0} | "
            f"{len(m['rare_observed'])} | " + " | ".join(cells) + " |"
        )
    lines += [
        "",
        "## 估计误差（代表性通道，预算 1200，相对误差）",        "",
        "| 类型 | 全量 | head | uniform | stratified | kind | stratified_rare |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    by_method = {m["method"]: m for m in payload["methods"] if m["budget"] == 1200}
    for kind in payload["full_counts"]:
        row = [f"| {kind} | {payload['full_counts'][kind]:,} "]
        for method in ("head", "uniform", "stratified", "kind", "stratified_rare"):
            m = by_method.get(method)
            est = m["estimates"] if m else {"estimable": False}
            if not est.get("estimable"):
                row.append("| — ")
            else:
                info = est["kinds"].get(kind)
                if not info or info["relative_error"] is None:
                    row.append("| — ")
                else:
                    row.append(f"| {info['relative_error']*100:+.1f}% ")
        lines.append("".join(row) + "|")
    lines += [
        "",
        "## 结论",
        "",
        "- `head` 读取最少但只是文件前缀：低占比类型可能完全缺席，且没有设计权重，不能外推。",
        "- `uniform` 严格比例分配；预算远小于文件数（如 20 条 vs 144 文件）时会让分区整块消失，"
        "预算足够覆盖每个文件后缺口才消失。",
        "- `stratified` 每分区保底 1 条，牺牲少量估计精度换取覆盖完整性。",
        "- `kind` 单通道按类型分层，稀有类型观测稳定，但高频类型受稀疏索引帧上限约束。",
        "- `stratified_rare` 用代表性通道做无偏估计、稀有通道补覆盖；两通道权重不可相加，"
        "这正是“预览统计不能冒充全量结果”的实现约束。",
        "",
        "读取预算说明：每行样本约触发 1 次 seek，物理读取约为 `CHECKPOINT_STRIDE` 行/样本；"
        "全量精确计数需要另跑 full query，服务不会用预览数字替代它。",
        "",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
