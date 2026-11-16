"""Estimate statistics from a preview sample, explicitly marked as estimates.

All rates use the declared design weights.  For a stratified probability
sample the weight of a record is N_h / n_h within its file stratum.  A Hájek
ratio estimator gives the kind proportion

    p_hat = sum_i w_i y_i / sum_i w_i,          y_i = 1[kind_i == kind]

with linearised values z_i = w_i (y_i - p_hat) / sum_j w_j and a
with-replacement variance per stratum (conservative under SRSWOR):

    var(p_hat) = sum_h (1 - n_h/N_h) * n_h/(n_h-1) * sum_{i in h} z_i^2

The estimated count is p_hat * N_population (N_population is exact from the
committed snapshot).  The rare-supplement channel is reported separately: it
is aimed at coverage, and mixing it into the representative estimate would
double count file mass.  The head method has no design weights, so no
estimate is published at all.
"""

from __future__ import annotations

import math
from collections import defaultdict

Z95 = 1.959963984540054


def _fpc(n: int, population: float) -> float:
    if population <= 0:
        return 0.0
    return max(0.0, 1.0 - n / population)


def estimate_counts(rows: list[dict], total_population: int) -> dict:
    usable = [
        r
        for r in rows
        if r.get("weight") and r.get("channel") in ("representative", "uniform")
    ]
    if not usable:
        return {
            "estimable": False,
            "reason": (
                "no design-weighted representative rows: this method is a "
                "non-probability sample (head) or covers only kind/rare strata, "
                "so it is not pooled into a population estimate"
            ),
            "kinds": [],
            "total_weight": 0.0,
            "population": total_population,
        }

    strata: dict[str, dict] = {}
    for row in usable:
        stratum = row["stratum"]
        entry = strata.setdefault(
            stratum,
            {"population": float(row["population"]), "n": 0, "weight": float(row["weight"])},
        )
        entry["n"] += 1
    total_weight = sum(s["n"] * s["weight"] for s in strata.values())
    if total_weight <= 0:
        return {"estimable": False, "reason": "total design weight is zero", "kinds": []}

    kinds = sorted({str(r["kind"]) for r in usable})
    per_kind: list[dict] = []
    for kind in kinds:
        mass = sum(float(r["weight"]) for r in usable if str(r["kind"]) == kind)
        proportion = mass / total_weight
        variance = 0.0
        for stratum, info in strata.items():
            group = [
                r
                for r in usable
                if r["stratum"] == stratum and str(r["kind"]) == kind
            ]
            n_h = info["n"]
            z2 = sum(
                ((float(r["weight"]) * (1.0 - proportion)) / total_weight) ** 2
                for r in group
            )
            # Rows of other kinds in the stratum contribute -p*w/total_w.
            n_other = n_h - len(group)
            z2_other = n_other * ((info["weight"] * proportion) / total_weight) ** 2
            factor = n_h / (n_h - 1) if n_h >= 2 else 1.0
            variance += _fpc(n_h, info["population"]) * factor * (z2 + z2_other)
        se_prop = math.sqrt(max(0.0, variance))
        estimate_count = proportion * total_population
        count_se = se_prop * total_population
        per_kind.append(
            {
                "kind": kind,
                "estimated_count": round(estimate_count, 2),
                "count_ci95": [
                    round(max(0.0, estimate_count - Z95 * count_se), 2),
                    round(min(float(total_population), estimate_count + Z95 * count_se), 2),
                ],
                "proportion": round(proportion, 6),
                "proportion_ci95": [
                    round(max(0.0, proportion - Z95 * se_prop), 6),
                    round(min(1.0, proportion + Z95 * se_prop), 6),
                ],
                "stderr_proportion": round(se_prop, 6),
                "observed": sum(1 for r in usable if str(r["kind"]) == kind),
                "strata_observed": len(
                    {r["stratum"] for r in usable if str(r["kind"]) == kind}
                ),
            }
        )
    return {
        "estimable": True,
        "population": total_population,
        "total_weight": round(total_weight, 2),
        "sample_rows": len(usable),
        "strata": len(strata),
        "kinds": per_kind,
        "method_note": (
            "Hájek weighted proportions with a with-replacement variance "
            "approximation and finite-population correction.  Estimates over the "
            "committed snapshot; not a full-query result."
        ),
    }


def rare_coverage(rows: list[dict], plan: dict | None) -> dict:
    """Observed reach of rare kinds across both channels."""
    observed: dict[str, int] = defaultdict(int)
    via_rare: dict[str, int] = defaultdict(int)
    for row in rows:
        kind = str(row.get("kind"))
        observed[kind] += 1
        if row.get("channel") == "rare-supplement":
            via_rare[kind] += 1
    coverage = (plan or {}).get("coverage", {})
    kind_rows = {r["kind"]: r for r in coverage.get("kinds", [])}
    rare_kinds = (plan or {}).get("rare_kinds", [])
    out = []
    target = 12  # the supplement's per-kind aim, for display only
    for kind in rare_kinds:
        info = kind_rows.get(kind, {})
        population = int(info.get("population", 0))
        seen = observed.get(kind, 0)
        out.append(
            {
                "kind": kind,
                "population": population,
                "share": info.get("share"),
                "observations": seen,
                "observations_via_rare_channel": via_rare.get(kind, 0),
                "target": target,
                "covered": seen > 0,
                "reached_share_of_kind": (seen / population if population else None),
            }
        )
    total_rare = sum(r["population"] for r in out)
    total_observed = sum(r["observations"] for r in out)
    return {
        "rare_kinds": out,
        "rare_population": total_rare,
        "observed_rare_rows": total_observed,
        "rare_kinds_covered": sum(1 for r in out if r["covered"]),
        "rare_kinds_total": len(out),
        "note": (
            "Rare-channel observations are a second-phase supplement aimed at "
            "coverage; they are not the basis of the representative estimate."
        ),
    }


def channel_breakdown(rows: list[dict]) -> dict:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[row.get("channel", "?")] += 1
    return dict(sorted(counts.items()))


def compare_with_full(
    preview_rows: list[dict],
    full_counts: dict[str, int],
    plan: dict | None,
) -> dict:
    """Preview estimate vs exact full-result kind counts on the same snapshot.

    Two different questions are answered separately:
      * `observed_in_preview` counts rows from *any* channel (including the
        rare supplement): did the preview surface this kind at all?
      * `preview_estimate` comes only from the representative channel: can the
        preview project that kind's population size, and how close is it?
    Conflating the two is exactly how a coverage supplement gets mistaken for
    an unbiased estimate.
    """
    full_total = sum(int(v) for v in full_counts.values())
    observed_all: dict[str, int] = defaultdict(int)
    for row in preview_rows:
        observed_all[str(row.get("kind"))] += 1
    estimate = estimate_counts(preview_rows, full_total)
    est_by_kind = {r["kind"]: r for r in estimate.get("kinds", [])}
    rows = []
    for kind in sorted(set(full_counts) | set(est_by_kind) | set(observed_all)):
        true_count = int(full_counts.get(kind, 0))
        est = est_by_kind.get(kind)
        observed = observed_all.get(kind, 0)
        base = {
            "kind": kind,
            "full_count": true_count,
            "full_share": (true_count / full_total) if full_total else None,
            "observed_in_preview": observed,
            "observed_representative": est["observed"] if est else 0,
            "caught_by_preview": observed > 0,
        }
        if est:
            base.update(
                {
                    "preview_estimate": est["estimated_count"],
                    "preview_ci95": est["count_ci95"],
                    "error": round(est["estimated_count"] - true_count, 2),
                    "relative_error": (
                        round((est["estimated_count"] - true_count) / true_count, 6)
                        if true_count
                        else None
                    ),
                }
            )
        else:
            base.update(
                {
                    "preview_estimate": None,
                    "preview_ci95": None,
                    "error": None,
                    "relative_error": None,
                }
            )
        rows.append(base)
    return {
        "estimable": estimate["estimable"],
        "full_total": full_total,
        "preview_population": (plan or {}).get("population"),
        "rows": rows,
        "missed_kinds": [r["kind"] for r in rows if not r["caught_by_preview"]],
        "unestimated_kinds": [r["kind"] for r in rows if r["preview_estimate"] is None],
        "estimator": (
            estimate.get("method_note") if estimate["estimable"] else estimate.get("reason")
        ),
        "disclaimer": (
            "The preview estimate column is a weighted estimate with sampling "
            "error over representative rows only. 'observed_in_preview' also "
            "counts the rare-coverage supplement, which has no population "
            "estimate. The full column is an exact count over the same snapshot."
        ),
    }
