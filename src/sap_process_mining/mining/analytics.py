"""Process-mining analytics.

Deliberately implemented without pm4py or other heavy frameworks. The algorithms here are
straightforward pandas and are easy to inspect, extend, or port. For larger-scale mining
(millions of events) you'd swap in pm4py or an in-database implementation — the Findings
shape on output is the contract and doesn't change.
"""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from ..event_log import EventLog
from ..processes import ProcessDefinition, get_process, o2c_process
from .findings import AnomalyCase, DimensionalFinding, Findings, TransitionStat, VariantStat


# Map of log.process_name → process slug, so analyze() can auto-select the process
# if the caller doesn't pass one in.
_PROCESS_NAME_TO_SLUG = {
    "order_to_cash": "o2c",
    "procure_to_pay": "p2p",
}


def analyze(
    log: EventLog,
    process: ProcessDefinition | None = None,
    top_variants: int = 6,
    top_bottlenecks: int = 12,
    anomaly_z: float = 3.0,
) -> Findings:
    """Produce a Findings object describing bottlenecks, variants, anomalies, rework.

    If `process` is omitted, it's inferred from `log.process_name`. Fall back to O2C
    for backwards compatibility with logs that predate the multi-process refactor.
    """
    if process is None:
        slug = _PROCESS_NAME_TO_SLUG.get(log.process_name)
        process = get_process(slug) if slug else o2c_process

    df = log.df
    cycle = _cycle_times(df)
    transitions = _transition_stats(df)
    bottlenecks = _dimensional_bottlenecks(
        df, transitions, dimensions=list(process.dimensions), top_k=top_bottlenecks
    )
    variants = _variant_stats(df, cycle, process.happy_path, top_k=top_variants)
    rework_rate, rework_activities = _rework(df)
    anomalies = _anomalies(df, cycle, z=anomaly_z)

    start, end = log.time_range
    happy_path_cycle_median = _happy_path_median_cycle(variants, cycle)

    return Findings(
        process=log.process_name,
        source=log.source,
        window_start=start.isoformat(),
        window_end=end.isoformat(),
        n_cases=log.n_cases,
        n_events=log.n_events,
        median_cycle_hours=float(cycle["cycle_hours"].median()) if not cycle.empty else 0.0,
        p90_cycle_hours=float(cycle["cycle_hours"].quantile(0.9)) if not cycle.empty else 0.0,
        on_time_rate=(
            float((cycle["cycle_hours"] <= happy_path_cycle_median).mean())
            if happy_path_cycle_median and not cycle.empty
            else None
        ),
        transitions=transitions,
        bottlenecks=bottlenecks,
        variants=variants,
        rework_rate=rework_rate,
        rework_activities=rework_activities,
        anomalies=anomalies,
    )


# --- cycle time -----------------------------------------------------------------------


def _cycle_times(df: pd.DataFrame) -> pd.DataFrame:
    grp = df.groupby("case_id")["timestamp"].agg(["min", "max"]).reset_index()
    grp["cycle_hours"] = (grp["max"] - grp["min"]).dt.total_seconds() / 3600.0
    return grp


# --- transition stats -----------------------------------------------------------------


def _transition_stats(df: pd.DataFrame) -> list[TransitionStat]:
    """Duration between consecutive activities within each case (directly-follows)."""
    df = df.sort_values(["case_id", "timestamp"]).reset_index(drop=True)
    df["next_activity"] = df.groupby("case_id")["activity"].shift(-1)
    df["next_ts"] = df.groupby("case_id")["timestamp"].shift(-1)
    pairs = df.dropna(subset=["next_activity", "next_ts"]).copy()
    pairs["hours"] = (pairs["next_ts"] - pairs["timestamp"]).dt.total_seconds() / 3600.0
    pairs = pairs[pairs["hours"] >= 0]

    agg = (
        pairs.groupby(["activity", "next_activity"])["hours"]
        .agg(["size", "median", lambda s: s.quantile(0.9), "mean", "std"])
        .reset_index()
    )
    agg.columns = ["from_activity", "to_activity", "n_cases", "median", "p90", "mean", "std"]
    agg = agg.sort_values("median", ascending=False)

    return [
        TransitionStat(
            from_activity=r.from_activity,
            to_activity=r.to_activity,
            n_cases=int(r.n_cases),
            median_hours=float(r.median),
            p90_hours=float(r.p90),
            mean_hours=float(r.mean),
            std_hours=float(r.std) if not pd.isna(r.std) else 0.0,
        )
        for r in agg.itertuples(index=False)
    ]


# --- dimensional bottlenecks ----------------------------------------------------------


def _dimensional_bottlenecks(
    df: pd.DataFrame,
    transitions: list[TransitionStat],
    dimensions: list[str],
    top_k: int,
    min_cases: int = 10,
    min_lift: float = 1.5,
) -> list[DimensionalFinding]:
    """For each transition, find dimension values with disproportionately high duration.

    We look across *all* transitions — not just the globally-slowest ones — because a
    bottleneck may be concentrated in a minority of cases (e.g. Plant 1000 credit-check
    adds days to 20% of orders but leaves the overall median untouched).

    Findings are ranked by `impact = (median_hours - overall_median_hours) * n_cases`,
    i.e. total wasted hours attributable to the slice. Ties broken by lift.
    """
    df = df.sort_values(["case_id", "timestamp"]).copy()
    df["next_activity"] = df.groupby("case_id")["activity"].shift(-1)
    df["next_ts"] = df.groupby("case_id")["timestamp"].shift(-1)
    pairs = df.dropna(subset=["next_activity", "next_ts"]).copy()
    pairs["hours"] = (pairs["next_ts"] - pairs["timestamp"]).dt.total_seconds() / 3600.0
    pairs = pairs[pairs["hours"] >= 0]

    findings: list[tuple[float, DimensionalFinding]] = []

    for tr in transitions:
        subset = pairs[(pairs["activity"] == tr.from_activity) & (pairs["next_activity"] == tr.to_activity)]
        if len(subset) < min_cases:
            continue
        overall_median = float(subset["hours"].median())
        for dim in dimensions:
            if dim not in subset.columns:
                continue
            grouped = subset.groupby(dim)["hours"].agg(["size", "median"]).reset_index()
            grouped = grouped[grouped["size"] >= min_cases]
            if grouped.empty:
                continue
            grouped["lift"] = grouped["median"] / max(overall_median, 1e-6)
            worst = grouped[grouped["lift"] >= min_lift]
            for r in worst.itertuples(index=False):
                impact = (float(r.median) - overall_median) * int(r.size)
                finding = DimensionalFinding(
                    id="",  # assigned after sort below
                    transition=f"{tr.from_activity} → {tr.to_activity}",
                    dimension=dim,
                    value=str(getattr(r, dim)),
                    n_cases=int(r.size),
                    median_hours=float(r.median),
                    overall_median_hours=overall_median,
                    lift=float(r.lift),
                )
                findings.append((impact, finding))

    findings.sort(key=lambda pair: (pair[0], pair[1].lift), reverse=True)
    ranked = [f for _, f in findings[:top_k]]
    for i, f in enumerate(ranked, start=1):
        f.id = f"B{i}"
    return ranked


# --- variant analysis -----------------------------------------------------------------


def _variant_stats(
    df: pd.DataFrame,
    cycle: pd.DataFrame,
    happy_path: tuple[str, ...],
    top_k: int,
) -> list[VariantStat]:
    sequences = (
        df.sort_values(["case_id", "timestamp"])
        .groupby("case_id")["activity"]
        .apply(tuple)
    )
    variant_counts = sequences.value_counts()
    total = int(variant_counts.sum())
    cycle_by_case = cycle.set_index("case_id")["cycle_hours"]

    results: list[VariantStat] = []
    for seq, n in variant_counts.head(top_k).items():
        cases_with_seq = sequences[sequences == seq].index
        med = float(cycle_by_case.reindex(cases_with_seq).median()) if len(cases_with_seq) else 0.0
        results.append(VariantStat(
            sequence=list(seq),
            n_cases=int(n),
            share=float(n) / total if total else 0.0,
            is_happy_path=(tuple(seq) == happy_path),
            median_cycle_hours=med,
        ))
    return results


def _happy_path_median_cycle(variants: list[VariantStat], cycle: pd.DataFrame) -> float | None:
    for v in variants:
        if v.is_happy_path:
            return v.median_cycle_hours
    if cycle.empty:
        return None
    return float(cycle["cycle_hours"].median())


# --- rework ---------------------------------------------------------------------------


def _rework(df: pd.DataFrame) -> tuple[float, dict[str, int]]:
    """Share of cases that repeat at least one activity, plus a count per activity."""
    per_case = df.groupby(["case_id", "activity"]).size().reset_index(name="n")
    repeated = per_case[per_case["n"] > 1]
    n_cases_with_rework = repeated["case_id"].nunique()
    total_cases = df["case_id"].nunique()
    rework_rate = float(n_cases_with_rework) / total_cases if total_cases else 0.0
    per_activity = repeated.groupby("activity")["case_id"].nunique().to_dict()
    return rework_rate, {k: int(v) for k, v in per_activity.items()}


# --- anomaly detection ----------------------------------------------------------------


def _anomalies(df: pd.DataFrame, cycle: pd.DataFrame, z: float = 3.0, max_report: int = 10) -> list[AnomalyCase]:
    if cycle.empty:
        return []
    med = cycle["cycle_hours"].median()
    std = cycle["cycle_hours"].std()
    if std == 0 or np.isnan(std):
        return []
    threshold = med + z * std
    outliers = cycle[cycle["cycle_hours"] > threshold].sort_values("cycle_hours", ascending=False).head(max_report)

    # Grab representative attributes from the first event of each case.
    first_events = df.drop_duplicates(subset=["case_id"], keep="first").set_index("case_id")
    attr_cols = [c for c in df.columns if c not in {"case_id", "activity", "timestamp", "note"}]

    results: list[AnomalyCase] = []
    for i, r in enumerate(outliers.itertuples(index=False), start=1):
        attrs = {}
        if r.case_id in first_events.index:
            row = first_events.loc[r.case_id]
            attrs = {c: (row[c].item() if hasattr(row[c], "item") else row[c]) for c in attr_cols if c in first_events.columns and pd.notna(row[c])}
        results.append(AnomalyCase(
            id=f"A{i}",
            case_id=str(r.case_id),
            reason=f"cycle time {r.cycle_hours:.1f}h exceeds {z}σ above median ({threshold:.1f}h)",
            cycle_hours=float(r.cycle_hours),
            attributes=attrs,
        ))
    return results
