"""Root-cause analysis tools exposed to the Investigator agent.

Each tool is a Python function bound to the persisted event log + findings from the
most recent run. Results must be JSON-serialisable (dicts / lists of primitives).

Tool design notes:
 - Keep payloads small. The agent can call more tools; don't dump thousands of rows.
 - Always include aggregate statistics (median, n) — raw row dumps make the agent
   guess. Return things it can cite.
 - Fail loudly with a helpful `error` key if inputs don't match the data.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

import pandas as pd

from ..event_log import EventLog
from ..llm.base import Tool
from ..mining import Findings
from ..mining.findings import AnomalyCase, DimensionalFinding
from ..processes import ProcessDefinition, get_process


# Used by list_cases to produce a useful per-case attribute summary. We pull from the
# process dimensions where available, plus a couple of process-specific basics so the
# agent sees key context even on low-column event logs.
_O2C_EXTRA_COLS = ("plant", "sales_org", "region", "customer",
                   "primary_item_category", "item_category_mix",
                   "has_mto_item", "has_configurable_item")
_P2P_EXTRA_COLS = ("purchasing_org", "purchasing_group", "plant", "supplier",
                   "primary_item_category", "item_category_mix",
                   "has_service_item", "has_consumable_item",
                   "primary_account_assignment")


# --- tool implementations ------------------------------------------------------------


def _describe_finding(findings: Findings, args: dict) -> dict:
    fid = args["finding_id"]
    f = findings.lookup(fid)
    if f is None:
        return {"error": f"No finding with id '{fid}'. Valid ids: "
                f"{[b.id for b in findings.bottlenecks] + [a.id for a in findings.anomalies]}"}
    return {"kind": type(f).__name__, **asdict(f)}


def _list_cases(df: pd.DataFrame, args: dict, extra_cols: tuple[str, ...]) -> dict:
    """Cases matching a dimension-value slice, ranked by duration in a transition
    (if given) or by total cycle time. Returns up to `limit` case summaries."""
    dim = args["dimension"]
    value = args["value"]
    transition = args.get("transition")
    limit = int(args.get("limit", 15))

    if dim not in df.columns:
        return {"error": f"Unknown dimension '{dim}'. Columns: {sorted(df.columns)}"}

    # Compare as string to tolerate bool/str mismatch coming from JSON args.
    sub = df[df[dim].astype(str) == str(value)]
    if sub.empty:
        return {"n_cases": 0, "cases": [], "note": "no cases match this slice"}

    case_ids = sub["case_id"].unique().tolist()
    scope = df[df["case_id"].isin(case_ids)].sort_values(["case_id", "timestamp"])

    dur = _case_transition_hours(scope, transition) if transition else _case_cycle_hours(scope)
    dur_sorted = sorted(dur.items(), key=lambda kv: kv[1], reverse=True)[:limit]

    cases = []
    for case_id, hours in dur_sorted:
        first = scope[scope["case_id"] == case_id].iloc[0]
        summary = {"case_id": case_id, "hours": round(hours, 2)}
        for col in extra_cols:
            if col in df.columns:
                summary[col] = _safe(first.get(col))
        cases.append(summary)

    return {
        "dimension": dim, "value": str(value), "transition": transition,
        "n_matching_cases": len(case_ids), "returned": len(cases), "cases": cases,
    }


def _get_case_timeline(df: pd.DataFrame, args: dict) -> dict:
    case_id = args["case_id"]
    sub = df[df["case_id"] == case_id].sort_values("timestamp")
    if sub.empty:
        return {"error": f"No events for case_id '{case_id}'"}

    first = sub.iloc[0]
    events = []
    prev_ts = None
    for _, r in sub.iterrows():
        gap = None if prev_ts is None else round(
            (pd.Timestamp(r["timestamp"]) - pd.Timestamp(prev_ts)).total_seconds() / 3600.0, 2
        )
        events.append({
            "activity": r["activity"],
            "timestamp": str(r["timestamp"]),
            "hours_since_prev": gap,
        })
        prev_ts = r["timestamp"]

    attrs_cols = [c for c in df.columns if c not in {"case_id", "activity", "timestamp", "note"}]
    attributes = {c: _safe(first[c]) for c in attrs_cols if pd.notna(first[c])}

    return {
        "case_id": case_id,
        "attributes": attributes,
        "n_events": len(events),
        "total_cycle_hours": round(
            (pd.Timestamp(sub["timestamp"].iloc[-1]) - pd.Timestamp(sub["timestamp"].iloc[0])).total_seconds() / 3600.0,
            2,
        ),
        "events": events,
    }


def _compare_slice_attributes(
    df: pd.DataFrame, args: dict, process_dims: tuple[str, ...]
) -> dict:
    """For the cases in a given slice, what's the distribution of *other* attributes?

    Lets the agent spot interaction effects. E.g. for Plant 1000 slow cases it might see
    that 85 % are also MTO, hinting that the real cause is MTO routing — not the plant
    on its own. The candidate dimensions come from the process definition so the tool
    adapts automatically to O2C, P2P, or future processes.
    """
    dim = args["dimension"]
    value = args["value"]
    if dim not in df.columns:
        return {"error": f"Unknown dimension '{dim}'"}
    sub = df[df[dim].astype(str) == str(value)]
    if sub.empty:
        return {"error": "no cases in slice"}

    cases_in_slice = sub["case_id"].unique()
    case_level = df[df["case_id"].isin(cases_in_slice)].drop_duplicates("case_id", keep="first")
    overall_case_level = df.drop_duplicates("case_id", keep="first")

    other_dims = [c for c in process_dims if c in df.columns and c != dim]

    breakdown = {}
    for d in other_dims:
        slice_counts = case_level[d].value_counts(dropna=False).to_dict()
        overall_counts = overall_case_level[d].value_counts(dropna=False).to_dict()
        # Top 5 values in the slice and how over/under-represented they are vs overall
        top = sorted(slice_counts.items(), key=lambda kv: kv[1], reverse=True)[:5]
        n_slice = len(case_level)
        n_overall = len(overall_case_level)
        rows = []
        for v, n in top:
            slice_share = n / n_slice if n_slice else 0
            overall_share = overall_counts.get(v, 0) / n_overall if n_overall else 0
            rows.append({
                "value": str(v),
                "n_in_slice": int(n),
                "share_in_slice": round(slice_share, 3),
                "share_overall": round(overall_share, 3),
                "over_representation": round(
                    slice_share / overall_share, 2
                ) if overall_share > 0 else None,
            })
        breakdown[d] = rows

    return {
        "dimension": dim, "value": str(value),
        "n_cases_in_slice": int(len(case_level)),
        "n_cases_overall": int(len(overall_case_level)),
        "breakdown": breakdown,
    }


def _cross_reference(df: pd.DataFrame, args: dict) -> dict:
    """Median transition duration across two dimensions. Finds interaction effects."""
    dim_a = args["dim_a"]
    dim_b = args["dim_b"]
    transition = args["transition"]
    if dim_a not in df.columns or dim_b not in df.columns:
        return {"error": f"unknown dimension(s): {dim_a}, {dim_b}"}

    pairs = _transition_pairs(df, transition)
    if pairs.empty:
        return {"error": f"no observations of transition '{transition}'"}

    grouped = pairs.groupby([dim_a, dim_b])["hours"].agg(["size", "median"]).reset_index()
    grouped = grouped[grouped["size"] >= 5].sort_values("median", ascending=False).head(20)

    rows = [
        {
            dim_a: _safe(r[dim_a]),
            dim_b: _safe(r[dim_b]),
            "n_cases": int(r["size"]),
            "median_hours": round(float(r["median"]), 2),
        }
        for _, r in grouped.iterrows()
    ]
    return {
        "transition": transition, "dim_a": dim_a, "dim_b": dim_b,
        "overall_median_hours": round(float(pairs["hours"].median()), 2),
        "rows": rows,
    }


def _temporal_trend(df: pd.DataFrame, args: dict) -> dict:
    """Durations bucketed by week (optionally filtered by a single dim=value)."""
    transition = args["transition"]
    dim = args.get("dimension")
    value = args.get("value")
    bucket = args.get("bucket", "W")

    pairs = _transition_pairs(df, transition)
    if pairs.empty:
        return {"error": f"no observations of transition '{transition}'"}
    if dim and value is not None:
        if dim not in pairs.columns:
            return {"error": f"unknown dimension '{dim}'"}
        pairs = pairs[pairs[dim].astype(str) == str(value)]
        if pairs.empty:
            return {"error": "no observations matching filter"}

    pairs = pairs.set_index("timestamp")
    trend = pairs["hours"].resample(bucket).agg(["size", "median"]).reset_index()
    trend = trend.rename(columns={"size": "n_cases"})

    rows = [
        {
            "period_start": str(r["timestamp"].date()),
            "n_cases": int(r["n_cases"]),
            "median_hours": round(float(r["median"]), 2) if pd.notna(r["median"]) else None,
        }
        for _, r in trend.iterrows() if r["n_cases"] > 0
    ]
    return {
        "transition": transition,
        "filter": {"dimension": dim, "value": str(value) if value is not None else None} if dim else None,
        "bucket": bucket, "rows": rows,
    }


# --- helpers -------------------------------------------------------------------------


def _case_cycle_hours(df: pd.DataFrame) -> dict[str, float]:
    out: dict[str, float] = {}
    for case_id, grp in df.groupby("case_id"):
        out[case_id] = (
            pd.Timestamp(grp["timestamp"].max()) - pd.Timestamp(grp["timestamp"].min())
        ).total_seconds() / 3600.0
    return out


def _transition_pairs(df: pd.DataFrame, transition: str) -> pd.DataFrame:
    """Return one row per case with the duration of the requested directly-follows
    transition. `transition` is formatted 'A → B'."""
    parts = [p.strip() for p in transition.replace("->", "→").split("→")]
    if len(parts) != 2:
        return pd.DataFrame()
    frm, to = parts
    s = df.sort_values(["case_id", "timestamp"]).copy()
    s["next_activity"] = s.groupby("case_id")["activity"].shift(-1)
    s["next_ts"] = s.groupby("case_id")["timestamp"].shift(-1)
    pairs = s[(s["activity"] == frm) & (s["next_activity"] == to)].copy()
    pairs["hours"] = (pairs["next_ts"] - pairs["timestamp"]).dt.total_seconds() / 3600.0
    return pairs


def _case_transition_hours(df: pd.DataFrame, transition: str) -> dict[str, float]:
    pairs = _transition_pairs(df, transition)
    return {r["case_id"]: float(r["hours"]) for _, r in pairs.iterrows() if pd.notna(r["hours"])}


def _safe(v: Any) -> Any:
    """Coerce pandas / numpy scalars to plain Python types for JSON."""
    if v is None:
        return None
    if pd.isna(v):
        return None
    if hasattr(v, "item"):
        try:
            return v.item()
        except (AttributeError, ValueError):
            pass
    return str(v) if not isinstance(v, (bool, int, float, str)) else v


# --- tool factory ---------------------------------------------------------------------


def build_rca_tools(
    event_log: EventLog,
    findings: Findings,
    process: ProcessDefinition | None = None,
) -> list[Tool]:
    df = event_log.df

    # Resolve process from the event log if the caller didn't pass one in.
    if process is None:
        slug_by_name = {"order_to_cash": "o2c", "procure_to_pay": "p2p"}
        slug = slug_by_name.get(event_log.process_name, "o2c")
        process = get_process(slug)

    extra_cols = _P2P_EXTRA_COLS if process.slug == "p2p" else _O2C_EXTRA_COLS

    return [
        Tool(
            name="describe_finding",
            description=(
                "Look up the full details of a flagged finding by its ID (e.g. 'B1', 'A1'). "
                "Use this first to understand what you're investigating."
            ),
            input_schema={
                "type": "object",
                "properties": {"finding_id": {"type": "string"}},
                "required": ["finding_id"],
            },
            fn=lambda a: _describe_finding(findings, a),
        ),
        Tool(
            name="list_cases",
            description=(
                "List sales orders matching a dimension=value slice, sorted by duration "
                "(longest first). Optionally filter by a specific transition "
                "(e.g. 'OrderCreated → CreditChecked'). Use this to identify specific "
                "cases to drill into."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "dimension": {"type": "string"},
                    "value": {"type": "string"},
                    "transition": {"type": "string"},
                    "limit": {"type": "integer", "default": 15},
                },
                "required": ["dimension", "value"],
            },
            fn=lambda a: _list_cases(df, a, extra_cols),
        ),
        Tool(
            name="get_case_timeline",
            description=(
                "Return the full event sequence for a single case, with the time gap "
                "between each activity and all case attributes. Use this to see exactly "
                "where a specific order is stuck."
            ),
            input_schema={
                "type": "object",
                "properties": {"case_id": {"type": "string"}},
                "required": ["case_id"],
            },
            fn=lambda a: _get_case_timeline(df, a),
        ),
        Tool(
            name="compare_slice_attributes",
            description=(
                "For cases matching a dimension=value slice, show how their OTHER "
                "attributes (plant, region, MTO flag, etc.) are distributed vs. the "
                "overall population. Use this to detect interaction effects — e.g. "
                "finding that 85%% of Plant 1000 slow cases are also MTO orders."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "dimension": {"type": "string"},
                    "value": {"type": "string"},
                },
                "required": ["dimension", "value"],
            },
            fn=lambda a: _compare_slice_attributes(df, a, process.dimensions),
        ),
        Tool(
            name="cross_reference",
            description=(
                "Median duration of a transition grouped by two dimensions. Use this to "
                "check whether a bottleneck lives on one dimension or at the intersection "
                "of two (e.g. plant × has_mto_item)."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "transition": {"type": "string"},
                    "dim_a": {"type": "string"},
                    "dim_b": {"type": "string"},
                },
                "required": ["transition", "dim_a", "dim_b"],
            },
            fn=lambda a: _cross_reference(df, a),
        ),
        Tool(
            name="temporal_trend",
            description=(
                "Median duration of a transition bucketed by week (default) or another "
                "pandas freq (e.g. 'D' daily). Optionally filter by a dimension=value "
                "slice. Use this to check whether a bottleneck is chronic or started at "
                "a specific point in time."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "transition": {"type": "string"},
                    "dimension": {"type": "string"},
                    "value": {"type": "string"},
                    "bucket": {"type": "string", "default": "W"},
                },
                "required": ["transition"],
            },
            fn=lambda a: _temporal_trend(df, a),
        ),
    ]
