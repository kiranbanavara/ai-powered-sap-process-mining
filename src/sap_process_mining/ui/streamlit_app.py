"""Streamlit UI for SAP Process Mining (Flagger briefing + Investigator).

Layout:
  Sidebar  — config picker, process picker, manifest of selected run.
  Landing  — one card per persisted process with headline metrics, click to open.
  Detail   — briefing, findings cards, Investigator with live tool-call stream.

Launch via `sap-mining ui --config <yaml>`, or directly:
    streamlit run src/sap_process_mining/ui/streamlit_app.py
"""

from __future__ import annotations

import os
from pathlib import Path

import streamlit as st

from sap_process_mining import _env  # noqa: F401 — side-effect: loads .env
from sap_process_mining.config import load_config
from sap_process_mining.investigator import Investigator
from sap_process_mining.llm import get_provider
from sap_process_mining.llm.base import ToolTrace
from sap_process_mining.mining.findings import AnomalyCase, DimensionalFinding, Findings
from sap_process_mining.persistence import list_saved_runs, load_run
from sap_process_mining.processes import PROCESSES, get_process


# =========================================================================
# Helpers (top so Streamlit's top-to-bottom execution finds them)
# =========================================================================


def _fmt(hours: float | None) -> str:
    if hours is None or hours != hours:
        return "—"
    if hours < 1:
        return f"{hours * 60:.0f}m"
    if hours < 24:
        return f"{hours:.1f}h"
    return f"{hours / 24:.1f}d"


def _list_config_files() -> list[Path]:
    root = Path.cwd() / "config"
    return sorted(root.glob("config*.yaml")) if root.exists() else []


def _build_provider(cfg):
    kwargs: dict = {}
    if cfg.llm.model:
        kwargs["model"] = cfg.llm.model
    if cfg.llm.api_key:
        kwargs["api_key"] = cfg.llm.api_key
    if cfg.llm.base_url and cfg.llm.provider == "openai":
        kwargs["base_url"] = cfg.llm.base_url
    return get_provider(cfg.llm.provider, **kwargs)


def _render_selected_finding(f) -> None:
    if f is None:
        st.warning("Finding no longer exists — it may have been removed by a newer run.")
        return
    if isinstance(f, DimensionalFinding):
        cols = st.columns([2, 1, 1, 1])
        cols[0].markdown(f"**Transition**  \n`{f.transition}`")
        cols[1].markdown(f"**Slice**  \n`{f.dimension} = {f.value}`")
        cols[2].markdown(f"**Cases**  \n{f.n_cases}")
        cols[3].markdown(f"**Lift**  \n{f.lift:.1f}×")
        st.caption(
            f"Median in slice: {_fmt(f.median_hours)} · overall {_fmt(f.overall_median_hours)}"
        )
    elif isinstance(f, AnomalyCase):
        st.markdown(f"**Case** `{f.case_id}` — cycle {_fmt(f.cycle_hours)}  \n{f.reason}")
        if f.attributes:
            with st.expander("Case attributes"):
                st.json(f.attributes)


def _format_traces_running(traces: list[ToolTrace]) -> str:
    if not traces:
        return "_Waiting for first tool call…_"
    lines = []
    for i, t in enumerate(traces, 1):
        args_str = ", ".join(f"{k}={v!r}" for k, v in t.args.items())
        icon = "❌" if t.error else "✓"
        lines.append(f"{icon} **{i}. `{t.name}`** — {args_str}")
        if t.error:
            lines.append(f"&nbsp;&nbsp;&nbsp;&nbsp;error: `{t.error}`")
    return "\n\n".join(lines)


def _render_md_for_download(result, finding) -> str:
    from sap_process_mining.reporting import render_rca_report
    return render_rca_report(result, finding)


def _load_process_artifacts(output_dir: str) -> dict:
    """Load all persisted runs; cache in session-state per process to avoid parquet
    re-reads on every rerun."""
    cache_key = "_artifact_cache"
    cache: dict = st.session_state.setdefault(cache_key, {})
    saved = list_saved_runs(output_dir)
    # Drop entries for processes that no longer have a saved run.
    for stale in list(cache):
        if stale not in saved:
            cache.pop(stale, None)
    for slug in saved:
        if slug not in cache:
            try:
                cache[slug] = load_run(output_dir, slug)
            except FileNotFoundError:
                continue
    return cache


def _top_finding_lift(findings: Findings | None) -> float | None:
    if findings is None or not findings.bottlenecks:
        return None
    return findings.bottlenecks[0].lift


# =========================================================================
# Page config + sidebar
# =========================================================================

st.set_page_config(
    page_title="SAP Process Mining",
    page_icon="🔎",
    layout="wide",
)

with st.sidebar:
    st.title("🔎 Process Mining")
    st.caption("Flagger briefings + Investigator root-cause analysis.")

    default_cfg = os.environ.get("SAP_MINING_CONFIG") or (
        str(_list_config_files()[0]) if _list_config_files() else ""
    )
    config_path = st.text_input(
        "Config file",
        value=default_cfg,
        help="YAML config for LLM + output paths. Same file used by the CLI.",
    )

    if st.button("↻ Reload from disk", use_container_width=True):
        for k in list(st.session_state.keys()):
            if k.startswith("_") or k in ("selected_process", "selected_finding_id", "investigation"):
                st.session_state.pop(k, None)

    max_turns = st.slider("Max tool-use turns", 3, 20, 10)


# =========================================================================
# Load config + artifact cache
# =========================================================================

if not config_path or not Path(config_path).exists():
    st.warning("Point the sidebar to a valid config YAML to begin.")
    st.stop()

try:
    cfg = load_config(config_path)
except Exception as e:  # noqa: BLE001
    st.error(f"Failed to load config: {e}")
    st.stop()

artifact_cache = _load_process_artifacts(cfg.output.directory)

if not artifact_cache:
    st.error(
        f"No persisted runs found in `{cfg.output.directory}/latest/`. "
        "Run `sap-mining run --config <cfg>` for at least one process first."
    )
    st.stop()

# Sidebar: process picker shown once we have data.
with st.sidebar:
    st.divider()
    options = ["(Overview)"] + [slug for slug in PROCESSES if slug in artifact_cache]
    current = st.session_state.get("selected_process")
    index = options.index(current) if current in options else 0
    choice = st.radio("Process", options, index=index)
    st.session_state.selected_process = None if choice == "(Overview)" else choice


# =========================================================================
# Landing: overview of all persisted processes
# =========================================================================


def _render_landing() -> None:
    st.title("SAP Process Mining")
    st.caption("Select a process below to open the Flagger briefing and run the Investigator.")

    process_slugs = [s for s in PROCESSES if s in artifact_cache]
    cols_per_row = min(3, max(1, len(process_slugs)))
    for row_start in range(0, len(process_slugs), cols_per_row):
        row = st.columns(cols_per_row)
        for col, slug in zip(row, process_slugs[row_start : row_start + cols_per_row]):
            proc = get_process(slug)
            art = artifact_cache[slug]
            findings = art.findings
            top_lift = _top_finding_lift(findings)
            with col:
                with st.container(border=True):
                    st.markdown(f"### {proc.name.replace('_', '-').upper()}")
                    st.caption(proc.description)
                    sub = st.columns(2)
                    sub[0].metric("Cases", f"{findings.n_cases:,}")
                    sub[1].metric("Events", f"{findings.n_events:,}")
                    sub2 = st.columns(2)
                    sub2[0].metric("Median cycle", _fmt(findings.median_cycle_hours))
                    sub2[1].metric(
                        "Top lift",
                        f"{top_lift:.1f}×" if top_lift else "—",
                        help=f"Highest-lift bottleneck (of {len(findings.bottlenecks)} total)",
                    )
                    st.caption(
                        f"Saved: `{art.manifest.get('saved_at', '—')[:19]}`  ·  "
                        f"source: `{art.manifest.get('source', '—')}`"
                    )
                    if st.button("Open →", key=f"open_{slug}", use_container_width=True, type="primary"):
                        st.session_state.selected_process = slug
                        st.session_state.pop("selected_finding_id", None)
                        st.session_state.pop("investigation", None)
                        st.rerun()


if st.session_state.get("selected_process") is None:
    _render_landing()
    st.stop()


# =========================================================================
# Process detail view
# =========================================================================

slug = st.session_state.selected_process
if slug not in artifact_cache:
    st.warning(f"No saved run for `{slug}`. Back to overview.")
    if st.button("← Back to overview"):
        st.session_state.selected_process = None
        st.rerun()
    st.stop()

artifacts = artifact_cache[slug]
findings: Findings = artifacts.findings
manifest = artifacts.manifest
proc = get_process(slug)

with st.sidebar:
    st.divider()
    st.subheader("Current run")
    st.caption(f"Process: `{proc.name}`")
    st.caption(f"Saved: `{manifest.get('saved_at', '—')}`")
    st.caption(f"Source: `{manifest.get('source', '—')}`")
    st.caption(f"{manifest.get('n_cases', 0):,} cases · {manifest.get('n_events', 0):,} events")

# Header with back navigation
top_cols = st.columns([1, 9])
if top_cols[0].button("← Overview"):
    st.session_state.selected_process = None
    st.session_state.pop("selected_finding_id", None)
    st.session_state.pop("investigation", None)
    st.rerun()
top_cols[1].title(f"{proc.name.replace('_', '-').upper()}")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Cases", f"{findings.n_cases:,}")
c2.metric("Events", f"{findings.n_events:,}")
c3.metric("Median cycle", _fmt(findings.median_cycle_hours))
c4.metric("p90 cycle", _fmt(findings.p90_cycle_hours))


# --- Briefing -----------------------------------------------------------------------

briefing = artifacts.briefing
if briefing:
    provider = manifest.get("llm_provider")
    model = manifest.get("llm_model")
    caption = f"Generated by `{provider}:{model}`" if (provider and model) else ""
    with st.expander("📋  Briefing  —  operations narrative from the Flagger", expanded=True):
        if caption:
            st.caption(caption)
        st.markdown(briefing)
else:
    st.info(
        "No briefing persisted for this run. "
        "Run `sap-mining run` (without `--dry-run`) to generate the Flagger narrative."
    )


# --- Findings cards ----------------------------------------------------------------

st.subheader("Flagged findings")
st.caption("Click **Investigate** on a card to run the RCA agent on that finding.")

if not findings.bottlenecks and not findings.anomalies:
    st.info("No findings in this run.")
    st.stop()

cols_per_row = 3
for row_start in range(0, len(findings.bottlenecks), cols_per_row):
    cols = st.columns(cols_per_row)
    for col, b in zip(cols, findings.bottlenecks[row_start : row_start + cols_per_row]):
        with col:
            with st.container(border=True):
                st.markdown(f"**{b.id}**   🔥 **{b.lift:.1f}× lift**")
                st.markdown(f"`{b.transition}`")
                st.markdown(f"**{b.dimension}** = `{b.value}`")
                st.caption(
                    f"{b.n_cases} cases · median {_fmt(b.median_hours)} · "
                    f"overall {_fmt(b.overall_median_hours)}"
                )
                if st.button("Investigate →", key=f"btn_{slug}_{b.id}", use_container_width=True):
                    st.session_state.selected_finding_id = b.id
                    st.session_state.pop("investigation", None)

if findings.anomalies:
    with st.expander(f"+ {len(findings.anomalies)} anomaly cases"):
        for a in findings.anomalies:
            c1, c2, c3 = st.columns([1, 5, 1])
            c1.markdown(f"**{a.id}**")
            c2.markdown(f"`{a.case_id}` — {a.reason}")
            if c3.button("Investigate", key=f"btn_{slug}_{a.id}"):
                st.session_state.selected_finding_id = a.id
                st.session_state.pop("investigation", None)


# --- Selected finding → comment → investigate --------------------------------------

finding_id = st.session_state.get("selected_finding_id")
if not finding_id:
    st.info("Select a finding above to investigate.")
    st.stop()

selected = findings.lookup(finding_id)
st.divider()
st.subheader(f"Investigation · {finding_id}")
_render_selected_finding(selected)

comment = st.text_area(
    "Analyst comment (optional)",
    placeholder=(
        "Context that steers the RCA — e.g. 'New credit approver at Plant 1000 since "
        "March — check if that's when this started.' or 'Known issue with DE-SOUTH "
        "freight partner.'"
    ),
    height=90,
    key=f"comment_{slug}_{finding_id}",
)

run_btn = st.button(
    "▶ Run Investigator",
    type="primary",
    disabled=st.session_state.get("investigation_running", False),
)


# --- Run the investigator with live tool-call streaming ----------------------------

if run_btn:
    try:
        provider = _build_provider(cfg)
    except Exception as e:  # noqa: BLE001
        st.error(f"Could not initialise LLM provider: {e}")
        st.stop()

    investigator = Investigator(provider, artifacts.event_log, findings)
    st.session_state.investigation_running = True

    status = st.status("Investigator running …", expanded=True)
    trace_slot = status.empty()
    traces_so_far: list[ToolTrace] = []

    def _on_tool(t: ToolTrace) -> None:
        traces_so_far.append(t)
        trace_slot.markdown(_format_traces_running(traces_so_far))

    try:
        result = investigator.investigate(
            finding_id,
            human_comment=comment.strip() or None,
            max_turns=max_turns,
            on_tool_call=_on_tool,
        )
        status.update(
            label=f"✓ Investigation complete — {len(result.traces)} tool calls, {result.turns} turns",
            state="complete",
        )
        st.session_state.investigation = result
    except Exception as e:  # noqa: BLE001
        status.update(label=f"✗ Investigator failed: {e}", state="error")
        st.exception(e)
    finally:
        st.session_state.investigation_running = False


# --- Render the final result -------------------------------------------------------

result = st.session_state.get("investigation")
if result and result.finding_id == finding_id:
    st.divider()
    st.markdown(result.hypothesis_markdown)

    st.caption(
        f"_{result.provider}:{result.model} · {result.input_tokens:,} tokens in / "
        f"{result.output_tokens:,} out · {result.turns} turns_"
    )

    with st.expander(f"Audit trail — {len(result.traces)} tool calls"):
        for i, t in enumerate(result.traces, 1):
            st.markdown(f"**{i}. `{t.name}`**")
            payload = {"args": t.args}
            if t.error:
                payload["error"] = t.error
            else:
                payload["result"] = t.result
            st.json(payload, expanded=False)

    st.download_button(
        "Download RCA as Markdown",
        data=_render_md_for_download(result, selected),
        file_name=f"rca-{slug}-{finding_id}.md",
        mime="text/markdown",
    )
