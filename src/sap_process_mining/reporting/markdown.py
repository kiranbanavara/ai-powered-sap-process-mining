"""Render an AnalysisResult to Markdown suitable for email or Slack."""

from __future__ import annotations

from datetime import datetime

from ..analysis import AnalysisResult


def render_report(result: AnalysisResult) -> str:
    f = result.findings
    lines: list[str] = []
    lines.append("# SAP O2C — Operations Briefing")
    lines.append("")
    lines.append(
        f"*Process:* **{f.process}**   *Source:* `{f.source}`   "
        f"*Window:* {_fmt_dt(f.window_start)} → {_fmt_dt(f.window_end)}   "
        f"*Model:* `{result.provider}:{result.model}`"
    )
    lines.append("")
    lines.append(
        f"**{f.n_cases:,}** cases · **{f.n_events:,}** events · "
        f"median cycle **{_fmt_duration(f.median_cycle_hours)}** · "
        f"p90 cycle **{_fmt_duration(f.p90_cycle_hours)}** · "
        f"rework **{f.rework_rate * 100:.1f}%**"
    )
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(result.narrative.strip())
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Supporting data")
    lines.append("")
    if f.bottlenecks:
        lines.append("### Bottlenecks (top by lift)")
        lines.append("")
        lines.append("| Transition | Dimension | Value | Cases | Median | Overall | Lift |")
        lines.append("|---|---|---|---:|---:|---:|---:|")
        for b in f.bottlenecks[:10]:
            lines.append(
                f"| {b.transition} | {b.dimension} | {b.value} | {b.n_cases} | "
                f"{_fmt_duration(b.median_hours)} | {_fmt_duration(b.overall_median_hours)} | "
                f"{b.lift:.1f}× |"
            )
        lines.append("")
    if f.variants:
        lines.append("### Top variants")
        lines.append("")
        lines.append("| Share | Cases | Median cycle | Sequence |")
        lines.append("|---:|---:|---:|---|")
        for v in f.variants:
            marker = " ✓ happy path" if v.is_happy_path else ""
            lines.append(
                f"| {v.share * 100:.1f}% | {v.n_cases} | {_fmt_duration(v.median_cycle_hours)} | "
                f"{' → '.join(v.sequence)}{marker} |"
            )
        lines.append("")
    if f.anomalies:
        lines.append("### Top anomalies")
        lines.append("")
        for a in f.anomalies[:5]:
            lines.append(
                f"- **{a.case_id}** — cycle {_fmt_duration(a.cycle_hours)} — {a.reason}"
            )
        lines.append("")
    return "\n".join(lines)


def render_rca_report(result, finding) -> str:
    """Render an InvestigationResult to Markdown.

    `result` is an investigator.InvestigationResult (circular-import dodge — typed
    in the docstring, not signature).
    """
    lines: list[str] = []
    lines.append(f"# Root-Cause Analysis — Finding {result.finding_id}")
    lines.append("")
    summary = _finding_one_liner(finding)
    if summary:
        lines.append(f"**Flagged:** {summary}")
        lines.append("")
    if result.human_comment:
        lines.append(f"**Analyst comment:** _{result.human_comment}_")
        lines.append("")
    lines.append(
        f"*Investigator:* `{result.provider}:{result.model}`  "
        f"*Tool calls:* {len(result.traces)}  *Turns:* {result.turns}"
    )
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(result.hypothesis_markdown.strip())
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Audit trail (tool calls)")
    lines.append("")
    if not result.traces:
        lines.append("_No tools were invoked._")
    else:
        for i, t in enumerate(result.traces, start=1):
            lines.append(f"**{i}. `{t.name}`**  ")
            args_oneline = ", ".join(f"{k}=`{v}`" for k, v in t.args.items())
            lines.append(f"args: {args_oneline}")
            if t.error:
                lines.append(f"→ error: `{t.error}`")
            lines.append("")
    return "\n".join(lines)


def _finding_one_liner(finding) -> str:
    if finding is None:
        return ""
    # DimensionalFinding
    if hasattr(finding, "transition"):
        return (
            f"`{finding.transition}` runs {finding.lift:.1f}× slower when "
            f"`{finding.dimension}={finding.value}` "
            f"({finding.n_cases} cases, median {_fmt_duration(finding.median_hours)} "
            f"vs overall {_fmt_duration(finding.overall_median_hours)})"
        )
    # AnomalyCase
    if hasattr(finding, "cycle_hours"):
        return f"anomaly case {finding.case_id} — cycle {_fmt_duration(finding.cycle_hours)}"
    return ""


def _fmt_dt(iso: str) -> str:
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return iso


def _fmt_duration(hours: float) -> str:
    if hours < 1:
        return f"{hours * 60:.0f}m"
    if hours < 24:
        return f"{hours:.1f}h"
    return f"{hours / 24:.1f}d"
