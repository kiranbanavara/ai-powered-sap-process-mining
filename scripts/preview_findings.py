"""Run the synthetic connector + analytics, skip the LLM, and write a findings-only
Markdown preview to `reports/preview-findings.md`. Useful for verifying the pipeline
before you spend tokens, or for internal review.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

# Make the local src/ importable without needing `pip install -e .`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from sap_process_mining.connectors import SyntheticConnector  # noqa: E402
from sap_process_mining.mining import analyze  # noqa: E402
from sap_process_mining.reporting.markdown import _fmt_duration  # noqa: E402


def main() -> None:
    log = SyntheticConnector(seed=42, n_cases=800, days_back=60).extract_o2c()
    f = analyze(log)

    lines = []
    lines.append("# SAP O2C — Findings Preview (no LLM narrative)")
    lines.append("")
    lines.append(
        f"*Process:* **{f.process}**  *Source:* `{f.source}`  "
        f"*Generated:* {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    lines.append("")
    lines.append(
        f"**{f.n_cases:,}** cases · **{f.n_events:,}** events · "
        f"median cycle **{_fmt_duration(f.median_cycle_hours)}** · "
        f"p90 cycle **{_fmt_duration(f.p90_cycle_hours)}** · "
        f"rework **{f.rework_rate * 100:.1f}%**"
    )
    lines.append("")

    lines.append("## Bottlenecks (top by absolute time impact)")
    lines.append("")
    lines.append("| Transition | Dimension | Value | Cases | Median | Overall | Lift |")
    lines.append("|---|---|---|---:|---:|---:|---:|")
    for b in f.bottlenecks:
        lines.append(
            f"| {b.transition} | {b.dimension} | {b.value} | {b.n_cases} | "
            f"{_fmt_duration(b.median_hours)} | {_fmt_duration(b.overall_median_hours)} | "
            f"{b.lift:.1f}× |"
        )
    lines.append("")

    lines.append("## Top variants")
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
        lines.append("## Anomalies")
        lines.append("")
        for a in f.anomalies[:5]:
            lines.append(f"- **{a.case_id}** — cycle {_fmt_duration(a.cycle_hours)} — {a.reason}")
        lines.append("")

    out = Path("reports") / "preview-findings.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
