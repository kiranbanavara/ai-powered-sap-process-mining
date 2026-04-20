"""`sap-mining` CLI.

Typical usage:
    sap-mining run --config config/config.yaml
    sap-mining run --config config/config.yaml --dry-run   # mine + print JSON, skip LLM
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click
from rich.console import Console
from rich.markdown import Markdown

from . import _env  # noqa: F401 — side-effect: loads .env into os.environ
from .analysis import Analyzer
from .config import AppConfig, load_config
from .connectors import get_connector
from .event_log import EventLog
from .investigator import Investigator
from .llm import get_provider
from .mining import analyze
from .persistence import load_run, save_run
from .reporting import render_rca_report, render_report


console = Console()


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging.")
def main(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@main.command()
@click.option("--config", "config_path", type=click.Path(exists=True, dir_okay=False), required=True)
@click.option("--dry-run", is_flag=True, help="Extract and mine, but skip the LLM call.")
@click.option("--window-days", type=int, default=None, help="Override run.window_days.")
@click.option("--output-dir", type=click.Path(), default=None, help="Override output.directory.")
def run(config_path: str, dry_run: bool, window_days: int | None, output_dir: str | None) -> None:
    """Extract, mine, narrate, and write a report."""
    cfg = load_config(config_path)
    if window_days is not None:
        cfg.run.window_days = window_days
    if output_dir is not None:
        cfg.output.directory = output_dir

    log = _extract(cfg)

    if dry_run:
        findings = analyze(log)
        save_run(cfg.output.directory, log, findings, process_slug=cfg.run.process)
        console.print_json(findings.as_prompt_json())
        return

    result = _narrate(cfg, log)
    report_md = render_report(result)

    out_path = _write_report(cfg, report_md)
    save_run(
        cfg.output.directory,
        log,
        result.findings,
        process_slug=cfg.run.process,
        briefing=result.narrative,
        llm_provider=result.provider,
        llm_model=result.model,
    )
    console.rule("[bold cyan]Briefing")
    console.print(Markdown(result.narrative))
    console.rule()
    console.print(f"[green]Saved[/green] {out_path}")


@main.command()
@click.option("--config", "config_path", type=click.Path(exists=True, dir_okay=False), required=True)
def check(config_path: str) -> None:
    """Validate config, connect to SAP, and print the event log summary."""
    cfg = load_config(config_path)
    log = _extract(cfg)
    click.echo(json.dumps(log.summary(), indent=2, default=str))


@main.command()
@click.option("--config", "config_path", type=click.Path(exists=True, dir_okay=False), required=True)
@click.option("--finding", "finding_id", type=str, default=None,
              help="Finding id (e.g. B1, A1). If omitted, the latest findings are listed.")
@click.option("--comment", type=str, default=None,
              help="Optional human context passed to the investigator.")
@click.option("--process", "process_override", type=click.Choice(["o2c", "p2p"]), default=None,
              help="Process to investigate. Defaults to run.process from the config.")
@click.option("--max-turns", type=int, default=10)
def investigate(config_path: str, finding_id: str | None, comment: str | None,
                process_override: str | None, max_turns: int) -> None:
    """Investigate one flagged finding from the most recent run.

    \b
    Examples:
      sap-mining investigate --config config/config.synthetic.yaml
      sap-mining investigate --config ... --finding B1
      sap-mining investigate --config ... --process p2p --finding B2
      sap-mining investigate --config ... --finding B1 --comment "New approver at Plant 1000 since March"
    """
    cfg = load_config(config_path)
    process_slug = process_override or cfg.run.process
    artifacts = load_run(cfg.output.directory, process_slug=process_slug)

    if not finding_id:
        _print_findings_index(artifacts.findings)
        return

    target = artifacts.findings.lookup(finding_id)
    if target is None:
        valid = (
            [b.id for b in artifacts.findings.bottlenecks]
            + [a.id for a in artifacts.findings.anomalies]
        )
        raise click.BadParameter(
            f"Unknown finding id '{finding_id}'. Valid: {valid}"
        )

    provider = _build_provider(cfg)
    investigator = Investigator(provider, artifacts.event_log, artifacts.findings)

    console.print(f"[cyan]Investigating[/cyan] {finding_id} via [bold]{provider.name}[/bold] "
                  f"(max {max_turns} turns) …")
    result = investigator.investigate(finding_id, human_comment=comment, max_turns=max_turns)
    console.print(
        f"  → {len(result.traces)} tool calls across {result.turns} turns, "
        f"{result.input_tokens:,} tokens in / {result.output_tokens:,} out"
    )

    report_md = render_rca_report(result, target)
    out_path = _write_rca_report(cfg, finding_id, report_md)

    console.rule(f"[bold cyan]RCA — {finding_id}")
    console.print(Markdown(result.hypothesis_markdown))
    console.rule()
    console.print(f"[green]Saved[/green] {out_path}")


@main.command()
@click.option("--config", "config_path", type=click.Path(exists=True, dir_okay=False), default=None,
              help="Config YAML the UI should default to. Set via sidebar if omitted.")
@click.option("--port", type=int, default=8501, help="Streamlit port (default 8501).")
def ui(config_path: str | None, port: int) -> None:
    """Launch the Streamlit Investigator UI.

    Install with:  pip install 'sap-process-mining[ui,anthropic]'
    """
    try:
        import streamlit  # noqa: F401
    except ImportError:
        raise click.ClickException(
            "Streamlit is not installed. Install with: pip install 'sap-process-mining[ui]'"
        )

    from . import ui as ui_pkg
    app_path = Path(ui_pkg.__file__).parent / "streamlit_app.py"

    env = os.environ.copy()
    if config_path:
        env["SAP_MINING_CONFIG"] = str(Path(config_path).resolve())

    cmd = [sys.executable, "-m", "streamlit", "run", str(app_path),
           "--server.port", str(port), "--server.headless", "true"]
    console.print(f"[cyan]Launching UI[/cyan] on http://localhost:{port} …")
    subprocess.run(cmd, env=env)


def _build_provider(cfg: AppConfig):
    llm_kwargs: dict = {}
    if cfg.llm.model:
        llm_kwargs["model"] = cfg.llm.model
    if cfg.llm.api_key:
        llm_kwargs["api_key"] = cfg.llm.api_key
    if cfg.llm.base_url and cfg.llm.provider == "openai":
        llm_kwargs["base_url"] = cfg.llm.base_url
    return get_provider(cfg.llm.provider, **llm_kwargs)


def _print_findings_index(findings) -> None:
    from rich.table import Table
    t = Table(title="Flagged findings from last run — pass one id to --finding")
    t.add_column("ID", style="cyan", no_wrap=True)
    t.add_column("Kind")
    t.add_column("Detail")
    t.add_column("Cases", justify="right")
    t.add_column("Lift", justify="right")
    for b in findings.bottlenecks:
        t.add_row(b.id, "bottleneck",
                  f"{b.transition} @ {b.dimension}={b.value}",
                  str(b.n_cases), f"{b.lift:.1f}×")
    for a in findings.anomalies:
        t.add_row(a.id, "anomaly", f"{a.case_id} — {a.reason}",
                  "1", "—")
    console.print(t)


def _write_rca_report(cfg: AppConfig, finding_id: str, report_md: str) -> Path:
    out_dir = Path(cfg.output.directory)
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = f"rca-{finding_id}-{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H%M')}.md"
    out_path = out_dir / filename
    out_path.write_text(report_md)
    return out_path


def _extract(cfg: AppConfig) -> EventLog:
    kind = cfg.connector.kind
    kwargs = cfg.connector.model_dump(exclude={"kind"})
    connector = get_connector(kind, **{k: v for k, v in kwargs.items() if v is not None})

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=cfg.run.window_days)

    console.print(f"[cyan]Extracting[/cyan] {cfg.run.process} from [bold]{kind}[/bold] "
                  f"for last {cfg.run.window_days} days …")
    scope = {
        "sales_orgs": cfg.run.sales_orgs,
        "purchasing_orgs": cfg.run.purchasing_orgs,
        "company_codes": cfg.run.company_codes,
    }
    log = connector.extract(cfg.run.process, start=start, end=end, **scope)
    console.print(f"  → {log.n_cases:,} cases, {log.n_events:,} events")
    return log


def _narrate(cfg: AppConfig, log: EventLog):
    console.print(f"[cyan]Analyzing[/cyan] via [bold]{cfg.llm.provider}[/bold] …")
    provider = _build_provider(cfg)
    analyzer = Analyzer(provider)
    result = analyzer.run(log)
    console.print(
        f"  → {result.usage.input_tokens:,} tokens in / {result.usage.output_tokens:,} out"
    )
    return result


def _write_report(cfg: AppConfig, report_md: str) -> Path:
    out_dir = Path(cfg.output.directory)
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = cfg.output.filename_template.format(
        date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M"),
    )
    out_path = out_dir / filename
    out_path.write_text(report_md)
    return out_path


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
