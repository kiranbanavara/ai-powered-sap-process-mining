"""Persist run artifacts so the Investigator can reload them later.

After `sap-mining run` completes we save per-process:
    reports/latest/<process_slug>/event-log.parquet
    reports/latest/<process_slug>/findings.json
    reports/latest/<process_slug>/briefing.md
    reports/latest/<process_slug>/manifest.json

`sap-mining investigate` and the Streamlit UI read these back. A `list_saved_runs()`
helper enumerates which processes have a persisted run in a given base directory so the
UI can offer only those.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .event_log import EventLog
from .mining import Findings


LATEST_DIR = "latest"
EVENT_LOG_FILE = "event-log.parquet"
FINDINGS_FILE = "findings.json"
MANIFEST_FILE = "manifest.json"
BRIEFING_FILE = "briefing.md"


@dataclass
class RunArtifacts:
    event_log: EventLog
    findings: Findings
    manifest: dict
    briefing: str | None = None   # Flagger narrative (None for --dry-run)


def _process_dir(base_dir: Path | str, process_slug: str) -> Path:
    return Path(base_dir) / LATEST_DIR / process_slug


def save_run(
    base_dir: Path | str,
    event_log: EventLog,
    findings: Findings,
    process_slug: str,
    briefing: str | None = None,
    llm_provider: str | None = None,
    llm_model: str | None = None,
) -> Path:
    base = _process_dir(base_dir, process_slug)
    base.mkdir(parents=True, exist_ok=True)

    event_log.df.to_parquet(base / EVENT_LOG_FILE, index=False)
    (base / FINDINGS_FILE).write_text(findings.as_prompt_json())

    manifest = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "process_slug": process_slug,
        "process": event_log.process_name,
        "source": event_log.source,
        "n_cases": event_log.n_cases,
        "n_events": event_log.n_events,
        "llm_provider": llm_provider,
        "llm_model": llm_model,
        "has_briefing": briefing is not None,
    }
    (base / MANIFEST_FILE).write_text(json.dumps(manifest, indent=2))

    briefing_path = base / BRIEFING_FILE
    if briefing:
        briefing_path.write_text(briefing)
    else:
        # Avoid stale briefings from a prior non-dry run lingering after a dry run.
        briefing_path.unlink(missing_ok=True)

    return base


def load_run(base_dir: Path | str, process_slug: str) -> RunArtifacts:
    base = _process_dir(base_dir, process_slug)
    if not base.exists():
        raise FileNotFoundError(
            f"No previous run for '{process_slug}' at {base}. "
            f"Run `sap-mining run --config <cfg>` first."
        )
    df = pd.read_parquet(base / EVENT_LOG_FILE)
    findings = Findings.from_dict(json.loads((base / FINDINGS_FILE).read_text()))
    manifest = json.loads((base / MANIFEST_FILE).read_text())
    event_log = EventLog(
        df=df,
        process_name=manifest.get("process", "unknown"),
        source=manifest.get("source", "unknown"),
    )
    briefing_path = base / BRIEFING_FILE
    briefing = briefing_path.read_text() if briefing_path.exists() else None
    return RunArtifacts(
        event_log=event_log,
        findings=findings,
        manifest=manifest,
        briefing=briefing,
    )


def list_saved_runs(base_dir: Path | str) -> list[str]:
    """Return the process slugs that have a persisted run in `base_dir`.

    The UI uses this to offer only processes with real data; it also lets the user
    tell at a glance which pipelines have been run recently.
    """
    latest = Path(base_dir) / LATEST_DIR
    if not latest.exists():
        return []
    return sorted(
        p.name for p in latest.iterdir()
        if p.is_dir() and (p / MANIFEST_FILE).exists()
    )
