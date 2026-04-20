"""Canonical event log representation.

Follows the XES-style convention: a flat table of (case_id, activity, timestamp, ...attributes).
All connectors produce an EventLog with this shape regardless of the underlying SAP table layout.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

import pandas as pd


REQUIRED_COLUMNS = ("case_id", "activity", "timestamp")


def _to_utc_ts(dt: datetime) -> pd.Timestamp:
    """Accept naive or tz-aware datetime and return a tz-aware UTC Timestamp."""
    ts = pd.Timestamp(dt)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts


@dataclass(slots=True)
class EventLog:
    """An event log backed by a pandas DataFrame.

    The frame must contain at minimum:
      - case_id:   stable identifier for the process instance (e.g. sales order number)
      - activity:  short human-readable activity name (e.g. 'OrderCreated')
      - timestamp: pandas datetime64[ns, UTC]

    Additional columns are free-form attributes (plant, region, material, user, value, etc.).
    """

    df: pd.DataFrame
    process_name: str = "unknown"
    source: str = "unknown"
    extracted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        missing = [c for c in REQUIRED_COLUMNS if c not in self.df.columns]
        if missing:
            raise ValueError(f"EventLog missing required columns: {missing}")
        if not pd.api.types.is_datetime64_any_dtype(self.df["timestamp"]):
            self.df["timestamp"] = pd.to_datetime(self.df["timestamp"], utc=True)
        self.df = self.df.sort_values(["case_id", "timestamp"]).reset_index(drop=True)

    @classmethod
    def from_records(
        cls,
        records: Iterable[dict],
        process_name: str = "unknown",
        source: str = "unknown",
    ) -> "EventLog":
        df = pd.DataFrame(list(records))
        return cls(df=df, process_name=process_name, source=source)

    @property
    def n_cases(self) -> int:
        return self.df["case_id"].nunique()

    @property
    def n_events(self) -> int:
        return len(self.df)

    @property
    def activities(self) -> list[str]:
        return list(self.df["activity"].unique())

    @property
    def time_range(self) -> tuple[datetime, datetime]:
        return (self.df["timestamp"].min(), self.df["timestamp"].max())

    def filter_window(self, start: datetime | None = None, end: datetime | None = None) -> "EventLog":
        df = self.df
        if start is not None:
            df = df[df["timestamp"] >= _to_utc_ts(start)]
        if end is not None:
            df = df[df["timestamp"] <= _to_utc_ts(end)]
        return EventLog(df=df.reset_index(drop=True), process_name=self.process_name, source=self.source)

    def summary(self) -> dict:
        start, end = self.time_range
        return {
            "process": self.process_name,
            "source": self.source,
            "n_cases": self.n_cases,
            "n_events": self.n_events,
            "activities": self.activities,
            "time_range": {"start": start.isoformat(), "end": end.isoformat()},
        }
