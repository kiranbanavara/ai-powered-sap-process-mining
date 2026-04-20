"""Typed findings produced by the mining layer and consumed by the LLM layer.

Everything is JSON-serialisable: the LLM receives `findings.as_prompt_json()` and returns
plain-language narrative, so the data contract lives here.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field


@dataclass
class TransitionStat:
    """Duration between two consecutive activities, aggregated across cases."""
    from_activity: str
    to_activity: str
    n_cases: int
    median_hours: float
    p90_hours: float
    mean_hours: float
    std_hours: float


@dataclass
class DimensionalFinding:
    """A bottleneck concentrated in a particular slice of the data.

    Example:
      id:         B1
      transition: GoodsIssued ← PickingCompleted
      dimension:  region
      value:      DE-SOUTH
      median_hours: 94.0   (vs overall 12.0)
      n_cases:    47
      lift:       7.8      (how much slower than global median)
    """
    id: str  # stable reference like "B1" — assigned by analyze()
    transition: str
    dimension: str
    value: str
    n_cases: int
    median_hours: float
    overall_median_hours: float
    lift: float  # median_hours / overall_median_hours


@dataclass
class VariantStat:
    """A distinct activity sequence and its frequency."""
    sequence: list[str]
    n_cases: int
    share: float
    is_happy_path: bool
    median_cycle_hours: float


@dataclass
class AnomalyCase:
    id: str  # stable reference like "A1"
    case_id: str
    reason: str
    cycle_hours: float
    attributes: dict


@dataclass
class Findings:
    process: str
    source: str
    window_start: str
    window_end: str
    n_cases: int
    n_events: int
    median_cycle_hours: float
    p90_cycle_hours: float
    on_time_rate: float | None  # cases where cycle < happy-path median

    transitions: list[TransitionStat] = field(default_factory=list)
    bottlenecks: list[DimensionalFinding] = field(default_factory=list)
    variants: list[VariantStat] = field(default_factory=list)
    rework_rate: float = 0.0
    rework_activities: dict[str, int] = field(default_factory=dict)
    anomalies: list[AnomalyCase] = field(default_factory=list)

    def as_dict(self) -> dict:
        return asdict(self)

    def as_prompt_json(self, indent: int = 2) -> str:
        """A compact JSON view suitable for dropping straight into an LLM prompt."""
        return json.dumps(self.as_dict(), indent=indent, default=str)

    def lookup(self, finding_id: str) -> DimensionalFinding | AnomalyCase | None:
        """Find a finding by its stable id (B1, A1, etc.)."""
        for b in self.bottlenecks:
            if b.id == finding_id:
                return b
        for a in self.anomalies:
            if a.id == finding_id:
                return a
        return None

    @classmethod
    def from_dict(cls, data: dict) -> "Findings":
        """Inverse of as_dict — used by the investigator to reload the last run."""
        bottlenecks = [DimensionalFinding(**b) for b in data.get("bottlenecks", [])]
        variants = [VariantStat(**v) for v in data.get("variants", [])]
        anomalies = [AnomalyCase(**a) for a in data.get("anomalies", [])]
        transitions = [TransitionStat(**t) for t in data.get("transitions", [])]
        payload = {
            k: v for k, v in data.items()
            if k not in {"bottlenecks", "variants", "anomalies", "transitions"}
        }
        return cls(
            **payload,
            bottlenecks=bottlenecks,
            variants=variants,
            anomalies=anomalies,
            transitions=transitions,
        )
