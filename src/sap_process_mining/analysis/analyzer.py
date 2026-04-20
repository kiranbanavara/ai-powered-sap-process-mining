"""Orchestrator: EventLog → Findings → LLM narrative."""

from __future__ import annotations

from dataclasses import dataclass

from ..event_log import EventLog
from ..llm.base import LLMProvider, LLMUsage
from ..mining import Findings, analyze


SYSTEM_PROMPT = """\
You are a senior SAP operations analyst writing for a COO / Head of Operations.

You receive structured process-mining findings (JSON) extracted from an SAP Order-to-Cash
event log. Your job is to turn them into a brief, concrete operations briefing that a
non-technical executive can act on Monday morning.

HARD RULES:
- Only use numbers that appear in the JSON. Never invent counts, hours, or percentages.
- Round clock times sensibly: < 24h in hours (e.g. "3.2 hours"), ≥ 24h in days.
- Name the specific dimension value causing the slowdown (plant, region, customer, etc.),
  don't say "some plants" when the data names them.
- Refer to cases, not transactions. Refer to activities by their business name, not the
  raw SAP field.
- Be direct and short. No executive-deck clichés. No generic "consider optimizing".

OUTPUT STRUCTURE (Markdown):
1. **Headline finding** — one sentence stating the single biggest issue, with the number.
2. **Top 3 bottlenecks** — bullet list. Each bullet: <what> + <where> + <how much> + <how many cases>.
3. **Rework & anomalies** — one short paragraph.
4. **Recommended next actions** — 3–5 bullets, each tied to a specific finding above.

Tone: clear, decisive, factual. Assume the reader is busy and will act on what you write.
"""


USER_PROMPT_TEMPLATE = """\
Here are the process-mining findings for the window {start} → {end}.
The process is {process} extracted from source '{source}'.

FINDINGS (JSON):
```json
{findings_json}
```

Write the operations briefing per the rules in the system prompt.
"""


@dataclass
class AnalysisResult:
    findings: Findings
    narrative: str
    usage: LLMUsage
    provider: str
    model: str


class Analyzer:
    def __init__(self, llm: LLMProvider):
        self.llm = llm

    def run(self, log: EventLog) -> AnalysisResult:
        findings = analyze(log)
        return self.narrate(findings)

    def narrate(self, findings: Findings) -> AnalysisResult:
        user = USER_PROMPT_TEMPLATE.format(
            start=findings.window_start,
            end=findings.window_end,
            process=findings.process,
            source=findings.source,
            findings_json=findings.as_prompt_json(),
        )
        text, usage = self.llm.complete(SYSTEM_PROMPT, user, max_tokens=1500, temperature=0.2)
        return AnalysisResult(
            findings=findings,
            narrative=text,
            usage=usage,
            provider=self.llm.name,
            model=getattr(self.llm, "model", "unknown"),
        )
