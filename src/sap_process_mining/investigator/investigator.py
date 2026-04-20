"""Root-cause Investigator agent.

Takes one flagged finding, optional human context, and the tool set. Drives a tool-use
loop against the chosen LLM and returns a structured RCA result.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Callable

from ..event_log import EventLog
from ..llm.base import LLMProvider, ToolLoopResult, ToolTrace
from ..mining import Findings
from .tools import build_rca_tools


SYSTEM_PROMPT = """\
You are a senior SAP root-cause investigator. A process-mining pipeline has flagged
one specific bottleneck (or anomaly) in an Order-to-Cash event log. Your job is to
investigate it using the tools provided and return a concrete root-cause hypothesis.

INVESTIGATION STYLE:
  - Always call describe_finding first to load the full context of the flagged item.
  - Form a hypothesis, then use tools to confirm or reject it. Do not guess.
  - Probe for interaction effects with compare_slice_attributes — a single-dimension
    finding often hides a deeper pattern (e.g. "Plant 1000" is really "MTO-on-Plant-1000").
  - Use temporal_trend to check whether the problem is chronic or started recently.
  - Spot-check a few specific cases with get_case_timeline to ground the numbers in
    real order behaviour.
  - Cross-reference with cross_reference when you suspect two dimensions interact.

RULES:
  - Only cite numbers that came back from a tool. Never invent a statistic.
  - If the user provided a "human comment", treat it as a steering hint that shapes
    which hypotheses you prioritise.
  - Stop calling tools once you have enough evidence. Budget: ≤ 10 tool calls.
  - Refer to cases by case_id (e.g. SO-1000064), not "some orders".

OUTPUT (Markdown, in this exact order):

## Hypothesis
One short sentence stating the likely root cause.

## Evidence
3–6 bullets. Each cites specific numbers / case IDs / dimensions from tool results.

## Confidence
Exactly one of: High / Medium / Low, followed by a one-line reason grounded in evidence.

## Recommended actions
2–4 bullets. Each tied to a specific piece of evidence above. Concrete and operational.

## Open questions
Optional. Things the tools couldn't answer that a human needs to verify.
"""


USER_TEMPLATE = """\
Investigate the following flagged finding.

Finding ID: {finding_id}
Process: {process} (source: {source})

The finding summary below was produced by the automated Flagger. Use the tools to
confirm, refute, or refine the picture.

```json
{finding_json}
```
{comment_block}
Begin by loading the finding with describe_finding, then work from there.
"""


@dataclass
class InvestigationResult:
    finding_id: str
    hypothesis_markdown: str
    traces: list[ToolTrace] = field(default_factory=list)
    provider: str = ""
    model: str = ""
    turns: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    human_comment: str | None = None

    def as_dict(self) -> dict:
        d = asdict(self)
        d["traces"] = [
            {"name": t.name, "args": t.args, "result": t.result, "error": t.error}
            for t in self.traces
        ]
        return d


class Investigator:
    def __init__(
        self,
        llm: LLMProvider,
        event_log: EventLog,
        findings: Findings,
    ):
        self.llm = llm
        self.event_log = event_log
        self.findings = findings
        # build_rca_tools resolves the process from the event log's process_name.
        self.tools = build_rca_tools(event_log, findings)

    def investigate(
        self,
        finding_id: str,
        human_comment: str | None = None,
        max_turns: int = 10,
        on_tool_call: Callable[[ToolTrace], None] | None = None,
    ) -> InvestigationResult:
        finding = self.findings.lookup(finding_id)
        if finding is None:
            valid = [b.id for b in self.findings.bottlenecks] + [a.id for a in self.findings.anomalies]
            raise ValueError(f"Unknown finding id '{finding_id}'. Valid ids: {valid}")

        comment_block = ""
        if human_comment:
            comment_block = (
                f"\nHuman context from the analyst (factor this into your hypothesis "
                f"ranking):\n> {human_comment}\n"
            )

        user = USER_TEMPLATE.format(
            finding_id=finding_id,
            process=self.findings.process,
            source=self.findings.source,
            finding_json=json.dumps(asdict(finding), indent=2, default=str),
            comment_block=comment_block,
        )

        loop: ToolLoopResult = self.llm.run_with_tools(
            SYSTEM_PROMPT,
            user,
            self.tools,
            max_turns=max_turns,
            max_tokens=2500,
            temperature=0.2,
            on_tool_call=on_tool_call,
        )

        return InvestigationResult(
            finding_id=finding_id,
            hypothesis_markdown=loop.text,
            traces=loop.traces,
            provider=self.llm.name,
            model=getattr(self.llm, "model", "unknown"),
            turns=loop.turns,
            input_tokens=loop.usage.input_tokens,
            output_tokens=loop.usage.output_tokens,
            human_comment=human_comment,
        )
