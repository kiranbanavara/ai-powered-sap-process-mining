"""Unit tests for the Investigator tools and a mocked tool-use loop.

No LLM calls. The tools must be deterministic and well-behaved on their own before we
trust a real agent to orchestrate them.
"""

from __future__ import annotations

from dataclasses import dataclass

from sap_process_mining.connectors import SyntheticConnector
from sap_process_mining.investigator.tools import build_rca_tools
from sap_process_mining.investigator import Investigator
from sap_process_mining.llm.base import LLMProvider, LLMUsage, Tool, ToolLoopResult, ToolTrace
from sap_process_mining.mining import analyze


def _setup():
    log = SyntheticConnector(seed=7, n_cases=400, days_back=60).extract_o2c()
    findings = analyze(log)
    assert findings.bottlenecks, "precondition: expected bottlenecks"
    return log, findings


def _tools_by_name(log, findings):
    return {t.name: t for t in build_rca_tools(log, findings)}


def test_describe_finding():
    log, findings = _setup()
    tools = _tools_by_name(log, findings)
    b1 = findings.bottlenecks[0]
    result = tools["describe_finding"].fn({"finding_id": b1.id})
    assert result["id"] == b1.id
    assert result["transition"] == b1.transition


def test_describe_finding_unknown():
    log, findings = _setup()
    tools = _tools_by_name(log, findings)
    result = tools["describe_finding"].fn({"finding_id": "ZZZ"})
    assert "error" in result


def test_list_cases_returns_ranked_slice():
    log, findings = _setup()
    tools = _tools_by_name(log, findings)
    b = findings.bottlenecks[0]
    result = tools["list_cases"].fn({
        "dimension": b.dimension, "value": b.value,
        "transition": b.transition, "limit": 5,
    })
    assert result["n_matching_cases"] >= 1
    assert 0 < len(result["cases"]) <= 5
    # Ordered by hours descending
    hours = [c["hours"] for c in result["cases"]]
    assert hours == sorted(hours, reverse=True)


def test_get_case_timeline():
    log, findings = _setup()
    tools = _tools_by_name(log, findings)
    sample_case = log.df["case_id"].iloc[0]
    result = tools["get_case_timeline"].fn({"case_id": sample_case})
    assert result["case_id"] == sample_case
    assert result["n_events"] >= 1
    assert result["events"][0]["hours_since_prev"] is None  # first event


def test_compare_slice_attributes_includes_over_representation():
    log, findings = _setup()
    tools = _tools_by_name(log, findings)
    # Use an MTO finding if available
    mto = next((b for b in findings.bottlenecks if b.dimension == "has_mto_item"), None)
    if mto is None:
        return  # seed variability — skip
    result = tools["compare_slice_attributes"].fn({"dimension": "has_mto_item", "value": "True"})
    assert "breakdown" in result
    assert result["n_cases_in_slice"] > 0
    # Primary item category rows include an over_representation field
    rows = result["breakdown"].get("primary_item_category", [])
    assert any("over_representation" in r for r in rows)


def test_cross_reference():
    log, findings = _setup()
    tools = _tools_by_name(log, findings)
    result = tools["cross_reference"].fn({
        "transition": "OrderCreated → CreditChecked",
        "dim_a": "plant",
        "dim_b": "has_mto_item",
    })
    assert "rows" in result
    assert result["overall_median_hours"] > 0


def test_temporal_trend_filtered():
    log, findings = _setup()
    tools = _tools_by_name(log, findings)
    result = tools["temporal_trend"].fn({
        "transition": "OrderCreated → CreditChecked",
        "dimension": "plant",
        "value": "1000",
    })
    assert "rows" in result
    if result.get("rows"):
        row = result["rows"][0]
        assert "period_start" in row and "median_hours" in row


# --- mocked tool-use loop ------------------------------------------------------------


@dataclass
class _ScriptedProvider(LLMProvider):
    """LLM stand-in that follows a scripted sequence of tool calls and a final answer.

    Lets us test the Investigator orchestrator without hitting a real API.
    """
    name: str = "scripted"
    model: str = "scripted-v1"

    def complete(self, system: str, user: str, **kwargs):  # pragma: no cover — unused
        return ("", LLMUsage())

    def run_with_tools(self, system, user, tools, **kwargs) -> ToolLoopResult:
        tools_by_name = {t.name: t for t in tools}
        traces: list[ToolTrace] = []
        # Script: load the finding, list cases, produce final answer.
        for call in [
            {"name": "describe_finding", "args": {"finding_id": "B1"}},
            {"name": "compare_slice_attributes",
             "args": {"dimension": "plant", "value": "1000"}},
        ]:
            t = tools_by_name[call["name"]]
            try:
                result = t.fn(call["args"])
                traces.append(ToolTrace(name=call["name"], args=call["args"], result=result))
            except Exception as e:  # pragma: no cover
                traces.append(ToolTrace(name=call["name"], args=call["args"], result=None, error=str(e)))
        return ToolLoopResult(
            text="## Hypothesis\nScripted hypothesis.\n## Evidence\n- cited.\n## Confidence\nHigh",
            traces=traces,
            usage=LLMUsage(input_tokens=100, output_tokens=50),
            turns=3,
            stopped_because="end_turn",
        )


def test_investigator_end_to_end_with_scripted_provider():
    log, findings = _setup()
    provider = _ScriptedProvider()
    inv = Investigator(provider, log, findings)
    result = inv.investigate(findings.bottlenecks[0].id, human_comment="Test comment")
    assert "Hypothesis" in result.hypothesis_markdown
    assert len(result.traces) == 2
    assert result.human_comment == "Test comment"
    assert result.provider == "scripted"
