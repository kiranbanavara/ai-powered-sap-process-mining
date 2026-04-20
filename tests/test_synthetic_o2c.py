"""Smoke tests for the synthetic connector + mining analytics.

These run offline (no SAP, no LLM) and keep the core contract honest:
  - synthetic connector produces a valid EventLog
  - analytics return a non-empty Findings with expected bottleneck dimensions
"""

from __future__ import annotations

from sap_process_mining.connectors import SyntheticConnector
from sap_process_mining.mining import analyze


def test_synthetic_extract_basic_shape():
    log = SyntheticConnector(seed=1, n_cases=200, days_back=30).extract_o2c()
    assert log.n_cases == 200
    assert log.n_events > 0
    assert "OrderCreated" in log.activities
    assert "PaymentReceived" in log.activities


def test_analytics_surface_seeded_bottlenecks():
    log = SyntheticConnector(seed=1, n_cases=500, days_back=60).extract_o2c()
    findings = analyze(log)

    assert findings.n_cases == 500
    assert findings.median_cycle_hours > 0
    assert findings.transitions, "transitions should not be empty"
    # The synthetic generator seeds DE-SOUTH and Plant 1000 slowdowns; at n=500 we
    # expect at least one dimensional bottleneck to surface.
    assert findings.bottlenecks, "expected seeded bottlenecks to surface"
    dims_seen = {b.dimension for b in findings.bottlenecks}
    assert dims_seen & {"region", "plant"}, f"expected region or plant bottlenecks, got {dims_seen}"


def test_findings_json_round_trip():
    log = SyntheticConnector(seed=1, n_cases=100, days_back=30).extract_o2c()
    findings = analyze(log)
    js = findings.as_prompt_json()
    assert "n_cases" in js
    assert "bottlenecks" in js


def test_mto_bottleneck_surfaces():
    """The synthetic generator seeds a production-delay pattern on has_mto_item=True
    (MTO items add 4–8d at DeliveryCreated). At n=600 this should surface as a
    dimensional bottleneck."""
    log = SyntheticConnector(seed=1, n_cases=600, days_back=60).extract_o2c()
    findings = analyze(log)

    mto_findings = [b for b in findings.bottlenecks if b.dimension == "has_mto_item" and b.value in ("True", "true")]
    config_findings = [b for b in findings.bottlenecks if b.dimension == "has_configurable_item" and b.value in ("True", "true")]

    assert mto_findings or config_findings, (
        f"expected MTO or configurable item bottleneck; got dims "
        f"{[b.dimension for b in findings.bottlenecks]}"
    )


def test_item_attributes_present_on_events():
    log = SyntheticConnector(seed=1, n_cases=50, days_back=30).extract_o2c()
    df = log.df
    for col in ("primary_material", "primary_item_category", "item_category_mix",
                "has_mto_item", "has_configurable_item", "n_items"):
        assert col in df.columns, f"missing rollup column: {col}"
