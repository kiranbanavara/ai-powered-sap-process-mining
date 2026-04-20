"""Smoke tests for synthetic P2P + analytics.

Seeded patterns that must surface at n ≥ 500:
  - Supplier V-9000 slow goods receipt
  - Purchasing group A1 slow PO release
  - Service items (TAK/D, has_service_item=True) slow invoice matching
"""

from __future__ import annotations

from sap_process_mining.connectors import SyntheticConnector
from sap_process_mining.mining import analyze
from sap_process_mining.processes import p2p_process


def _run(n_cases: int = 800):
    log = SyntheticConnector(seed=11, n_cases=n_cases, days_back=60).extract_p2p()
    findings = analyze(log, process=p2p_process)
    return log, findings


def test_p2p_extract_basic_shape():
    log = SyntheticConnector(seed=3, n_cases=200, days_back=30).extract_p2p()
    assert log.n_cases == 200
    assert log.n_events > 0
    assert "PurchaseOrderCreated" in log.activities
    assert "GoodsReceived" in log.activities
    assert "InvoiceMatched" in log.activities


def test_p2p_seeded_bottlenecks_surface():
    log, findings = _run(600)
    dims = {b.dimension for b in findings.bottlenecks}
    values = {(b.dimension, b.value) for b in findings.bottlenecks}

    # Expect at least one bottleneck on each of the three seeded axes.
    assert any("supplier" == d for d in dims), f"expected supplier bottleneck, got {dims}"
    assert ("purchasing_group", "A1") in values, \
        f"expected purchasing_group=A1 bottleneck, got {values}"
    service_found = any(
        (d == "has_service_item" and v in ("True", "true"))
        or (d == "primary_item_category" and v == "D")
        for d, v in values
    )
    assert service_found, f"expected service-item bottleneck, got {values}"


def test_p2p_item_rollup_present():
    log, _ = _run(50)
    df = log.df
    for col in ("primary_material", "primary_item_category", "item_category_mix",
                "has_service_item", "has_consumable_item",
                "has_account_assignment", "primary_account_assignment", "n_items"):
        assert col in df.columns, f"missing rollup column: {col}"


def test_p2p_analyze_auto_selects_process_from_log_name():
    log = SyntheticConnector(seed=5, n_cases=200, days_back=30).extract_p2p()
    findings = analyze(log)  # no explicit process — should auto-pick p2p
    assert findings.process == "procure_to_pay"
    assert findings.bottlenecks  # something should surface


def test_connector_dispatch():
    c = SyntheticConnector(seed=1, n_cases=50, days_back=30)
    o2c_log = c.extract("o2c")
    p2p_log = c.extract("p2p")
    assert o2c_log.process_name == "order_to_cash"
    assert p2p_log.process_name == "procure_to_pay"
