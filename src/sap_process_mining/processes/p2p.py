"""Procure-to-Pay process definition.

P2P runs from the moment a requirement enters the system (purchase requisition or direct
PO) through supplier fulfillment, invoice verification (the 3-way match), and finally
vendor payment. Bottlenecks typically concentrate at:

  - PO release strategy (approval hierarchy stalls, especially for high-value POs)
  - Supplier goods-receipt lead time (one bad supplier can dominate)
  - Invoice matching (service items and account-assigned items often break automated
    3-way match, forcing manual review)
  - Payment terms + cash-position logic in Treasury
"""

from __future__ import annotations

from .o2c import ProcessDefinition


# Canonical P2P activities. Order below is the idealised happy path.
P2P_ACTIVITIES: tuple[str, ...] = (
    "PurchaseRequisitionCreated",
    "PurchaseRequisitionApproved",
    "PurchaseOrderCreated",
    "PurchaseOrderReleased",       # release strategy / approval hierarchy
    "PurchaseOrderChanged",        # rework
    "GoodsReceived",
    "InvoiceReceived",
    "InvoiceBlocked",              # price/quantity block during verification
    "InvoiceMatched",              # 3-way match completed
    "PaymentMade",
)

P2P_HAPPY_PATH: tuple[str, ...] = (
    "PurchaseRequisitionCreated",
    "PurchaseRequisitionApproved",
    "PurchaseOrderCreated",
    "PurchaseOrderReleased",
    "GoodsReceived",
    "InvoiceReceived",
    "InvoiceMatched",
    "PaymentMade",
)

# Dimensions for bottleneck breakdown. Header-level come from EKKO / A_PurchaseOrder;
# item-level rollups come from EKPO / A_PurchaseOrderItem (see connectors).
#
# P2P item-category legend (SAP standard EKPO-PSTYP):
#   0 / NORM  — standard material (stock)
#   3 / D     — services (DIEN) — often breaks 3-way match, routes to manual review
#   7 / K     — consignment
#   9 / L     — subcontracting
# Account assignment categories (EKPO-KNTTP):
#   K — cost center, A — asset, P — project, Q — WBS element, F — order, U — unknown
P2P_DIMENSIONS: tuple[str, ...] = (
    # Header
    "purchasing_org",
    "purchasing_group",
    "plant",
    "company_code",
    "supplier",
    "material_group",
    "po_type",
    "requester",
    "payment_terms",
    # Item-level rollups
    "primary_item_category",
    "item_category_mix",
    "has_service_item",
    "has_consumable_item",
    "has_account_assignment",
    "primary_account_assignment",
    "primary_material",
)


p2p_process = ProcessDefinition(
    slug="p2p",
    name="procure_to_pay",
    activities=P2P_ACTIVITIES,
    happy_path=P2P_HAPPY_PATH,
    dimensions=P2P_DIMENSIONS,
    description=(
        "Procure-to-Pay covers the vendor-side lifecycle from purchase requisition "
        "through goods receipt, invoice verification (3-way match), and vendor "
        "payment. Typical bottlenecks live at PO release, supplier lead time, and "
        "invoice matching for services."
    ),
)
