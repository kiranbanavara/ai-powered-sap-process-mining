"""Order-to-Cash process definition.

The activity set below is the canonical O2C flow. Each connector (S/4HANA, ECC, synthetic)
maps its raw tables/CDS views onto these activity names so downstream analytics are
data-source-agnostic.
"""

from __future__ import annotations

from dataclasses import dataclass


# Canonical activity names — order is the idealised happy path.
O2C_ACTIVITIES: tuple[str, ...] = (
    "OrderCreated",
    "CreditChecked",
    "OrderApproved",
    "OrderChanged",         # rework
    "DeliveryCreated",
    "PickingCompleted",
    "GoodsIssued",
    "InvoiceCreated",
    "InvoicePosted",
    "PaymentReceived",
)

O2C_HAPPY_PATH: tuple[str, ...] = (
    "OrderCreated",
    "CreditChecked",
    "OrderApproved",
    "DeliveryCreated",
    "PickingCompleted",
    "GoodsIssued",
    "InvoiceCreated",
    "InvoicePosted",
    "PaymentReceived",
)

# Attributes (columns) the analytics pipeline uses for dimensional breakdowns.
# Connectors should populate as many as possible.
#
# Header-level dimensions come from A_SalesOrder / VBAK.
# Item-level rollups (primary_item_category, has_mto_item, etc.) come from
# A_SalesOrderItem / VBAP, aggregated up to the case. An order is flagged as MTO / config
# if *any* item triggers that category — in practice that item is usually what drags the
# whole case's cycle time.
#
# MTO-relevant SAP item categories:
#   TAN  — standard stock (make-to-stock)
#   TAK  — make-to-order
#   TAC  — configurable (variant configuration)
#   TAD  — service item
#   TAS  — third-party (drop-ship)
O2C_DIMENSIONS: tuple[str, ...] = (
    # Header
    "plant",
    "sales_org",
    "distribution_channel",
    "region",
    "customer",
    "material_group",
    "sold_to_country",
    "order_type",
    "responsible_user",
    # Item-level rollups (see A_SalesOrderItem / VBAP)
    "primary_item_category",
    "item_category_mix",
    "has_mto_item",
    "has_configurable_item",
    "primary_material",
)


@dataclass(frozen=True)
class ProcessDefinition:
    """A business process the pipeline can mine.

    `slug` is a short identifier (used as a subdir name in `reports/latest/` and as the
    CLI/UI key). `name` is the human-readable long form used in briefings.
    """
    slug: str
    name: str
    activities: tuple[str, ...]
    happy_path: tuple[str, ...]
    dimensions: tuple[str, ...]
    description: str


o2c_process = ProcessDefinition(
    slug="o2c",
    name="order_to_cash",
    activities=O2C_ACTIVITIES,
    happy_path=O2C_HAPPY_PATH,
    dimensions=O2C_DIMENSIONS,
    description=(
        "Order-to-Cash covers the full revenue cycle from sales order receipt to "
        "cash collection. Bottlenecks typically appear at credit check, goods issue, "
        "and invoice clearing."
    ),
)
