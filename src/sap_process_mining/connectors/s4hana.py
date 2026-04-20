"""S/4HANA connector.

Extracts O2C and P2P events via SAP's standard OData APIs / CDS views. The connector
speaks OData v2 (the format published on api.sap.com for S/4HANA Cloud and on-prem).
It can run against:

  * S/4HANA Cloud public edition (SAP-managed)
  * S/4HANA private edition / on-prem via API Management
  * Sandbox tenants on api.sap.com (for testing)

O2C services & CDS views used (all standard-delivered):

  API_SALES_ORDER_SRV          → A_SalesOrder, A_SalesOrderItem
      Header:  CreationDateTime, LastChangeDateTime, OverallSDProcessStatus,
               OverallCreditStatus, SalesOrderDate, SalesOrganization,
               DistributionChannel, SalesOrderType, SoldToParty, ...
      Item:    Material, MaterialGroup, SalesDocumentItemCategory, RequirementType,
               ProductionPlant, ConfigurationID, ... (used for MTO/variant-config
               signals; rolled up to case-level flags has_mto_item /
               has_configurable_item / item_category_mix)

  API_OUTBOUND_DELIVERY_SRV    → A_OutboundDeliveryHeader, A_OutboundDeliveryItem
      CreationDateTime, ActualGoodsMovementDate, ActualDeliveryDate, PickingDate,
      ShippingPoint, Plant, ...

  API_BILLING_DOCUMENT_SRV     → A_BillingDocument, A_BillingDocumentItem
      CreationDateTime, BillingDocumentDate, AccountingDocumentClearingDate,
      AccountingDocument, ...

P2P services & CDS views used:

  API_PURCHASEREQ_PROCESS_SRV      → A_PurchaseRequisitionHeader, A_PurchaseReqnItem
      CreationDate, ReleaseDate, PurchaseRequisition, RequisitionerName, ...
  API_PURCHASEORDER_PROCESS_SRV    → A_PurchaseOrder, A_PurchaseOrderItem
      CreationDate, ReleaseDate, LastChangeDateTime, PurchaseOrder, Supplier,
      PurchasingOrganization, PurchasingGroup, CompanyCode, DocumentCurrency,
      PurchaseOrderType, ...
      Item:   Material, MaterialGroup, PurchasingDocumentCategory, ItemCategory,
              AccountAssignmentCategory, IsSubcontracting, IsServiceItem, ...
  API_MATERIAL_DOCUMENT_SRV        → A_MaterialDocumentHeader, A_MatDocumentItem
      PostingDate, DocumentDate, GoodsMovementType (101 = GR against PO), ...
  API_SUPPLIERINVOICE_PROCESS_SRV  → A_SupplierInvoice
      CreationDateTime, DocumentDate, InvoiceIsBlockedForPosting (flag),
      ClearingDate (via FI), ReferencedPurchaseOrder, ...

Pilot-time the URL base and auth are configured via .env / YAML. If your landscape uses
custom Z-CDS views for the event log instead, subclass this connector and override
`_fetch_sales_orders` etc. — everything else is kept in `_rows_to_events` which is
deliberately small.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable

import httpx
import pandas as pd

from ..event_log import EventLog
from .base import BaseConnector

log = logging.getLogger(__name__)


# --- Field → activity mapping ----------------------------------------------------------
# The keys are CDS/OData field names; the values are the canonical activity names from
# the process definitions. The connector emits one event per non-null timestamp per case.

# O2C
SO_ACTIVITY_MAP: dict[str, str] = {
    "CreationDateTime": "OrderCreated",
    "LastChangeDateTime": "OrderChanged",   # only emitted if > CreationDateTime by > 1h
}
DELIVERY_ACTIVITY_MAP: dict[str, str] = {
    "CreationDateTime": "DeliveryCreated",
    "PickingDate": "PickingCompleted",
    "ActualGoodsMovementDate": "GoodsIssued",
}
BILLING_ACTIVITY_MAP: dict[str, str] = {
    "CreationDateTime": "InvoiceCreated",
    "BillingDocumentDate": "InvoicePosted",
    "AccountingDocumentClearingDate": "PaymentReceived",
}

# P2P
PR_ACTIVITY_MAP: dict[str, str] = {
    "CreationDate": "PurchaseRequisitionCreated",
    "ReleaseDate": "PurchaseRequisitionApproved",
}
PO_ACTIVITY_MAP: dict[str, str] = {
    "CreationDate": "PurchaseOrderCreated",
    "ReleaseDate": "PurchaseOrderReleased",
    "LastChangeDateTime": "PurchaseOrderChanged",
}
SUPPLIER_INVOICE_ACTIVITY_MAP: dict[str, str] = {
    "CreationDateTime": "InvoiceReceived",
    "ClearingDate": "PaymentMade",
}


class S4HanaConnector(BaseConnector):
    """Pulls O2C events from a live S/4HANA system via standard OData APIs."""

    name = "s4hana"

    def __init__(
        self,
        base_url: str,
        user: str | None = None,
        password: str | None = None,
        oauth_token: str | None = None,
        verify_ssl: bool = True,
        timeout: float = 60.0,
        page_size: int = 500,
    ):
        if not (oauth_token or (user and password)):
            raise ValueError("S/4HANA connector requires either oauth_token or user/password")
        self.base_url = base_url.rstrip("/")
        self._auth = None if oauth_token else httpx.BasicAuth(user, password)  # type: ignore[arg-type]
        self._bearer = oauth_token
        self._verify = verify_ssl
        self._timeout = timeout
        self._page_size = page_size

    # ----- public -----------------------------------------------------------------------

    def extract_o2c(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        sales_orgs: list[str] | None = None,
    ) -> EventLog:
        start = start or datetime(2000, 1, 1, tzinfo=timezone.utc)
        end = end or datetime.now(timezone.utc)

        so_df = self._fetch_sales_orders(start, end, sales_orgs)
        items_df = self._fetch_sales_order_items(start, end, sales_orgs) if not so_df.empty else so_df.iloc[0:0]
        item_rollup = self._rollup_items(items_df)
        dl_df = self._fetch_deliveries(start, end, sales_orgs)
        bl_df = self._fetch_billing(start, end, sales_orgs)

        events = list(self._rows_to_events(so_df, dl_df, bl_df, item_rollup))
        log_obj = EventLog.from_records(events, process_name="order_to_cash", source="s4hana")
        return log_obj.filter_window(start, end)

    # =================================================================================
    # P2P
    # =================================================================================

    def extract_p2p(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        purchasing_orgs: list[str] | None = None,
        company_codes: list[str] | None = None,
    ) -> EventLog:
        start = start or datetime(2000, 1, 1, tzinfo=timezone.utc)
        end = end or datetime.now(timezone.utc)

        po = self._fetch_purchase_orders(start, end, purchasing_orgs, company_codes)
        po_items = self._fetch_purchase_order_items(start, end, purchasing_orgs) if not po.empty else po.iloc[0:0]
        item_rollup = self._rollup_po_items(po_items)
        mat_docs = self._fetch_material_documents(start, end)
        inv = self._fetch_supplier_invoices(start, end, company_codes)

        events = list(self._p2p_rows_to_events(po, mat_docs, inv, item_rollup))
        log_obj = EventLog.from_records(events, process_name="procure_to_pay", source="s4hana")
        return log_obj.filter_window(start, end)

    def _fetch_purchase_orders(
        self,
        start: datetime,
        end: datetime,
        purchasing_orgs: list[str] | None,
        company_codes: list[str] | None,
    ) -> pd.DataFrame:
        filt = (
            f"CreationDate ge datetime'{self._odata_dt(start)}' and "
            f"CreationDate le datetime'{self._odata_dt(end)}'"
        )
        if purchasing_orgs:
            orgs = " or ".join(f"PurchasingOrganization eq '{o}'" for o in purchasing_orgs)
            filt = f"({filt}) and ({orgs})"
        if company_codes:
            ccs = " or ".join(f"CompanyCode eq '{c}'" for c in company_codes)
            filt = f"({filt}) and ({ccs})"
        select = ",".join([
            "PurchaseOrder", "PurchasingOrganization", "PurchasingGroup",
            "CompanyCode", "Supplier", "PurchaseOrderType",
            "CreationDate", "ReleaseDate", "LastChangeDateTime",
            "DocumentCurrency", "PaymentTerms", "CreatedByUser",
        ])
        return self._odata_get(
            "/sap/opu/odata/sap/API_PURCHASEORDER_PROCESS_SRV/A_PurchaseOrder",
            filt, select,
        )

    def _fetch_purchase_order_items(
        self, start: datetime, end: datetime, purchasing_orgs: list[str] | None
    ) -> pd.DataFrame:
        filt = f"CreationDate ge datetime'{self._odata_dt(start)}' and CreationDate le datetime'{self._odata_dt(end)}'"
        select = ",".join([
            "PurchaseOrder", "PurchaseOrderItem",
            "Material", "MaterialGroup", "Plant",
            "PurchaseOrderItemCategory",       # 0/NORM/D/K/L
            "AccountAssignmentCategory",       # K/A/P/Q/F
            "IsSubcontracting", "IsServiceItem",
        ])
        return self._odata_get(
            "/sap/opu/odata/sap/API_PURCHASEORDER_PROCESS_SRV/A_PurchaseOrderItem",
            filt, select,
        )

    def _rollup_po_items(self, items: pd.DataFrame) -> dict[str, dict]:
        if items.empty:
            return {}
        v = items.copy()
        # Service detection: explicit flag or category D
        v["_is_service"] = (
            v.get("IsServiceItem", pd.Series(dtype=bool)).fillna(False).astype(bool)
            | (v.get("PurchaseOrderItemCategory") == "D")
        )
        v["_is_consumable"] = v.get("PurchaseOrderItemCategory") == "K"
        v["_has_acct"] = v.get("AccountAssignmentCategory").notna() if "AccountAssignmentCategory" in v.columns else False

        rollup: dict[str, dict] = {}
        for po, grp in v.groupby("PurchaseOrder"):
            cats = sorted(grp.get("PurchaseOrderItemCategory", pd.Series()).dropna().unique().tolist())
            primary = grp.iloc[0]
            rollup[str(po)] = {
                "n_items": int(len(grp)),
                "primary_material": str(primary.get("Material") or "") or None,
                "primary_item_category": str(primary.get("PurchaseOrderItemCategory") or "") or None,
                "item_category_mix": "+".join(cats) if cats else None,
                "has_service_item": bool(grp["_is_service"].any()),
                "has_consumable_item": bool(grp["_is_consumable"].any()),
                "has_account_assignment": bool(grp["_has_acct"].any() if isinstance(grp["_has_acct"], pd.Series) else grp["_has_acct"]),
                "primary_account_assignment": str(primary.get("AccountAssignmentCategory") or "") or None,
            }
        return rollup

    def _fetch_material_documents(self, start: datetime, end: datetime) -> pd.DataFrame:
        """Goods receipts (movement type 101 against a PO) — emit GoodsReceived."""
        filt = (
            f"PostingDate ge datetime'{self._odata_dt(start)}' and "
            f"PostingDate le datetime'{self._odata_dt(end)}' and "
            f"GoodsMovementType eq '101'"
        )
        select = ",".join([
            "MaterialDocument", "MaterialDocumentYear",
            "PurchaseOrder",                    # case linkage
            "PostingDate", "DocumentDate", "GoodsMovementType",
        ])
        return self._odata_get(
            "/sap/opu/odata/sap/API_MATERIAL_DOCUMENT_SRV/A_MaterialDocumentItem",
            filt, select,
        )

    def _fetch_supplier_invoices(
        self, start: datetime, end: datetime, company_codes: list[str] | None
    ) -> pd.DataFrame:
        filt = (
            f"CreationDateTime ge datetimeoffset'{self._odata_dt(start)}' and "
            f"CreationDateTime le datetimeoffset'{self._odata_dt(end)}'"
        )
        if company_codes:
            ccs = " or ".join(f"CompanyCode eq '{c}'" for c in company_codes)
            filt = f"({filt}) and ({ccs})"
        select = ",".join([
            "SupplierInvoice", "CompanyCode", "Supplier",
            "CreationDateTime", "DocumentDate", "InvoiceIsBlockedForPosting",
            "ClearingDate", "ReferencedPurchaseOrder",
        ])
        return self._odata_get(
            "/sap/opu/odata/sap/API_SUPPLIERINVOICE_PROCESS_SRV/A_SupplierInvoice",
            filt, select,
        )

    def _p2p_rows_to_events(
        self,
        po: pd.DataFrame,
        mat: pd.DataFrame,
        inv: pd.DataFrame,
        item_rollup: dict[str, dict],
    ) -> Iterable[dict]:
        po = po.rename(columns={"PurchaseOrder": "case_id"}) if not po.empty else po
        for _, row in po.iterrows():
            case_id = str(row["case_id"])
            attrs = {
                "purchasing_org": row.get("PurchasingOrganization"),
                "purchasing_group": row.get("PurchasingGroup"),
                "company_code": row.get("CompanyCode"),
                "supplier": row.get("Supplier"),
                "po_type": row.get("PurchaseOrderType"),
                "payment_terms": row.get("PaymentTerms"),
                "requester": row.get("CreatedByUser"),
                **item_rollup.get(case_id, {}),
            }
            for field_, activity in PO_ACTIVITY_MAP.items():
                ts = row.get(field_)
                if pd.isna(ts):
                    continue
                if activity == "PurchaseOrderChanged":
                    created = row.get("CreationDate")
                    if pd.notna(created) and (pd.Timestamp(ts) - pd.Timestamp(created)).total_seconds() < 3600:
                        continue
                yield {"case_id": case_id, "activity": activity, "timestamp": ts, **attrs}

        for _, row in mat.iterrows():
            case_id = str(row.get("PurchaseOrder") or "")
            if not case_id or pd.isna(row.get("PostingDate")):
                continue
            yield {
                "case_id": case_id, "activity": "GoodsReceived",
                "timestamp": row["PostingDate"], **item_rollup.get(case_id, {}),
            }

        for _, row in inv.iterrows():
            case_id = str(row.get("ReferencedPurchaseOrder") or row.get("SupplierInvoice"))
            attrs = item_rollup.get(case_id, {})
            if pd.notna(row.get("CreationDateTime")):
                yield {
                    "case_id": case_id, "activity": "InvoiceReceived",
                    "timestamp": row["CreationDateTime"], **attrs,
                }
            # Blocked flag: emit a point-in-time InvoiceBlocked at the creation moment
            if bool(row.get("InvoiceIsBlockedForPosting")) and pd.notna(row.get("CreationDateTime")):
                yield {
                    "case_id": case_id, "activity": "InvoiceBlocked",
                    "timestamp": row["CreationDateTime"], **attrs,
                }
            if pd.notna(row.get("ClearingDate")):
                # When cleared, we also mark the invoice as matched (invoices can't clear
                # without matching in S/4). We emit both so the process is observable.
                yield {
                    "case_id": case_id, "activity": "InvoiceMatched",
                    "timestamp": row["ClearingDate"], **attrs,
                }
                yield {
                    "case_id": case_id, "activity": "PaymentMade",
                    "timestamp": row["ClearingDate"], **attrs,
                }

    # =================================================================================
    # O2C OData fetchers (unchanged)
    # =================================================================================

    def _fetch_sales_orders(
        self, start: datetime, end: datetime, sales_orgs: list[str] | None
    ) -> pd.DataFrame:
        filt = f"CreationDateTime ge datetimeoffset'{self._odata_dt(start)}' and CreationDateTime le datetimeoffset'{self._odata_dt(end)}'"
        if sales_orgs:
            orgs = " or ".join(f"SalesOrganization eq '{o}'" for o in sales_orgs)
            filt = f"({filt}) and ({orgs})"
        select = ",".join([
            "SalesOrder", "SalesOrganization", "DistributionChannel", "SalesOrderType",
            "SoldToParty", "OverallSDProcessStatus", "OverallCreditStatus",
            "CreationDateTime", "LastChangeDateTime",
        ])
        return self._odata_get("/sap/opu/odata/sap/API_SALES_ORDER_SRV/A_SalesOrder", filt, select)

    def _fetch_sales_order_items(
        self, start: datetime, end: datetime, sales_orgs: list[str] | None
    ) -> pd.DataFrame:
        """Pull items for the same window. Filter is on the *header* creation date via
        the parent SalesOrder, but API_SALES_ORDER_SRV only filters on item-entity fields,
        so we use `CreationDate` on the item (which mirrors the header in practice). If a
        customer needs exact header-based filtering, switch this to a `$expand=to_Item`
        call on A_SalesOrder instead.
        """
        filt = f"CreationDate ge datetime'{self._odata_dt(start)}' and CreationDate le datetime'{self._odata_dt(end)}'"
        select = ",".join([
            "SalesOrder", "SalesOrderItem",
            "Material", "MaterialGroup",
            "SalesDocumentItemCategory",  # TAN / TAK / TAC / TAD / TAS
            "RequirementType",            # KE, KEK, etc. — MTO vs. MTS requirement class
            "ProductionPlant",
        ])
        return self._odata_get(
            "/sap/opu/odata/sap/API_SALES_ORDER_SRV/A_SalesOrderItem", filt, select,
        )

    def _rollup_items(self, items: pd.DataFrame) -> dict[str, dict]:
        """Aggregate item-level attributes onto the sales-order (case) level."""
        if items.empty:
            return {}

        # MTO/config detection from the item category:
        mto_categories = {"TAK"}       # make-to-order
        config_categories = {"TAC"}    # variant-configurable
        # RequirementType prefixed 'KE' indicates a customer-individual stock (MTO-ish).
        items = items.copy()
        items["_is_mto"] = (
            items["SalesDocumentItemCategory"].isin(mto_categories)
            | items.get("RequirementType", pd.Series(dtype=object)).fillna("").astype(str).str.startswith("KE")
        )
        items["_is_config"] = items["SalesDocumentItemCategory"].isin(config_categories)

        rollup: dict[str, dict] = {}
        for so, grp in items.groupby("SalesOrder"):
            cats = sorted(grp["SalesDocumentItemCategory"].dropna().unique().tolist())
            primary = grp.iloc[0]
            rollup[str(so)] = {
                "n_items": int(len(grp)),
                "primary_material": str(primary.get("Material", "")) or None,
                "primary_item_category": str(primary.get("SalesDocumentItemCategory", "")) or None,
                "item_category_mix": "+".join(cats) if cats else None,
                "has_mto_item": bool(grp["_is_mto"].any()),
                "has_configurable_item": bool(grp["_is_config"].any()),
            }
        return rollup

    def _fetch_deliveries(
        self, start: datetime, end: datetime, sales_orgs: list[str] | None
    ) -> pd.DataFrame:
        filt = f"CreationDateTime ge datetimeoffset'{self._odata_dt(start)}' and CreationDateTime le datetimeoffset'{self._odata_dt(end)}'"
        if sales_orgs:
            orgs = " or ".join(f"SalesOrganization eq '{o}'" for o in sales_orgs)
            filt = f"({filt}) and ({orgs})"
        select = ",".join([
            "DeliveryDocument", "SalesOrganization", "Plant", "ShippingPoint",
            "CreationDateTime", "PickingDate", "ActualGoodsMovementDate",
            "ReferenceSDDocument",  # sales-order reference for case linkage
        ])
        return self._odata_get(
            "/sap/opu/odata/sap/API_OUTBOUND_DELIVERY_SRV/A_OutboundDeliveryHeader",
            filt, select,
        )

    def _fetch_billing(
        self, start: datetime, end: datetime, sales_orgs: list[str] | None
    ) -> pd.DataFrame:
        filt = f"CreationDateTime ge datetimeoffset'{self._odata_dt(start)}' and CreationDateTime le datetimeoffset'{self._odata_dt(end)}'"
        if sales_orgs:
            orgs = " or ".join(f"SalesOrganization eq '{o}'" for o in sales_orgs)
            filt = f"({filt}) and ({orgs})"
        select = ",".join([
            "BillingDocument", "SalesOrganization", "BillingDocumentType",
            "CreationDateTime", "BillingDocumentDate", "AccountingDocumentClearingDate",
            "SDDocumentReference",  # links to delivery/sales order
        ])
        return self._odata_get(
            "/sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV/A_BillingDocument",
            filt, select,
        )

    # ----- event mapping ---------------------------------------------------------------

    def _rows_to_events(
        self,
        so: pd.DataFrame,
        dl: pd.DataFrame,
        bl: pd.DataFrame,
        item_rollup: dict[str, dict],
    ) -> Iterable[dict]:
        so = so.rename(columns={"SalesOrder": "case_id"}) if not so.empty else so
        for _, row in so.iterrows():
            case_id = str(row["case_id"])
            attrs = {
                "sales_org": row.get("SalesOrganization"),
                "distribution_channel": row.get("DistributionChannel"),
                "order_type": row.get("SalesOrderType"),
                "customer": row.get("SoldToParty"),
                **item_rollup.get(case_id, {}),
            }
            for field_, activity in SO_ACTIVITY_MAP.items():
                ts = row.get(field_)
                if pd.isna(ts):
                    continue
                if activity == "OrderChanged":
                    created = row.get("CreationDateTime")
                    if pd.notna(created) and (pd.Timestamp(ts) - pd.Timestamp(created)).total_seconds() < 3600:
                        continue  # not a real change, just system-initial
                yield {"case_id": case_id, "activity": activity, "timestamp": ts, **attrs}

        # Deliveries → events, case_id taken from ReferenceSDDocument (the sales order)
        for _, row in dl.iterrows():
            case_id = str(row.get("ReferenceSDDocument") or row.get("DeliveryDocument"))
            attrs = {
                "plant": row.get("Plant"),
                "shipping_point": row.get("ShippingPoint"),
                **item_rollup.get(case_id, {}),
            }
            for field_, activity in DELIVERY_ACTIVITY_MAP.items():
                ts = row.get(field_)
                if pd.notna(ts):
                    yield {"case_id": case_id, "activity": activity, "timestamp": ts, **attrs}

        # Billing → events, case_id from SDDocumentReference
        for _, row in bl.iterrows():
            case_id = str(row.get("SDDocumentReference") or row.get("BillingDocument"))
            attrs = item_rollup.get(case_id, {})
            for field_, activity in BILLING_ACTIVITY_MAP.items():
                ts = row.get(field_)
                if pd.notna(ts):
                    yield {"case_id": case_id, "activity": activity, "timestamp": ts, **attrs}

    # ----- OData helpers ---------------------------------------------------------------

    def _odata_get(self, path: str, filter_expr: str, select: str) -> pd.DataFrame:
        """Page through an OData v2 entity set and return a flat DataFrame."""
        headers = {"Accept": "application/json"}
        if self._bearer:
            headers["Authorization"] = f"Bearer {self._bearer}"

        params = {
            "$filter": filter_expr,
            "$select": select,
            "$format": "json",
            "$top": self._page_size,
            "$skip": 0,
        }
        rows: list[dict] = []
        with httpx.Client(
            base_url=self.base_url,
            auth=self._auth,
            headers=headers,
            verify=self._verify,
            timeout=self._timeout,
        ) as client:
            while True:
                resp = client.get(path, params=params)
                resp.raise_for_status()
                batch = resp.json().get("d", {}).get("results", [])
                if not batch:
                    break
                rows.extend(batch)
                if len(batch) < self._page_size:
                    break
                params["$skip"] += self._page_size

        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        # OData v2 emits /Date(ms)/ strings for datetimes — parse those.
        for col in df.columns:
            if df[col].dtype == object and df[col].astype(str).str.startswith("/Date(").any():
                df[col] = pd.to_datetime(
                    df[col].astype(str).str.extract(r"/Date\((-?\d+)\)/", expand=False).astype(float),
                    unit="ms", utc=True, errors="coerce",
                )
        return df

    @staticmethod
    def _odata_dt(dt: datetime) -> str:
        """Format a datetime the way S/4HANA's OData v2 edm.DateTimeOffset wants."""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
