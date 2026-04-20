"""ECC connector.

Classic ECC has no public OData surface by default, so this connector supports two
alternative access paths (either works; pick what your landscape allows):

  (a) Direct SQL over the primary database (Oracle/MSSQL/DB2/HANA) via SQLAlchemy — the
      fastest path for read-only extraction on systems where DBA access is available.

  (b) RFC via `pyrfc` calling standard function modules like RFC_READ_TABLE (or a custom
      Z-FM that SELECTs from the source tables). Slower but needs no DB credentials.

O2C tables (all standard-delivered ECC):
    VBAK   — Sales order header        (ERDAT/ERZET created, AEDAT changed)
    VBAP   — Sales order items
    LIKP   — Delivery header           (ERDAT created, WADAT_IST actual goods issue)
    LIPS   — Delivery items
    VBRK   — Billing header            (ERDAT created, FKDAT billing date)
    VBRP   — Billing items
    BKPF   — Accounting header         (cleared date via AUGDT on BSEG / payment doc)
    BSEG   — Accounting segments
    VBFA   — Sales document flow       (joins SO → Delivery → Invoice → Accounting)
    CDHDR  — Change document header
    CDPOS  — Change document items     (for credit-block / status transitions)

P2P tables:
    EBAN   — Purchase requisition      (ERDAT created, FRGDT release date)
    EKKO   — Purchase order header     (AEDAT created, FRGDT release, LIFNR supplier)
    EKPO   — Purchase order items      (PSTYP item category, KNTTP account assignment)
    EKBE   — PO history per item       (BWART 101 = GR, BEWTP E = GR event)
    MKPF/MSEG — Goods movement         (GR against PO, BUDAT posting date)
    RBKP   — Supplier invoice header   (BUDAT posting, ZLSPR payment block)
    RSEG   — Supplier invoice items
    BSAK   — Vendor cleared items      (AUGDT clearing date = payment)
    BSIK   — Vendor open items
    CDHDR/CDPOS — Change history for release status, invoice blocks

Both access paths produce the same canonical EventLog regardless of process.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from ..event_log import EventLog
from .base import BaseConnector

log = logging.getLogger(__name__)


# SQL templates, parameterised. Works across Oracle / HANA / MSSQL with minor dialect
# changes (which can be handled by SQLAlchemy if needed).
VBAK_SQL = """
SELECT VBELN AS case_id,
       VKORG AS sales_org,
       VTWEG AS distribution_channel,
       AUART AS order_type,
       KUNNR AS customer,
       ERDAT, ERZET, AEDAT,
       NETWR AS order_value,
       WAERK AS currency
FROM   VBAK
WHERE  ERDAT BETWEEN :start_date AND :end_date
"""

# Item-level signals from VBAP. PSTYV is the schedule-line / item category:
#   TAN  = standard MTS, TAK = MTO, TAC = configurable (variant config),
#   TAD  = service, TAS = third-party/drop-ship. KZVBR='E' (individual customer stock)
#   is the traditional ECC MTO flag independent of item category.
VBAP_SQL = """
SELECT VBELN AS case_id,
       POSNR AS item,
       MATNR AS material,
       MATKL AS material_group,
       PSTYV AS item_category,
       KZVBR AS consumption_indicator,
       WERKS AS item_plant
FROM   VBAP
WHERE  VBELN IN (SELECT VBELN FROM VBAK WHERE ERDAT BETWEEN :start_date AND :end_date)
"""

LIKP_LIPS_SQL = """
SELECT LIKP.VBELN   AS delivery,
       VBFA.VBELV   AS case_id,              -- sales order
       LIKP.WERKS   AS plant,
       LIKP.VSTEL   AS shipping_point,
       LIKP.ERDAT   AS delivery_created,
       LIKP.WADAT_IST AS goods_issued,
       LIPS.KOMKZ   AS picking_status_ignored
FROM   LIKP
JOIN   LIPS ON LIPS.VBELN = LIKP.VBELN
JOIN   VBFA ON VBFA.VBELN = LIKP.VBELN AND VBFA.VBTYP_N = 'J' AND VBFA.VBTYP_V = 'C'
WHERE  LIKP.ERDAT BETWEEN :start_date AND :end_date
"""

VBRK_SQL = """
SELECT VBRK.VBELN   AS billing_doc,
       VBFA.VBELV   AS case_id_delivery,     -- delivery number, map back to SO via VBFA chain
       VBRK.ERDAT   AS invoice_created,
       VBRK.FKDAT   AS billing_date,
       VBRK.NETWR   AS invoice_value,
       BSEG.AUGDT   AS cleared_date          -- payment clearing
FROM   VBRK
LEFT JOIN VBFA ON VBFA.VBELN = VBRK.VBELN AND VBFA.VBTYP_N = 'M'
LEFT JOIN BKPF ON BKPF.XBLNR = VBRK.VBELN
LEFT JOIN BSEG ON BSEG.BELNR = BKPF.BELNR AND BSEG.KOART = 'D'
WHERE  VBRK.ERDAT BETWEEN :start_date AND :end_date
"""

# --- P2P SQL --------------------------------------------------------------------------

EKKO_SQL = """
SELECT EBELN AS case_id,
       EKORG AS purchasing_org,
       EKGRP AS purchasing_group,
       BUKRS AS company_code,
       LIFNR AS supplier,
       BSART AS po_type,
       ZTERM AS payment_terms,
       ERNAM AS requester,
       AEDAT AS po_changed,
       BEDAT AS po_date,               -- document date
       FRGDT AS po_released,
       ERDAT AS po_created,
       NETWR AS order_value,
       WAERS AS currency
FROM   EKKO
WHERE  ERDAT BETWEEN :start_date AND :end_date
"""

EKPO_SQL = """
SELECT EBELN AS case_id,
       EBELP AS item,
       MATNR AS material,
       MATKL AS material_group,
       WERKS AS plant,
       PSTYP AS item_category,         -- 0 NORM, 3 D (service), 7 K (cons), 9 L (subcon)
       KNTTP AS account_assignment,    -- K cost ctr, A asset, P project, Q WBS, F order
       DIENS AS is_service_flag
FROM   EKPO
WHERE  EBELN IN (SELECT EBELN FROM EKKO WHERE ERDAT BETWEEN :start_date AND :end_date)
"""

# Goods receipts: BWART 101 (GR against PO) posted to MSEG
GR_SQL = """
SELECT MSEG.EBELN AS case_id,
       MKPF.BUDAT AS posting_date,
       MKPF.CPUDT AS entry_date,
       MSEG.BWART AS movement_type
FROM   MKPF
JOIN   MSEG ON MSEG.MBLNR = MKPF.MBLNR AND MSEG.MJAHR = MKPF.MJAHR
WHERE  MSEG.BWART = '101'
  AND  MKPF.BUDAT BETWEEN :start_date AND :end_date
  AND  MSEG.EBELN IS NOT NULL
"""

# Supplier invoices + vendor clearing for payment
RBKP_SQL = """
SELECT RBKP.BELNR  AS invoice_doc,
       RBKP.GJAHR  AS fiscal_year,
       RBKP.BUKRS  AS company_code,
       RBKP.LIFNR  AS supplier,
       RBKP.BUDAT  AS invoice_posted,
       RBKP.CPUDT  AS invoice_entered,
       RBKP.ZLSPR  AS payment_block,     -- 'A'=blocked, etc.
       RSEG.EBELN  AS case_id,           -- PO reference from invoice item
       BSAK.AUGDT  AS cleared_date       -- vendor clearing = payment
FROM   RBKP
JOIN   RSEG ON RSEG.BELNR = RBKP.BELNR AND RSEG.GJAHR = RBKP.GJAHR
LEFT JOIN BSAK ON BSAK.BELNR = RBKP.BELNR AND BSAK.GJAHR = RBKP.GJAHR AND BSAK.BUKRS = RBKP.BUKRS
WHERE  RBKP.CPUDT BETWEEN :start_date AND :end_date
"""


class EccConnector(BaseConnector):
    name = "ecc"

    def __init__(
        self,
        mode: str = "sql",
        sqlalchemy_url: str | None = None,
        rfc_config: dict[str, Any] | None = None,
    ):
        """Parameters:
          mode:            "sql" or "rfc"
          sqlalchemy_url:  e.g. "oracle+oracledb://user:pw@host:1521/?service_name=PRD"
          rfc_config:      dict passed to pyrfc.Connection(**rfc_config)
        """
        if mode not in {"sql", "rfc"}:
            raise ValueError("ECC connector mode must be 'sql' or 'rfc'")
        self.mode = mode
        self._sa_url = sqlalchemy_url
        self._rfc_config = rfc_config or {}

    # -----------------------------------------------------------------------------------

    def extract_o2c(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        sales_orgs: list[str] | None = None,
    ) -> EventLog:
        start = start or datetime(2000, 1, 1, tzinfo=timezone.utc)
        end = end or datetime.now(timezone.utc)

        if self.mode == "sql":
            vbak, vbap, likp, vbrk = self._fetch_sql(start, end)
        else:
            vbak, vbap, likp, vbrk = self._fetch_rfc(start, end)

        if sales_orgs and not vbak.empty:
            vbak = vbak[vbak["sales_org"].isin(sales_orgs)]

        item_rollup = self._rollup_items(vbap)
        events = list(self._rows_to_events(vbak, likp, vbrk, item_rollup))
        return EventLog.from_records(events, process_name="order_to_cash", source="ecc")

    # -----------------------------------------------------------------------------------

    def _fetch_sql(self, start: datetime, end: datetime):
        try:
            from sqlalchemy import create_engine, text
        except ImportError as e:
            raise RuntimeError(
                "SQLAlchemy not installed. Run `pip install sqlalchemy` plus your DB driver."
            ) from e
        if not self._sa_url:
            raise RuntimeError("ECC SQL mode requires sqlalchemy_url")

        eng = create_engine(self._sa_url)
        params = {"start_date": start.strftime("%Y%m%d"), "end_date": end.strftime("%Y%m%d")}
        with eng.connect() as conn:
            vbak = pd.read_sql(text(VBAK_SQL), conn, params=params)
            vbap = pd.read_sql(text(VBAP_SQL), conn, params=params)
            likp = pd.read_sql(text(LIKP_LIPS_SQL), conn, params=params)
            vbrk = pd.read_sql(text(VBRK_SQL), conn, params=params)
        return vbak, vbap, likp, vbrk

    def _fetch_rfc(self, start: datetime, end: datetime):
        try:
            from pyrfc import Connection  # noqa: F401
        except ImportError as e:
            raise RuntimeError(
                "pyrfc not installed. It requires the SAP NW RFC SDK on the host. "
                "See https://github.com/SAP/PyRFC."
            ) from e
        # Implementation via RFC_READ_TABLE is straightforward but verbose; for pilot
        # engagements we usually ship a small custom Z-function module and call it here.
        # The stub should return four empty DataFrames (vbak, vbap, likp, vbrk) to match
        # the SQL mode signature.
        raise NotImplementedError(
            "RFC mode is stubbed. Wire up RFC_READ_TABLE or Z_O2C_EVENTS here per "
            "the customer's approved RFC interface — must return (vbak, vbap, likp, vbrk)."
        )

    # -----------------------------------------------------------------------------------

    def _rollup_items(self, vbap: pd.DataFrame) -> dict[str, dict]:
        """Aggregate VBAP items onto the sales-order (case) level.

        MTO detection uses both PSTYV item category (TAK) and the traditional
        KZVBR='E' (individual customer stock) flag — some landscapes use one, some the
        other, some both.
        """
        if vbap.empty:
            return {}
        v = vbap.copy()
        v["_is_mto"] = (v.get("item_category") == "TAK") | (v.get("consumption_indicator") == "E")
        v["_is_config"] = v.get("item_category") == "TAC"

        rollup: dict[str, dict] = {}
        for case_id, grp in v.groupby("case_id"):
            cats = sorted(grp["item_category"].dropna().unique().tolist())
            primary = grp.iloc[0]
            rollup[str(case_id)] = {
                "n_items": int(len(grp)),
                "primary_material": str(primary.get("material") or "") or None,
                "primary_item_category": str(primary.get("item_category") or "") or None,
                "item_category_mix": "+".join(cats) if cats else None,
                "has_mto_item": bool(grp["_is_mto"].any()),
                "has_configurable_item": bool(grp["_is_config"].any()),
                "material_group": primary.get("material_group"),
            }
        return rollup

    def _rows_to_events(
        self,
        vbak: pd.DataFrame,
        likp: pd.DataFrame,
        vbrk: pd.DataFrame,
        item_rollup: dict[str, dict],
    ):
        # VBAK → OrderCreated (+ OrderChanged if AEDAT > ERDAT)
        for _, r in vbak.iterrows():
            case_id = str(r["case_id"])
            attrs = {
                "sales_org": r.get("sales_org"),
                "distribution_channel": r.get("distribution_channel"),
                "order_type": r.get("order_type"),
                "customer": r.get("customer"),
                "order_value_eur": r.get("order_value"),
                **item_rollup.get(case_id, {}),
            }
            created = self._combine(r.get("ERDAT"), r.get("ERZET"))
            if pd.notna(created):
                yield {"case_id": case_id, "activity": "OrderCreated", "timestamp": created, **attrs}
            changed = self._combine(r.get("AEDAT"), None)
            if pd.notna(changed) and pd.notna(created) and changed > created:
                yield {"case_id": case_id, "activity": "OrderChanged", "timestamp": changed, **attrs}

        # LIKP / LIPS → DeliveryCreated, GoodsIssued (picking requires status-table join, omitted)
        for _, r in likp.iterrows():
            case_id = str(r["case_id"])
            attrs = {
                "plant": r.get("plant"),
                "shipping_point": r.get("shipping_point"),
                **item_rollup.get(case_id, {}),
            }
            if pd.notna(r.get("delivery_created")):
                yield {
                    "case_id": case_id,
                    "activity": "DeliveryCreated",
                    "timestamp": pd.to_datetime(r["delivery_created"], utc=True),
                    **attrs,
                }
            if pd.notna(r.get("goods_issued")):
                yield {
                    "case_id": case_id,
                    "activity": "GoodsIssued",
                    "timestamp": pd.to_datetime(r["goods_issued"], utc=True),
                    **attrs,
                }

        # VBRK / BSEG → InvoiceCreated, PaymentReceived
        for _, r in vbrk.iterrows():
            case_id = str(r.get("case_id_delivery"))
            attrs = item_rollup.get(case_id, {})
            if pd.notna(r.get("invoice_created")):
                yield {
                    "case_id": case_id,
                    "activity": "InvoiceCreated",
                    "timestamp": pd.to_datetime(r["invoice_created"], utc=True),
                    **attrs,
                }
            if pd.notna(r.get("cleared_date")):
                yield {
                    "case_id": case_id,
                    "activity": "PaymentReceived",
                    "timestamp": pd.to_datetime(r["cleared_date"], utc=True),
                    **attrs,
                }

    # ==================================================================================
    # P2P
    # ==================================================================================

    def extract_p2p(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        purchasing_orgs: list[str] | None = None,
        company_codes: list[str] | None = None,
    ) -> EventLog:
        start = start or datetime(2000, 1, 1, tzinfo=timezone.utc)
        end = end or datetime.now(timezone.utc)

        if self.mode == "sql":
            ekko, ekpo, gr, rbkp = self._fetch_p2p_sql(start, end)
        else:
            raise NotImplementedError("RFC mode for P2P is not yet wired.")

        if purchasing_orgs and not ekko.empty:
            ekko = ekko[ekko["purchasing_org"].isin(purchasing_orgs)]
        if company_codes and not ekko.empty:
            ekko = ekko[ekko["company_code"].isin(company_codes)]

        item_rollup = self._rollup_ekpo(ekpo)
        events = list(self._p2p_rows_to_events(ekko, gr, rbkp, item_rollup))
        return EventLog.from_records(events, process_name="procure_to_pay", source="ecc")

    def _fetch_p2p_sql(self, start: datetime, end: datetime):
        try:
            from sqlalchemy import create_engine, text
        except ImportError as e:
            raise RuntimeError("SQLAlchemy required for ECC SQL mode") from e
        if not self._sa_url:
            raise RuntimeError("ECC SQL mode requires sqlalchemy_url")
        eng = create_engine(self._sa_url)
        params = {"start_date": start.strftime("%Y%m%d"), "end_date": end.strftime("%Y%m%d")}
        with eng.connect() as conn:
            ekko = pd.read_sql(text(EKKO_SQL), conn, params=params)
            ekpo = pd.read_sql(text(EKPO_SQL), conn, params=params)
            gr = pd.read_sql(text(GR_SQL), conn, params=params)
            rbkp = pd.read_sql(text(RBKP_SQL), conn, params=params)
        return ekko, ekpo, gr, rbkp

    def _rollup_ekpo(self, ekpo: pd.DataFrame) -> dict[str, dict]:
        if ekpo.empty:
            return {}
        v = ekpo.copy()
        # Service detection: PSTYP 'D' or the DIENS flag set.
        v["_is_service"] = (
            (v.get("item_category") == "D")
            | v.get("is_service_flag", pd.Series(dtype=object)).fillna("").astype(str).str.upper().isin({"X", "TRUE"})
        )
        v["_is_consumable"] = v.get("item_category") == "K"
        v["_has_acct"] = v.get("account_assignment").notna() if "account_assignment" in v.columns else False

        rollup: dict[str, dict] = {}
        for case_id, grp in v.groupby("case_id"):
            cats = sorted(grp["item_category"].dropna().unique().tolist())
            primary = grp.iloc[0]
            rollup[str(case_id)] = {
                "n_items": int(len(grp)),
                "primary_material": str(primary.get("material") or "") or None,
                "primary_item_category": str(primary.get("item_category") or "") or None,
                "item_category_mix": "+".join(cats) if cats else None,
                "has_service_item": bool(grp["_is_service"].any()),
                "has_consumable_item": bool(grp["_is_consumable"].any()),
                "has_account_assignment": bool(grp["_has_acct"].any() if isinstance(grp["_has_acct"], pd.Series) else grp["_has_acct"]),
                "primary_account_assignment": str(primary.get("account_assignment") or "") or None,
                "material_group": primary.get("material_group"),
                "plant": primary.get("plant"),
            }
        return rollup

    def _p2p_rows_to_events(
        self,
        ekko: pd.DataFrame,
        gr: pd.DataFrame,
        rbkp: pd.DataFrame,
        item_rollup: dict[str, dict],
    ):
        for _, r in ekko.iterrows():
            case_id = str(r["case_id"])
            attrs = {
                "purchasing_org": r.get("purchasing_org"),
                "purchasing_group": r.get("purchasing_group"),
                "company_code": r.get("company_code"),
                "supplier": r.get("supplier"),
                "po_type": r.get("po_type"),
                "payment_terms": r.get("payment_terms"),
                "requester": r.get("requester"),
                "order_value_eur": r.get("order_value"),
                **item_rollup.get(case_id, {}),
            }
            created = self._combine(r.get("po_created"), None)
            if pd.notna(created):
                yield {"case_id": case_id, "activity": "PurchaseOrderCreated", "timestamp": created, **attrs}
            released = self._combine(r.get("po_released"), None)
            if pd.notna(released):
                yield {"case_id": case_id, "activity": "PurchaseOrderReleased", "timestamp": released, **attrs}
            changed = self._combine(r.get("po_changed"), None)
            if pd.notna(changed) and pd.notna(created) and changed > created:
                yield {"case_id": case_id, "activity": "PurchaseOrderChanged", "timestamp": changed, **attrs}

        for _, r in gr.iterrows():
            case_id = str(r["case_id"])
            posted = self._combine(r.get("posting_date"), None)
            if pd.notna(posted):
                yield {"case_id": case_id, "activity": "GoodsReceived",
                       "timestamp": posted, **item_rollup.get(case_id, {})}

        for _, r in rbkp.iterrows():
            case_id = str(r.get("case_id") or "")
            if not case_id:
                continue
            attrs = item_rollup.get(case_id, {})
            entered = self._combine(r.get("invoice_entered"), None)
            if pd.notna(entered):
                yield {"case_id": case_id, "activity": "InvoiceReceived",
                       "timestamp": entered, **attrs}
            if pd.notna(r.get("payment_block")) and str(r.get("payment_block")).strip() and pd.notna(entered):
                yield {"case_id": case_id, "activity": "InvoiceBlocked",
                       "timestamp": entered, **attrs}
            cleared = self._combine(r.get("cleared_date"), None)
            if pd.notna(cleared):
                yield {"case_id": case_id, "activity": "InvoiceMatched",
                       "timestamp": cleared, **attrs}
                yield {"case_id": case_id, "activity": "PaymentMade",
                       "timestamp": cleared, **attrs}

    # ==================================================================================

    @staticmethod
    def _combine(date_val, time_val) -> pd.Timestamp:
        if pd.isna(date_val):
            return pd.NaT
        dt = pd.to_datetime(str(date_val), format="%Y%m%d", errors="coerce")
        if pd.isna(dt):
            return pd.NaT
        if time_val is not None and not pd.isna(time_val):
            try:
                secs = int(str(time_val).zfill(6))
                dt = dt + pd.Timedelta(hours=secs // 10000, minutes=(secs // 100) % 100, seconds=secs % 100)
            except (ValueError, TypeError):
                pass
        return dt.tz_localize("UTC")
