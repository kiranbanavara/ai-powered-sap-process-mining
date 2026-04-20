"""Synthetic O2C + P2P event log generator.

Produces a realistic SAP-shaped event log with several intentional bottleneck patterns
so the analytics + LLM pipeline has something meaningful to find.

O2C seeded patterns:
  - Plant 1000 has a single-approver credit-check bottleneck (adds 2–4d delay)
  - Region DE-SOUTH vendors cause goods-issue delay (3–5d extra)
  - MTO cases (~15%, item category TAK) add 4–8 production days before DeliveryCreated
  - Configurable cases (~10%, item category TAC) add 2–4 engineering days at approval
  - ~8% of cases have 'OrderChanged' rework between approval and delivery
  - ~5% of cases hit a credit block loop
  - Large orders (>€50k) are delayed at credit check

P2P seeded patterns:
  - Supplier V-9000 has a 5–8 day GR lead-time overhang (new / problem supplier)
  - Purchasing group A1 is slow at PO release (+3–5d)
  - High-value POs (>€100k) add a further 2–4d at release (hierarchy review)
  - Service items (category D) stall at invoice matching (+4–7d, manual 3-way match)
  - Account-assigned items (cost center / project) add invoice-match friction (+1–3d)
  - ~10% of invoices hit an InvoiceBlocked step (price/quantity block)

The numbers are tunable — this is for POCs and demos, not for production.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Iterator

import pandas as pd

from ..event_log import EventLog
from .base import BaseConnector


# --- realistic master-data pools --------------------------------------------------------
PLANTS = ["1000", "1010", "1100", "1200", "2000"]
SALES_ORGS = ["DE01", "DE02", "FR01", "NL01"]
DISTRIBUTION_CHANNELS = ["10", "20", "30"]
MATERIAL_GROUPS = ["MECH-A", "MECH-B", "ELEC-A", "ELEC-B", "CONS-A", "PACK-C"]
COUNTRIES = ["DE", "FR", "NL", "BE", "AT", "IT", "PL"]
REGIONS_BY_COUNTRY = {
    "DE": ["DE-NORTH", "DE-SOUTH", "DE-WEST", "DE-EAST"],
    "FR": ["FR-IDF", "FR-PACA"],
    "NL": ["NL-RAND"],
    "BE": ["BE-FLA"],
    "AT": ["AT-VIE"],
    "IT": ["IT-LOM"],
    "PL": ["PL-MAZ"],
}
ORDER_TYPES = ["ZOR", "OR", "RE"]
USERS = ["M.Mueller", "A.Schmidt", "L.Dubois", "T.deVries", "R.Khanna", "S.Bianchi"]

# --- P2P master data pools ---------------------------------------------------------
PURCHASING_ORGS = ["1000", "2000", "3000"]
PURCHASING_GROUPS = ["A1", "A2", "B1", "B2", "C1"]   # A1 is the slow one
COMPANY_CODES = ["1000", "1100", "2000"]
SUPPLIERS = [
    "V-1001", "V-1002", "V-1003", "V-2001", "V-2002",
    "V-3001", "V-3002", "V-9000",   # V-9000: problem supplier
]
PO_TYPES = ["NB", "FO", "UB", "ZNB"]   # NB standard, FO framework, UB stock transfer
PAYMENT_TERMS = ["Z000", "Z014", "Z030", "Z045", "Z060"]
REQUESTERS = ["J.Weber", "P.Lange", "A.Novak", "S.Chen", "R.Iyer", "K.Diop"]

# Account-assignment categories (KNTTP) and item categories (PSTYP) from EKPO.
P2P_ITEM_CATEGORIES = [
    ("NORM", False, False),   # standard stock
    ("D", True, False),       # service — breaks 3-way match
    ("K", False, True),       # consignment
]
ACCOUNT_ASSIGNMENTS = [None, "K", "A", "P", "Q"]  # K=cost ctr, A=asset, P=project, Q=WBS

# Item-category archetypes (weight → category). An order picks an archetype; its items are
# drawn consistent with that archetype so analytics see clean MTO/config signals.
ARCHETYPES: list[tuple[str, float]] = [
    ("MTS", 0.70),   # all TAN
    ("MTO", 0.15),   # at least one TAK
    ("CONFIG", 0.10),  # at least one TAC
    ("MIXED", 0.05),  # mix of TAN + TAK/TAC
]

# Materials per group, with explicit MTO/config flags on a few. In a real system this
# comes from the material master (MARA) + variant configuration (CUKB/VC tables).
MATERIALS: dict[str, list[tuple[str, bool, bool]]] = {
    # material_group: [(material_number, is_mto_default, is_configurable_default), ...]
    "MECH-A": [("M-1001", False, False), ("M-1002", False, False), ("M-1100-VC", False, True)],
    "MECH-B": [("M-2001", False, False), ("M-2050-MTO", True, False)],
    "ELEC-A": [("E-1001", False, False), ("E-1020-VC", False, True)],
    "ELEC-B": [("E-2001", False, False), ("E-2150-MTO", True, False)],
    "CONS-A": [("C-1001", False, False), ("C-1002", False, False)],
    "PACK-C": [("P-1001", False, False)],
}


class SyntheticConnector(BaseConnector):
    name = "synthetic"

    def __init__(self, seed: int = 42, n_cases: int = 800, days_back: int = 60):
        self.seed = seed
        self.n_cases = n_cases
        self.days_back = days_back

    def extract_o2c(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        sales_orgs: list[str] | None = None,
    ) -> EventLog:
        rng = random.Random(self.seed)
        end = end or datetime.now(timezone.utc)
        start = start or (end - timedelta(days=self.days_back))

        events: list[dict] = list(self._generate_cases(rng, start, end, sales_orgs))

        log = EventLog.from_records(
            events,
            process_name="order_to_cash",
            source="synthetic",
        )
        if start or end:
            log = log.filter_window(start, end)
        return log

    # --- generation internals ------------------------------------------------------------

    def _generate_cases(
        self,
        rng: random.Random,
        start: datetime,
        end: datetime,
        sales_orgs: list[str] | None,
    ) -> Iterator[dict]:
        total_seconds = (end - start).total_seconds()
        orgs = sales_orgs or SALES_ORGS

        for i in range(self.n_cases):
            case_id = f"SO-{1000000 + i}"

            # Scatter order creation across the window, with mild weekend suppression
            created = start + timedelta(seconds=rng.uniform(0, max(total_seconds, 1.0)))
            if created.weekday() >= 5 and rng.random() < 0.7:
                created += timedelta(days=2)
                if created > end:
                    created = end - timedelta(hours=1)

            plant = rng.choice(PLANTS)
            country = rng.choice(COUNTRIES)
            region = rng.choice(REGIONS_BY_COUNTRY[country])
            material_group = rng.choice(MATERIAL_GROUPS)
            items = self._build_items(rng, material_group)
            item_rollup = self._rollup_items(items)

            attrs = {
                "plant": plant,
                "sales_org": rng.choice(orgs),
                "distribution_channel": rng.choice(DISTRIBUTION_CHANNELS),
                "region": region,
                "sold_to_country": country,
                "customer": f"C-{rng.randint(10000, 99999)}",
                "material_group": material_group,
                "order_type": rng.choices(ORDER_TYPES, weights=[0.78, 0.18, 0.04])[0],
                "responsible_user": rng.choice(USERS),
                "order_value_eur": round(rng.lognormvariate(9.5, 0.8), 2),  # ~€13k median
                **item_rollup,
            }
            yield from self._walk_case(rng, case_id, created, attrs)

    # --- item generation & case-level rollup --------------------------------------------

    def _build_items(self, rng: random.Random, material_group: str) -> list[dict]:
        """One case has 1–4 items. The case archetype determines the category mix."""
        archetype = rng.choices([a for a, _ in ARCHETYPES], weights=[w for _, w in ARCHETYPES])[0]
        n_items = rng.choices([1, 2, 3, 4], weights=[0.55, 0.30, 0.10, 0.05])[0]
        pool = MATERIALS[material_group]
        items: list[dict] = []

        for _ in range(n_items):
            material, is_mto_default, is_config_default = rng.choice(pool)
            if archetype == "MTS":
                category = "TAN"
                is_mto, is_config = False, False
            elif archetype == "MTO":
                category = "TAK" if rng.random() < 0.8 else "TAN"
                is_mto = category == "TAK" or is_mto_default
                is_config = False
            elif archetype == "CONFIG":
                category = "TAC" if rng.random() < 0.8 else "TAN"
                is_mto = False
                is_config = category == "TAC" or is_config_default
            else:  # MIXED
                category = rng.choice(["TAN", "TAK", "TAC"])
                is_mto = category == "TAK"
                is_config = category == "TAC"
            items.append({
                "material": material,
                "item_category": category,
                "is_mto": is_mto,
                "is_configurable": is_config,
            })
        return items

    @staticmethod
    def _rollup_items(items: list[dict]) -> dict:
        cats = [i["item_category"] for i in items]
        return {
            "n_items": len(items),
            "primary_material": items[0]["material"],
            "primary_item_category": items[0]["item_category"],
            "item_category_mix": "+".join(sorted(set(cats))),
            "has_mto_item": any(i["is_mto"] for i in items),
            "has_configurable_item": any(i["is_configurable"] for i in items),
        }

    def _walk_case(
        self,
        rng: random.Random,
        case_id: str,
        t0: datetime,
        attrs: dict,
    ) -> Iterator[dict]:
        t = t0
        yield self._ev(case_id, "OrderCreated", t, attrs)

        # Credit check — bottleneck on Plant 1000, extra delay on large orders
        credit_delay = rng.uniform(2, 8)  # hours
        if attrs["plant"] == "1000":
            credit_delay += rng.uniform(48, 96)  # +2–4 days
        if attrs["order_value_eur"] > 50000:
            credit_delay += rng.uniform(24, 72)  # +1–3 days

        t = t + timedelta(hours=credit_delay)
        yield self._ev(case_id, "CreditChecked", t, attrs)

        # ~5% hit a credit block and loop
        credit_blocked = rng.random() < 0.05
        if credit_blocked:
            t = t + timedelta(hours=rng.uniform(12, 72))
            yield self._ev(case_id, "CreditChecked", t, attrs, note="re-check after block")

        # Approval — configurable items add engineering review time
        approval_hours = rng.uniform(1, 6)
        if attrs.get("has_configurable_item"):
            approval_hours += rng.uniform(48, 96)  # +2–4d variant-config engineering
        t = t + timedelta(hours=approval_hours)
        yield self._ev(case_id, "OrderApproved", t, attrs)

        # ~8% rework (order change after approval)
        if rng.random() < 0.08:
            t = t + timedelta(hours=rng.uniform(4, 36))
            yield self._ev(case_id, "OrderChanged", t, attrs)

        # Delivery creation — MTO items wait for production
        delivery_hours = rng.uniform(4, 24)
        if attrs.get("has_mto_item"):
            delivery_hours += rng.uniform(96, 192)  # +4–8d production lead time
        t = t + timedelta(hours=delivery_hours)
        yield self._ev(case_id, "DeliveryCreated", t, attrs)

        # Picking
        t = t + timedelta(hours=rng.uniform(2, 18))
        yield self._ev(case_id, "PickingCompleted", t, attrs)

        # Goods issue — bottleneck in DE-SOUTH
        gi_hours = rng.uniform(4, 24)
        if attrs["region"] == "DE-SOUTH":
            gi_hours += rng.uniform(72, 120)  # +3–5 days
        t = t + timedelta(hours=gi_hours)
        yield self._ev(case_id, "GoodsIssued", t, attrs)

        # Invoice creation
        t = t + timedelta(hours=rng.uniform(2, 48))
        yield self._ev(case_id, "InvoiceCreated", t, attrs)

        # Invoice posted to FI
        t = t + timedelta(hours=rng.uniform(0.5, 6))
        yield self._ev(case_id, "InvoicePosted", t, attrs)

        # Payment (net terms) — not every case clears in the window
        payment_days = rng.choices([14, 30, 45, 60], weights=[0.15, 0.55, 0.2, 0.1])[0]
        jitter_days = rng.uniform(-2, 5)
        t = t + timedelta(days=payment_days + jitter_days)
        yield self._ev(case_id, "PaymentReceived", t, attrs)

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
        rng = random.Random(self.seed + 101)  # separate stream from o2c
        end = end or datetime.now(timezone.utc)
        start = start or (end - timedelta(days=self.days_back))

        events: list[dict] = list(
            self._generate_p2p_cases(rng, start, end, purchasing_orgs, company_codes)
        )
        log = EventLog.from_records(
            events,
            process_name="procure_to_pay",
            source="synthetic",
        )
        return log.filter_window(start, end)

    def _generate_p2p_cases(
        self,
        rng: random.Random,
        start: datetime,
        end: datetime,
        purchasing_orgs: list[str] | None,
        company_codes: list[str] | None,
    ):
        total_seconds = (end - start).total_seconds()
        p_orgs = purchasing_orgs or PURCHASING_ORGS
        c_codes = company_codes or COMPANY_CODES

        for i in range(self.n_cases):
            case_id = f"PO-{3000000 + i}"
            created = start + timedelta(seconds=rng.uniform(0, max(total_seconds, 1.0)))
            if created.weekday() >= 5 and rng.random() < 0.7:
                created += timedelta(days=2)
                if created > end:
                    created = end - timedelta(hours=1)

            items = self._build_p2p_items(rng)
            item_rollup = self._rollup_p2p_items(items)
            attrs = {
                "purchasing_org": rng.choice(p_orgs),
                "purchasing_group": rng.choice(PURCHASING_GROUPS),
                "plant": rng.choice(PLANTS),
                "company_code": rng.choice(c_codes),
                "supplier": rng.choice(SUPPLIERS),
                "material_group": rng.choice(MATERIAL_GROUPS),
                "po_type": rng.choices(PO_TYPES, weights=[0.70, 0.18, 0.08, 0.04])[0],
                "requester": rng.choice(REQUESTERS),
                "payment_terms": rng.choice(PAYMENT_TERMS),
                "order_value_eur": round(rng.lognormvariate(10.5, 1.0), 2),  # ~€36k median
                **item_rollup,
            }
            yield from self._walk_p2p_case(rng, case_id, created, attrs)

    # --- P2P item generation & rollup --------------------------------------------------

    def _build_p2p_items(self, rng: random.Random) -> list[dict]:
        n_items = rng.choices([1, 2, 3, 4], weights=[0.55, 0.25, 0.12, 0.08])[0]
        items: list[dict] = []
        for _ in range(n_items):
            cat, is_service, is_consumable = rng.choice(P2P_ITEM_CATEGORIES)
            acct = rng.choices(ACCOUNT_ASSIGNMENTS, weights=[0.65, 0.15, 0.08, 0.07, 0.05])[0]
            items.append({
                "material": f"M-{rng.randint(4000, 4999)}",
                "item_category": cat,
                "is_service": is_service,
                "is_consumable": is_consumable,
                "account_assignment": acct,
            })
        return items

    @staticmethod
    def _rollup_p2p_items(items: list[dict]) -> dict:
        cats = [i["item_category"] for i in items]
        accts = [i["account_assignment"] for i in items if i["account_assignment"]]
        return {
            "n_items": len(items),
            "primary_material": items[0]["material"],
            "primary_item_category": items[0]["item_category"],
            "item_category_mix": "+".join(sorted(set(cats))),
            "has_service_item": any(i["is_service"] for i in items),
            "has_consumable_item": any(i["is_consumable"] for i in items),
            "has_account_assignment": bool(accts),
            "primary_account_assignment": items[0]["account_assignment"],
        }

    # --- P2P walker --------------------------------------------------------------------

    def _walk_p2p_case(
        self,
        rng: random.Random,
        case_id: str,
        t0: datetime,
        attrs: dict,
    ):
        t = t0
        yield self._ev(case_id, "PurchaseRequisitionCreated", t, attrs)

        # PR approval
        t = t + timedelta(hours=rng.uniform(2, 24))
        yield self._ev(case_id, "PurchaseRequisitionApproved", t, attrs)

        # PO created
        t = t + timedelta(hours=rng.uniform(2, 12))
        yield self._ev(case_id, "PurchaseOrderCreated", t, attrs)

        # PO release — purchasing group A1 is slow; high-value compounds
        release_hours = rng.uniform(4, 18)
        if attrs["purchasing_group"] == "A1":
            release_hours += rng.uniform(72, 120)   # +3–5d
        if attrs["order_value_eur"] > 100_000:
            release_hours += rng.uniform(48, 96)    # +2–4d hierarchy
        t = t + timedelta(hours=release_hours)
        yield self._ev(case_id, "PurchaseOrderReleased", t, attrs)

        # ~8% rework
        if rng.random() < 0.08:
            t = t + timedelta(hours=rng.uniform(4, 48))
            yield self._ev(case_id, "PurchaseOrderChanged", t, attrs)

        # Goods received — normal lead time + bad-supplier overhang
        gr_hours = rng.uniform(48, 240)   # 2–10d typical
        if attrs["supplier"] == "V-9000":
            gr_hours += rng.uniform(120, 192)  # +5–8d
        t = t + timedelta(hours=gr_hours)
        yield self._ev(case_id, "GoodsReceived", t, attrs)

        # Invoice received — 1–3d after GR
        t = t + timedelta(hours=rng.uniform(12, 72))
        yield self._ev(case_id, "InvoiceReceived", t, attrs)

        # ~10% blocked invoices
        if rng.random() < 0.10:
            t = t + timedelta(hours=rng.uniform(24, 96))
            yield self._ev(case_id, "InvoiceBlocked", t, attrs)

        # Invoice match — services stall, account-assigned also slower
        match_hours = rng.uniform(4, 48)
        if attrs.get("has_service_item"):
            match_hours += rng.uniform(96, 168)   # +4–7d manual review
        if attrs.get("has_account_assignment"):
            match_hours += rng.uniform(24, 72)    # +1–3d
        t = t + timedelta(hours=match_hours)
        yield self._ev(case_id, "InvoiceMatched", t, attrs)

        # Payment — payment terms + cash-position jitter
        term_days = {"Z000": 0, "Z014": 14, "Z030": 30, "Z045": 45, "Z060": 60}.get(
            attrs["payment_terms"], 30
        )
        t = t + timedelta(days=term_days + rng.uniform(-1, 4))
        yield self._ev(case_id, "PaymentMade", t, attrs)

    # ==================================================================================

    @staticmethod
    def _ev(case_id: str, activity: str, t: datetime, attrs: dict, note: str | None = None) -> dict:
        ev = {
            "case_id": case_id,
            "activity": activity,
            "timestamp": pd.Timestamp(t).tz_convert("UTC") if pd.Timestamp(t).tzinfo else pd.Timestamp(t, tz="UTC"),
            **attrs,
        }
        if note:
            ev["note"] = note
        return ev
