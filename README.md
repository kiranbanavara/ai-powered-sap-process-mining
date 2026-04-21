# AI-Powered SAP Process Mining

Continuous operational intelligence — from SAP event logs to plain-language insight.

This is the pilot-ready foundation behind the 4-week pilot described in the business
case. It extracts SAP workflow events for **Order-to-Cash** and **Procure-to-Pay**
(S/4HANA *or* ECC), runs process-mining analytics, and drives two LLM agents:

- **Flagger** — one-shot briefing: "here are this week's top bottlenecks, phrased for a COO"
- **Investigator** — tool-using RCA: pick any flagged finding, optionally add a human
  comment, and the agent inspects the event log to propose a root cause with evidence

```
   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
   │   S/4HANA    │    │    Event     │    │   Mining     │    │   Flagger    │
   │      or      │──▶ │     Log      │──▶ │  Analytics   │──▶ │  (one-shot   │
   │     ECC      │    │ (canonical)  │    │              │    │   briefing)  │
   └──────────────┘    └──────────────┘    └──────────────┘    └──────┬───────┘
                                                                      │
                                    ┌────────── persistence ◀──────────┤
                                    │         reports/latest/          ▼
                                    ▼        {o2c,p2p}/             briefing.md
                              ┌──────────────┐
                              │ Investigator │  ◀── human comment
                              │  (agentic    │
                              │   tool loop) │
                              └──────┬───────┘
                                     ▼
                                 RCA.md
```

Both agents are accessible via the CLI *or* the Streamlit UI (multi-process landing +
per-process detail view with live tool-call streaming).

## What's in the box

| Layer | Module | Pluggable by customer? |
|---|---|---|
| Data source | `connectors/` | ✔ `s4hana` (OData / CDS views), `ecc` (SQL or RFC), `synthetic` |
| Process definitions | `processes/` | ✔ **O2C and P2P** ship in-box. Add more by dropping a `ProcessDefinition` alongside them. |
| Mining analytics | `mining/analytics.py` | Cycle times · dimensional bottlenecks · variants · rework · anomalies — process-aware |
| LLM (Flagger) | `llm/` + `analysis/` | ✔ `anthropic` · `openai` · `gemini` — customer picks their trusted model |
| LLM (Investigator) | `investigator/` | Tool-using agent with 6 RCA tools — ✔ `anthropic` · ✔ `openai` · ✔ `gemini` |
| UI | `ui/streamlit_app.py` | Multi-process landing + per-process detail + streamed RCA |
| Report | `reporting/markdown.py` | Markdown output, cron-friendly |

### Dimensions used for bottleneck breakdown

Analytics slice each transition by the dimensions below and surface slices with
disproportionate delay. Header-level dimensions come from the document header;
item-level rollups are computed from item data (A_SalesOrderItem / VBAP for O2C,
A_PurchaseOrderItem / EKPO for P2P) and aggregated to the case so one custom-configured
or service line on a multi-line order still flags the case.

**Order-to-Cash:**

| Source | Dimension | SAP field |
|---|---|---|
| Header | plant, sales_org, distribution_channel, region, customer, order_type, responsible_user | A_SalesOrder / VBAK |
| Header | material_group | MATKL (derived on case) |
| Item rollup | `has_mto_item` | SalesDocumentItemCategory = TAK, or RequirementType LIKE 'KE%' (ECC: KZVBR='E') |
| Item rollup | `has_configurable_item` | SalesDocumentItemCategory = TAC (variant configuration) |
| Item rollup | `primary_item_category` | first-line category (TAN / TAK / TAC / TAD / TAS) |
| Item rollup | `item_category_mix` | sorted-distinct categories (e.g. `TAK+TAN`) |
| Item rollup | `primary_material` | first-line MATNR |

**Procure-to-Pay:**

| Source | Dimension | SAP field |
|---|---|---|
| Header | purchasing_org, purchasing_group, plant, company_code, supplier, po_type, requester, payment_terms | A_PurchaseOrder / EKKO |
| Header | material_group | MATKL (derived on case) |
| Item rollup | `has_service_item` | PurchaseOrderItemCategory = D (DIEN) or IsServiceItem flag |
| Item rollup | `has_consumable_item` | PurchaseOrderItemCategory = K (consignment) |
| Item rollup | `has_account_assignment` | AccountAssignmentCategory non-null |
| Item rollup | `primary_account_assignment` | first-line KNTTP (K cost ctr / A asset / P project / Q WBS / F order) |
| Item rollup | `primary_item_category` | first-line category (NORM / D / K / L) |
| Item rollup | `item_category_mix` | sorted-distinct categories (e.g. `D+NORM`) |
| Item rollup | `primary_material` | first-line MATNR |

No lock-in on the LLM. Switch providers with a one-line config change — Flagger *and*
Investigator both run on all three today:

| Capability | Anthropic | OpenAI | Gemini |
|---|:-:|:-:|:-:|
| Flagger briefing | ✅ | ✅ | ✅ |
| Investigator (tool-use) | ✅ | ✅ | ✅ |
| Streaming tool callbacks | ✅ | ✅ | ✅ |

Default models: `claude-sonnet-4-6`, `gpt-4o`, `gemini-2.5-pro`. Override any of them in
the YAML's `llm.model` field.

## Quick start (demo with synthetic data)

```bash
python -m venv .venv && source .venv/bin/activate

# Pick the provider extras you want; combine freely with [ui]
pip install -e '.[anthropic,ui]'              # Anthropic (default demo path)
#   or: pip install -e '.[openai,ui]'
#   or: pip install -e '.[gemini,ui]'
#   or: pip install -e '.[all]'               # everything

cp .env.example .env                          # fill in ANTHROPIC_API_KEY / OPENAI_API_KEY
                                              # / GEMINI_API_KEY — auto-loaded, no export

# Run both pipelines so the UI landing has two tiles to pick from
sap-mining run --config config/config.synthetic.yaml        # O2C
sap-mining run --config config/config.synthetic-p2p.yaml    # P2P

# Then browse the briefings + kick off Investigator sessions
sap-mining ui  --config config/config.synthetic.yaml
```

Swap the LLM by pointing at a different config instead:
```bash
sap-mining run --config config/config.synthetic-openai.yaml    # OpenAI (gpt-4o)
sap-mining run --config config/config.synthetic-gemini.yaml    # Gemini (gemini-2.5-pro)
```

The synthetic generator ships with realistic seeded patterns so both processes have
bottlenecks worth finding:

**Order-to-Cash:**
| Pattern | Where it shows | Typical lift |
|---|---|---|
| Plant 1000 single-approver credit-check | `OrderCreated → CreditChecked` @ `plant=1000` | ~12× |
| DE-SOUTH region goods-issue drag | `PickingCompleted → GoodsIssued` @ `region=DE-SOUTH` | ~8× |
| MTO items wait for production (TAK) | `OrderApproved → DeliveryCreated` @ `has_mto_item=True` | ~10× |
| Variant-configurable items (TAC) | `CreditChecked → OrderApproved` @ `has_configurable_item=True` | ~19× |

**Procure-to-Pay:**
| Pattern | Where it shows | Typical lift |
|---|---|---|
| Purchasing group A1 slow PO release | `PurchaseOrderCreated → PurchaseOrderReleased` @ `purchasing_group=A1` | ~8× |
| Supplier V-9000 goods-receipt overhang | `PurchaseOrderReleased → GoodsReceived` @ `supplier=V-9000` | ~1.7× |
| Service items stall 3-way match | `InvoiceReceived → InvoiceMatched` @ `has_service_item=True` | ~2× |
| High-value POs delayed at hierarchy | `PurchaseOrderCreated → PurchaseOrderReleased` (compounds on A1) | varies |

### Dry run

Mine the event log and print the raw Findings JSON — skipping the LLM call — to see
what the model will be working with:

```bash
sap-mining run --config config/config.synthetic.yaml --dry-run
```

### Connectivity check

Extracts the event log and prints a summary. Useful during go-live to verify SAP auth,
CDS-view permissions, and window coverage before spending tokens.

```bash
sap-mining check --config config/config.s4hana.yaml
```

### Investigating a flagged finding

Each `run` persists the event log + findings to `reports/latest/<process>/`. The
Investigator loads them, picks one finding (referenced by ID like `B1`, `B2`, `A1`),
and drives a tool-use agent loop against the configured LLM (Anthropic / OpenAI /
Gemini all work today) to find the root cause.

```bash
# List findings from the last run of the process in the config
sap-mining investigate --config config/config.synthetic.yaml

# Investigate a specific one
sap-mining investigate --config config/config.synthetic.yaml --finding B1

# Override process (useful when you have both O2C and P2P persisted)
sap-mining investigate --config config/config.synthetic.yaml --process p2p --finding B2

# Steer the RCA with human context
sap-mining investigate --config config/config.synthetic.yaml --finding B1 \
  --comment "New credit approver onboarded at Plant 1000 since March"
```

The Investigator has 6 tools on the event log:

| Tool | Purpose |
|---|---|
| `describe_finding` | Look up full detail of a flagged finding by ID |
| `list_cases` | Rank cases in a slice by duration on a transition |
| `get_case_timeline` | Full event sequence + attributes for one case |
| `compare_slice_attributes` | Distribution of other dimensions within the slice vs overall (finds interaction effects) |
| `cross_reference` | Median duration crossed by two dimensions (e.g. `plant × has_mto_item`) |
| `temporal_trend` | Week-by-week median, optionally filtered, to spot regressions |

Output lands as `reports/rca-<finding>-<timestamp>.md` with a full audit trail of every
tool call and its arguments.

**Provider note:** the Investigator runs on all three providers —
**Anthropic Claude**, **OpenAI** (chat-completions tool-use), and **Google Gemini**
(function-calling). Drop the matching API key into `.env` and pick one of:

- `config/config.synthetic.yaml`         — Anthropic (default)
- `config/config.synthetic-openai.yaml`  — OpenAI (`gpt-4o` default)
- `config/config.synthetic-gemini.yaml`  — Gemini (`gemini-2.5-pro` default)

### Streamlit UI

```bash
sap-mining ui --config config/config.synthetic.yaml
```

Opens a local app at `http://localhost:8501` with:

- **Landing** — one card per persisted process (O2C, P2P) with headline metrics and
  top-lift indicator. Click to open.
- **Detail view** — Flagger briefing (expandable), findings cards, an analyst comment
  box, and the Investigator with live tool-call streaming (each call appears as it
  fires). Final RCA renders inline with a collapsible audit trail and a download button.
- **Sidebar radio** — switch between processes without leaving the app.

## Going to a real SAP system

### S/4HANA (OData on standard CDS views)

The S/4HANA connector uses standard-delivered OData APIs. All services below are
published on the SAP API Business Hub for both Cloud and on-prem S/4HANA.

**O2C:**

| Service | CDS view | Role |
|---|---|---|
| `API_SALES_ORDER_SRV` | `A_SalesOrder` | Header events: `OrderCreated`, `OrderChanged` |
| `API_SALES_ORDER_SRV` | `A_SalesOrderItem` | Item attributes (Material, SalesDocumentItemCategory, RequirementType) — rolled up for MTO / variant-config flags |
| `API_OUTBOUND_DELIVERY_SRV` | `A_OutboundDeliveryHeader` | `DeliveryCreated`, `PickingCompleted`, `GoodsIssued` |
| `API_BILLING_DOCUMENT_SRV` | `A_BillingDocument` | `InvoiceCreated`, `InvoicePosted`, `PaymentReceived` |

**P2P:**

| Service | CDS view | Role |
|---|---|---|
| `API_PURCHASEORDER_PROCESS_SRV` | `A_PurchaseOrder` | Header events: `PurchaseOrderCreated`, `PurchaseOrderReleased`, `PurchaseOrderChanged` |
| `API_PURCHASEORDER_PROCESS_SRV` | `A_PurchaseOrderItem` | Item attributes (Material, PurchaseOrderItemCategory, AccountAssignmentCategory, IsServiceItem) — rolled up to service / account-assignment flags |
| `API_MATERIAL_DOCUMENT_SRV` | `A_MaterialDocumentItem` | `GoodsReceived` (movement type 101 against the PO) |
| `API_SUPPLIERINVOICE_PROCESS_SRV` | `A_SupplierInvoice` | `InvoiceReceived`, `InvoiceBlocked`, `InvoiceMatched`, `PaymentMade` |

Populate `config/config.s4hana.yaml` and the `.env`, then:

```bash
sap-mining check --config config/config.s4hana.yaml     # verify connectivity
sap-mining run   --config config/config.s4hana.yaml
```

If the landscape uses custom Z-CDS views for the event log, subclass `S4HanaConnector`
and override the relevant `_fetch_*` methods — `_rows_to_events` /
`_p2p_rows_to_events` stay as-is.

### ECC (classic tables)

Two access paths are supported:
- **SQL** (fastest): direct read on the source tables via SQLAlchemy. Pilot-time the
  customer provides a read-only DB user.
  - O2C tables: `VBAK` / `VBAP` / `LIKP` / `VBRK` / `VBFA` / `BKPF` / `BSEG` (VBAP carries
    PSTYV and KZVBR for the MTO / variant-config rollup).
  - P2P tables: `EKKO` / `EKPO` / `MKPF` / `MSEG` / `RBKP` / `RSEG` / `BSAK` (EKPO carries
    PSTYV and KNTTP for the service / account-assignment rollup).
- **RFC**: stubbed for landscapes where DB access is not granted. Wire your approved
  `RFC_READ_TABLE` or custom Z-FM in `connectors/ecc.py`.

### Processes

Each process is a single `ProcessDefinition` in `processes/`:

| Slug | Name | Activities | Happy path | Seeded in synthetic |
|---|---|---|---|---|
| `o2c` | Order-to-Cash | 10 | `OrderCreated → … → PaymentReceived` | ✔ 4 bottleneck patterns |
| `p2p` | Procure-to-Pay | 10 | `PurchaseRequisitionCreated → … → PaymentMade` | ✔ 4 bottleneck patterns |

To add a third process (e.g. Record-to-Report): drop a new `ProcessDefinition` in
`processes/r2r.py`, register it in `processes/__init__.py`, and add an `extract_r2r()`
to each connector. The CLI / UI / analytics pick it up automatically.

## Scheduling

`sap-mining` is a plain CLI, so any cron / Airflow / GitHub Actions scheduler works.
Example hourly run at 07:00:

```cron
0 7 * * * cd /opt/sap-mining && .venv/bin/sap-mining run --config config/config.s4hana.yaml >> logs/run.log 2>&1
```

## Configuration

One YAML file per scheduled run. `.env` is auto-loaded (no `export` needed) and
`${VAR}` references in YAML are expanded at load time so secrets never touch disk.
See `config/config.*.yaml` for ready examples.

**O2C (sales-org scoped):**
```yaml
connector:
  kind: s4hana        # or ecc or synthetic
  base_url: ${S4_BASE_URL}
  user: ${S4_USER}
  password: ${S4_PASSWORD}

llm:
  provider: anthropic # or openai or gemini
  model: claude-sonnet-4-6
  api_key: ${ANTHROPIC_API_KEY}

run:
  process: o2c
  window_days: 30
  sales_orgs: ["DE01", "DE02"]
```

**P2P (purchasing-org / company-code scoped):**
```yaml
connector:
  kind: s4hana
  base_url: ${S4_BASE_URL}
  user: ${S4_USER}
  password: ${S4_PASSWORD}

llm:
  provider: anthropic
  model: claude-sonnet-4-6

run:
  process: p2p
  window_days: 30
  purchasing_orgs: ["1000", "2000"]
  company_codes: ["1000"]
```

Running both configs in sequence (cron-friendly) populates both processes so the UI
shows a two-tile landing.

## Repository layout

```
src/sap_process_mining/
├── cli.py                 CLI entry (click): run, investigate, check, ui
├── config.py              Pydantic config + env-var expansion
├── _env.py                .env auto-loader (imported for side-effect)
├── event_log.py           Canonical EventLog dataclass
├── persistence.py         save/load per-process run artifacts under reports/latest/
├── connectors/            Data sources: s4hana, ecc, synthetic
├── processes/             Process definitions: o2c.py, p2p.py (+ registry)
├── mining/                Analytics (cycle / bottleneck / variant / anomaly / findings)
├── llm/                   Pluggable LLM layer (anthropic, openai, gemini) + Tool primitives
├── analysis/              Flagger orchestrator: EventLog → Findings → briefing
├── investigator/          Investigator agent + 6 RCA tools (tool-use loop)
├── reporting/             Markdown renderers (briefing + RCA)
└── ui/                    Streamlit app
config/                    YAML configs (synthetic, synthetic-p2p, s4hana, ecc)
scripts/                   Utilities (findings preview, etc.)
tests/                     pytest suite (synthetic O2C, synthetic P2P, investigator tools)
```

## Status

- [x] Event-log core + process-definition abstraction
- [x] **Order-to-Cash** process end-to-end (synthetic + S/4HANA + ECC)
- [x] **Procure-to-Pay** process end-to-end (synthetic + S/4HANA + ECC)
- [x] Item-level rollups for both processes (MTO / variant-config for O2C, service /
      account-assignment for P2P)
- [x] Cycle / dimensional bottleneck / variant / anomaly analytics
- [x] Anthropic / OpenAI / Gemini provider registry (Flagger)
- [x] **Investigator agent** — tool-using RCA on all three providers (Anthropic, OpenAI, Gemini)
- [x] **Streamlit UI** — multi-process landing + scoped detail + streamed tool calls
- [x] `.env` auto-load (no `export` needed)
- [ ] Dedup overlapping dimensional findings before handing to LLM
- [ ] Email / Slack delivery of briefings + RCA
- [ ] Cross-period delta ("vs. last week") in findings
- [ ] Record-to-Report process

See the business case in `docs/` for the commercial framing.
