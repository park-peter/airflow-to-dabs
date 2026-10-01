# Plan: Airflow → Databricks migration **landing page** (discoverability)

Status: **proposal for review** — nothing published.

**Goal:** when someone internally looks for "Airflow migration," they land on one page that routes
them to the tooling that actually exists (`flowx` Airflow path + the `airflow-to-dabs` skill) and the
positioning assets already written. This is a **discoverability fix**, not a GTM play.

**Not in scope** (deliberately dropped): target account lists, funding/PS packages, UCO sizing
guidance, an internal pitch deck, sponsorship. Those belong to a *play*; if a play ever gets funded,
this page becomes its landing page. See §7 for the play material parked for later.

---

## 1. The problem, evidenced

Searching Glean/Confluence for Airflow migration resources (done 2026-07-29) returns:

| What surfaces | Verdict |
|---|---|
| Airflow Battlecard (`go/airflow/battle`) | Good — but compete positioning, not migration |
| Lakeflow Jobs Orchestration battlecard (`go/orchestration/battle`) | Same |
| "Airflow → Databricks Workflows Migration Guide – Concepts" (Google Doc) | Useful, unclear owner/freshness |
| Public blog: *How to move from Apache Airflow to Lakeflow Jobs* | External narrative only |
| **wkmigrate wiki** — lists "Airflow DAGs" under *Planned* Use-Cases | **Actively misleading.** wkmigrate is deprecated (pivoted to flowx), and this page tells a reader Airflow tooling does not exist yet. |
| **flowx** (Airflow path) | **Did not surface** |
| **`airflow-to-dabs` skill** | **Did not surface** |

So the two tools that *do* solve the problem are invisible, and the most authoritative-looking hit is
a stale page that says the capability is unbuilt. Fixing the stale signal matters as much as adding
the new page.

Supporting justification for why this is worth someone's attention (not page content): Airflow is the
**largest external orchestrator of Databricks — $308M TTM vs ADF's $268M** (see
`reference_airflow_revenue_at_risk` / §7). ADF has a full funded play; Airflow has no discoverable
migration entry point at all.

---

## 2. What the page must do

Route, not duplicate. Four reader intents, answered in the first screen:

1. *"I need to assess a customer's Airflow estate"* → flowx `discover` (inventory + complexity report)
2. *"I need to convert DAGs"* → flowx `convert`/`package` for bulk; `airflow-to-dabs` for the hard ones
3. *"I need to position Lakeflow vs Airflow"* → existing battlecards + blog
4. *"Should this customer even migrate?"* → the honest hybrid / when-to-stay section

Everything else is a link.

## 3. Proposed page outline

**Title:** `Airflow to Databricks Migration` — with the phrases people actually search in the body:
*Airflow migration, Airflow to Lakeflow Jobs, Airflow to Workflows, convert DAGs, MWAA, Astronomer,
Google Cloud Composer, DAG to DABs*.

1. **Start here** — 4-row "what do you want to do → go here" table (the intents above).
2. **Tooling**
   - **flowx** (`databricks-field-eng/flowx`) — deterministic Python library + agent skills, ADF **and**
     Airflow paths on a shared Pipeline IR. `discover` (static `ast` parse of DAG `.py`, no Airflow
     install; emits `inventory.json` + `profile_report.csv` complexity report) → `convert` (~35
     operator/sensor families deterministic, agentic fallback, gaps in `gaps.json` — never a silent
     drop) → `package` (DAB YAML + notebooks + setup scripts). Also runs as an MCP server on a
     Databricks App for Genie Code.
   - **`airflow-to-dabs` skill** — depth/fidelity for the hard parts: source-aware operator routing by
     *connection* not class, Lakeflow Connect as an ingestion target, dbt factory mode (one Lakeflow
     task per dbt node), Airflow 3 (`airflow.sdk`, Assets, TaskFlow, dynamic mapping,
     deferrable/async/resumable), execution-date semantics wired to native Databricks backfill,
     runnable schema-validated example bundles.
   - **Which one?** flowx for inventory, complexity scoring, and bulk deterministic conversion;
     `airflow-to-dabs` for high-fidelity conversion of complex/idiomatic DAGs and the patterns flowx
     flags as gaps. Same target (DABs), so the outputs compose. *State this explicitly — it is the
     "why two tools?" answer.*
3. **Positioning & compete** — link the battlecards and the blog. Do not restate them.
4. **Concept mapping** — link the existing Concepts doc; note the skill's `references/` as the deeper,
   maintained version (operator mapping, schedule/trigger, DABs schema, Airflow 3, Lakeflow Connect).
5. **When *not* to migrate** — Airflow is healthy and often deliberately chosen for heterogeneous,
   multi-system orchestration. Recommend hybrid (migrate the Databricks-heavy DAGs, keep Airflow for
   the rest) over rip-and-replace. Note that Lakeflow's own external-orchestration
   (`python_operator_task`) is still pre-GA — the honest reason some customers stay.
6. **Related** — ADF Migration Play (`go/adfmigration`) as the sibling motion, Lakeflow product page
   (`go/lakeflow`), the Orchestration Layer page, External Orchestration status.
7. **Owner + last-reviewed date** — the whole reason this page is needed is that wkmigrate's page
   rotted. Name an owner and a review cadence in the page itself.

## 4. Discoverability mechanics (the actual deliverable)

A page nobody finds changes nothing. In rough priority order:

1. **Fix the stale signal first.** Update the wkmigrate wiki (4428825810) to state it is deprecated and
   point to flowx — it currently outranks everything and says Airflow is unbuilt. Highest
   value-per-effort item on this list; needs Greg (page owner, already said he hasn't updated it).
2. **`go/` shortlink** — `go/airflowmigration`, mirroring `go/adfmigration`.
3. **Placement** — FE space, ideally a sibling of the ADF Migration Play (parent `1094161644`) so it
   inherits browse traffic from people already looking at migration plays.
4. **Inbound links from where people actually start** — Airflow Battlecard, Orchestration Layer page,
   `go/lakeflow`, ADF play ("migrating from Airflow instead? →"), flowx README, and this skill's README.
   Inbound links are what make Glean rank it.
5. **Confluence labels** + search-term-rich body so Glean indexes it for the phrasings in §3.
6. **Announce once** in the relevant channels (e.g. `#field-devx`, Lakeflow/orchestration channels).

## 5. Sequencing

- **Step 1 (hours):** fix the wkmigrate page → flowx. Independent of everything else, immediate value.
- **Step 2 (~a day):** draft the landing page per §3, circulate to the flowx co-maintainers + whoever
  owns the Airflow battlecard for a sanity read.
- **Step 3 (hours):** publish, request the `go/` link, add the inbound links in §4.
- **Step 4 (ongoing):** owner + review date on the page; revisit when Lakeflow external orchestration
  goes GA (it changes the "when not to migrate" section).

## 6. Open questions

- **Q1 — Placement/name.** `go/airflowmigration` as an FE-space sibling of the ADF play, or a section
  inside an existing Lakeflow/orchestration page? Sibling placement is my recommendation (inherits
  traffic, matches how people found the ADF play).
- **Q2 — Who can edit the wkmigrate page?** Greg owns it. Needs his sign-off to mark deprecated.
- **Q3 — Is the "Airflow → Workflows Concepts" doc still owned?** If yes, link it; if abandoned, say so
  and point at the skill's `references/` instead.
- **Q4 — flowx external-readiness.** It's `0.1.0`. What can the page promise to customer-facing use,
  and is the MCP/Databricks App path field-ready?

## 7. Parked: the play material (if this ever becomes a funded motion)

Researched and verified this session; kept out of the landing page on purpose.

- **Revenue:** Airflow-orchestrated Databricks = **$308.0M TTM**, the largest external orchestrator
  (ADF $268.4M, generic external $284.9M, dbt $31.5M). 2,202 accounts / 6,364 workspaces — ~$140K per
  account vs ADF's ~$19K (≈7x denser).
- **Concentration:** 34 accounts ≥$1M = $231.3M. **But OpenAI alone is $160.5M = 52% of the total** —
  ex-outlier tier-1 is **$70.8M / 33 accounts** (median $1.43M). Any exec framing must disclose this.
  Named list: `AIRFLOW_PLAY_TIER1_ACCOUNTS.md` (internal).
- **Serverless (O1):** Airflow workspaces are **17.7% serverless vs 23.9% global**, 82.3% still classic
  → real conversion headroom. **But their all-purpose share (26.0%) is well below ADF's (44.7%)**, so
  the ADF play's "61% on inefficient AP compute" line does **not** transfer — using it would be an
  overclaim.
- **Framing constraint:** ADF's play works because of a deprecation forcing function. Airflow has none,
  so a play would have to be TCO/reliability/serverless-led and explicit about when to stay.
- **Would need:** sponsorship (Lakeflow adoption/EPL lead + PS contact), and a decision on whether
  Accelerate funding extends to Airflow. Not solo-shippable.
