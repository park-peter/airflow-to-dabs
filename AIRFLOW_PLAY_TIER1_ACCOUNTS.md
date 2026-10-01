# Airflow Migration Play — Tier-1 Target Accounts (working data)

Source: `metric_store.adhoc_pft_consolidated_revenue_union_slim`, `feature_name='airflow_orchestrated'`,
trailing 365 days, measured 2026-07-29 on logfood warehouse `9fb2ea023126d1f4`. Names resolved by
joining `customer_id` → PUM `sfdc_account_id` (**not** metric_store `account_id`, which is the
Databricks account UUID and does not join to Salesforce).

**INTERNAL — contains account names and revenue. Do not paste into customer-facing material.**

| # | Account | Airflow $M TTM | Workspaces |
|---|---|---|---|
| 1 | OpenAI | 160.52 | 5 |
| 2 | The Trade Desk | 8.54 | 8 |
| 3 | DoorDash | 5.78 | 4 |
| 4 | Disney Streaming Services | 5.19 | 11 |
| 5 | Goldman Sachs | 4.28 | 6 |
| 6 | UnitedHealth Group (UHG) | 3.28 | 168 |
| 7 | Adobe Systems | 2.94 | 71 |
| 8 | Instacart | 2.90 | 3 |
| 9 | Magnite | 2.88 | 7 |
| 10 | Nextdoor | 2.27 | 4 |
| 11 | Epic Games | 2.06 | 9 |
| 12 | Walmart | 2.00 | 62 |
| 13 | PicPay | 1.96 | 5 |
| 14 | Match Group | 1.88 | 7 |
| 15 | Alphonso Inc. dba LG Ad Solutions | 1.70 | 7 |
| 16 | Sovrn Inc | 1.66 | 9 |
| 17 | GE Vernova International LLC | 1.44 | 4 |
| 18 | FanDuel | 1.43 | 5 |
| 19 | NBCU Data & Analytics | 1.37 | 12 |
| 20 | Stellantis | 1.34 | 18 |
| 21 | R1 RCM | 1.31 | 11 |
| 22 | AB-InBev | 1.21 | 37 |
| 23 | CVS Health | 1.18 | 15 |
| 24 | Nike | 1.18 | 8 |
| 25 | Riot Games | 1.18 | 2 |
| 26 | Inscape dba Vizio LLC | 1.17 | 5 |
| 27 | Point72 Asset Management | 1.14 | 12 |
| 28 | ABN AMRO Group | 1.14 | 120 |
| 29 | Amadeus | 1.13 | 12 |
| 30 | Activision Blizzard | 1.10 | 5 |
| 31 | Samba TV Inc. | 1.09 | 1 |
| 32 | Depop | 1.03 | 2 |
| 33 | Comcast | 1.03 | 35 |
| 34 | Scribd | 1.02 | 2 |

**Total: $231.3M across 34 accounts.**

## Read this before using the list

- **OpenAI alone is $160.5M = 69% of the tier.** Excluding it, tier-1 is **$70.8M across 33 accounts
  (avg $2.1M, median $1.43M, max $8.54M)**. Any exec framing must state this — both the headline
  "$308M" and this tier-1 total are dominated by one atypical account. Qualify whether OpenAI is a
  realistic migration target at all before letting it anchor a business case; if it is not, the
  honest addressable tier-1 number is **~$71M**, which is still comparable to the ADF play's $68M.
- Sanity-check each account's Airflow spend against its *total* Databricks spend before outreach — a
  large absolute number can still be a small share of a very large estate.
- **Workspace counts vary from 1 to 168.** High-workspace accounts (UHG 168, ABN AMRO 120, Adobe 71,
  Walmart 62, AB-InBev 37, Comcast 35) imply a federated estate: migration is a per-team motion, not
  a single project. Size UCOs accordingly.
- **Cohort skews heavily to tech / media / adtech** (OpenAI, Trade Desk, DoorDash, Disney, Instacart,
  Magnite, Epic, Riot, Activision, Vizio, Samba TV, Scribd, Depop). These are sophisticated Airflow
  users who chose it deliberately — the "when to stay on Airflow / hybrid" framing in the plan matters
  most with exactly this cohort. A pure rip-and-replace pitch will not land here.
- Cloud-agnostic cohort, unlike the ADF play's Azure-share filter — this play reaches AWS and GCP
  accounts too.
- Tier 2 (≥$100K/yr) is 223 accounts; tier 3 (≥$1K/mo) is 532. Pull those when tier-1 outreach is
  underway.
