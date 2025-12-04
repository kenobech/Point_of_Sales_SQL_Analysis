
# Point of Sales SQL Analysis

A production-ready Point of Sale (PoS) ETL and analytics pipeline for an 800,000-row dataset from Hugging Face Ltd.'s retail operations in Nigeria.

This README consolidates architecture, implementation steps, findings, and recommended next steps. It keeps pointers to the SQL scripts included in this repository for schema creation and job scheduling.

## Contents

- Introduction
- Database & schemas
- Ingestion and staging
- Profiling and normalization
- ETL pipeline and automation
- Indexing and performance
- Analytics & KPIs
- Recommendations
- Files in this repository
- Summary

---

## Introduction

Dataset summary

- Rows: **800,000**
- Columns: **11** — transaction_id, store_name, city, transaction_date, cashier_id, items_count, total_amount_ngn, payment_method, discount_applied, loyalty_points_earned, receipt_number

The rest of this document describes what was built and suggests practical next steps.

---

## Database & schemas

Database: **RetailPoSDB**

Schemas and purpose:

- staging — raw CSV landing tables (no transformations)
- ref — reference/dimension tables (stores, payment methods)
- sales — normalized transactional data (fact tables)
- analytics — aggregates and KPI tables/views
- log — ETL/audit and error logging

This separation increases maintainability and clarifies ownership for each layer.

---

## Ingestion and staging

1. Created `staging.PoSRaw` with the same column layout as the CSV.
2. Fast bulk import using BULK INSERT into `staging.PoSRaw` (800k rows).
3. Staging holds raw data for validation and controlled transformation.

See `PoS_Nigeria.sql` for table definitions and ingestion examples.

---

## Profiling and normalization

Profiling performed on staging to measure:

- duplicates (receipt_number and other keys)
- null distributions per column
- outliers in `items_count` and `total_amount_ngn`
- invalid or future transaction_date values

Findings and actions:

- Repeated values: `store_name`, `city`, `payment_method` were normalized into `ref` tables.
- Duplicate receipts and malformed dates were flagged and quarantined during ETL.
- No reliable natural primary key in the raw file — surrogate keys were used in targets.

Created normalized tables:

- `ref.stores` (store_id PK, store_name, city)
- `ref.payment_methods` (payment_id PK, payment_method)
- `sales.transactions` (transaction_id or surrogate PK, store_id FK, payment_id FK, cashier_id, transaction_date, items_count, total_amount_ngn, discount_applied, loyalty_points_earned, receipt_number)

Relationships: `ref.stores` -> `sales.transactions` (1:N), `ref.payment_methods` -> `sales.transactions` (1:N).

---

## ETL pipeline and automation

- Master stored procedure: `etl.sp_master_etl_merge` performs MERGE-based upserts for reference tables and the transactions fact table.
- Deduplication via ROW_NUMBER(): keeps canonical rows per business key.
- Error handling: TRY…CATCH with logging to `log.error_log` (stores error message, step, and sample rows).
- Idempotent runs: carefully designed MERGE and deterministic keys allow safe replays.

Automation

- A SQL Server Agent job `PoS_ETL_Daily_Refresh` was created (daily at 01:00) to run the master ETL and capture run metrics.
- See `Run_ETL_Daily_PoSNigeria.sql` for the job script.

---

## Indexing and performance

- Created indexes on common predicates and join keys (examples: `sales.transaction_date`, `ref.stores.store_id`, `ref.payment_methods.payment_id`).
- For large-scale analytics consider partitioning `sales.transactions` by month/year and using columnstore indexes to accelerate aggregations.
- Use covering indexes for the most common KPI queries.

---

## Analytics & KPIs (summary)

- Stores: **10** across **15** cities
- Cashiers: **~900**
- Transactions/receipts: **800,000**
- Total revenue: **~₦80 billion**; average transaction: **~₦100,435**
- Discounted transactions: **160,246**; full-price: **639,754**
- Loyalty points total: **400,438,676**

Top performers

- Cashiers: CASH763, CASH460 (>₦100M each)
- Lowest: CASH410, CASH943, CASH738, CASH106 (~₦70M each)
- Top city: Abeoluta; Top store: Pointek; Peak revenue date: 2024-09-16

Payment methods

- Cash: 400,000 txns (~₦40B)
- Card: 319,000 txns (~₦32B)
- Mobile money: 79,000 txns (~₦8B)

Temporal trends

- Best month: August; worst: October
- Best quarter: Q4; worst: Q1
- Peak week: Week 13; lowest: Week 44

Example KPI use-case

- With a ₦9M monthly sales target: 465 cashiers met the target in January while 435 did not. Balogun Market met its store target for January.

---

## Business recommendations (management actions)

Below are practical actions management can take based on the analytics findings.

1) Seasonal & monthly performance (August high, October low)
	- Why it matters: Knowing peak and trough months enables better inventory planning, staffing, and marketing spend allocation to maximize revenue and minimize wastage.
	- Actions:
	  - Run targeted promotions in low months (October) — time-limited discounts, bundle offers, or loyalty multipliers to stimulate demand.
	  - Shift marketing budget from peak to pre-peak months to amplify demand before and during August, and run retention campaigns for October.
	  - Adjust inventory orders and supplier contracts ahead of anticipated peaks to avoid stockouts and reduce expedited shipping costs.
	  - Temporarily increase staffing levels and opening hours around peak periods (August peaks, Week 13) and reduce or reallocate staff in low periods.

2) Payment-method strategy (cash dominant)
	- Why it matters: Cash dominates volume and revenue; promoting higher-margin payment options or incentivizing digital payments can reduce cash-handling costs and fraud risk.
	- Actions:
	  - Introduce small incentives for card/mobile payments (e.g., loyalty points bonus or instant discount for digital payments).
	  - Review reconciliation processes and cash controls to reduce shrinkage; invest in POS terminals and staff training for digital acceptance.
	  - Negotiate lower card fees or co-marketing deals with payment providers if volume supports it.
	- KPIs to track: % revenue by payment type, average ticket by payment type.

3) Cashier & store performance (top & bottom performers)
	- Why it matters: Identifying top performers helps replicate best practices; supporting low performers improves overall revenue.
	- Actions:
	  - Run targeted training and coaching programs for bottom performers; create playbooks capturing top cashiers' successful approaches.
      - Recognize and reward top performers through bonuses, public acknowledgment, or career advancement opportunities.
	  - Implement incentive schemes tied to clean KPIs (revenue, average basket, refund rate) to align behavior without encouraging fraud.
	  - Use shift-level dashboards to provide near-real-time feedback to cashiers and managers.
	- KPIs to track: revenue per cashier
    

4) Store-level strategies
    - Identify underperforming stores and conduct root-cause analysis (location, competition, staffing).
    - Tailor local marketing and community engagement to boost foottraffic.
    - Optimize inventory mix based on local preferences and sales data.


5) Quick wins (30–90 day  experiments)
	- Launch a targeted October promo in a subset of stores to test responsiveness.
	- Run a “digital payment week” with incentives to measure change in digital adoption.
	- Implement cashier coaching with an hourly dashboard in a pilot store.

---

## Files in this repository

- `PoS_Nigeria.sql` — schema and ETL SQL scripts
- `Run_ETL_Daily_PoSNigeria.sql` — sample SQL Agent job to schedule the ETL
- `README.md`
- `PosCSVzip.zip` — raw CSV dataset

## Summary

This project produces a maintainable, auditable and efficient PoS data pipeline from raw CSV ingestion through to analytical KPIs. It demonstrates best practices for staging, normalization, idempotent ETL, automation and monitoring.
