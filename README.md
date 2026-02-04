# Multi-Tenant Finance Analytics Platform (Cost & Usage Domain)

A production-oriented Spark data platform that models, aggregates, and publishes canonical cloud cost datasets with deterministic backfills, explicit data ownership, and finance-grade correctness guarantees.

This was built as a reusable **data platform** rather than a one-off analytics pipeline. The emphasis is on data modeling, recomputation safety, and operational semantics rather than dashboards or visualization.

---

## Problem Statement

Cloud cost and usage data presents several non-trivial data engineering challenges:

- Billing events often arrive **late or out of order**
- Previously reported costs may be **corrected retroactively**
- Financial reporting requires **deterministic recomputation** for audits
- Aggregations must avoid **double counting** while remaining performant
- Downstream consumers require **stable, canonical datasets**

This platform simulates a multi-tenant cost & usage domain and demonstrates how these challenges are handled in a production-grade data engineering system.

---

## Platform Overview

The platform is structured as a **three-stage Spark data pipeline**, with each stage owning a specific responsibility and data contract.

```text
Raw Billing Events (CSV)
        |
        v
Stage 1 — Normalize & Deduplicate (event-level)
        |
        v
Stage 2 — Aggregate Daily Cost Facts (account-service-day)
        |
        v
Stage 3 — Publish Canonical Daily Dataset (account-day)
```


Each stage produces an immutable output and can be recomputed independently.

---

## How to Run

This project is fully executable locally using Spark in local mode.

### Prerequisites
- Python 3.10+
- Apache Spark (local)
- Java 17+

### Run Stage 1 — Normalize Raw Events

```bash
spark-submit src/stage1_read_raw.py
```

### Run Stage 2 — Build Daily Cost Fact
```bash
spark-submit src/stage2_aggregate_daily_cost.py
```

### Run Stage 3 — Publish Canonical Dataset
```bash
spark-submit src/stage3_publish_daily_account_cost.py
```
Each stage writes deterministic outputs and can be re-run safely to validate backfills and recomputation.

---

## Repository Structure

```text
src/
  stage1_read_raw.py                    # Normalize & deduplicate raw billing events
  stage2_aggregate_daily_cost.py        # Build account-service-day cost fact
  stage3_publish_daily_account_cost.py  # Publish canonical account-day dataset

data/
  raw_billing_events.csv                # Simulated raw billing input

outputs/
  normalized/                           # Event-level normalized data
  facts/                                # Aggregated daily fact tables

```

---

## Data Model

The platform is built around **explicit canonical data models**, with clearly defined grains and ownership semantics.

### Stage 1 — Normalized Billing Events

- **Grain:** `event_id`
- **Characteristics:**
    - Immutable raw ingestion
    - Deduplicated using latest-ingestion-wins semantics
    - Late and corrected events are preserved for recomputation

This dataset serves as the **system of record** for all downstream processing.

---

### Stage 2 — Daily Cost Fact

**Table:** `account_service_day_cost`

- **Grain:** `(account_id, service_name, usage_date)`
- **Metric:** `total_cost_usd`

This fact table is optimized for analytical queries and controlled joins. Aggregation at this grain prevents event-level explosion and ensures predictable query performance.

---

### Stage 3 — Canonical Published Dataset

**Table:** `daily_account_cost`

- **Grain:** `(account_id, usage_date)`
- **Columns:**
    - `total_cost_usd`
    - `data_complete`
    - `published_at`

This dataset represents the **authoritative daily cost view** for downstream consumers. All SLAs, backfill guarantees, and data quality enforcement occur at this layer.

---

## Sample Output

Example records from the canonical dataset:

```sql
SELECT * FROM daily_account_cost LIMIT 5;
```

| account_id | usage_date  | total_cost_usd |
|------------|-------------|----------------|
| A1         | 2025-01-01  | 15.0           |
| A2         | 2025-01-01  | 7.0            |

---

### Logical Dimensions

Dimensions are represented as separate lookup datasets and are designed to evolve independently of fact tables.

- `dim_account` — account metadata and billing ownership
- `dim_service` — service identifiers (EC2, S3, etc.)
- `dim_region` — geographic cost attribution
- `dim_pricing_model` — on-demand, reserved, spot

This separation allows fact table grains to remain stable while dimensions are extended or corrected without requiring downstream rewrites.

---

### Why This Model

- **Account-day** is the canonical financial grain required for finance, forecasting, and reporting
- **Service-day** facts enable controlled drill-down without inflating joins
- Explicit grains eliminate ambiguous aggregations and double counting
- Downstream SQL becomes simple, predictable, and performant

---

## Backfills & Schema Evolution

### Backfills

The platform is designed to support **deterministic backfills**.

- All raw data is immutable
- Deduplication is deterministic (`event_id`, latest `ingestion_date`)
- Reprocessing the same input produces the same output

**Typical backfill scenario:**
- Late or corrected billing events arrive
- A rolling window (e.g., last 90 days) is recomputed
- Downstream aggregates are overwritten atomically

This guarantees correctness without manual intervention.

---

### Backfill Validation Example

To validate deterministic recomputation, the pipeline was re-run across a rolling window after simulating late-arriving billing events.

For example, reprocessing the window **2025-01-01 to 2025-01-07** after introducing a corrected event for **2025-01-03** resulted in:
- Identical outputs for unaffected days
- Updated aggregates only for the impacted partition
- No double counting or metric drift

This confirms that recomputation is deterministic and safely scoped to affected partitions.

---

### Schema Evolution

Schema evolution is handled explicitly:

- New cost attributes are additive
- Existing aggregations remain backward-compatible
- Unknown columns are ignored until explicitly modeled

If a new billing field is introduced:
- Stage 1 ingests it without breaking
- Stage 2 aggregation logic is extended intentionally
- Historical recomputation applies the new logic consistently

This prevents silent metric drift.

---

### Double Counting Prevention

Double counting is prevented by:
- Enforcing a single event-level system of record
- Deduplicating before aggregation
- Aggregating before joins

Recomputation always starts from normalized events, not derived datasets.

---

## SLAs & Operational Ownership

### Data Availability SLA

- **Daily dataset availability:** T+1 (next-day)
- **Publishing cutoff:** All data for a given date must be complete before publication

---

## Monitoring & Validation

Each pipeline stage emits basic operational and data quality metrics to validate correctness and detect anomalies:

- Record counts per stage to detect missing or partial data
- Null and negative cost checks on critical fields
- Daily cost deltas compared to prior runs
- Dataset-level validation checks implemented in `dq/validate_daily_account_cost.py`

These checks are used to validate deterministic recomputation during backfills and to prevent incomplete or incorrect data from being published to downstream consumers.

---

### Failure Semantics

If a stage fails:
- Downstream datasets are **not published**
- Previously published partitions remain available
- Partial or incomplete data is never exposed

This ensures consumers never read ambiguous or mixed-state data.

---

### Data Quality Ownership

The platform enforces the following invariants:

- No nulls in primary keys
- No negative cost values
- No empty daily partitions

Publishing fails fast if these conditions are violated.

---

## Design Tradeoffs

### Batch vs Streaming

**Chosen:** Batch  
**Why:** Billing data is naturally batch-oriented and frequently corrected. Streaming would add complexity without improving correctness.

---

### Scale Assumptions

The platform is designed to scale to **tens of millions of billing events per day**, with partitioning and pruning on `usage_date` ensuring that backfills and daily recomputation remain bounded and predictable.

---

### Local Execution vs Cloud Deployment

**Chosen:** Local Spark execution  
**Why:** Logic correctness, modeling, and determinism are environment-agnostic. The same code maps 1:1 to EMR + S3 in production.

---

### Parquet over CSV

**Chosen:** Parquet  
**Why:** Column pruning, schema enforcement, and storage efficiency are required for scalable analytics.

---

### Cost vs Freshness

**Chosen:** Cost-optimized  
**Why:** Finance workloads prioritize correctness and auditability over near-real-time freshness.

---

## Cloud Execution Mapping (Conceptual)

While executed locally for validation, the platform maps directly to a cloud deployment:

| Local Component | Cloud Equivalent |
|---------------|------------------|
| Local Spark | EMR / Dataproc |
| Local filesystem | S3 / GCS |
| Parquet datasets | Glue / Hive Metastore |
| Manual execution | Airflow / Dagster |

No code changes are required — only environment wiring.

---

## Key Takeaways

- Explicit canonical data modeling
- Deterministic recomputation and backfill safety
- Clear ownership and failure semantics
- Platform-oriented thinking over ad-hoc analytics

This project reflects how production finance data platforms are designed, operated, and reasoned about in real-world environments.
