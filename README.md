# Multi-Tenant Cost Analytics Platform

### Spark Data Lake + Athena + RAG-Powered Cost Intelligence

An end-to-end cloud cost analytics platform built using Spark on EC2, Amazon S3, and Athena, extended with a Retrieval-Augmented Generation (RAG) layer for cost anomaly explanation.

The system models raw billing events into canonical financial datasets and exposes an API layer that generates context-aware explanations using vector retrieval and an LLM abstraction layer.

---

## 1. Problem Statement

Cloud cost and usage data introduces several non-trivial data engineering challenges:

- Billing events arrive **late or out of order**
- Previously reported costs may be **retroactively corrected**
- Financial reporting requires **deterministic recomputation** for audits
- Aggregations must prevent **double counting**
- Downstream consumers require **stable, canonical financial datasets**

In parallel, modern finance and engineering teams increasingly expect:

- Natural-language explanations of anomalies
- Self-serve analytical access
- Context-aware insights rather than raw cost tables

This platform addresses both layers:

1. A finance-grade data foundation built on explicit modeling and partition-scoped recomputation.
2. An explainable AI layer that retrieves historical anomalies and generates structured explanations via a RAG pipeline.

---

## 2. Architecture Overview

The system is implemented as two clearly separated layers:

1. **Deterministic Data Platform (Spark + S3 + Athena)**
2. **Retrieval-Augmented Generation (RAG) Service**

This separation enforces clean ownership boundaries between data correctness and AI reasoning.

---

### 2.1 Structured Data Platform (Spark)

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
        |
        v
Partitioned Parquet in S3 (partitioned by usage_date)
        |
        v
Queryable via Amazon Athena

```
Key characteristics:
- Explicit data grains at each stage 
- Deterministic recomputation from normalized source data 
- Partition-scoped overwrite semantics in S3
- Canonical dataset optimized for financial reporting queries
Each stage can be recomputed independently, enabling safe backfills and audit validation.

###  2.2 GenAI Layer (RAG Service)

The GenAI layer operates strictly on published canonical datasets.

```text
Daily Account Cost Dataset
        |
        v
Anomaly Detection (percentage change logic)
        |
        v
Text Chunk Generation
        |
        v
Embedding Generation (Amazon Titan)
        |
        v
FAISS Vector Index
        |
        v
LLM Explanation API (FastAPI)
```
This layer enables:
- Retrieval of historically similar anomalies 
- Context-aware explanation generation 
- Decoupled LLM provider abstraction 
- Observable API latency tracking

The AI layer does not modify source data — it consumes published outputs only.

---

## 3. Cloud Deployment (Implemented)

The platform was deployed and executed on AWS to validate real-world integration.

### Compute Layer
- **Spark on EC2 (t3.micro)**
  - Spark installed manually
  - IAM role attached for S3 access
  - Jobs executed via `spark-submit`

### Storage Layer
- **Amazon S3**
  - Partitioned Parquet datasets written using `s3a://`
  - `usage_date` partitioning for efficient pruning
  - Partition-level overwrite for deterministic backfills

### Metadata & Query Layer
- **AWS Glue Catalog**
  - External table registered over S3 location
  - Partition discovery via `MSCK REPAIR TABLE`

- **Amazon Athena**
  - Canonical dataset queried directly over Parquet
  - Partition pruning verified
  - Query results stored in S3

### Security Model
- IAM instance profile attached to EC2
- Role-based S3 access (no hardcoded credentials)
- Principle of least privilege for Bedrock access (embeddings only)

This validates:
- End-to-end cloud execution (compute → storage → query)
- Lakehouse-style partitioned dataset design
- Secure role-based access control
- Reproducible execution outside local environment

---
## 4. Data Pipeline (Spark + S3 + Athena)

This section describes the structured data platform responsible for ingesting, modeling, aggregating, and publishing canonical cost datasets.

All datasets are stored as **partitioned Parquet files in Amazon S3** and exposed via **AWS Glue + Amazon Athena**.

---

### 4.1 Execution Flow

Spark jobs were executed on an EC2 instance using local Spark mode with S3-backed storage (`s3a://`).

Run sequentially:

```bash
spark-submit src/stage1_read_raw.py
spark-submit src/stage2_aggregate_daily_cost.py
spark-submit src/stage3_publish_daily_account_cost.py
```
End-to-End Flow:
1. Spark jobs execute on EC2.
2. Partitioned Parquet datasets are written to S3.
3. Glue table is registered over the S3 location.
4. Athena queries the canonical dataset.

This validates:
- Cloud storage integration via s3a://
- Partitioned lakehouse design
- Queryable analytics dataset
- Secure IAM-based access
- Deterministic recomputation capability

---

### 4.2 Repository Structure

```text
src/                            # Spark data platform
  stage1_read_raw.py
  stage2_aggregate_daily_cost.py
  stage3_publish_daily_account_cost.py

data/
  raw_billing_events.csv

dq/
  validate_daily_account_cost.py

genai/                          # GenAI + RAG Layer (see Section 5)

docs/
  data_product.md

README.md
```
The `src/` directory contains the deterministic data pipeline.

The `genai/` directory contains the Retrieval-Augmented Generation layer (described later).

---

### 4.3 Data Model

The platform is built around explicit canonical data models with well-defined grains, partitioning strategy, and deterministic recomputation guarantees.

All datasets are stored as **partitioned Parquet files in S3** and exposed to downstream consumers via **AWS Glue + Amazon Athena**.

---

### Stage 1 — Normalized Billing Events

**Location:** `s3://cost-analytics-ashwin-0310/normalized/`  
**Grain:** `event_id`

**Purpose**
- Deduplicate raw billing events using latest-ingestion-wins semantics
- Preserve late-arriving and corrected events
- Establish a stable system of record for recomputation

**Characteristics**
- Immutable writes
- Idempotent reruns
- Source-of-truth dataset for all downstream transformations

---

### Stage 2 — Daily Cost Fact

**Table:** `account_service_day_cost`  
**Location:** `s3://cost-analytics-ashwin-0310/facts/daily_account_cost/`  
**Partitioned by:** `usage_date`  
**Grain:** `(account_id, service_name, usage_date)`  
**Metric:** `total_cost_usd`

**Why this grain?**
- Prevents event-level explosion
- Enables service-level cost attribution
- Supports efficient Athena queries with partition pruning

This layer is optimized for analytical workloads and serves as the primary fact table for finance reporting.

---

### Stage 3 — Canonical Published Dataset

**Table:** `daily_account_cost`  
**Grain:** `(account_id, usage_date)`  
**Derived From:** Stage 2 fact table

**Columns**
- `account_id`
- `usage_date`
- `total_cost_usd`
- `data_complete`
- `published_at`

This represents the authoritative daily cost view for consumers.

All SLAs, data quality validation, and backfill guarantees are enforced at this layer before publication.

---
### Dimensional Modeling Strategy

Dimensions are logically separated from fact tables and designed to evolve independently.

- `dim_account` — account metadata and billing ownership
- `dim_service` — service identifiers (EC2, S3, etc.)
- `dim_region` — geographic cost attribution
- `dim_pricing_model` — on-demand, reserved, spot

This separation ensures:
- Fact table grains remain stable
- Schema evolution in dimensions does not require fact rewrites
- Downstream joins remain controlled and predictable

### Why This Model

- Account-day is the canonical financial grain required for reporting and forecasting.
- Service-day enables controlled drill-down and anomaly detection.
- Explicit grains prevent ambiguous rollups.
- Partitioning by usage_date enables Athena partition pruning and cost-efficient scans.

### Double Counting Prevention

Double counting is prevented through:

- Event-level deduplication before aggregation
- Explicit aggregation grain enforcement
- Partition-level overwrite semantics in S3
- Reprocessing always starting from normalized source events

---

## 5. GenAI RAG Pipeline


The GenAI layer augments the canonical cost dataset with natural-language explanations of cost anomalies using a Retrieval-Augmented Generation (RAG) architecture.

The objective is not chatbot functionality, but **context-aware analytical reasoning** over structured financial data.

---

### 5.1 Anomaly Chunking
Cost anomalies are first detected using percentage change logic at the `(account_id, service_name, usage_date)` grain.

Each anomaly is converted into a structured natural-language chunk:

Example:
```text
On 2025-01-01, account A1 spent $18.50 on EC2,
a 54% increase compared to the previous day.
```
These chunks are stored as structured JSON:

- `account_id`
- `service_name`
- `usage_date`
- `text`
- `embedding` (added later)

**Why chunking matters:**

- Converts structured financial events into semantic retrieval units
- Enables vector similarity search
- Prevents raw table dumps into LLM context
- Preserves grain consistency with Stage 2 fact table

Chunking is deterministic and reproducible from canonical data.

---
### 5.2 Embedding Generation (Titan)
Each anomaly chunk is embedded using Amazon Titan Text Embeddings:

```python
modelId="amazon.titan-embed-text-v1"
```
Embeddings are generated once and stored as:
```text
[
  {
    "text": "...",
    "embedding": [0.0123, 0.9871, ...]
  }
]
```
Why embeddings:
- Transform text into high-dimensional semantic vectors 
- Enable cosine similarity search 
- Allow context retrieval without keyword matching

Embeddings are computed separately from explanation generation to:
- Reduce LLM invocation cost 
- Support re-indexing without re-chunking 
- Maintain separation of retrieval and generation layers

---

### 5.3 Vector Index (FAISS)
Embeddings are indexed locally using FAISS.

Why FAISS (instead of a managed vector DB):
- Free-tier friendly 
- No additional infrastructure cost 
- Sufficient for low-volume anomaly indexing
- Enables deterministic local testing

Index construction:

```text
index = faiss.IndexFlatL2(dimension)
index.add(embedding_matrix)
```
This enables fast nearest-neighbor search at inference time.

The vector index is rebuilt whenever anomaly chunks are regenerated.

---

### 5.4 Retrieval (Top-K)
When a user submits a query:

Example:

```text
"Why did EC2 costs increase significantly?"
```
The system:
1. Embeds the user query 
2. Computes cosine similarity against stored anomaly embeddings 
3. Selects Top-K most relevant anomaly chunks 
4. Passes retrieved context to the LLM

This ensures:
- The LLM receives only relevant financial context 
- Token usage remains bounded 
- Hallucination risk is reduced 
- Explanations are grounded in actual cost data 
- Retrieval is deterministic and observable.

---

### 5.5 LLM Abstraction Layer

The LLM layer is implemented via a provider-agnostic abstraction:

```python
class LLMClient:
    def generate(self, prompt: str) -> str:
        ...
```
This abstraction allows:
- Switching between Anthropic / OpenAI / local LLM 
- Avoiding vendor lock-in 
- Handling marketplace restrictions 
- Enforcing consistent prompt structure

The final prompt structure:
```text
User Question
+
Retrieved Financial Context
+
Instruction to explain causality
```
The LLM never receives raw tables — only curated anomaly context.

---
### RAG Design Principles

This implementation enforces:
- Clear separation of retrieval and generation 
- Deterministic chunk generation from canonical data 
- Vector similarity over keyword search 
- Provider abstraction for resilience 
- Bounded context windows for cost control

The RAG layer augments — but does not replace — the structured data platform.

---

## 6. API Layer

The RAG pipeline is exposed via a lightweight FastAPI service to enable programmatic access to cost explanations.

This service acts as a thin orchestration layer — it does not contain retrieval logic itself.

---

### 6.1 Service Architecture

The API layer is structured as:

```text
FastAPI (HTTP layer)
        |
        v
rag_service_logic.py  (orchestration)
        |
        v
FAISS retrieval
        |
        v
LLM abstraction layer
```
This separation ensures:
- HTTP concerns are isolated from business logic 
- Retrieval logic can be reused outside the API 
- LLM providers can be swapped without API changes 
- Unit testing is simplified
---
### 6.2 Endpoint Design

Endpoint:
```bash
POST /explain
```

Request Body:
```json
{
"query": "Why did EC2 costs increase?"
}
```

Response:
```json
{
"explanation": "...natural language explanation...",
"latency_ms": 142,
"retrieved_context_count": 3
}
```

The API:
1. Accepts a user query 
2. Embeds the query 
3. Retrieves Top-K relevant anomaly chunks 
4. Constructs structured prompt 
5. Invokes LLM via abstraction layer 
6. Returns explanation with metadata

---

### 6.3 Provider-Agnostic LLM Integration

The API does not directly call Anthropic or OpenAI.

Instead, it uses:
```text
LLMClient.generate(prompt)
```



This abstraction was introduced after encountering:
- Bedrock marketplace restrictions 
- Inference profile limitations 
- Payment instrument constraints

By decoupling the provider:
- The API remains stable 
- Only the LLM client implementation changes 
- Infrastructure friction does not affect consumers
---

### 6.4 Deployment Model

The API is deployed on the same EC2 instance as Spark for demonstration purposes.

Production mapping would involve:
- Containerization (Docker)
- Deployment behind a load balancer 
- Auto-scaling group 
- Centralized logging (CloudWatch)

The current implementation validates:
- End-to-end RAG functionality 
- Cloud IAM access integration 
- Real HTTP exposure of GenAI workflows

---
### 6.5 Why an API Layer?

Exposing RAG as an API enables:
- Integration with BI tools 
- Slack / Teams bots 
- Internal finance dashboards 
- Automated anomaly alerts

This moves the system from a batch analytics pipeline to an interactive intelligence service.

---

## 7. Observability & Operational Controls

The platform enforces correctness, recomputation safety, and publishing guarantees through explicit validation and partition-scoped semantics.

---

### 7.1 Data Availability SLA

The canonical dataset (`daily_account_cost`) follows a **T+1 publishing model**:

- Daily aggregates are published the next day after ingestion.
- A `usage_date` partition is only marked complete once validation checks pass.
- Publishing is partition-scoped and overwrite-safe.

This mirrors finance workloads where correctness and auditability outweigh real-time freshness.

---

### 7.2 Data Quality & Validation

Data quality validation is implemented as executable logic in:
 `dq/validate_daily_account_cost.py`


Validation invariants:

- No nulls in primary keys (`account_id`, `usage_date`)
- No negative `total_cost_usd`
- No empty daily partitions
- Deterministic row counts across recomputation windows

If validation fails:

- The partition is not published
- The job exits with non-zero status
- Previously published partitions remain intact

This prevents incomplete or corrupt financial data from being exposed downstream.

---

### 7.3 Deterministic Backfills

The pipeline supports partition-scoped deterministic recomputation.

Key guarantees:

- Raw billing data is immutable.
- Deduplication is deterministic using `(event_id, ingestion_date)` (latest-ingestion-wins).
- Aggregations are recomputed from normalized source data — never from derived outputs.
- Only impacted `usage_date` partitions are overwritten.

**Backfill workflow:**

1. Late or corrected billing events arrive.
2. A rolling window (e.g., 30–90 days) is reprocessed.
3. Only affected partitions are overwritten in S3.
4. Unaffected partitions remain unchanged.

Recomputation is idempotent and bounded.

---

### 7.4 Failure Semantics

Each pipeline stage owns its output contract.

If a stage fails:

- Downstream stages do not execute.
- No partial partitions are published.
- Previously valid partitions remain queryable in Athena.

All writes use partition-scoped overwrite semantics in S3, ensuring atomic visibility at the partition level.

---

### 7.5 Monitoring & Observability

Monitoring is implemented at both the data platform and GenAI layers.

#### Spark Pipeline Signals

- Record counts per stage
- Partition-level write confirmation
- Validation pass/fail status
- Deterministic recomputation checks

#### GenAI / RAG Service Signals

The FastAPI service includes:

- Structured request logging
- Latency tracking (retrieval + generation)
- Deterministic Top-K retrieval via FAISS
- Error surface for LLM failures

This provides visibility into:

- Query latency
- Retrieval behavior
- Explanation generation stability
- Failure modes

Monitoring is lightweight but aligned with production observability patterns.

---

### 7.6 Ownership Boundaries

Ownership is explicitly separated:

- Stage 1 — event normalization correctness
- Stage 2 — aggregation grain guarantees
- Stage 3 — canonical publishing contract
- RAG layer — retrieval accuracy and explanation generation

Each layer enforces its own invariants and does not rely on downstream correction.

This separation simplifies debugging, recomputation, and incident response.

---
## 8. Execution Evidence

The platform was executed end-to-end on AWS infrastructure and validated through compute, storage, query, and API layers.

---
### Spark Execution on EC2

Spark aggregation job executed on AWS EC2, writing partitioned Parquet outputs to S3.

![Spark Execution](docs/images/spark_ec2_execution.png)

---

### Partitioned Parquet Output in S3

Daily cost fact table stored as partitioned Parquet files in S3 (`usage_date` partition).

![S3 Partitioned Output](docs/images/s3_partitioned_output.png)

---

### Athena Query on Canonical Dataset

Canonical dataset queried via Amazon Athena over partitioned Parquet.

![Athena Query](docs/images/athena_query.png)

---

### RAG API Execution

FastAPI service retrieving contextual anomaly and generating explanation.

![RAG API](docs/images/rag_api_response.png)

---

## 9. Design Decisions & Tradeoffs

This platform was intentionally designed with finance-grade correctness, deterministic recomputation, and operational simplicity as primary constraints.

---

### 9.1 Batch vs Streaming

**Decision:** Batch processing (T+1 model)

Cloud billing data is correction-heavy and frequently retroactively adjusted.  
Streaming ingestion would introduce state management complexity without improving financial accuracy.

Batch processing enables:

- Deterministic recomputation
- Partition-scoped backfills
- Clear publish cutoffs (T+1)
- Simplified validation and failure handling

For finance workloads, correctness and auditability take precedence over low-latency ingestion.

---

### 9.2 Partitioning Strategy

**Partition Key:** `usage_date`

All fact tables are partitioned by `usage_date` in S3.

This enables:

- Bounded recomputation (only affected dates are overwritten)
- Efficient Athena partition pruning
- Predictable daily compute cost
- Atomic partition-level publish semantics

Backfills overwrite only impacted partitions, avoiding full-table rewrites and minimizing scan cost.

---

### 9.3 Execution Environment — EC2 over EMR

**Decision:** Spark on EC2 (local mode)

The objective was to demonstrate:

- Explicit Spark configuration control
- Direct S3 integration via `s3a://`
- IAM-based secure access patterns
- Dependency management (Hadoop AWS packages)

EMR was intentionally avoided to:

- Reduce infrastructure overhead
- Stay within AWS free-tier constraints
- Keep focus on data modeling and correctness

The Spark code remains portable to EMR without modification.

---

### 9.4 Parquet over CSV

**Decision:** Parquet

Reasons:

- Columnar storage and predicate pushdown
- Reduced storage footprint
- Schema enforcement
- Native Athena compatibility
- Alignment with lakehouse patterns

Partitioned Parquet in S3 enables scalable analytical querying.

---

### 9.5 Cost vs Freshness

**Decision:** Cost-optimized batch architecture

This system targets finance reporting use cases where:

- Determinism
- Reproducibility
- Backfill safety
- Audit guarantees

are more important than real-time ingestion.

The architecture intentionally avoids complexity that does not materially improve financial correctness.

---

## 10. Key Engineering Learnings

Building this platform surfaced several real-world cloud engineering constraints:

- **Filesystem abstraction differences** (local vs S3) required explicit `s3a://` configuration and Hadoop AWS package integration.
- **Spark dependency management** required injection of Hadoop S3A and AWS SDK bundles for cloud storage compatibility.
- **IAM and service access policies** blocked Bedrock invocation, reinforcing the importance of least-privilege configuration.
- **Marketplace model constraints** required redesigning the LLM layer to be provider-agnostic.
- **Git remote conflicts** required merge + pull strategy correction.
- **SSH key permission hardening** (`chmod 400`) was required for secure EC2 access.

These issues reflect realistic infrastructure debugging scenarios encountered in production cloud environments.

---

## 11. Business Impact

This platform demonstrates how structured data engineering and GenAI capabilities can be combined to improve financial observability.

### Operational Impact

- Finance-grade daily cost reporting at the `account_id + usage_date` grain
- Deterministic partition-scoped backfills for audit safety
- No full-table recomputation during corrections
- Athena-compatible, cost-efficient analytical querying

### Analytical Impact

- Automated anomaly context generation from historical cost patterns
- Vector-based retrieval (FAISS) for relevant anomaly matching
- LLM-generated explanations via provider-agnostic abstraction
- Structured JSON API enabling downstream integration

### Efficiency Gains (Conceptual)

The RAG layer reduces manual anomaly investigation time by:
- Retrieving historical analogs automatically
- Generating contextual summaries instead of raw metrics
- Providing reproducible explanation logic

---

## 12. Project Highlights

- Canonical data modeling with explicit grain ownership
- Partitioned lakehouse design (Spark + S3 + Athena)
- Deterministic recomputation and backfill safety
- Vector search integration using FAISS
- LLM abstraction layer decoupled from provider
- Observable FastAPI service with latency tracking

This project combines production-oriented data engineering patterns with modern GenAI infrastructure design.

---

