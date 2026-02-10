# Product Requirements Document: Claims Processing Analytics Pipeline 

**Document generated with AI Assistance**

## Executive Summary

This document describes a PySpark-based data pipeline designed to transform raw insurance claims data into actionable analytics. The pipeline processes three primary data sources (claims, claim status logs, payouts) through a series of SQL transformations to produce KPI dashboards focused on operational efficiency, payment performance, and workflow bottleneck identification.

The pipeline is modular, testable, and designed for iterative analytics in a Spark SQL environment.

---

## Data Architecture

### Source Data (Raw Layer)

The pipeline ingests three CSV files representing operational claims data:

| **Dataset** | **File** | **Key Fields** | **Granularity** |
|------------|----------|----------------|-----------------|
| **Claims** | `sample_claims.csv` | ClaimId, ClaimReference, QuoteId, InsuranceVertical, ClaimTypeCategory, ReportedDate, LossDate, PolicyStartDate, PolicyEndDate | One row per claim |
| **Claim Status Logs** | `sample_claim_logs.csv` | ClaimId, ClaimReference, TransitionDate, TransitionType, CurrentStatusCategory, CurrentStatusLabel | One row per status transition (temporal event log) |
| **Claim Payouts** | `sample_claim_payouts.csv` | ClaimReference, QuoteId, TransactionDate, PaymentReference, PayoutStatus, PayoutAmount | One row per payment transaction |

**Key Identifiers:**
- `ClaimId`: Unique internal claim identifier
- `ClaimReference`: External claim reference (joins to payouts)
- `QuoteId`: Insurance quote identifier
- `PaymentReference`: Unique payment transaction identifier

---

## Pipeline Execution Flow

The pipeline follows a **three-tier transformation pattern**: Raw → Fact → Aggregation → KPI.

### Execution Order (from `process_data.py`)

sql_execution_order = [
    # LAYER 1: ENHANCED RAW FACTS
    "fact_claim.sql",           # Core claim attributes + policy validation flags
    "fact_claim_status.sql",    # Status transitions with sequencing & lag
    "fact_claim_payout.sql",    # Payment transactions with sequencing
    
    # LAYER 2: STEP AGGREGATION
    "agg_logs.sql",             # Claim-level operational metrics
    "agg_payout.sql",           # Claim-level payment metrics
    
    # LAYER 3: INTERMEDIATE MASTER TABLE
    "int_claim_master.sql",     # Unified claim master with ops + finance metrics
    
    # LAYER 4: KPI REPORTS
    "kpi_step_timing.sql",
    "kpi_workflow_efficiency.sql",
    "kpi_bottleneck_analysis.sql",
    "kpi_friction_correlation.sql",
    "kpi_summary_dashboard.sql"
]

**Dependencies:** Each layer depends on the completion of the previous layer. The pipeline creates temporary Spark SQL views that are consumed by downstream queries.

---

## Data Model & Transformations

### LAYER 1: Fact Tables (Enhanced Raw)

#### **fact_claim**
**Purpose:** Core claim attributes with derived policy coverage validation flags.

**Source:** `raw_claims`

**Key Transformations:**
- Standardizes column names (snake_case)
- Adds `report_date_within_policy` flag (validates reported date falls within policy coverage period)
- Adds `loss_date_within_policy` flag (validates loss date falls within policy coverage period)
- Adds `is_current` flag (policy end date >= current date)

**Output Schema:**
claim_id, claim_reference, quote_id, insurance_vertical, claim_type_category,
reported_date, loss_date, policy_start_date, policy_end_date,
report_date_within_policy, loss_date_within_policy, is_current

---

#### **fact_claim_status**
**Purpose:** Event log of claim status transitions with temporal sequencing.

**Source:** `raw_claim_logs`

**Key Transformations:**
- Converts `TransitionDate` to TIMESTAMP
- Assigns `step_sequence` (ROW_NUMBER ordered by transition time per claim)
- Calculates `prev_transition_ts` (LAG of transition timestamp)
- Derives `days_in_step` (DATEDIFF between current and previous transition)
- Flags `is_current_step` (latest transition per claim)

**Output Schema:**
claim_id, claim_reference, transition_ts, transition_type, status_category, status_label,
step_sequence, prev_transition_ts, days_in_step, is_current_step

**Business Logic:**
- `step_sequence` enables workflow progression analysis
- `days_in_step` measures dwell time in each status
- `is_current_step` identifies active claim state

---

#### **fact_claim_payout**
**Purpose:** Payment transaction log with temporal sequencing.

**Source:** `raw_claim_payouts`

**Key Transformations:**
- Converts `TransactionDate` to TIMESTAMP
- Casts `PayoutAmount` to DOUBLE
- Assigns `payment_sequence` (ROW_NUMBER ordered by transaction time per claim)

**Output Schema:**
claim_reference, quote_id, transaction_ts, payment_reference, 
payout_status, payout_amount_usd, payment_sequence

---

### LAYER 2: Aggregation Tables

#### **agg_logs**
**Purpose:** Claim-level operational workflow metrics aggregated from status transitions.

**Source:** `fact_claim_status`

**Key Metrics:**

| **Metric** | **Definition** | **Business Use** |
|-----------|----------------|------------------|
| `first_transition_ts` | Earliest status transition timestamp | Claim start time |
| `last_transition_ts` | Latest status transition timestamp | Current workflow position |
| `reached_settled_flag` | 1 if claim reached "Settled" status | Settlement success rate |
| `reached_settled_flag_first_ts` | First timestamp claim reached "Settled" | Time to settlement |
| `reached_payment_complete_flag` | 1 if claim reached "Payment Complete" or "Paid" | Payment completion rate |
| `avg_days_between_steps` | Average days between consecutive status changes | Workflow velocity |
| `total_steps_count` | Total number of status transitions | Workflow complexity |
| `cnt_action_required_status` | Count of "Action Required" statuses | Customer friction indicator |
| `cnt_doc_missing_status` | Count of document/missing-related statuses | Documentation friction |

**Grouping:** `claim_id`

**Critical Filters:**
- `avg_days_between_steps` excludes first step (no previous transition)
- Pattern matching uses `LOWER()` and `LIKE` for robustness

---

#### **agg_payout**
**Purpose:** Claim-level payment performance metrics aggregated from payment transactions.

**Source:** `fact_claim_payout`

**Key Metrics:**

| **Metric** | **Definition** | **Business Use** |
|-----------|----------------|------------------|
| `has_any_payout_flag` | 1 if claim has any payout transaction | Payout coverage |
| `has_completed_payout_flag` | 1 if claim has completed payout | Successful payment rate |
| `total_payout_amount_usd` | Sum of completed payout amounts | Claim financial exposure |
| `total_payouts` | Count of distinct completed payment references | Multiple payout handling |
| `total_payout_attempts` | Count of distinct payment references (all statuses) | Payment retry rate |
| `avg_payout_days` | Average days from first transaction to each payout | Payment processing time |
| `min_payout_steps_to_first_payout` | Rank of first completed payment | Initial payment efficiency |
| `max_payout_steps_to_last_payout` | Rank of last completed payment | Final payment position |
| `time_to_latest_payout` | Days from first to last transaction | Total payment window |
| `first_transaction_ts` | Earliest transaction timestamp | Payment initiation time |
| `last_transaction_ts` | Latest transaction timestamp | Payment finalization time |
| `first_payout_ts` | Earliest completed payout timestamp | First successful payment |
| `last_payout_ts` | Latest completed payout timestamp | Most recent payment |

**Grouping:** `claim_reference`

**Design Notes:**
- Uses window functions (`MIN/MAX OVER`) to compute per-claim timestamps in CTE
- `payment_status_rank` tracks attempt sequence (useful for retry analysis)
- Filters completed payouts using `UPPER(payout_status) LIKE '%COMPLETED%'`

---

### LAYER 3: Intermediate Master Table

#### **int_claim_master**
**Purpose:** Unified claim-level master table joining operational and financial metrics with clear domain prefixes.

**Sources:** `fact_claim`, `agg_logs`, `fact_claim_status`, `agg_payout`

**Join Strategy:**
- LEFT JOIN from `fact_claim` (preserves all claims)
- Joins `agg_logs` on `claim_id`
- Joins `fact_claim_status` filtered to `is_current_step = true` (current status only)
- Joins `agg_payout` on `claim_reference`

**Column Prefixes:**
- `ops_*`: Operational workflow metrics
- `fin_*`: Financial/payment metrics
- `biz_*`: Cross-domain composite metrics

**Key Derived Metrics:**

| **Metric** | **Formula** | **Business Use** |
|-----------|-------------|------------------|
| `ops_is_terminal_status` | Status in (closed, declined, settled, withdrawn, invalid, duplicate) | Identifies completed workflows |
| `ops_time_to_latest_status` | Days from first to last transition | Total workflow duration |
| `biz_time_to_latest_payout_from_settled` | Days from settled timestamp to last payout | Settlement-to-payment lag |
| `biz_time_to_latest_payout_from_claim_start` | Days from claim start to last payout | End-to-end payment cycle |

**Output Schema (40+ columns):**
- Core claim attributes (claim_id, insurance_vertical, etc.)
- Current status indicators (ops_latest_status_category, ops_days_in_latest_step)
- Operational metrics (ops_time_to_latest_status, ops_reached_settled_flag, etc.)
- Financial metrics (fin_total_payout_amount_usd, fin_has_completed_payout_flag, etc.)
- Composite metrics (biz_time_to_latest_payout_from_settled)

---

### LAYER 4: KPI Reports

#### **kpi_summary_dashboard**
**Purpose:** Executive-level summary KPIs by insurance vertical.

**Source:** `int_claim_master`

**Metrics Computed:**
- `total_claims`: Count of claims
- `avg_resolution_days`: Average time to latest status
- `avg_workflow_steps`: Average number of status transitions
- `settled_rate_pct`: % of claims reaching "Settled" status
- `payout_rate_pct`: % of claims with completed payout
- `total_payout_usd`: Total payout amount disbursed
- `avg_days_current_status`: Average days in current status
- `avg_doc_friction`: Average count of document-related issues
- `claims_stuck_14days`: Count of non-terminal claims stuck >14 days

**Grouping:** `insurance_vertical`

**Business Use:** High-level dashboard for monitoring claim processing performance across insurance product lines.

---

## Technical Implementation

### Pipeline Execution (`process_data.py`)

**Key Steps:**

1. **Initialize Spark Session**
   spark = SparkSession.builder.getOrCreate()

2. **Load Raw Data**
   - Reads CSV files from `raw_data/` directory
   - Creates temporary views: `raw_claims`, `raw_claim_logs`, `raw_claim_payouts`

3. **Execute SQL Transformations**
   - Iterates through `sql_execution_order` list
   - Reads SQL file content using `read_sql_file()` helper
   - Executes SQL to create temporary views
   - Prints progress indicators

4. **Export Results**
   - Retrieves all temporary views using `SHOW TABLES`
   - Writes each view to CSV in `spark_output/` directory (with Spark metadata)
   - Copies clean CSV files to `final_output/` directory using `save_spark_csv_to_output_csv()`
   - Cleans directories before writing using `clear_directory()` helper

### Helper Functions (`helper_functions.py`)

**`read_sql_file(sql_file_path)`**
- Reads SQL file content as UTF-8 string
- Strips whitespace
- Returns SQL query text

**`save_spark_csv_to_output_csv(input_dir, output_dir)`**
- Navigates Spark output folder structure
- Identifies CSV files (ignores metadata)
- Copies CSV files to clean output directory
- Logs copy operations

**`clear_directory(path)`**
- Recursively deletes all files and subdirectories
- Uses `os.walk(topdown=False)` for bottom-up traversal
- Preserves parent directory structure

---

## Data Flow Diagram

## Data Flow Diagram

┌─────────────────────────────────────────────────────────┐
│                    RAW DATA (CSV)                       │
│  • sample_claims.csv                                    │
│  • sample_claim_logs.csv                                │
│  • sample_claim_payouts.csv                             │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│              LAYER 1: FACT TABLES                       │
│  • fact_claim (claim attributes + validation flags)     │
│  • fact_claim_status (status transitions + sequencing)  │
│  • fact_claim_payout (payment transactions)             │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│           LAYER 2: AGGREGATIONS                         │
│  • agg_logs (operational metrics per claim)             │
│  • agg_payout (payment metrics per claim)               │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│        LAYER 3: INTERMEDIATE MASTER                     │
│  • int_claim_master (unified claim view with ops +      │
│    finance metrics)                                     │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│            LAYER 4: KPI REPORTS                         │
│  • kpi_summary_dashboard (executive metrics by vertical)│
│  • kpi_step_timing                                      │
│  • kpi_workflow_efficiency                              │
│  • kpi_bottleneck_analysis                              │
│  • kpi_friction_correlation                             │
└─────────────────────────────────────────────────────────┘

**Data Dependencies:**
- `int_claim_master` requires: `fact_claim`, `agg_logs`, `fact_claim_status`, `agg_payout`
- `agg_logs` requires: `fact_claim_status`
- `agg_payout` requires: `fact_claim_payout`
- All KPI reports require: `int_claim_master`

---

## Key Business Logic

### Metric Calculations

**Time to Latest Status (ops_time_to_latest_status):**
ABS(DATEDIFF(first_transition_ts, last_transition_ts))
Measures total workflow duration from claim initiation to current state.

**Settled Rate (settled_rate_pct):**
ROUND(100.0 * AVG(ops_reached_settled_flag), 1)
Percentage of claims that progressed to "Settled" status.

**Time to Latest Payout from Settled (biz_time_to_latest_payout_from_settled):**
DATEDIFF(reached_settled_flag_first_ts, last_payout_ts)
Lag between settlement approval and payment disbursement.

**Claims Stuck >14 Days:**
SUM(CASE WHEN ops_days_in_latest_step > 14 AND ops_is_terminal_status = 0 THEN 1 ELSE 0 END)
Identifies active claims with no status change for 14+ days (bottleneck alert).

---

## Status Categories & Terminal States

### Terminal Status Categories
Claims in these states are considered workflow-complete:
- `Closed`
- `Declined`
- `Settled`
- `Withdrawn`
- `Invalid`
- `Duplicate Complaint`

### Active Workflow Categories
Claims in these states require ongoing monitoring:
- `Submitted`
- `Under Assessment`
- `Action Required`
- `Processing`
- `Payment Processing`
- `Payment Complete` (terminal for payment, but may require follow-up)

---

## Sample Identifiers (for Testing)

**Sample Claim ID:**  
`faa5f524f46ca8fad25181a5ab4afc1b5cae77c478f279aa92079367093bfa56`

**Sample Claim Reference:**  
`fa5b517753e393c3fd72493d1587b8ca8de914206281a237946ad0c18fb8b4a9`

These identifiers can be used for spot-checking transformations and debugging pipeline execution.

---

## Output Artifacts

### Generated CSV Files (final_output/)
- `fact_claim.csv`
- `fact_claim_status.csv`
- `fact_claim_payout.csv`
- `agg_logs.csv`
- `agg_payout.csv`
- `int_claim_master.csv`
- `kpi_summary_dashboard.csv`
- `kpi_step_timing.csv`
- `kpi_workflow_efficiency.csv`
- `kpi_bottleneck_analysis.csv`
- `kpi_friction_correlation.csv`

Each CSV contains headers and is ready for downstream consumption (BI tools, visualization, further analysis).

---

## Design Principles

1. **Modularity:** Each SQL file performs a single, well-defined transformation. Dependencies are explicit via execution order.

2. **Idempotency:** Queries use `CREATE OR REPLACE TEMP VIEW` to enable re-execution without side effects.

3. **Defensive Coding:** 
   - Uses `COALESCE()` for LEFT JOIN null handling
   - Uses `LOWER()` and `LIKE` for case-insensitive pattern matching
   - Casts data types explicitly (TIMESTAMP, DOUBLE)

4. **Domain Separation:** Column prefixes (`ops_`, `fin_`, `biz_`) clearly separate operational, financial, and composite metrics.

5. **Temporal Integrity:** Window functions (`ROW_NUMBER`, `LAG`, `MIN/MAX OVER`) preserve temporal ordering and enable lag analysis.

6. **Output Cleanliness:** Helper functions ensure CSV output directories are cleaned before writing (prevents stale data accumulation).

---

## Appendix: SQL File Reference

| **File** | **Type** | **Purpose** | **Key Outputs** |
|----------|----------|-------------|-----------------|
| `fact_claim.sql` | Fact | Claim attributes + policy validation | claim_id, insurance_vertical, policy flags |
| `fact_claim_status.sql` | Fact | Status transitions + sequencing | step_sequence, days_in_step, is_current_step |
| `fact_claim_payout.sql` | Fact | Payment transactions + sequencing | payment_sequence, payout_amount_usd |
| `agg_logs.sql` | Aggregation | Operational workflow metrics | reached_settled_flag, total_steps_count, friction counts |
| `agg_payout.sql` | Aggregation | Payment performance metrics | total_payout_amount_usd, avg_payout_days |
| `int_claim_master.sql` | Master | Unified claim view (ops + finance) | 40+ columns with ops_*, fin_*, biz_* prefixes |
| `kpi_summary_dashboard.sql` | KPI | Executive dashboard by vertical | settled_rate_pct, claims_stuck_14days |

