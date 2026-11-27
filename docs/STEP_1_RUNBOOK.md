# Step 1 runbook: data → Bronze → Silver → controls

Status labels follow the brief: **Implemented** = runs in this repo. **Simulated** = runs, but stands in for a
real integration (e.g. notifications). **Designed** = documented only.

## 0. One-time setup (5 min)

1. Push this folder to a Git repo (GitHub/Azure DevOps).
2. In Databricks: **Workspace → Create → Git folder** → paste the repo URL. Notebooks under `/notebooks`
   import automatically (they start with `# Databricks notebook source`).
3. Attach to **Serverless** compute (or a DBR 15.4 LTS+ cluster with Unity Catalog).
4. If `CREATE CATALOG` is not allowed for you, create the catalog `examiner_compliance` once in
   Catalog Explorer (or ask an admin). Everything else is created by the notebooks.

Repo layout used by the notebooks (paths are resolved relative to `/notebooks`):

```
src/examiner/synthetic_data.py      generator (pure Python, unit-tested)
src/controls/sql/<test_type>.sql    one SQL file per control test type
resources/policies/*.md             7 synthetic policy documents (used by RAG later)
notebooks/00..03                    run in order
tests/test_synthetic_data.py        pytest
```

## 1a. Generate synthetic data — `notebooks/00_generate_data`  *(Implemented)*

Run all cells. It creates the schemas `bronze, silver, gold, controls, features, ml, serving, monitoring, reference`,
the volumes `raw_files, checkpoints, evidence`, and writes:

| File | Rows | Notes |
|---|---|---|
| `customer_account.csv` | 197 | messy statuses (`Active `, `INACTIVE`), mixed date formats |
| `employee_access.csv` | 140 | messy flags (`Y/Yes/TRUE/1`) |
| `regulatory_reporting.csv` | 22 | one report missing, one late, one unapproved, one unreconciled |
| `control_catalogue.csv` | 8 | 6 active controls + 1 inactive + 1 duplicate |
| `compliance_issues.csv` | 27 | includes **ISS-3025 → EXC-10025**, overdue |
| `transactions/transactions_2026_{08,09,10}.json` | ~350 each | JSON lines, read by Auto Loader |
| `reference/*.csv` | | SoD rules, high-risk countries, FX rates, report calendar |
| `policies/*.md` | 7 | title / version / effective date / owner / classification front matter |

How the fake data works (if you're asked in the interview):
* **Seeded** `random.Random(...)` per dataset, so the same IDs come out every run and tests can assert on them.
* **Background data is generated to be clean on purpose.** Random amounts never land in the 9,000–9,999 structuring
  band, nobody random gets both SoD roles, and privileged reviews are recent. So **every exception is one you planted**.
* **Plants are written explicitly** after the random rows (Bob, Alice, the 4 deposits, ...).
* **Mess is added on purpose** (casing, `C$`, US dates) so Silver standardization has something to do.
* **Duplicates** = `dict(rows[i])` copies; **malformed** = copies with a broken field (`"N/A"` amount,
  `"31/02/2025"`, missing key).

## 1b-i. Bronze — `notebooks/01_bronze`  *(Implemented)*

Run all cells. Then **run it again**: row counts must not change.

* Metadata on every row: `ingestion_timestamp, batch_id, source_file, source_system, record_hash` (+ `dup_seq`).
* `MERGE ... WHEN NOT MATCHED THEN INSERT` keyed on `(source_file, record_hash, dup_seq)`, so reruns are idempotent.
* Transactions: `spark.readStream.format("cloudFiles")` + `foreachBatch(MERGE)` + `trigger(availableNow=True)`.

**Incremental demo:** uncomment the last cell of `00` (`write_late_batch`), run it, rerun `01`. Only
`transactions_2026_11_01_to_25.json` is read (see the per-file query at the bottom of `01`).

## 1b-ii. Silver — `notebooks/02_silver`  *(Implemented)*

Expected `silver.quarantine_records` after the first load:

| source_table | reasons |
|---|---|
| bronze_customer_account | 2 × DUPLICATE_RECORD, MISSING_KEY, INVALID_OPEN_DATE, INVALID_STATUS |
| bronze_employee_access | 2 × DUPLICATE_RECORD, MISSING_KEY, INVALID_GRANTED_DATE, INVALID_PRIVILEGED_FLAG |
| bronze_transaction | 2 × DUPLICATE_RECORD, MISSING_TRANSACTION_ID, INVALID_AMOUNT, INVALID_TIMESTAMP |
| bronze_regulatory_reporting | DUPLICATE_RECORD, INVALID_AMOUNT, MISSING_KEY |
| bronze_control_catalogue | DUPLICATE_RECORD |
| bronze_compliance_issues | DUPLICATE_RECORD, INVALID_OPENED_DATE |

After the late batch: the stale `PENDING` version is **ignored** (older `last_modified_ts`). The `REVERSED`
version **wins**. The 3 late October rows get `is_late_arriving = true`. The cross-file copy is quarantined.

## 1c. Control framework — `notebooks/03_run_controls`  *(Implemented; notifications Simulated)*

Widgets default to the previous month (on 2026-11-26 that's **2026-10-01 → 2026-10-31**, the agent scenario's period).

Expected result for October 2026:

| Control | Status | Exceptions |
|---|---|---|
| CTL-ACC-001 access recertification | FAIL | **EXC-10025** (EA-0201, Bob admin, *repeat*, linked ISS-3025), EA-0202 (Bob initiator), EA-0206 (DBA review 230 days old) |
| CTL-ACC-002 segregation of duties | FAIL | E1098 Alice (SOD-001) |
| CTL-REG-001 reporting completeness | FAIL | LARGE_CASH (late), CAPITAL_ADEQUACY (not approved), LIQUIDITY_COVERAGE (missing) |
| CTL-TXN-001 threshold approval | FAIL | TXN-2026109001 (no approver), TXN-2026109002 (self-approved) |
| CTL-TXN-002 unusual pattern | FAIL | CUST-09002 (structuring), CUST-09003 (IR) |
| CTL-TXN-003 dormant activity | FAIL | TXN-2026109008 |

New exceptions get IDs from EXC-10026 upward. Order depends on the run, so quote IDs from the table,
not from this doc. EXC-10025 is always Bob's admin access.

Rerun checks:
* Same period again → `gold_control_test_results` gains 6 new rows (history). `gold_compliance_exceptions`
  row count is unchanged; `detected_periods` does not grow.
* `period_start=2026-09-01, period_end=2026-09-30` → September history (Bob already terminated on 09-30, so
  EA-0201 is detected there too and `is_repeat` stays true).
* `control_ids=CTL-ACC-001` → reruns a single control.

Tables produced: `gold.gold_control_test_results`, `gold.gold_compliance_exceptions`, `gold.gold_compliance_evidence`,
`controls.notification_outbox`, plus JSON evidence files under `/Volumes/examiner_compliance/reference/evidence/<run>/`.

## 2. Risk scoring — `04a_toy_mlflow` then `04_risk_scoring`  *(Implemented; labels Simulated)*

1. **Toy first (15 min):** run `04a_toy_mlflow`. Open Experiments → compare the two runs; open Catalog →
   `ml.toy_escalation_model` to see the version and `@champion` alias.
2. Run `04_risk_scoring`. It builds `features.exception_features`, trains and logs to the MLflow experiment
   `examiner_risk_scoring`, registers `ml.exception_risk_model` in UC, sets `@champion`, scores every open exception
   into `ml.exception_risk_scores` (history), and writes `risk_score` / `risk_band` onto `gold_compliance_exceptions`.

Expected ranking (approximate): **EXC-10025 ≈ 90 CRITICAL** (severity, overdue, repeat) > Alice SoD, DBA review,
Bob initiator ≈ 63–66 HIGH > 72k self-approval, missing LCR ≈ 56–58 HIGH > structuring ≈ 53 MEDIUM > dormant ≈ 44 MEDIUM.
See `docs/MODEL_CARD.md`.

## 3. Gold data products — `05_gold_products`  *(Implemented)*

1. Build history for trends: in `03_run_controls` set the widgets to `2026-08-01`/`2026-08-31` → Run all, then
   `2026-09-01`/`2026-09-30` → Run all. Expected exceptions: August 2 (Alice SoD, DBA review), September 5
   (+ Bob EA-0201 and EA-0202, + the 2026-08 suspicious-activity report variance), October about 14.
2. Rerun `04_risk_scoring` with `retrain=false`, so September's new exceptions get scores.
3. Run `05_gold_products` (a SQL notebook).

Checks:
* `gold_remediation_status`: ISS-3025 / EXC-10025 `is_overdue = true` (target 2026-10-31); ISS-3019 open but not overdue;
  the other open exceptions appear as `EXCEPTION_WITHOUT_ISSUE` with an implied SLA target date.
* `gold_compliance_dashboard`: 6 controls × 3 periods with `is_control_row = true`; `exception_count_change` populated
  for September and October.
* `gold_examiner_summary`, `ACCESS_CONTROL` for October: 2 controls evaluated, 2 failed, EXC-10025 first in
  `high_risk_exceptions`, 1 overdue item, `human_review_status = PENDING_HUMAN_REVIEW`.

## 4. Policy RAG — `06a_toy_rag` then `06_rag_policies`  *(Implemented)*

1. **Practice:** `06a_toy_rag`. BM25 finds "privileged access recertified" but misses "re-check what admins can do";
   embeddings find both; INTERNAL clearance gets NOT_FOUND for the confidential chunk.
2. `06_rag_policies` with widgets: `retriever=vector_search`, `embedding_endpoint` = your embedding endpoint,
   `llm_endpoint` = your chat endpoint (blank = quote-only answers). First run creates Vector Search endpoint `examiner_vs`
   (5–15 min) and index `serving.policy_chunks_index`.
3. Checks: 45 chunks from 7 documents; the 5 brief questions answered with `[POL-… v… §…]` citations; dress-code question
   and INTERNAL-clearance privileged-access question return NOT_FOUND; `ml.rag_evaluation` has 8 rows per retriever;
   `monitoring.rag_query_log` has one row per question. Local baseline (BM25 + extractive): 8/8 correct, citation accuracy
   1.0, groundedness 1.0.

## Not covered yet (next steps)
Examiner agent, Examiner agent, governance tags
and masks, dashboards, Asset Bundle/CI. `risk_score` is deliberately NULL until the scoring step.

## Known limitations of step 1
* Dormant-account test uses current account status, not point-in-time status (would need SCD2 in production).
* Silver re-reads all of Bronze each run. Fine at this size; production would use Change Data Feed / streaming.
* Exception IDs are assigned in the driver. Fine for a single runner; production would use an identity column or a sequence table.
