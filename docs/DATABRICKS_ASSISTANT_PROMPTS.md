# Prompts for building Step 1 with Databricks Assistant

**Which tool:** use **Databricks Assistant in agent mode**, from inside a notebook (Assistant panel → Agent).
It can create cells, run them and fix its own errors. *Genie spaces* are a different feature: they answer
questions over existing tables, so they are useful **after** Gold exists (prompt 6), not for building pipelines.

**How to use these:** one prompt per notebook, run in order. Paste the "Context" block at the top of every
prompt. Review each cell before running. After each prompt, use the "verify" prompt.
If you already imported this repo, you don't need prompts 1–4. Use them to regenerate or extend.

---

## Context block (paste first, every time)

```
Project: "Examiner" compliance-monitoring prototype on Databricks, Unity Catalog catalog `examiner_compliance`
with schemas bronze, silver, gold, controls, features, ml, serving, monitoring, reference.
Raw files live in the volume /Volumes/examiner_compliance/reference/raw_files/.
Compute: serverless. Use PySpark + Delta + Spark SQL only. Use DeltaTable MERGE, never plain append, for
Bronze and Silver. Do not hardcode secrets. Use synthetic data only. Keep code readable, with short comments.
Label each feature in a markdown cell as Implemented, Simulated or Designed.
```

## Prompt 1 — synthetic data (notebook `00_generate_data`)

```
Create a notebook that generates synthetic compliance data with plain Python and pandas (no Spark for
generation) and writes it to /Volumes/examiner_compliance/reference/raw_files/. Use a fixed random seed per
dataset so output is identical on every run. First cell: CREATE CATALOG/SCHEMA IF NOT EXISTS for all schemas
listed above and CREATE VOLUME IF NOT EXISTS for raw_files, checkpoints and evidence in the reference schema.

Datasets (CSV unless stated):
1. customer_account.csv (~200 rows): customer_id, account_id, customer_name, risk_rating (LOW/MEDIUM/HIGH),
   country_code, account_status (ACTIVE/DORMANT/CLOSED, with messy variants like "Active ", "INACTIVE"),
   account_open_date, account_close_date (mix ISO and MM/dd/yyyy formats), product_type.
2. transactions as JSON-lines, one file per month for 2026-08, 2026-09, 2026-10 in raw_files/transactions/
   (~350 per month): transaction_id, customer_id, account_id, transaction_timestamp, transaction_type,
   amount (string), currency_code (messy: "cad", "C$", "US$"), country_code, channel, status, source_system,
   initiated_by, approved_by, approval_reference, last_modified_ts, extract_window_start, extract_window_end.
   Any amount >= 6000 must have an approver different from the initiator. Keep random amounts out of the
   9,000-9,999 range, only use ACTIVE accounts, and use no high-risk countries.
3. employee_access.csv: access_id, employee_id, employee_name, employee_role, business_unit, access_role,
   application_name (CORE_BANKING, PAYMENTS_HUB, GENERAL_LEDGER, CRM, DATA_WAREHOUSE), access_status
   (ACTIVE/REVOKED), granted_date, access_review_date, termination_date, privileged_access_flag (messy Y/Yes/TRUE/1),
   manager_id. Nobody random may hold PAYMENT_INITIATOR+PAYMENT_APPROVER or JOURNAL_PREPARER+JOURNAL_APPROVER.
   Privileged access reviewed after 2026-08-20; standard access reviewed after 2026-01-15.
4. regulatory_reporting.csv plus reference/regulatory_report_calendar.csv (4 report types x periods
   2026-06..2026-10, due on the 15th of the following month): report_id, report_type, reporting_period,
   reported_amount, source_system_amount, submission_status, submission_timestamp, approval_status, approver_id.
5. control_catalogue.csv: control_id, control_name, control_domain, control_description, control_owner, severity,
   test_type, threshold, tolerance_pct, frequency, population, policy_reference, remediation_action, active_flag.
   Six active controls: CTL-TXN-001 threshold_exception (10000), CTL-TXN-002 pattern_detection (10000),
   CTL-TXN-003 dormant_activity (5000), CTL-ACC-001 access_recertification (90 days, Critical),
   CTL-ACC-002 segregation_of_duties (Critical), CTL-REG-001 reporting_completeness (0.01). Plus one inactive.
6. compliance_issues.csv: issue_id, exception_id, control_id, entity_id, issue_title, issue_status, issue_owner,
   business_unit, severity, opened_date, target_remediation_date, actual_remediation_date, validation_status,
   remediation_action. 24 closed historical issues EXC-10001..10024.
7. reference/sod_rules.csv, reference/high_risk_countries.csv (IR, KP, MM, SY), reference/fx_rates.csv.

Plant exactly these violations:
- Bob Martin E1099, PAYMENTS_OPERATIONS, terminated 2026-09-30, still ACTIVE on EA-0201 CORE_BANKING ADMIN
  (privileged) and EA-0202 PAYMENTS_HUB PAYMENT_INITIATOR; EA-0203 CRM correctly REVOKED.
- Issue ISS-3025 with exception_id EXC-10025, control CTL-ACC-001, entity_id EA-0201, status Open,
  opened 2026-10-05, target 2026-10-31 (now overdue), validation NOT_STARTED.
- Alice Chen E1098 holds PAYMENT_INITIATOR (EA-0204) and PAYMENT_APPROVER (EA-0205): SoD conflict.
- EA-0206: privileged DBA access last reviewed 2026-03-15.
- October transactions: TXN-2026109001 48,500 CAD wire with no approver; TXN-2026109002 72,000 CAD wire
  initiated and approved by E1098; TXN-2026109003..06 four BRANCH cash deposits of 9,400-9,850 CAD by
  CUST-09002 within 5 days; TXN-2026109007 wire to IR; TXN-2026109008 25,000 withdrawal on DORMANT ACC-900001.
- Period 2026-09: LARGE_CASH_TXN_REPORT submitted 2026-10-19 (due 10-15), CAPITAL_ADEQUACY_RETURN
  approval PENDING, LIQUIDITY_COVERAGE_RETURN missing.
- In every dataset add 1-2 exact duplicate rows and 1-3 malformed rows (missing key, "N/A" amount,
  "31/02/2025" date, invalid status).
Also add a function write_late_batch() that writes a November transactions file containing new November
rows, 3 late-arriving October rows, an older (stale) version of an existing October transaction, a newer
version of another one with status REVERSED, a cross-file duplicate and a row with currency "XYZ".
Print the row count of every file written.
```

**Verify:** `Read each file back with pandas and show me: Bob's access rows, Alice's roles, the 2026-10
transactions with amount >= 10000 and no independent approver, and the duplicate count per file. Confirm the
only planted violations are the ones I listed.`

## Prompt 2 — Bronze (notebook `01_bronze`)

```
Create a Bronze ingestion notebook.
- Read customer_account.csv, employee_access.csv, regulatory_reporting.csv, control_catalogue.csv and
  compliance_issues.csv with header=true, inferSchema=false (all strings), and select _metadata.file_path as source_file.
- Write a function add_metadata(df, source_system, batch_id) that adds ingestion_timestamp, batch_id,
  source_system (only if the data has no source_system column), record_hash = sha2 of all business columns
  concatenated with "||" (nulls as "<NULL>", ignore columns starting with "_"), and dup_seq = row_number
  within (source_file, record_hash), so exact duplicates in one file are kept.
- Write merge_bronze(df, table) that creates the table if missing and does an insert-only DeltaTable MERGE
  on (source_file, record_hash, dup_seq). Use df.sparkSession so it works inside foreachBatch.
- Target tables: examiner_compliance.bronze.bronze_customer_account, bronze_employee_access,
  bronze_regulatory_reporting, bronze_control_catalogue, bronze_compliance_issues.
- Load the reference CSVs into examiner_compliance.reference.* with overwrite.
- Transactions: Auto Loader spark.readStream.format("cloudFiles") with cloudFiles.format=json,
  cloudFiles.inferColumnTypes=false, schemaLocation and checkpointLocation under
  /Volumes/examiner_compliance/reference/checkpoints/bronze_transaction, foreachBatch calling merge_bronze
  into bronze_transaction, trigger(availableNow=True), then awaitTermination.
- End with a SQL cell that shows rows per source_file and batch_id and the in-file duplicate count.
```

**Verify:** `Run the notebook twice and prove that the row counts of every bronze table did not change on the second run.`

## Prompt 3 — Silver (notebook `02_silver`)

```
Create a Silver notebook with one generic function to_silver(name, keys, columns, rules, version_col=None, enrich=None):
1. Read examiner_compliance.bronze.bronze_<name>.
2. Rules are a dict {ERROR_CODE: SQL boolean expression on the raw string columns}. Build an array dq_errors
   of failed codes (wrap each in coalesce(expr,false)) and add DUPLICATE_RECORD when the same record_hash was
   already seen (row_number over record_hash ordered by ingestion_timestamp, source_file, dup_seq > 1).
3. Rows with errors: insert-only MERGE into examiner_compliance.silver.quarantine_records (source_table, source_file,
   record_hash, dup_seq, batch_id, dq_errors, raw_record as JSON, quarantined_at).
4. Valid rows: select typed/standardized columns from the `columns` dict of SQL expressions, keep lineage
   columns record_hash, source_file, batch_id, source_system, ingestion_timestamp, optionally call enrich(df).
5. Deduplicate on keys: latest version_col first, then latest ingestion_timestamp.
6. MERGE into examiner_compliance.silver.silver_<name>: update when s.version_col > t.version_col (or when the
   record_hash differs if no version_col), insert when not matched.
Helpers: dates accept ISO and MM/dd/yyyy via try_cast/try_to_timestamp; trim/upper statuses; map INACTIVE->DORMANT,
C$->CAD, US$->USD, COMPLETE->COMPLETED; flags Y/YES/TRUE/1 -> true.
Configure all six tables (customer_account, control_catalogue, transaction, employee_access, regulatory_reporting,
compliance_issues). Transactions use version_col=last_modified_ts and an enrich step that joins reference.fx_rates
to add amount_cad, joins silver_customer_account to add account_found and customer_account_mismatch, and sets
is_late_arriving = transaction_timestamp < extract_window_start. Add a RESCUED_DATA rule if _rescued_data exists.
End with a SQL cell: quarantine counts by source_table and reason.
```

**Verify:** `Show me the quarantine reasons per table, and after running write_late_batch + Bronze + Silver again,
show that the stale PENDING version did not overwrite the October transaction but the REVERSED version did.`

## Prompt 4 — control framework (notebook `03_run_controls`)

```
Build a metadata-driven control runner.
- Widgets: period_start, period_end (default = previous calendar month), control_ids (blank = all active).
- Load active controls from examiner_compliance.silver.silver_control_catalogue.
- For each control, read ../src/controls/sql/<test_type>.sql (one file per test type) and run
  spark.sql(text, args=...) with named parameters :period_start, :period_end, :threshold (only those present
  in the SQL outside comments). Each SQL returns the full population with these columns: entity_id, entity_type,
  business_unit, is_exception, exception_description, expected_condition, actual_condition, variance,
  financial_impact, detection_confidence, event_timestamp, source_table, source_record_id, src_record_hash,
  src_source_file, src_batch_id, related_records.
- population_count = rows, exception_count = rows where is_exception, exception_rate, status PASS/FAIL using
  tolerance_pct, ERROR on any exception (catch it, record the message, continue with the next control).
- Append one row per control to examiner_compliance.gold.gold_control_test_results (test_run_id, control_id,
  period, population_count, exception_count, exception_rate, threshold, test_status, sql_file, sql_sha256,
  execution_timestamp, executed_by, error_message).
- MERGE exceptions into gold.gold_compliance_exceptions on exception_key = control_id|entity_id. Exception IDs
  are stable: reuse an existing ID; else reuse the exception_id from silver_compliance_issues with the same
  control_id and entity_id (mark is_repeat=true and linked_issue_id); else allocate EXC-<max+1>. Track
  detected_periods with array_union. Never change status on update: closing is a human decision.
  Leave risk_score and risk_band NULL.
- For each exception write gold.gold_compliance_evidence (evidence_id, exception_id, source_table,
  source_record_id, evidence_summary, control_logic incl. sql sha, expected/actual/variance, timestamps,
  related_records, policy_reference, evidence_completeness and evidence_gaps based on missing required fields,
  review_status=PENDING_HUMAN_REVIEW, lineage_reference = "<silver table>@v<delta version> <- bronze
  record_hash=..."), plus a JSON copy in /Volumes/examiner_compliance/reference/evidence/<run_id>/<control_id>/.
- For CRITICAL controls with exceptions, append a SIMULATED notification to examiner_compliance.controls.notification_outbox.
Then write the six SQL files: threshold_exception, pattern_detection (structuring via a 7-day self-join +
high-risk countries), dormant_activity, access_recertification (terminated / never reviewed / privileged
> threshold days / standard > 365 days, as of period_end), segregation_of_duties (join reference.sod_rules),
reporting_completeness (calendar LEFT JOIN reports: missing, late, not approved, variance > threshold).
Use employee IDs, not names, in descriptions.
```

**Verify:** `Run for 2026-10-01..2026-10-31 and show results per control. Confirm EXC-10025 is EA-0201 with
is_repeat=true and linked_issue_id=ISS-3025. Then rerun the same period and prove the exception count in
gold_compliance_exceptions did not change while gold_control_test_results gained new rows.`

## Prompt 5 — tests

```
Write pytest tests in /tests that import src/examiner/synthetic_data.py, generate into a temp folder and
assert: output is deterministic; only E1099's EA-0201/EA-0202 are terminated-with-active-access; only E1098
has an SoD conflict; ISS-3025 is overdue; only TXN-2026109001/2 breach the approval rule; each file has
duplicates and malformed rows; every policy file has title, version, effective_date, owner and access_classification.
```

## Prompt 6 — Genie space (after Gold exists)

Create a Genie space on `gold_control_test_results`, `gold_compliance_exceptions`, `gold_compliance_evidence`,
`silver_compliance_issues`. Put this in **General instructions**:

```
You answer questions about compliance control testing. Exceptions are in gold_compliance_exceptions; join
gold_compliance_evidence on evidence_reference = evidence_id. "Overdue" means silver_compliance_issues where
issue_status <> 'CLOSED' and target_remediation_date < current_date(). "Previous month" means the calendar
month before today. Never describe an exception as resolved unless status = 'CLOSED'. Show employee IDs, not names.
```

Sample questions to add: "Which controls failed last month?", "Show evidence for EXC-10025",
"List overdue remediation items in ACCESS_CONTROL".
