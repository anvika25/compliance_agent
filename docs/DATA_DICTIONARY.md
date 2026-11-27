# Data dictionary

All tables are in catalog `examiner_compliance`. Live column-level detail, comments and tags:
`DESCRIBE TABLE EXTENDED <table>` or Catalog Explorer. PII columns are marked **(PII)**.

## Lineage columns (on every Bronze and Silver row)
| Column | Meaning |
|---|---|
| `ingestion_timestamp` | when the row was loaded into Bronze |
| `batch_id` | load run (batch CSVs) or `<run>-<epoch>` (Auto Loader micro-batch) |
| `source_file` | full volume path of the raw file (`_metadata.file_path`) |
| `source_system` | originating system (e.g. `IAM_PLATFORM`, `PAYMENTS_HUB`) |
| `record_hash` | SHA-256 of all business columns. The row's fingerprint, used for idempotency, duplicates and lineage |
| `dup_seq` (Bronze) | occurrence number of an identical row inside one file |
| `silver_processed_at` (Silver) | when the row was last merged into Silver |

## Bronze (`bronze`): raw strings, one table per source
`bronze_customer_account`, `bronze_transaction` (+ `_rescued_data`), `bronze_employee_access`, `bronze_regulatory_reporting`,
`bronze_control_catalogue`, `bronze_compliance_issues`. Columns = raw source columns (all STRING) + lineage columns.

## Silver (`silver`)
| Table | Grain / key | Key columns |
|---|---|---|
| `silver_customer_account` | account / `account_id` | customer_id, customer_name **(PII, masked)**, risk_rating LOW/MEDIUM/HIGH, country_code, account_status ACTIVE/DORMANT/CLOSED, account_open_date, account_close_date, product_type |
| `silver_transaction` | transaction / `transaction_id` | customer_id, account_id, transaction_timestamp, transaction_type, amount DECIMAL(18,2), currency_code (ISO), amount_cad, country_code, channel, status COMPLETED/PENDING/REVERSED, initiated_by, approved_by, approval_reference, last_modified_ts (version for late-arriving), extract_window_start, is_late_arriving, account_found, customer_account_mismatch |
| `silver_employee_access` | access grant / `access_id` | employee_id, employee_name **(PII, masked)**, employee_role, business_unit, application_name, access_role, access_status ACTIVE/REVOKED, granted_date, access_review_date, termination_date, privileged_access_flag BOOLEAN, manager_id |
| `silver_regulatory_reporting` | report / `report_id` | report_type, reporting_period yyyy-MM, reported_amount, source_system_amount, submission_status, submission_timestamp, approval_status, approver_id, prepared_by |
| `silver_control_catalogue` | control / `control_id` | control_name, control_domain, control_description, control_owner, severity CRITICAL/HIGH/MEDIUM/LOW, test_type (= SQL file), threshold, tolerance_pct, frequency, population, policy_reference, remediation_action, active_flag |
| `silver_compliance_issues` | issue / `issue_id` | exception_id, control_id, entity_id, issue_title, issue_status OPEN/IN_PROGRESS/CLOSED, issue_owner, business_unit, severity, opened_date, target_remediation_date, actual_remediation_date, validation_status, remediation_action, control_found |
| `quarantine_records` | rejected row | source_table, source_file, record_hash, dup_seq, batch_id, dq_errors ARRAY, raw_record JSON, quarantined_at |

## Reference (`reference`)
`sod_rules` (rule_id, application_name, role_a, role_b), `high_risk_countries`, `fx_rates` (currency_code, rate_to_cad),
`regulatory_report_calendar` (report_type, reporting_period, due_date, regulator), `policy_chunks` (chunk_id, doc_id, title,
version, effective_date, owner, access_classification, section_number, section_title, heading_path, chunk_text,
retrieval_text, content_sha256, …), view `policy_chunks_secure`; functions `mask_name`, `can_read_classification`;
volumes `raw_files`, `checkpoints`, `evidence`.

## Gold (`gold`)
| Table | Grain | Columns |
|---|---|---|
| `gold_control_test_results` | control × test run (append-only) | test_run_id, run_batch_id, control_id, control_name, control_domain, severity, period_start, period_end, population_count, exception_count, exception_rate, threshold, tolerance_pct, test_status PASS/FAIL/ERROR, sql_file, sql_sha256, execution_timestamp, executed_by, error_message |
| `gold_compliance_exceptions` | control × entity (stable) | exception_id, exception_key, control_id, control_domain, entity_id, entity_type, business_unit, exception_description, severity, risk_score, risk_band, evidence_reference, owner, status, policy_reference, remediation_action, financial_impact, detection_confidence, is_repeat, linked_issue_id, detected_periods, first/last_test_run_id, creation_timestamp, last_detected_timestamp |
| `gold_compliance_evidence` | exception × test run | evidence_id, exception_id, test_run_id, control_id, source_table, source_record_id, evidence_summary, control_logic, expected_condition, actual_condition, variance, event_timestamp, related_records, policy_reference, evidence_completeness, evidence_gaps, review_status, lineage_reference, bronze_record_hash, source_file, source_batch_id, source_table_version, evidence_file, evidence_timestamp |
| `gold_remediation_status` | issue, or open exception without issue | issue_id, exception_id, control_id, control_domain, entity_id, description, owner, business_unit, severity, remediation_status, opened_date, target_remediation_date, actual_remediation_date, validation_status, is_overdue, days_overdue, closed_without_validation, record_type |
| `gold_compliance_dashboard` | period × control × business unit | period, control, status, population, exceptions, rate, prior/change, bu_* counts, evidence completeness, open/overdue remediation, `is_control_row` |
| `gold_examiner_summary` | review_scope × period | controls_evaluated/failed/errored, key_findings, high_risk_exceptions, remediation_status, overdue_remediation_items, evidence_gaps, human_review_status, reviewed_by/at, generated_by |
| `v_dash_*` | views for the Controls dashboard | |

## Features / ML / Monitoring / Controls
| Table | Contents |
|---|---|
| `features.exception_features` | 9 factors (0–1) per open exception + context |
| `ml.exception_risk_model` | UC registered model; alias `@champion` |
| `ml.exception_risk_scores` | exception_id, risk_score, rule_score, model_probability, risk_band, top_risk_factors, recommended_priority, model_or_rule_version, human_review_required, scoring_timestamp, mlflow_run_id (history) |
| `ml.rag_evaluation`, `ml.agent_evaluation`, `ml.platform_model_metrics` | evaluation runs and MLflow metrics |
| `monitoring.rag_query_log` | every policy question, retrieval, answer, citations, latency |
| `monitoring.agent_requests` / `agent_tool_calls` | every agent request (masked) and each tool call / LLM prompt with timestamps |
| `monitoring.integration_test_results`, `v_plat_*` | test results; Platform dashboard views |
| `controls.notification_outbox` | simulated critical-exception notifications |
| `controls.governance_apply_log` | every governance statement and its result |
