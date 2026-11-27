-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · Gold data products: remediation, dashboard, examiner summary  *(Implemented)*
-- MAGIC
-- MAGIC | Table | Grain | Feeds |
-- MAGIC |---|---|---|
-- MAGIC | `gold_remediation_status` | one row per issue **plus** one per open exception with no issue raised | agent "remediation status" tool, dashboard |
-- MAGIC | `gold_compliance_dashboard` | period × control × business unit | Compliance Controls Dashboard |
-- MAGIC | `gold_examiner_summary` | review scope (each control domain + `ALL_DOMAINS`) × period | agent "compliance-scope summary" tool |
-- MAGIC
-- MAGIC All three are full rebuilds (`CREATE OR REPLACE`). They are derived, so reruns are idempotent.
-- MAGIC History lives in the upstream tables (`gold_control_test_results` is append-only).
-- MAGIC
-- MAGIC **Run after** `03_run_controls` (ideally for several periods) and `04_risk_scoring`.
-- MAGIC `gold_examiner_summary` contains **facts only**. The AI narrative is added by the Examiner agent, and every row is
-- MAGIC `PENDING_HUMAN_REVIEW`.

-- COMMAND ----------

USE CATALOG examiner_compliance;

-- COMMAND ----------

-- MAGIC %md ## 1 · Remediation status
-- MAGIC Open exceptions with no issue get an **implied target date** from the POL-REM-005 §3 SLA
-- MAGIC (Critical 30 / High 60 / Medium 90 / Low 180 days), so they can't hide from overdue tracking.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.gold_remediation_status
COMMENT 'Remediation tracker: issues plus open exceptions without an issue. Snapshot as of refreshed_at.' AS
WITH issue_rows AS (
  SELECT
    i.issue_id, i.exception_id, i.control_id, c.control_domain, i.entity_id,
    i.issue_title AS description, i.issue_owner AS owner, i.business_unit, i.severity,
    i.issue_status AS remediation_status, i.opened_date, i.target_remediation_date, i.actual_remediation_date,
    i.validation_status, i.remediation_action, 'ISSUE' AS record_type, i.record_hash AS source_record_hash
  FROM silver.silver_compliance_issues i
  LEFT JOIN silver.silver_control_catalogue c ON c.control_id = i.control_id
),
exception_rows AS (
  SELECT
    CAST(NULL AS STRING) AS issue_id, e.exception_id, e.control_id, e.control_domain, e.entity_id,
    e.exception_description AS description, e.owner, e.business_unit, e.severity,
    'NO_ISSUE_RAISED' AS remediation_status, CAST(e.creation_timestamp AS DATE) AS opened_date,
    date_add(CAST(e.creation_timestamp AS DATE),
             CASE e.severity WHEN 'CRITICAL' THEN 30 WHEN 'HIGH' THEN 60 WHEN 'MEDIUM' THEN 90 ELSE 180 END) AS target_remediation_date,
    CAST(NULL AS DATE) AS actual_remediation_date, 'NOT_STARTED' AS validation_status, e.remediation_action,
    'EXCEPTION_WITHOUT_ISSUE' AS record_type, CAST(NULL AS STRING) AS source_record_hash
  FROM gold.gold_compliance_exceptions e
  WHERE e.status <> 'CLOSED' AND e.linked_issue_id IS NULL
),
all_rows AS (SELECT * FROM issue_rows UNION ALL SELECT * FROM exception_rows)
SELECT
  *,
  coalesce(remediation_status <> 'CLOSED' AND target_remediation_date < current_date(), false) AS is_overdue,
  CASE WHEN remediation_status <> 'CLOSED' AND target_remediation_date < current_date()
       THEN datediff(current_date(), target_remediation_date) END AS days_overdue,
  coalesce(remediation_status = 'CLOSED' AND validation_status <> 'VALIDATED', false) AS closed_without_validation,
  current_timestamp() AS refreshed_at
FROM all_rows;

SELECT * FROM gold.gold_remediation_status WHERE remediation_status <> 'CLOSED' ORDER BY is_overdue DESC, days_overdue DESC;

-- COMMAND ----------

-- MAGIC %md ## 2 · Compliance dashboard
-- MAGIC Control-level columns (`test_status`, `population_count`, `control_exception_count`, ...) repeat on every business-unit row.
-- MAGIC **Filter `is_control_row = true` before summing them.** Sum the `bu_*` columns without the filter.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.gold_compliance_dashboard
COMMENT 'Controls dashboard: period x control x business unit. Use is_control_row for control-level totals.' AS
WITH latest AS (           -- if a period was rerun, keep its latest run
  SELECT * FROM gold.gold_control_test_results
  QUALIFY row_number() OVER (PARTITION BY control_id, period_start, period_end ORDER BY execution_timestamp DESC) = 1
),
tests AS (
  SELECT *,
         concat(CAST(period_start AS STRING), '..', CAST(period_end AS STRING)) AS period_label,
         lag(exception_count) OVER (PARTITION BY control_id ORDER BY period_end) AS prior_period_exception_count
  FROM latest
),
exc AS (
  SELECT e.exception_id, e.control_id, e.business_unit, e.severity, e.risk_band, e.is_repeat, e.evidence_reference, p AS period_label
  FROM gold.gold_compliance_exceptions e
  LATERAL VIEW explode(e.detected_periods) dp AS p
),
exc_bu AS (
  SELECT x.period_label, x.control_id, x.business_unit,
         count(*)                                         AS bu_exception_count,
         count_if(x.severity = 'CRITICAL')                AS bu_critical_severity_count,
         count_if(x.risk_band IN ('CRITICAL', 'HIGH'))    AS bu_high_risk_count,
         count_if(x.is_repeat)                            AS bu_repeat_exception_count,
         round(avg(v.evidence_completeness), 3)           AS bu_avg_evidence_completeness,
         count_if(v.evidence_completeness < 1)            AS bu_exceptions_with_evidence_gaps
  FROM exc x
  LEFT JOIN gold.gold_compliance_evidence v ON v.evidence_id = x.evidence_reference
  GROUP BY x.period_label, x.control_id, x.business_unit
),
rem AS (
  SELECT control_id,
         count_if(remediation_status <> 'CLOSED') AS open_remediation_items,
         count_if(is_overdue)                     AS overdue_remediation_items
  FROM gold.gold_remediation_status GROUP BY control_id
)
SELECT
  t.period_label, t.period_start, t.period_end,
  t.control_id, t.control_name, t.control_domain, t.severity AS control_severity,
  t.test_status, t.population_count, t.exception_count AS control_exception_count,
  round(t.exception_rate, 4) AS exception_rate, t.threshold,
  t.prior_period_exception_count,
  t.exception_count - t.prior_period_exception_count AS exception_count_change,
  coalesce(b.business_unit, 'NO_EXCEPTIONS') AS business_unit,
  coalesce(b.bu_exception_count, 0)               AS bu_exception_count,
  coalesce(b.bu_critical_severity_count, 0)       AS bu_critical_severity_count,
  coalesce(b.bu_high_risk_count, 0)               AS bu_high_risk_count,
  coalesce(b.bu_repeat_exception_count, 0)        AS bu_repeat_exception_count,
  b.bu_avg_evidence_completeness,
  coalesce(b.bu_exceptions_with_evidence_gaps, 0) AS bu_exceptions_with_evidence_gaps,
  coalesce(r.open_remediation_items, 0)           AS open_remediation_items_current,
  coalesce(r.overdue_remediation_items, 0)        AS overdue_remediation_items_current,
  row_number() OVER (PARTITION BY t.control_id, t.period_label ORDER BY b.business_unit) = 1 AS is_control_row,
  t.test_run_id, t.error_message, current_timestamp() AS refreshed_at
FROM tests t
LEFT JOIN exc_bu b ON b.control_id = t.control_id AND b.period_label = t.period_label
LEFT JOIN rem r    ON r.control_id = t.control_id;

-- trend check: one line per control per period
SELECT period_label, control_id, test_status, control_exception_count, prior_period_exception_count, exception_count_change
FROM gold.gold_compliance_dashboard WHERE is_control_row ORDER BY control_id, period_label;

-- COMMAND ----------

-- MAGIC %md ## 3 · Examiner summary
-- MAGIC One row per review scope and period. `remediation_status` and `overdue_remediation_items` are a **current snapshot**,
-- MAGIC because remediation isn't tied to a test period.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.gold_examiner_summary
COMMENT 'Deterministic, fact-only review summary per scope and period. Always pending human review.' AS
WITH tests AS (
  SELECT period_label, period_start, period_end, control_id, control_name, control_domain,
         test_status, population_count, control_exception_count, exception_rate
  FROM gold.gold_compliance_dashboard WHERE is_control_row
),
scoped_tests AS (
  SELECT control_domain AS review_scope, * FROM tests
  UNION ALL
  SELECT 'ALL_DOMAINS' AS review_scope, * FROM tests
),
ctrl AS (
  SELECT review_scope, period_label, min(period_start) AS period_start, max(period_end) AS period_end,
         count(*)                         AS controls_evaluated,
         count_if(test_status = 'FAIL')   AS controls_failed,
         count_if(test_status = 'ERROR')  AS controls_errored,
         sum(control_exception_count)     AS total_exceptions,
         sort_array(collect_list(format_string('%s %s: %s, %d exception(s) in population of %d',
                    control_id, control_name, test_status, control_exception_count, population_count))) AS controls_evaluated_detail,
         sort_array(collect_list(CASE WHEN test_status = 'FAIL' THEN
                    format_string('%s (%s) failed: %d of %d records (%.1f%%) are exceptions',
                                  control_name, control_id, control_exception_count, population_count, exception_rate * 100) END)) AS key_findings,
         collect_list(CASE WHEN test_status = 'ERROR' THEN concat(control_id, ' did not execute; results missing') END) AS error_gaps
  FROM scoped_tests GROUP BY review_scope, period_label
),
exc AS (
  SELECT e.*, p AS period_label
  FROM gold.gold_compliance_exceptions e LATERAL VIEW explode(e.detected_periods) dp AS p
),
scoped_exc AS (
  SELECT control_domain AS review_scope, * FROM exc
  UNION ALL
  SELECT 'ALL_DOMAINS' AS review_scope, * FROM exc
),
exc_agg AS (
  SELECT x.review_scope, x.period_label,
         count_if(x.risk_band IN ('CRITICAL', 'HIGH')) AS high_risk_count,
         slice(sort_array(collect_list(CASE WHEN x.risk_band IN ('CRITICAL', 'HIGH') THEN
               named_struct('risk_score', x.risk_score, 'exception_id', x.exception_id, 'control_id', x.control_id,
                            'entity_id', x.entity_id, 'risk_band', x.risk_band, 'description', x.exception_description) END),
               false), 1, 10) AS high_risk_exceptions,
         count_if(x.risk_score IS NULL) AS unscored_count,
         collect_list(CASE WHEN v.evidence_completeness < 1 THEN
               format_string('%s evidence incomplete (missing: %s)', x.exception_id, array_join(v.evidence_gaps, ', ')) END) AS evidence_gap_list
  FROM scoped_exc x
  LEFT JOIN gold.gold_compliance_evidence v ON v.evidence_id = x.evidence_reference
  GROUP BY x.review_scope, x.period_label
),
scoped_rem AS (
  SELECT control_domain AS review_scope, * FROM gold.gold_remediation_status
  UNION ALL
  SELECT 'ALL_DOMAINS' AS review_scope, * FROM gold.gold_remediation_status
),
rem_agg AS (
  SELECT review_scope,
         count_if(remediation_status <> 'CLOSED')           AS open_items,
         count_if(is_overdue)                               AS overdue_items,
         count_if(record_type = 'EXCEPTION_WITHOUT_ISSUE')  AS exceptions_without_issue,
         count_if(closed_without_validation)                AS closed_without_validation,
         collect_list(CASE WHEN is_overdue THEN
               format_string('%s / %s (owner: %s): %d days overdue, target %s, validation %s',
                             coalesce(issue_id, 'no issue'), exception_id, owner, days_overdue,
                             CAST(target_remediation_date AS STRING), validation_status) END) AS overdue_detail
  FROM scoped_rem GROUP BY review_scope
)
SELECT
  c.review_scope, c.period_label, c.period_start, c.period_end,
  c.controls_evaluated, c.controls_failed, c.controls_errored, c.total_exceptions,
  c.controls_evaluated_detail, c.key_findings,
  coalesce(x.high_risk_count, 0) AS high_risk_exception_count,
  x.high_risk_exceptions,
  named_struct('open_items', coalesce(r.open_items, 0), 'overdue_items', coalesce(r.overdue_items, 0),
               'exceptions_without_issue', coalesce(r.exceptions_without_issue, 0),
               'closed_without_validation', coalesce(r.closed_without_validation, 0)) AS remediation_status,
  coalesce(r.overdue_detail, CAST(array() AS ARRAY<STRING>)) AS overdue_remediation_items,
  concat(
    coalesce(c.error_gaps, CAST(array() AS ARRAY<STRING>)),
    coalesce(x.evidence_gap_list, CAST(array() AS ARRAY<STRING>)),
    CASE WHEN x.unscored_count > 0 THEN array(format_string('%d exception(s) not yet risk-scored', x.unscored_count))
         ELSE CAST(array() AS ARRAY<STRING>) END,
    CASE WHEN r.exceptions_without_issue > 0
         THEN array(format_string('%d open exception(s) have no remediation issue raised', r.exceptions_without_issue))
         ELSE CAST(array() AS ARRAY<STRING>) END
  ) AS evidence_gaps,
  'PENDING_HUMAN_REVIEW'      AS human_review_status,
  CAST(NULL AS STRING)        AS reviewed_by,
  CAST(NULL AS TIMESTAMP)     AS reviewed_at,
  'deterministic-sql-v1'      AS generated_by,
  current_timestamp()         AS generated_at
FROM ctrl c
LEFT JOIN exc_agg x ON x.review_scope = c.review_scope AND x.period_label = c.period_label
LEFT JOIN rem_agg r ON r.review_scope = c.review_scope;

-- COMMAND ----------

-- MAGIC %md ## 4 · Preview: the agent scenario's scope (access control, previous month)

-- COMMAND ----------

SELECT review_scope, period_label, controls_evaluated, controls_failed, key_findings,
       high_risk_exceptions, remediation_status, overdue_remediation_items, evidence_gaps, human_review_status
FROM gold.gold_examiner_summary
WHERE review_scope = 'ACCESS_CONTROL'
ORDER BY period_end DESC;

-- COMMAND ----------

-- MAGIC %md ## 5 · Exception trend across periods (for the dashboard)

-- COMMAND ----------

SELECT period_end, control_domain, sum(control_exception_count) AS exceptions,
       count_if(test_status = 'FAIL') AS failed_controls
FROM gold.gold_compliance_dashboard WHERE is_control_row
GROUP BY ALL ORDER BY period_end, control_domain;
