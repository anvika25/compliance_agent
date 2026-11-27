-- test_type : reporting_completeness       (used by CTL-REG-001)
-- params    : :period_start, :period_end, :threshold (max reconciliation variance, e.g. 0.01 = 1%)
-- population: scheduled reports whose due_date falls in the period
-- exception : missing | not submitted | submitted after due date | not approved | variance > threshold
WITH cal AS (
  SELECT * FROM examiner_compliance.reference.regulatory_report_calendar
  WHERE CAST(due_date AS DATE) BETWEEN CAST(:period_start AS DATE) AND CAST(:period_end AS DATE)
),
rep AS (
  SELECT * FROM examiner_compliance.silver.silver_regulatory_reporting
  QUALIFY row_number() OVER (PARTITION BY report_type, reporting_period
                             ORDER BY submission_timestamp DESC NULLS LAST) = 1
),
joined AS (
  SELECT c.report_type AS cal_report_type, c.reporting_period AS cal_period,
         CAST(c.due_date AS DATE) AS due_date, c.regulator, r.*,
         abs(r.reported_amount - r.source_system_amount) / nullif(abs(r.source_system_amount), 0) AS variance_pct
  FROM cal c LEFT JOIN rep r ON r.report_type = c.report_type AND r.reporting_period = c.reporting_period
),
assessed AS (
  SELECT j.*,
    concat_ws(', ',
      CASE WHEN report_id IS NULL THEN 'MISSING' END,
      CASE WHEN report_id IS NOT NULL AND submission_status <> 'SUBMITTED' THEN 'NOT_SUBMITTED' END,
      CASE WHEN submission_timestamp >= CAST(date_add(due_date, 1) AS TIMESTAMP) THEN 'LATE' END,
      CASE WHEN report_id IS NOT NULL AND coalesce(approval_status, '') <> 'APPROVED' THEN 'NOT_APPROVED' END,
      CASE WHEN variance_pct > :threshold THEN 'UNRECONCILED' END) AS findings
  FROM joined j
)
SELECT
  concat(cal_report_type, '|', cal_period)                                  AS entity_id,
  'REGULATORY_REPORT'                                            AS entity_type,
  'FINANCE'                                                      AS business_unit,
  findings <> ''                                                 AS is_exception,
  format_string('%s for %s (due %s): %s', cal_report_type, cal_period, CAST(due_date AS STRING), findings) AS exception_description,
  format_string('Submitted and approved by %s; variance <= %s%%', CAST(due_date AS STRING),
                CAST(round(:threshold * 100, 2) AS STRING))      AS expected_condition,
  format_string('report_id=%s; status=%s; submitted=%s; approval=%s; variance=%s%%',
                coalesce(report_id, 'none'), coalesce(submission_status, 'n/a'),
                coalesce(CAST(submission_timestamp AS STRING), 'n/a'), coalesce(approval_status, 'n/a'),
                coalesce(CAST(round(variance_pct * 100, 2) AS STRING), 'n/a')) AS actual_condition,
  CASE WHEN submission_timestamp >= CAST(date_add(due_date, 1) AS TIMESTAMP)
       THEN format_string('%d days late', datediff(CAST(submission_timestamp AS DATE), due_date))
       WHEN variance_pct > :threshold THEN format_string('%s%% variance', CAST(round(variance_pct * 100, 2) AS STRING)) END AS variance,
  CAST(abs(reported_amount - source_system_amount) AS DOUBLE)    AS financial_impact,
  CAST(0.98 AS DOUBLE)                                           AS detection_confidence,
  coalesce(submission_timestamp, CAST(due_date AS TIMESTAMP))    AS event_timestamp,
  'silver.silver_regulatory_reporting'                           AS source_table,
  report_id                                                      AS source_record_id,
  record_hash                                                    AS src_record_hash,
  source_file                                                    AS src_source_file,
  batch_id                                                       AS src_batch_id,
  to_json(named_struct('regulator', regulator, 'calendar_due_date', CAST(due_date AS STRING),
                       'approver_id', approver_id, 'prepared_by', prepared_by)) AS related_records
FROM assessed
