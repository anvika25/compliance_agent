-- test_type : threshold_exception          (used by CTL-TXN-001)
-- params    : :period_start, :period_end (yyyy-MM-dd), :threshold (CAD)
-- population: completed transactions in the period
-- exception : amount_cad >= threshold without independent approval
--             (no approver, no approval reference, or approver = initiator)
-- Every control SQL returns the same 17 columns -- see src/controls/README.md
SELECT
  t.transaction_id                                              AS entity_id,
  'TRANSACTION'                                                 AS entity_type,
  coalesce(emp.business_unit, 'RETAIL_BANKING')                 AS business_unit,
  coalesce(t.amount_cad >= :threshold
           AND (t.approved_by IS NULL OR t.approval_reference IS NULL OR t.approved_by = t.initiated_by),
           false)                                               AS is_exception,
  format_string('%s %s for %s CAD released without independent approval',
                t.transaction_type, t.transaction_id, CAST(t.amount_cad AS STRING)) AS exception_description,
  format_string('Amount >= %s CAD has an approver different from the initiator and an approval reference',
                CAST(CAST(:threshold AS BIGINT) AS STRING))     AS expected_condition,
  CASE WHEN t.approved_by IS NULL              THEN 'No approver recorded'
       WHEN t.approval_reference IS NULL       THEN 'Approval reference missing'
       WHEN t.approved_by = t.initiated_by     THEN concat('Self-approved by ', t.initiated_by)
       ELSE 'Independent approval present' END                  AS actual_condition,
  format_string('%s CAD above threshold', CAST(round(t.amount_cad - :threshold, 2) AS STRING)) AS variance,
  CAST(t.amount_cad AS DOUBLE)                                  AS financial_impact,
  CAST(0.95 AS DOUBLE)                                          AS detection_confidence,
  t.transaction_timestamp                                       AS event_timestamp,
  'silver.silver_transaction'                                   AS source_table,
  t.transaction_id                                              AS source_record_id,
  t.record_hash                                                 AS src_record_hash,
  t.source_file                                                 AS src_source_file,
  t.batch_id                                                    AS src_batch_id,
  to_json(named_struct('account_id', t.account_id, 'customer_id', t.customer_id,
                       'initiated_by', t.initiated_by, 'approved_by', t.approved_by,
                       'approval_reference', t.approval_reference)) AS related_records
FROM examiner_compliance.silver.silver_transaction t
LEFT JOIN (SELECT employee_id, max(business_unit) AS business_unit
           FROM examiner_compliance.silver.silver_employee_access GROUP BY employee_id) emp
       ON emp.employee_id = t.initiated_by
WHERE t.status = 'COMPLETED'
  AND t.transaction_timestamp >= CAST(:period_start AS TIMESTAMP)
  AND t.transaction_timestamp <  CAST(date_add(CAST(:period_end AS DATE), 1) AS TIMESTAMP)
