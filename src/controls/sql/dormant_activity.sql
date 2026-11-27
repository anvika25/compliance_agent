-- test_type : dormant_activity             (used by CTL-TXN-003)
-- params    : :period_start, :period_end, :threshold (CAD)
-- population: completed transactions in the period
-- exception : account is DORMANT or CLOSED and amount_cad >= threshold
-- limitation: uses the account's current status, not its status on the transaction date
SELECT
  t.transaction_id                                               AS entity_id,
  'TRANSACTION'                                                  AS entity_type,
  'RETAIL_BANKING'                                               AS business_unit,
  coalesce(a.account_status IN ('DORMANT', 'CLOSED') AND t.amount_cad >= :threshold, false) AS is_exception,
  format_string('%s of %s CAD on %s account %s', t.transaction_type, CAST(t.amount_cad AS STRING),
                coalesce(a.account_status, 'UNKNOWN'), t.account_id) AS exception_description,
  format_string('No transaction >= %s CAD on a DORMANT or CLOSED account without reactivation review',
                CAST(CAST(:threshold AS BIGINT) AS STRING))      AS expected_condition,
  format_string('Account status %s; amount %s CAD', coalesce(a.account_status, 'UNKNOWN'),
                CAST(t.amount_cad AS STRING))                    AS actual_condition,
  format_string('%s CAD above threshold', CAST(round(t.amount_cad - :threshold, 2) AS STRING)) AS variance,
  CAST(t.amount_cad AS DOUBLE)                                   AS financial_impact,
  CAST(0.90 AS DOUBLE)                                           AS detection_confidence,
  t.transaction_timestamp                                        AS event_timestamp,
  'silver.silver_transaction'                                    AS source_table,
  t.transaction_id                                               AS source_record_id,
  t.record_hash                                                  AS src_record_hash,
  t.source_file                                                  AS src_source_file,
  t.batch_id                                                     AS src_batch_id,
  to_json(named_struct('account_id', t.account_id, 'account_status', a.account_status,
                       'customer_id', t.customer_id, 'account_record_hash', a.record_hash)) AS related_records
FROM examiner_compliance.silver.silver_transaction t
LEFT JOIN examiner_compliance.silver.silver_customer_account a ON a.account_id = t.account_id
WHERE t.status = 'COMPLETED'
  AND t.transaction_timestamp >= CAST(:period_start AS TIMESTAMP)
  AND t.transaction_timestamp <  CAST(date_add(CAST(:period_end AS DATE), 1) AS TIMESTAMP)
