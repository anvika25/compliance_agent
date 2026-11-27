-- test_type : pattern_detection            (used by CTL-TXN-002)
-- params    : :period_start, :period_end, :threshold (CAD reporting threshold)
-- population: customers with completed transactions in the period
-- exception : structuring (>= 3 cash deposits between 90% and 100% of threshold within 7 days)
--             OR any transaction involving a high-risk jurisdiction
WITH txn AS (
  SELECT * FROM examiner_compliance.silver.silver_transaction
  WHERE status = 'COMPLETED'
    AND transaction_timestamp >= CAST(:period_start AS TIMESTAMP)
    AND transaction_timestamp <  CAST(date_add(CAST(:period_end AS DATE), 1) AS TIMESTAMP)
),
near_threshold_cash AS (
  SELECT * FROM txn
  WHERE transaction_type = 'DEPOSIT' AND channel IN ('BRANCH', 'ATM')
    AND amount_cad >= 0.9 * :threshold AND amount_cad < :threshold
),
rolling_7d AS (
  SELECT a.customer_id, a.transaction_id AS anchor_id,
         count(*) AS cnt_7d, collect_list(b.transaction_id) AS txn_ids, sum(b.amount_cad) AS total_cad,
         first(a.record_hash) AS record_hash, first(a.source_file) AS source_file, first(a.batch_id) AS batch_id
  FROM near_threshold_cash a
  JOIN near_threshold_cash b
    ON b.customer_id = a.customer_id
   AND b.transaction_timestamp >= a.transaction_timestamp
   AND b.transaction_timestamp <  a.transaction_timestamp + INTERVAL 7 DAYS
  GROUP BY a.customer_id, a.transaction_id
),
structuring AS (
  SELECT customer_id,
         max_by(named_struct('cnt', cnt_7d, 'ids', txn_ids, 'total', total_cad,
                             'record_hash', record_hash, 'source_file', source_file, 'batch_id', batch_id),
                cnt_7d) AS best
  FROM rolling_7d GROUP BY customer_id
),
high_risk AS (
  SELECT t.customer_id, count(*) AS n, collect_list(t.transaction_id) AS ids, sum(t.amount_cad) AS total,
         array_join(collect_set(t.country_code), ',') AS countries,
         first(t.record_hash) AS record_hash, first(t.source_file) AS source_file, first(t.batch_id) AS batch_id
  FROM txn t JOIN examiner_compliance.reference.high_risk_countries h ON t.country_code = h.country_code
  GROUP BY t.customer_id
),
population AS (
  SELECT customer_id, max(transaction_timestamp) AS last_ts FROM txn GROUP BY customer_id
)
SELECT
  p.customer_id                                                  AS entity_id,
  'CUSTOMER'                                                     AS entity_type,
  'RETAIL_BANKING'                                               AS business_unit,
  (coalesce(sw.best.cnt, 0) >= 3 OR coalesce(h.n, 0) > 0)        AS is_exception,
  concat_ws('; ',
    CASE WHEN sw.best.cnt >= 3 THEN format_string('Possible structuring: %d cash deposits between 90-100%% of threshold within 7 days', sw.best.cnt) END,
    CASE WHEN h.n > 0 THEN format_string('%d transaction(s) involving high-risk jurisdiction %s', h.n, h.countries) END
  )                                                              AS exception_description,
  'Fewer than 3 near-threshold cash deposits in any 7 days; no high-risk jurisdiction activity without EDD' AS expected_condition,
  format_string('max near-threshold deposits in 7 days = %d; high-risk transactions = %d',
                coalesce(sw.best.cnt, 0), coalesce(h.n, 0))      AS actual_condition,
  format_string('%d deposits over limit of 2', greatest(coalesce(sw.best.cnt, 0) - 2, 0)) AS variance,
  CAST(coalesce(sw.best.total, 0) + coalesce(h.total, 0) AS DOUBLE) AS financial_impact,
  CAST(CASE WHEN sw.best.cnt >= 3 THEN 0.70 ELSE 0.85 END AS DOUBLE) AS detection_confidence,
  p.last_ts                                                      AS event_timestamp,
  'silver.silver_transaction'                                    AS source_table,
  array_join(array_distinct(concat(coalesce(sw.best.ids, CAST(array() AS ARRAY<STRING>)),
                                   coalesce(h.ids, CAST(array() AS ARRAY<STRING>)))), ',') AS source_record_id,
  coalesce(sw.best.record_hash, h.record_hash)                   AS src_record_hash,
  coalesce(sw.best.source_file, h.source_file)                   AS src_source_file,
  coalesce(sw.best.batch_id, h.batch_id)                         AS src_batch_id,
  to_json(named_struct('structuring_txn_ids', sw.best.ids, 'high_risk_txn_ids', h.ids,
                       'high_risk_countries', h.countries))      AS related_records
FROM population p
LEFT JOIN structuring sw ON sw.customer_id = p.customer_id
LEFT JOIN high_risk h    ON h.customer_id  = p.customer_id
