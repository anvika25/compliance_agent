-- test_type : segregation_of_duties        (used by CTL-ACC-002)
-- params    : :period_end
-- population: employees with at least one ACTIVE grant as of period_end
-- exception : employee holds both roles of any rule in reference.sod_rules
WITH active AS (
  SELECT * FROM examiner_compliance.silver.silver_employee_access
  WHERE access_status = 'ACTIVE' AND granted_date <= CAST(:period_end AS DATE)
),
conflicts AS (
  SELECT a.employee_id, r.rule_id, r.application_name, r.description,
         a.access_id AS access_id_a, b.access_id AS access_id_b, a.access_role AS role_a, b.access_role AS role_b,
         a.record_hash, a.source_file, a.batch_id,
         greatest(a.granted_date, b.granted_date) AS conflict_since
  FROM active a
  JOIN examiner_compliance.reference.sod_rules r
    ON a.application_name = r.application_name AND a.access_role = r.role_a
  JOIN active b
    ON b.employee_id = a.employee_id AND b.application_name = r.application_name AND b.access_role = r.role_b
),
by_employee AS (
  SELECT employee_id, count(*) AS n_conflicts,
         array_join(collect_list(format_string('%s %s: %s(%s) + %s(%s)', rule_id, application_name,
                                               role_a, access_id_a, role_b, access_id_b)), '; ') AS conflict_text,
         array_join(flatten(collect_list(array(access_id_a, access_id_b))), ',') AS access_ids,
         min(conflict_since) AS conflict_since,
         min_by(record_hash, conflict_since) AS record_hash,
         min_by(source_file, conflict_since) AS source_file,
         min_by(batch_id, conflict_since) AS batch_id
  FROM conflicts GROUP BY employee_id
),
population AS (
  SELECT employee_id, max(business_unit) AS business_unit FROM active GROUP BY employee_id
)
SELECT
  p.employee_id                                                  AS entity_id,
  'EMPLOYEE'                                                     AS entity_type,
  p.business_unit,
  c.employee_id IS NOT NULL                                      AS is_exception,
  format_string('Employee %s holds conflicting roles: %s', p.employee_id, c.conflict_text) AS exception_description,
  'No employee holds both roles of any SoD rule'                 AS expected_condition,
  format_string('%d conflicting combination(s) since %s', coalesce(c.n_conflicts, 0),
                coalesce(CAST(c.conflict_since AS STRING), 'n/a')) AS actual_condition,
  CASE WHEN c.employee_id IS NOT NULL
       THEN format_string('%d days in conflict at period end', datediff(CAST(:period_end AS DATE), c.conflict_since)) END AS variance,
  CAST(NULL AS DOUBLE)                                           AS financial_impact,
  CAST(0.99 AS DOUBLE)                                           AS detection_confidence,
  CAST(c.conflict_since AS TIMESTAMP)                            AS event_timestamp,
  'silver.silver_employee_access'                                AS source_table,
  c.access_ids                                                   AS source_record_id,
  c.record_hash                                                  AS src_record_hash,
  c.source_file                                                  AS src_source_file,
  c.batch_id                                                     AS src_batch_id,
  to_json(named_struct('conflicts', c.conflict_text, 'access_ids', c.access_ids)) AS related_records
FROM population p
LEFT JOIN by_employee c ON c.employee_id = p.employee_id
