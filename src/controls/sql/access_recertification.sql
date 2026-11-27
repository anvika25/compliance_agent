-- test_type : access_recertification       (used by CTL-ACC-001)
-- params    : :period_end, :threshold (max days between privileged reviews)
-- population: access grants ACTIVE as of period_end
-- exception : owner terminated on/before period_end | never reviewed |
--             privileged review older than threshold days | standard review older than 365 days
-- note      : employee IDs only, no names (pseudonymised evidence)
WITH pop AS (
  SELECT *,
    CASE
      WHEN termination_date IS NOT NULL AND termination_date <= CAST(:period_end AS DATE) THEN 'TERMINATED_WITH_ACTIVE_ACCESS'
      WHEN access_review_date IS NULL THEN 'NEVER_REVIEWED'
      WHEN privileged_access_flag
           AND access_review_date < date_sub(CAST(:period_end AS DATE), CAST(:threshold AS INT)) THEN 'PRIVILEGED_REVIEW_OVERDUE'
      WHEN NOT privileged_access_flag
           AND access_review_date < date_sub(CAST(:period_end AS DATE), 365) THEN 'ANNUAL_REVIEW_OVERDUE'
    END AS finding
  FROM examiner_compliance.silver.silver_employee_access
  WHERE access_status = 'ACTIVE' AND granted_date <= CAST(:period_end AS DATE)
)
SELECT
  access_id                                                      AS entity_id,
  'ACCESS_GRANT'                                                 AS entity_type,
  business_unit,
  finding IS NOT NULL                                            AS is_exception,
  CASE finding
    WHEN 'TERMINATED_WITH_ACTIVE_ACCESS' THEN format_string('Employee %s terminated %s still has ACTIVE %s%s access to %s',
         employee_id, CAST(termination_date AS STRING), CASE WHEN privileged_access_flag THEN 'privileged ' ELSE '' END,
         access_role, application_name)
    WHEN 'NEVER_REVIEWED' THEN format_string('%s access to %s for %s has never been reviewed', access_role, application_name, employee_id)
    WHEN 'PRIVILEGED_REVIEW_OVERDUE' THEN format_string('Privileged %s access to %s for %s last reviewed %s',
         access_role, application_name, employee_id, CAST(access_review_date AS STRING))
    WHEN 'ANNUAL_REVIEW_OVERDUE' THEN format_string('%s access to %s for %s last reviewed %s',
         access_role, application_name, employee_id, CAST(access_review_date AS STRING))
  END                                                            AS exception_description,
  CASE WHEN finding = 'TERMINATED_WITH_ACTIVE_ACCESS' THEN 'Access revoked within 1 business day of termination'
       WHEN privileged_access_flag THEN format_string('Privileged access reviewed within %d days', CAST(:threshold AS INT))
       ELSE 'Standard access reviewed within 365 days' END       AS expected_condition,
  format_string('finding=%s; status=ACTIVE; termination_date=%s; last_review=%s', coalesce(finding, 'NONE'),
                coalesce(CAST(termination_date AS STRING), 'n/a'), coalesce(CAST(access_review_date AS STRING), 'never')) AS actual_condition,
  CASE WHEN finding = 'TERMINATED_WITH_ACTIVE_ACCESS'
         THEN format_string('%d days since termination', datediff(CAST(:period_end AS DATE), termination_date))
       WHEN access_review_date IS NOT NULL
         THEN format_string('%d days since last review', datediff(CAST(:period_end AS DATE), access_review_date)) END AS variance,
  CAST(NULL AS DOUBLE)                                           AS financial_impact,
  CAST(0.99 AS DOUBLE)                                           AS detection_confidence,
  CAST(coalesce(termination_date, access_review_date, granted_date) AS TIMESTAMP) AS event_timestamp,
  'silver.silver_employee_access'                                AS source_table,
  access_id                                                      AS source_record_id,
  record_hash                                                    AS src_record_hash,
  source_file                                                    AS src_source_file,
  batch_id                                                       AS src_batch_id,
  to_json(named_struct('employee_id', employee_id, 'application_name', application_name,
                       'access_role', access_role, 'privileged', privileged_access_flag,
                       'granted_date', CAST(granted_date AS STRING), 'finding', finding)) AS related_records
FROM pop
