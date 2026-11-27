# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Dashboard datasets  *(Implemented)*
# MAGIC Creates one view per dashboard widget, so each AI/BI dashboard dataset is just `SELECT * FROM <view>`.
# MAGIC Rerun any time; the views are always live over the Gold, ML and monitoring tables.
# MAGIC
# MAGIC * `gold.v_dash_*` → **Compliance Controls Dashboard**
# MAGIC * `monitoring.v_plat_*` → **Examiner Platform Dashboard**
# MAGIC
# MAGIC Layout and chart types: `docs/DASHBOARDS.md`.

# COMMAND ----------

C = "examiner_compliance"
USER = spark.sql("SELECT current_user()").first()[0]
created, failed = [], []

def view(name, sql):
    try:
        spark.sql(f"CREATE OR REPLACE VIEW {C}.{name} AS {sql}")
        created.append(name)
    except Exception as e:
        failed.append((name, str(e).splitlines()[0][:200]))

# COMMAND ----------

# MAGIC %md ## Compliance Controls Dashboard

# COMMAND ----------

view("gold.v_dash_kpis", f"""
WITH latest AS (SELECT max(period_end) AS pe FROM {C}.gold.gold_compliance_dashboard)
SELECT d.period_label                                                             AS latest_period,
       count_if(d.is_control_row)                                                 AS controls_evaluated,
       count_if(d.is_control_row AND d.test_status = 'FAIL')                      AS controls_failed,
       sum(CASE WHEN d.is_control_row THEN d.control_exception_count END)         AS exceptions,
       round(sum(CASE WHEN d.is_control_row THEN d.control_exception_count END)
             / sum(CASE WHEN d.is_control_row THEN d.population_count END), 4)    AS overall_exception_rate,
       (SELECT count(*) FROM {C}.gold.gold_compliance_exceptions
         WHERE status <> 'CLOSED' AND risk_band IN ('CRITICAL', 'HIGH'))          AS open_high_risk_exceptions,
       (SELECT count(*) FROM {C}.gold.gold_remediation_status WHERE is_overdue)   AS overdue_remediation_items,
       (SELECT round(avg(v.evidence_completeness), 3)
          FROM {C}.gold.gold_compliance_exceptions e
          JOIN {C}.gold.gold_compliance_evidence v ON v.evidence_id = e.evidence_reference) AS avg_evidence_completeness
FROM {C}.gold.gold_compliance_dashboard d CROSS JOIN latest
WHERE d.period_end = latest.pe
GROUP BY d.period_label""")

view("gold.v_dash_control_results", f"""
SELECT period_end, period_label, control_id, control_name, control_domain, control_severity, test_status,
       population_count, control_exception_count AS exceptions, exception_rate,
       prior_period_exception_count, exception_count_change
FROM {C}.gold.gold_compliance_dashboard WHERE is_control_row""")

view("gold.v_dash_exceptions_by_bu_domain", f"""
SELECT period_end, control_domain, business_unit,
       sum(bu_exception_count) AS exceptions, sum(bu_high_risk_count) AS high_risk_exceptions,
       sum(bu_repeat_exception_count) AS repeat_exceptions
FROM {C}.gold.gold_compliance_dashboard WHERE business_unit <> 'NO_EXCEPTIONS'
GROUP BY ALL""")

view("gold.v_dash_exception_trend", f"""
SELECT period_end, control_domain, sum(control_exception_count) AS exceptions,
       count_if(test_status = 'FAIL') AS failed_controls, count(*) AS controls_evaluated
FROM {C}.gold.gold_compliance_dashboard WHERE is_control_row
GROUP BY ALL""")

view("gold.v_dash_high_risk_exceptions", f"""
SELECT exception_id, control_id, control_domain, business_unit, entity_id, severity, risk_score, risk_band,
       is_repeat, size(detected_periods) AS periods_detected, linked_issue_id, owner, status, exception_description
FROM {C}.gold.gold_compliance_exceptions WHERE status <> 'CLOSED'""")

view("gold.v_dash_repeat_exceptions", f"""
SELECT exception_id, control_id, control_domain, business_unit, entity_id, risk_band,
       size(detected_periods) AS periods_detected, array_join(detected_periods, ', ') AS periods,
       linked_issue_id, exception_description
FROM {C}.gold.gold_compliance_exceptions WHERE is_repeat AND status <> 'CLOSED'""")

view("gold.v_dash_overdue_remediation", f"""
SELECT coalesce(issue_id, '(no issue raised)') AS issue_id, exception_id, control_id, control_domain, business_unit,
       severity, owner, remediation_status, target_remediation_date, is_overdue, days_overdue, validation_status, record_type
FROM {C}.gold.gold_remediation_status WHERE remediation_status <> 'CLOSED'""")

view("gold.v_dash_evidence_completeness", f"""
SELECT e.control_id, e.control_domain, count(*) AS exceptions,
       round(avg(v.evidence_completeness), 3) AS avg_evidence_completeness,
       count_if(v.evidence_completeness < 1) AS exceptions_with_gaps,
       count_if(v.review_status = 'PENDING_HUMAN_REVIEW') AS pending_review
FROM {C}.gold.gold_compliance_exceptions e
JOIN {C}.gold.gold_compliance_evidence v ON v.evidence_id = e.evidence_reference
GROUP BY ALL""")

# COMMAND ----------

# MAGIC %md ## Examiner Platform Dashboard
# MAGIC Model, RAG and agent metrics live in MLflow; this cell copies them into a table so the dashboard can chart them.

# COMMAND ----------

import mlflow, pandas as pd
frames = []
for exp, component in [("examiner_risk_scoring", "risk_model"), ("examiner_rag_eval", "rag"), ("examiner_agent_eval", "agent")]:
    try:
        runs = mlflow.search_runs(experiment_names=[f"/Users/{USER}/{exp}"])
    except Exception as e:
        print(exp, "not found:", e); continue
    if runs.empty:
        continue
    mcols = [c for c in runs.columns if c.startswith("metrics.")]
    long = runs[["run_id", "start_time", "tags.mlflow.runName"] + mcols].melt(
        id_vars=["run_id", "start_time", "tags.mlflow.runName"], var_name="metric", value_name="value").dropna(subset=["value"])
    long["metric"] = long["metric"].str.replace("metrics.", "", regex=False)
    long["component"] = component
    frames.append(long.rename(columns={"tags.mlflow.runName": "run_name"}))
if frames:
    mm = pd.concat(frames)[["component", "run_id", "run_name", "start_time", "metric", "value"]]
    mm["start_time"] = pd.to_datetime(mm["start_time"], utc=True).dt.tz_convert(None)
    spark.createDataFrame(mm).write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.ml.platform_model_metrics")
    print(f"{len(mm)} metric values from {mm.run_id.nunique()} MLflow runs")

# COMMAND ----------

view("monitoring.v_plat_kpis", f"""
SELECT
  (SELECT count(*) FROM {C}.gold.gold_control_test_results)                                       AS control_tests_run,
  (SELECT count(*) FROM {C}.ml.exception_risk_scores)                                             AS exceptions_scored,
  (SELECT count(*) FROM {C}.monitoring.agent_requests)                                            AS agent_requests,
  (SELECT round(avg(CASE WHEN status = 'REFUSED' THEN 1.0 ELSE 0 END), 3) FROM {C}.monitoring.agent_requests) AS agent_refusal_rate,
  (SELECT round(avg(latency_ms) / 1000, 1) FROM {C}.monitoring.agent_requests WHERE status = 'COMPLETED')     AS agent_avg_latency_s,
  (SELECT count(*) FROM {C}.monitoring.rag_query_log)                                             AS rag_questions,
  (SELECT round(avg(CASE WHEN status = 'ANSWERED' THEN 1.0 ELSE 0 END), 3) FROM {C}.monitoring.rag_query_log)  AS rag_answered_rate,
  (SELECT round(avg(CASE WHEN status IN ('NOT_FOUND', 'UNSUPPORTED') THEN 1.0 ELSE 0 END), 3)
     FROM {C}.monitoring.rag_query_log)                                                           AS unsupported_answer_rate,
  (SELECT round(avg(CASE WHEN answer_correct THEN 1.0 ELSE 0 END), 3) FROM {C}.ml.rag_evaluation
    WHERE eval_run_id = (SELECT max(eval_run_id) FROM {C}.ml.rag_evaluation))                     AS rag_eval_accuracy_latest,
  (SELECT round(avg(CASE WHEN passed THEN 1.0 ELSE 0 END), 3) FROM {C}.ml.agent_evaluation
    WHERE eval_run_id = (SELECT max(eval_run_id) FROM {C}.ml.agent_evaluation))                   AS agent_eval_pass_rate_latest""")

view("monitoring.v_plat_control_test_volume", f"""
SELECT date(execution_timestamp) AS run_date, test_status, count(*) AS tests,
       sum(population_count) AS records_tested, sum(exception_count) AS exceptions
FROM {C}.gold.gold_control_test_results GROUP BY ALL""")

view("monitoring.v_plat_scoring_volume", f"""
SELECT date(scoring_timestamp) AS scoring_date, model_or_rule_version, count(*) AS exceptions_scored,
       round(avg(risk_score), 1) AS avg_risk_score
FROM {C}.ml.exception_risk_scores GROUP BY ALL""")

view("monitoring.v_plat_risk_distribution", f"""
WITH latest AS (
  SELECT * FROM {C}.ml.exception_risk_scores
  QUALIFY row_number() OVER (PARTITION BY exception_id ORDER BY scoring_timestamp DESC) = 1)
SELECT CAST(floor(risk_score / 10) * 10 AS INT) AS score_bucket, risk_band, count(*) AS exceptions
FROM latest GROUP BY ALL""")

view("monitoring.v_plat_model_metrics", f"""
SELECT component, run_name, start_time, metric, round(value, 4) AS value
FROM {C}.ml.platform_model_metrics""")

view("monitoring.v_plat_agent_requests", f"""
SELECT date(requested_at) AS request_date, status, count(*) AS requests,
       round(avg(latency_ms) / 1000, 1) AS avg_latency_s,
       round(percentile_approx(latency_ms, 0.95) / 1000, 1) AS p95_latency_s,
       round(avg(tool_call_count), 1) AS avg_tool_calls
FROM {C}.monitoring.agent_requests GROUP BY ALL""")

view("monitoring.v_plat_rag_retrieval", f"""
SELECT date(asked_at) AS ask_date, retriever, status, count(*) AS questions,
       count_if(citation_check_passed) AS citation_checks_passed, round(avg(latency_ms)) AS avg_latency_ms
FROM {C}.monitoring.rag_query_log GROUP BY ALL""")

view("monitoring.v_plat_pipeline_status", f"""
WITH f AS (
  SELECT 'bronze' AS layer, 'bronze_transaction' AS dataset, max(ingestion_timestamp) AS last_loaded, count(*) AS rows FROM {C}.bronze.bronze_transaction
  UNION ALL SELECT 'bronze', 'bronze_employee_access', max(ingestion_timestamp), count(*) FROM {C}.bronze.bronze_employee_access
  UNION ALL SELECT 'bronze', 'bronze_customer_account', max(ingestion_timestamp), count(*) FROM {C}.bronze.bronze_customer_account
  UNION ALL SELECT 'silver', 'silver_transaction', max(silver_processed_at), count(*) FROM {C}.silver.silver_transaction
  UNION ALL SELECT 'silver', 'silver_employee_access', max(silver_processed_at), count(*) FROM {C}.silver.silver_employee_access
  UNION ALL SELECT 'silver', 'quarantine_records', max(quarantined_at), count(*) FROM {C}.silver.quarantine_records
  UNION ALL SELECT 'controls', 'gold_control_test_results', max(execution_timestamp), count(*) FROM {C}.gold.gold_control_test_results
  UNION ALL SELECT 'ml', 'exception_risk_scores', max(scoring_timestamp), count(*) FROM {C}.ml.exception_risk_scores
  UNION ALL SELECT 'gold', 'gold_examiner_summary', max(generated_at), count(*) FROM {C}.gold.gold_examiner_summary
  UNION ALL SELECT 'rag', 'policy_chunks', max(ingested_at), count(*) FROM {C}.reference.policy_chunks)
SELECT *, round((unix_timestamp(current_timestamp()) - unix_timestamp(last_loaded)) / 3600, 1) AS hours_since_update,
       CASE WHEN last_loaded IS NULL THEN 'NO DATA'
            WHEN last_loaded < current_timestamp() - INTERVAL 2 DAYS THEN 'STALE' ELSE 'OK' END AS freshness
FROM f""")

view("monitoring.v_plat_failures", f"""
SELECT execution_timestamp AS event_time, 'control_test' AS component, control_id AS detail, error_message AS error
FROM {C}.gold.gold_control_test_results WHERE test_status = 'ERROR'
UNION ALL
SELECT CAST(started_at AS TIMESTAMP), 'agent_tool', tool, error FROM {C}.monitoring.agent_tool_calls WHERE status = 'ERROR'
UNION ALL
SELECT CAST(started_at AS TIMESTAMP), 'agent_llm_draft_rejected', tool, 'LLM draft failed output checks; rule-based text used'
FROM {C}.monitoring.agent_tool_calls WHERE tool LIKE 'llm:%' AND status = 'REJECTED'
UNION ALL
SELECT asked_at, 'rag_llm_fallback', question, error FROM {C}.monitoring.rag_query_log WHERE error IS NOT NULL
UNION ALL
SELECT asked_at, 'rag_citation_check', question, concat('invalid citations: ', array_join(invalid_citations, ','))
FROM {C}.monitoring.rag_query_log WHERE NOT citation_check_passed""")

# COMMAND ----------

print(f"{len(created)} views created")
for f in failed:
    print("FAILED:", f)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM examiner_compliance.gold.v_dash_kpis;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM examiner_compliance.monitoring.v_plat_kpis;
