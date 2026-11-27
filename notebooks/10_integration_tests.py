# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Integration and negative tests on the real tables  *(Implemented)*
# MAGIC Unit tests (`tests/`, run by CI) check the logic with fakes. This notebook checks the **deployed pipeline end to end**:
# MAGIC idempotency, metadata, quarantine, the planted violations being detected, evidence lineage, scoring, remediation,
# MAGIC RAG and agent results, and that **nothing was closed automatically**.
# MAGIC Results go to `monitoring.integration_test_results`. The notebook **fails** if any test fails, so the job fails too.

# COMMAND ----------

from datetime import datetime, timezone
C = "examiner_compliance"
RUN = f"ITEST-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
OCT = "2026-10-01..2026-10-31"
results = []

def one(sql):
    return spark.sql(sql).first()[0]

def check(test_id, category, description, fn):
    try:
        ok, detail = fn()
    except Exception as e:
        ok, detail = False, f"error: {str(e).splitlines()[0][:200]}"
    results.append((RUN, test_id, category, description, bool(ok), str(detail)[:500], datetime.now(timezone.utc)))
    print(("PASS " if ok else "FAIL ") + f"{test_id:<8} {description}  [{detail}]")

# COMMAND ----------

# MAGIC %md ## Bronze: metadata and idempotency

# COMMAND ----------

BRONZE = ["bronze_customer_account", "bronze_employee_access", "bronze_regulatory_reporting",
          "bronze_control_catalogue", "bronze_compliance_issues", "bronze_transaction"]
for t in BRONZE:
    check(f"BR-{t[7:12]}", "bronze", f"{t}: metadata populated on every row", lambda t=t: (
        (n := one(f"""SELECT count(*) FROM {C}.bronze.{t} WHERE ingestion_timestamp IS NULL OR batch_id IS NULL
                      OR source_file IS NULL OR source_system IS NULL OR record_hash IS NULL""")) == 0, f"{n} rows missing metadata"))
    check(f"BI-{t[7:12]}", "bronze", f"{t}: MERGE key unique (reruns don't duplicate)", lambda t=t: (
        (d := one(f"SELECT count(*) - count(DISTINCT source_file, record_hash, dup_seq) FROM {C}.bronze.{t}")) == 0, f"{d} duplicate keys"))

# COMMAND ----------

# MAGIC %md ## Silver: quarantine, uniqueness, standardisation

# COMMAND ----------

for t, key in [("silver_customer_account", "account_id"), ("silver_transaction", "transaction_id"),
               ("silver_employee_access", "access_id"), ("silver_regulatory_reporting", "report_id"),
               ("silver_control_catalogue", "control_id"), ("silver_compliance_issues", "issue_id")]:
    check(f"SK-{t[7:12]}", "silver", f"{t}: business key unique", lambda t=t, key=key: (
        (d := one(f"SELECT count(*) - count(DISTINCT {key}) FROM {C}.silver.{t}")) == 0, f"{d} duplicates"))
check("SQ-01", "silver", "quarantine holds duplicates AND malformed rows", lambda: (
    (r := {x[0] for x in spark.sql(f"SELECT DISTINCT explode(dq_errors) FROM {C}.silver.quarantine_records").collect()})
    >= {"DUPLICATE_RECORD", "MISSING_KEY", "INVALID_AMOUNT"}, sorted(r)))
check("SS-01", "silver", "currencies standardised (no C$, US$, lower case)", lambda: (
    (n := one(f"SELECT count(*) FROM {C}.silver.silver_transaction WHERE currency_code NOT IN ('CAD','USD','EUR','GBP')")) == 0, n))
check("SS-02", "silver", "account statuses standardised", lambda: (
    (n := one(f"SELECT count(*) FROM {C}.silver.silver_customer_account WHERE account_status NOT IN ('ACTIVE','DORMANT','CLOSED')")) == 0, n))
check("SL-01", "silver", "lineage carried to Silver (record_hash resolves in Bronze)", lambda: (
    (n := one(f"""SELECT count(*) FROM {C}.silver.silver_employee_access s
                  LEFT ANTI JOIN {C}.bronze.bronze_employee_access b ON b.record_hash = s.record_hash""")) == 0, f"{n} orphans"))

# COMMAND ----------

# MAGIC %md ## Controls: every planted violation detected for October 2026

# COMMAND ----------

EXPECTED = {   # control -> entities that MUST be exceptions
    "CTL-ACC-001": {"EA-0201", "EA-0202", "EA-0206"},
    "CTL-ACC-002": {"E1098"},
    "CTL-TXN-001": {"TXN-2026109001", "TXN-2026109002"},
    "CTL-TXN-002": {"CUST-09002", "CUST-09003"},
    "CTL-TXN-003": {"TXN-2026109008"},
    "CTL-REG-001": {"LARGE_CASH_TXN_REPORT|2026-09", "CAPITAL_ADEQUACY_RETURN|2026-09", "LIQUIDITY_COVERAGE_RETURN|2026-09"},
}
for ctl, ents in EXPECTED.items():
    check(f"CT-{ctl[4:]}", "controls", f"{ctl}: planted violations detected", lambda ctl=ctl, ents=ents: (
        ents <= (found := {r[0] for r in spark.sql(f"""SELECT entity_id FROM {C}.gold.gold_compliance_exceptions
                           WHERE control_id = '{ctl}' AND array_contains(detected_periods, '{OCT}')""").collect()}),
        f"missing {sorted(ents - found)}" if not ents <= found else f"{len(found)} found"))
check("CT-STAT", "controls", "all 6 controls executed for October (no ERROR)", lambda: (
    (n := one(f"""SELECT count(DISTINCT control_id) FROM {C}.gold.gold_control_test_results
                  WHERE period_start = '2026-10-01' AND test_status IN ('PASS','FAIL')""")) == 6, n))
check("CT-HIST", "controls", "history preserved (more than one period tested)", lambda: (
    (n := one(f"SELECT count(DISTINCT period_start) FROM {C}.gold.gold_control_test_results")) >= 2, f"{n} periods"))
check("EX-10025", "controls", "EXC-10025 = Bob's admin access, repeat, linked to ISS-3025", lambda: (
    (r := spark.sql(f"""SELECT entity_id, control_id, is_repeat, linked_issue_id FROM {C}.gold.gold_compliance_exceptions
                        WHERE exception_id = 'EXC-10025'""").first()) is not None
    and (r.entity_id, r.control_id, r.is_repeat, r.linked_issue_id) == ("EA-0201", "CTL-ACC-001", True, "ISS-3025"), r))
check("EX-UNIQ", "controls", "one exception per control+entity (stable IDs across reruns)", lambda: (
    (d := one(f"SELECT count(*) - count(DISTINCT exception_key) FROM {C}.gold.gold_compliance_exceptions")) == 0, d))

# COMMAND ----------

# MAGIC %md ## Evidence, scoring, remediation, summary

# COMMAND ----------

check("EV-01", "evidence", "every exception has an evidence record", lambda: (
    (n := one(f"""SELECT count(*) FROM {C}.gold.gold_compliance_exceptions e
                  LEFT ANTI JOIN {C}.gold.gold_compliance_evidence v ON v.evidence_id = e.evidence_reference""")) == 0, f"{n} without"))
check("EV-02", "evidence", "evidence traces to an existing Bronze row", lambda: (
    (n := one(f"""SELECT count(*) FROM {C}.gold.gold_compliance_evidence v
                  WHERE v.source_table = 'silver.silver_employee_access' AND NOT EXISTS (
                    SELECT 1 FROM {C}.bronze.bronze_employee_access b WHERE b.record_hash = v.bronze_record_hash)""")) == 0, n))
check("EV-03", "evidence", "evidence pending human review", lambda: (
    (n := one(f"SELECT count(*) FROM {C}.gold.gold_compliance_evidence WHERE review_status <> 'PENDING_HUMAN_REVIEW'")) == 0, n))
check("RS-01", "scoring", "every open exception scored; all flagged human_review_required", lambda: (
    (n := one(f"""SELECT count(*) FROM {C}.gold.gold_compliance_exceptions WHERE status <> 'CLOSED' AND risk_score IS NULL""")) == 0
    and one(f"SELECT count(*) FROM {C}.ml.exception_risk_scores WHERE NOT human_review_required") == 0, f"{n} unscored"))
check("RS-02", "scoring", "EXC-10025 is the highest-risk exception and CRITICAL", lambda: (
    (r := spark.sql(f"SELECT exception_id, risk_band FROM {C}.gold.gold_compliance_exceptions ORDER BY risk_score DESC LIMIT 1").first())
    and (r.exception_id, r.risk_band) == ("EXC-10025", "CRITICAL"), r))
check("RM-01", "remediation", "ISS-3025 is flagged overdue", lambda: (
    one(f"SELECT max(CAST(is_overdue AS INT)) FROM {C}.gold.gold_remediation_status WHERE issue_id = 'ISS-3025'") == 1, "ISS-3025"))
check("SU-01", "summary", "ACCESS_CONTROL October summary exists and is pending review", lambda: (
    (r := spark.sql(f"""SELECT controls_evaluated, human_review_status FROM {C}.gold.gold_examiner_summary
                        WHERE review_scope = 'ACCESS_CONTROL' AND period_label = '{OCT}'""").first()) is not None
    and r.controls_evaluated == 2 and r.human_review_status == "PENDING_HUMAN_REVIEW", r))

# COMMAND ----------

# MAGIC %md ## RAG, agent, governance; negative tests

# COMMAND ----------

check("RG-01", "rag", "45 policy chunks from 7 documents, all with classification", lambda: (
    (r := spark.sql(f"""SELECT count(*) AS n, count(DISTINCT doc_id) AS d, count_if(access_classification IS NULL) AS nul
                        FROM {C}.reference.policy_chunks""").first()) and r.d == 7 and r.n >= 40 and r.nul == 0, r))
check("RG-02", "rag", "latest RAG evaluation 100% correct", lambda: (
    (v := one(f"""SELECT avg(CAST(answer_correct AS INT)) FROM {C}.ml.rag_evaluation
                  WHERE eval_run_id = (SELECT max(eval_run_id) FROM {C}.ml.rag_evaluation)""")) == 1.0, v))
check("AG-01", "agent", "latest agent evaluation 100% passed", lambda: (
    (v := one(f"""SELECT avg(CAST(passed AS INT)) FROM {C}.ml.agent_evaluation
                  WHERE eval_run_id = (SELECT max(eval_run_id) FROM {C}.ml.agent_evaluation)""")) == 1.0, v))
check("NG-01", "negative", "no exception was closed automatically", lambda: (
    (n := one(f"SELECT count(*) FROM {C}.gold.gold_compliance_exceptions WHERE status <> 'OPEN'")) == 0, f"{n} non-OPEN"))
check("NG-02", "negative", "refused agent requests made zero tool calls", lambda: (
    (n := one(f"SELECT count(*) FROM {C}.monitoring.agent_requests WHERE status = 'REFUSED' AND tool_call_count > 0")) == 0, n))
check("NG-03", "negative", "no personal names in agent responses", lambda: (
    (n := one(f"""SELECT count(*) FROM {C}.monitoring.agent_requests r
                  WHERE exists(array('Bob Martin', 'Alice Chen'), n -> r.response LIKE concat('%', n, '%'))""")) == 0, n))
check("NG-04", "negative", "every agent response says Human Review Required: Yes", lambda: (
    (n := one(f"""SELECT count(*) FROM {C}.monitoring.agent_requests
                  WHERE status = 'COMPLETED' AND response NOT LIKE '%Human Review Required:%Yes%'""")) == 0, n))
check("NG-05", "negative", "Silver names are masked for non-PII viewers", lambda: (
    (r := spark.sql(f"""SELECT employee_name, is_account_group_member('examiner_pii_viewers') AS pii
                        FROM {C}.silver.silver_employee_access WHERE employee_id = 'E1099' LIMIT 1""").first())
    and (r.pii or str(r.employee_name).startswith("masked:")), r))
check("GV-01", "governance", "PII columns tagged", lambda: (
    (n := one(f"SELECT count(*) FROM {C}.information_schema.column_tags WHERE tag_name = 'pii' AND tag_value = 'name'")) >= 2, n))

# COMMAND ----------

df = spark.createDataFrame(results, "run_id STRING, test_id STRING, category STRING, description STRING, passed BOOLEAN, "
                                    "detail STRING, tested_at TIMESTAMP")
df.write.mode("append").saveAsTable(f"{C}.monitoring.integration_test_results")
display(df.groupBy("category").agg({"passed": "count"}).withColumnRenamed("count(passed)", "tests"))
failed = [r for r in results if not r[4]]
print(f"{len(results) - len(failed)}/{len(results)} passed")
assert not failed, "Integration tests failed: " + ", ".join(r[1] for r in failed)
