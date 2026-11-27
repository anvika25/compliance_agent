# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver cleansing and integration  *(Implemented)*
# MAGIC
# MAGIC One generic function, `to_silver()`, driven by a per-table config:
# MAGIC
# MAGIC 1. **Validate.** Each rule is a SQL boolean expression on the raw string columns. Rows that fail any rule,
# MAGIC    plus exact duplicates (same `record_hash` seen before), go to **`silver.quarantine_records`** with
# MAGIC    the reasons in `dq_errors` and the raw row as JSON. They are never silently dropped.
# MAGIC 2. **Standardize.** Types, dates (ISO + `MM/dd/yyyy`), currencies (`C$`→`CAD`), statuses (`Active `→`ACTIVE`), flags.
# MAGIC 3. **Deduplicate** by business key: the latest *source version* wins (`last_modified_ts` for transactions),
# MAGIC    then the latest ingestion.
# MAGIC 4. **MERGE** into the Silver table. For transactions the update only applies when the incoming version
# MAGIC    is newer. That handles late-arriving and out-of-order records.
# MAGIC 5. **Lineage**: `record_hash, source_file, batch_id, source_system, ingestion_timestamp` are carried
# MAGIC    through from Bronze, so every Silver row points back to its exact Bronze row.

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

C = "examiner_compliance"
B, S, REF = f"{C}.bronze", f"{C}.silver", f"{C}.reference"
LINEAGE_COLS = ["record_hash", "source_file", "batch_id", "source_system", "ingestion_timestamp"]
QUARANTINE = f"{S}.quarantine_records"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE} (
  source_table STRING, source_file STRING, record_hash STRING, dup_seq INT, batch_id STRING,
  dq_errors ARRAY<STRING>, raw_record STRING, quarantined_at TIMESTAMP)
COMMENT 'Rows rejected by Silver validation or identified as exact duplicates. Never deleted.'
""")

# --- SQL snippet helpers -------------------------------------------------------------
def s(c):  return f"nullif(trim({c}), '')"
def u(c):  return f"upper(nullif(trim({c}), ''))"
def d(c):  return f"coalesce(try_cast({c} AS DATE), CAST(try_to_timestamp({c}, 'MM/dd/yyyy') AS DATE))"
def ts(c): return f"coalesce(try_cast({c} AS TIMESTAMP), try_to_timestamp({c}, 'MM/dd/yyyy HH:mm:ss'))"
def dec(c): return f"try_cast({c} AS DECIMAL(18,2))"
def flag(c):
    return (f"CASE WHEN {u(c)} IN ('Y','YES','TRUE','1') THEN true "
            f"WHEN {u(c)} IN ('N','NO','FALSE','0') THEN false END")
def bad_optional_date(c): return f"{c} IS NOT NULL AND {d(c)} IS NULL"

# COMMAND ----------

def to_silver(name, keys, columns, rules, version_col=None, enrich=None):
    bronze = spark.table(f"{B}.bronze_{name}")
    target = f"{S}.silver_{name}"
    business = [c for c in bronze.columns if c not in LINEAGE_COLS + ["dup_seq"]]

    # 1. validate ---------------------------------------------------------------
    dup_rank = Window.partitionBy("record_hash").orderBy("ingestion_timestamp", "source_file", "dup_seq")
    flagged = bronze.withColumn("_dup_rank", F.row_number().over(dup_rank))
    checks = [F.when(F.expr(f"coalesce({expr}, false)"), F.lit(code)) for code, expr in rules.items()]
    checks.append(F.when(F.col("_dup_rank") > 1, F.lit("DUPLICATE_RECORD")))
    flagged = flagged.withColumn("dq_errors", F.filter(F.array(*checks), lambda x: x.isNotNull()))

    bad = flagged.filter(F.size("dq_errors") > 0)
    good = flagged.filter(F.size("dq_errors") == 0)

    quarantine = bad.select(
        F.lit(f"bronze_{name}").alias("source_table"), "source_file", "record_hash", "dup_seq", "batch_id",
        "dq_errors", F.to_json(F.struct(*business)).alias("raw_record"),
        F.current_timestamp().alias("quarantined_at"))
    (DeltaTable.forName(spark, QUARANTINE).alias("t")
        .merge(quarantine.alias("s"),
               "t.source_table = s.source_table AND t.source_file = s.source_file "
               "AND t.record_hash = s.record_hash AND t.dup_seq = s.dup_seq")
        .whenNotMatchedInsertAll().execute())

    # 2. standardize --------------------------------------------------------------
    typed = good.select(*[F.expr(expr).alias(col) for col, expr in columns.items()], *LINEAGE_COLS)
    if enrich:
        typed = enrich(typed)

    # 3. deduplicate on the business key ------------------------------------------
    order = ([F.col(version_col).desc_nulls_last()] if version_col else []) + \
            [F.col("ingestion_timestamp").desc(), F.col("source_file").desc()]
    latest = (typed.withColumn("_rn", F.row_number().over(Window.partitionBy(*keys).orderBy(*order)))
                   .filter("_rn = 1").drop("_rn")
                   .withColumn("silver_processed_at", F.current_timestamp()))

    # 4. MERGE --------------------------------------------------------------------
    if not spark.catalog.tableExists(target):
        latest.limit(0).write.format("delta").saveAsTable(target)
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    update_when = (f"s.{version_col} > t.{version_col} OR t.{version_col} IS NULL" if version_col
                   else "t.record_hash <> s.record_hash")
    (DeltaTable.forName(spark, target).alias("t")
        .merge(latest.alias("s"), on)
        .whenMatchedUpdateAll(condition=update_when)
        .whenNotMatchedInsertAll()
        .execute())

    q = spark.table(QUARANTINE).filter(F.col("source_table") == f"bronze_{name}").count()
    print(f"{target:<45} bronze={bronze.count():>5}  quarantined={q:>3}  silver={spark.table(target).count():>5}")

# COMMAND ----------

# MAGIC %md ## silver_customer_account

# COMMAND ----------

ACCOUNT_STATUS = f"CASE {u('account_status')} WHEN 'INACTIVE' THEN 'DORMANT' ELSE {u('account_status')} END"

to_silver(
    "customer_account", keys=["account_id"],
    columns={
        "customer_id": s("customer_id"), "account_id": s("account_id"),
        "customer_name": s("customer_name"),                       # PII: tagged + masked in governance step
        "risk_rating": u("risk_rating"), "country_code": u("country_code"),
        "account_status": ACCOUNT_STATUS,
        "account_open_date": d("account_open_date"), "account_close_date": d("account_close_date"),
        "product_type": u("product_type"),
    },
    rules={
        "MISSING_KEY": f"{s('customer_id')} IS NULL OR {s('account_id')} IS NULL",
        "INVALID_OPEN_DATE": f"{d('account_open_date')} IS NULL",
        "INVALID_CLOSE_DATE": bad_optional_date("account_close_date"),
        "INVALID_STATUS": f"coalesce({u('account_status')}, '') NOT IN ('ACTIVE','DORMANT','INACTIVE','CLOSED')",
        "INVALID_RISK_RATING": f"coalesce({u('risk_rating')}, '') NOT IN ('LOW','MEDIUM','HIGH')",
        "CLOSE_BEFORE_OPEN": f"{d('account_close_date')} < {d('account_open_date')}",
    })

# COMMAND ----------

# MAGIC %md ## silver_control_catalogue

# COMMAND ----------

to_silver(
    "control_catalogue", keys=["control_id"],
    columns={
        "control_id": s("control_id"), "control_name": s("control_name"), "control_domain": u("control_domain"),
        "control_description": s("control_description"), "control_owner": s("control_owner"),
        "severity": u("severity"), "test_type": f"lower({s('test_type')})",
        "threshold": "try_cast(threshold AS DOUBLE)", "tolerance_pct": "coalesce(try_cast(tolerance_pct AS DOUBLE), 0D)",
        "frequency": u("frequency"), "population": s("population"), "policy_reference": s("policy_reference"),
        "remediation_action": s("remediation_action"), "active_flag": flag("active_flag"),
    },
    rules={
        "MISSING_KEY": f"{s('control_id')} IS NULL",
        "INVALID_SEVERITY": f"coalesce({u('severity')}, '') NOT IN ('CRITICAL','HIGH','MEDIUM','LOW')",
        "INVALID_THRESHOLD": "try_cast(threshold AS DOUBLE) IS NULL",
        "INVALID_ACTIVE_FLAG": f"({flag('active_flag')}) IS NULL",
        "MISSING_TEST_TYPE": f"{s('test_type')} IS NULL",
    })

# COMMAND ----------

# MAGIC %md ## silver_transaction  (late-arriving aware)

# COMMAND ----------

CURRENCY = f"CASE {u('currency_code')} WHEN 'C$' THEN 'CAD' WHEN 'US$' THEN 'USD' ELSE {u('currency_code')} END"
TXN_STATUS = f"CASE {u('status')} WHEN 'COMPLETE' THEN 'COMPLETED' ELSE {u('status')} END"

def enrich_transactions(df):
    fx = spark.table(f"{REF}.fx_rates").select("currency_code", F.col("rate_to_cad").cast("decimal(18,6)"))
    acct = spark.table(f"{S}.silver_customer_account").select(
        "account_id", F.col("customer_id").alias("_acct_customer_id"))
    return (df.join(fx, "currency_code", "left")
              .withColumn("amount_cad", F.round(F.col("amount") * F.col("rate_to_cad"), 2).cast("decimal(18,2)"))
              .join(acct, "account_id", "left")
              # relationship resolution: flag, don't drop
              .withColumn("account_found", F.col("_acct_customer_id").isNotNull())
              .withColumn("customer_account_mismatch",
                          F.coalesce(F.col("_acct_customer_id") != F.col("customer_id"), F.lit(False)))
              # late-arriving = business event earlier than the extract window it arrived in
              .withColumn("is_late_arriving",
                          F.col("transaction_timestamp") < F.col("extract_window_start").cast("timestamp"))
              .drop("_acct_customer_id", "rate_to_cad"))

txn_rules = {
    "MISSING_TRANSACTION_ID": f"{s('transaction_id')} IS NULL",
    "MISSING_ACCOUNT_ID": f"{s('account_id')} IS NULL",
    "INVALID_TIMESTAMP": f"{ts('transaction_timestamp')} IS NULL",
    "INVALID_AMOUNT": f"{dec('amount')} IS NULL OR {dec('amount')} <= 0",
    "INVALID_CURRENCY": f"coalesce({CURRENCY}, '') NOT IN ('CAD','USD','EUR','GBP')",
    "INVALID_STATUS": f"coalesce({TXN_STATUS}, '') NOT IN ('COMPLETED','PENDING','REVERSED')",
}
if "_rescued_data" in spark.table(f"{B}.bronze_transaction").columns:
    txn_rules["RESCUED_DATA"] = "_rescued_data IS NOT NULL"          # unexpected fields from Auto Loader

to_silver(
    "transaction", keys=["transaction_id"], version_col="last_modified_ts",
    columns={
        "transaction_id": s("transaction_id"), "customer_id": s("customer_id"), "account_id": s("account_id"),
        "transaction_timestamp": ts("transaction_timestamp"), "transaction_type": u("transaction_type"),
        "amount": dec("amount"), "currency_code": CURRENCY, "country_code": u("country_code"),
        "channel": u("channel"), "status": TXN_STATUS,
        "initiated_by": s("initiated_by"), "approved_by": s("approved_by"),
        "approval_reference": s("approval_reference"),
        "last_modified_ts": ts("last_modified_ts"), "extract_window_start": d("extract_window_start"),
    },
    rules=txn_rules, enrich=enrich_transactions)

# COMMAND ----------

# MAGIC %md ## silver_employee_access

# COMMAND ----------

to_silver(
    "employee_access", keys=["access_id"],
    columns={
        "access_id": s("access_id"), "employee_id": s("employee_id"),
        "employee_name": s("employee_name"),                        # PII
        "employee_role": s("employee_role"), "business_unit": u("business_unit"),
        "access_role": u("access_role"), "application_name": u("application_name"),
        "access_status": u("access_status"),
        "granted_date": d("granted_date"), "access_review_date": d("access_review_date"),
        "termination_date": d("termination_date"),
        "privileged_access_flag": flag("privileged_access_flag"), "manager_id": s("manager_id"),
    },
    rules={
        "MISSING_KEY": f"{s('access_id')} IS NULL OR {s('employee_id')} IS NULL",
        "INVALID_GRANTED_DATE": f"{d('granted_date')} IS NULL",
        "INVALID_REVIEW_DATE": bad_optional_date("access_review_date"),
        "INVALID_TERMINATION_DATE": bad_optional_date("termination_date"),
        "INVALID_PRIVILEGED_FLAG": f"({flag('privileged_access_flag')}) IS NULL",
        "INVALID_ACCESS_STATUS": f"coalesce({u('access_status')}, '') NOT IN ('ACTIVE','REVOKED')",
    })

# COMMAND ----------

# MAGIC %md ## silver_regulatory_reporting

# COMMAND ----------

to_silver(
    "regulatory_reporting", keys=["report_id"],
    columns={
        "report_id": s("report_id"), "report_type": u("report_type"), "reporting_period": s("reporting_period"),
        "reported_amount": dec("reported_amount"), "source_system_amount": dec("source_system_amount"),
        "submission_status": u("submission_status"), "submission_timestamp": ts("submission_timestamp"),
        "approval_status": u("approval_status"), "approver_id": s("approver_id"), "prepared_by": s("prepared_by"),
    },
    rules={
        "MISSING_KEY": f"{s('report_id')} IS NULL",
        "INVALID_AMOUNT": f"{dec('reported_amount')} IS NULL",
        "INVALID_PERIOD": "reporting_period IS NULL OR NOT (trim(reporting_period) RLIKE '^[0-9]{4}-[0-9]{2}$')",
        "INVALID_SUBMISSION_TS": f"submission_timestamp IS NOT NULL AND {ts('submission_timestamp')} IS NULL",
    })

# COMMAND ----------

# MAGIC %md ## silver_compliance_issues

# COMMAND ----------

def std_status(c): return f"regexp_replace({u(c)}, '[ -]+', '_')"

def enrich_issues(df):
    ctl = spark.table(f"{S}.silver_control_catalogue").select("control_id", F.lit(True).alias("control_found"))
    return df.join(ctl, "control_id", "left").withColumn("control_found", F.coalesce("control_found", F.lit(False)))

to_silver(
    "compliance_issues", keys=["issue_id"],
    columns={
        "issue_id": s("issue_id"), "exception_id": s("exception_id"), "control_id": s("control_id"),
        "entity_id": s("entity_id"), "issue_title": s("issue_title"), "issue_status": std_status("issue_status"),
        "issue_owner": s("issue_owner"), "business_unit": u("business_unit"), "severity": u("severity"),
        "opened_date": d("opened_date"), "target_remediation_date": d("target_remediation_date"),
        "actual_remediation_date": d("actual_remediation_date"),
        "validation_status": std_status("validation_status"), "remediation_action": s("remediation_action"),
    },
    rules={
        "MISSING_KEY": f"{s('issue_id')} IS NULL",
        "INVALID_OPENED_DATE": f"{d('opened_date')} IS NULL",
        "INVALID_TARGET_DATE": f"{d('target_remediation_date')} IS NULL",
        "TARGET_BEFORE_OPENED": f"{d('target_remediation_date')} < {d('opened_date')}",
        "INVALID_STATUS": f"coalesce({std_status('issue_status')}, '') NOT IN ('OPEN','IN_PROGRESS','CLOSED')",
    },
    enrich=enrich_issues)

# COMMAND ----------

# MAGIC %md ## What was quarantined, and why

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_table, explode(dq_errors) AS reason, count(*) AS rows
# MAGIC FROM examiner_compliance.silver.quarantine_records
# MAGIC GROUP BY ALL ORDER BY source_table, reason

# COMMAND ----------

# MAGIC %sql
# MAGIC -- late-arriving handling (after write_late_batch + rerun of 01 and 02):
# MAGIC SELECT transaction_id, status, last_modified_ts, is_late_arriving, source_file
# MAGIC FROM examiner_compliance.silver.silver_transaction
# MAGIC WHERE is_late_arriving OR (source_file LIKE '%2026_11%' AND transaction_timestamp < '2026-11-01')
