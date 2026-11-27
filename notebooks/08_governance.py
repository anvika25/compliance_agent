# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Governance and access control  *(Implemented where your permissions allow; the rest Designed)*
# MAGIC
# MAGIC | Requirement | What this notebook does |
# MAGIC |---|---|
# MAGIC | Table ownership | `data_owner` / `data_steward` tags on every table; optional real `OWNER TO <group>` |
# MAGIC | Sensitive-data tags / PII identification | Column tags `pii`, `sensitivity`; table tags `layer`, `classification`, `retention` |
# MAGIC | Row/column-level controls | **Column mask** on names (only `pii_viewers` see them); **secure view** filtering policy chunks by clearance group |
# MAGIC | Role-based access / least privilege | GRANTs per persona: engineers, analysts, reviewers, the agent |
# MAGIC | Agent access restrictions | Agent identity: `SELECT` on named tables only, `MODIFY` only on its own log tables |
# MAGIC | Retention / evidence versioning | 7-year Delta history on evidence tables; time-travel example |
# MAGIC | Lineage / audit logs | Queries over `system.access.table_lineage` and `system.access.audit` |
# MAGIC | Environment separation | Designed: dev/prod catalogs via the Asset Bundle (step 8) |
# MAGIC
# MAGIC Every statement runs through `apply()`, which records **APPLIED** or **FAILED (reason)** instead of stopping. The
# MAGIC usual failure is a group that doesn't exist yet. Results go to `controls.governance_apply_log`, which is your evidence.

# COMMAND ----------

dbutils.widgets.text("grp_engineers", "examiner_engineers", "Group: data engineers")
dbutils.widgets.text("grp_analysts", "examiner_analysts", "Group: compliance analysts")
dbutils.widgets.text("grp_reviewers", "examiner_reviewers", "Group: compliance reviewers")
dbutils.widgets.text("grp_agent", "examiner_agent", "Group/SP: Examiner agent identity")
dbutils.widgets.text("grp_pii", "examiner_pii_viewers", "Group: may see names")
dbutils.widgets.text("grp_confidential", "examiner_confidential_readers", "Group: CONFIDENTIAL policies")
dbutils.widgets.text("grp_restricted", "examiner_restricted_readers", "Group: RESTRICTED policies")
dbutils.widgets.dropdown("change_ownership", "false", ["false", "true"], "Transfer table OWNERSHIP to groups")

G = {k: dbutils.widgets.get(f"grp_{k}") for k in ["engineers", "analysts", "reviewers", "agent", "pii", "confidential", "restricted"]}
C = "examiner_compliance"
from datetime import datetime, timezone
LOG = []

def apply(purpose, stmt):
    try:
        spark.sql(stmt)
        LOG.append((purpose, "APPLIED", " ".join(stmt.split())[:300], None))
    except Exception as e:
        LOG.append((purpose, "FAILED", " ".join(stmt.split())[:300], str(e).strip().splitlines()[0][:300]))

def tables(schema):
    return [r.tableName for r in spark.sql(f"SHOW TABLES IN {C}.{schema}").collect() if not r.isTemporary]

# COMMAND ----------

# MAGIC %md ## 1 · Ownership and classification tags (tables)

# COMMAND ----------

LAYER = {
    "bronze":     ("Data Engineering",        "Head of Data Engineering", "CONFIDENTIAL", "raw"),
    "silver":     ("Data Engineering",        "Head of Data Engineering", "CONFIDENTIAL", "conformed"),
    "gold":       ("Compliance Testing",      "Head of Compliance Testing", "CONFIDENTIAL", "data_product"),
    "controls":   ("Compliance Testing",      "Head of Compliance Testing", "INTERNAL", "control_framework"),
    "features":   ("Compliance Analytics",    "Head of Model Risk Management", "INTERNAL", "ml_features"),
    "ml":         ("Compliance Analytics",    "Head of Model Risk Management", "INTERNAL", "ml"),
    "monitoring": ("Platform Engineering",    "Head of Compliance Testing", "CONFIDENTIAL", "audit_logs"),
    "reference":  ("Compliance Policy Office", "Chief Compliance Officer", "INTERNAL", "reference"),
}
for schema, (owner, steward, cls, layer) in LAYER.items():
    for t in tables(schema):
        retention = "7y" if schema in ("gold", "controls", "monitoring", "ml") else "3y"
        apply("ownership+classification tags",
              f"ALTER TABLE {C}.{schema}.{t} SET TAGS ('data_owner' = '{owner}', 'data_steward' = '{steward}', "
              f"'classification' = '{cls}', 'layer' = '{layer}', 'retention' = '{retention}', 'domain' = 'compliance')")
        if dbutils.widgets.get("change_ownership") == "true":
            target = G["engineers"] if schema in ("bronze", "silver", "monitoring") else G["analysts"]
            apply("ownership transfer", f"ALTER TABLE {C}.{schema}.{t} OWNER TO `{target}`")

# COMMAND ----------

# MAGIC %md ## 2 · PII identification: column tags and comments

# COMMAND ----------

PII_COLUMNS = [
    ("silver.silver_customer_account", "customer_name", "name", "Customer legal or personal name"),
    ("silver.silver_employee_access",  "employee_name", "name", "Employee full name"),
    ("bronze.bronze_customer_account", "customer_name", "name", "Raw customer name (Bronze)"),
    ("bronze.bronze_employee_access",  "employee_name", "name", "Raw employee name (Bronze)"),
]
QUASI_IDENTIFIERS = [  # IDs that identify a person only together with other data: pseudonymous, still tagged
    ("silver.silver_customer_account", "customer_id"), ("silver.silver_employee_access", "employee_id"),
    ("silver.silver_transaction", "customer_id"), ("silver.silver_transaction", "initiated_by"),
    ("silver.silver_transaction", "approved_by"), ("gold.gold_compliance_exceptions", "entity_id"),
]
for tbl, col, kind, desc in PII_COLUMNS:
    apply("PII column tag", f"ALTER TABLE {C}.{tbl} ALTER COLUMN {col} SET TAGS ('pii' = '{kind}', 'sensitivity' = 'confidential')")
    apply("PII column comment", f"ALTER TABLE {C}.{tbl} ALTER COLUMN {col} COMMENT 'PII ({kind}): {desc}. Masked for non-PII viewers.'")
for tbl, col in QUASI_IDENTIFIERS:
    apply("pseudonymous ID tag", f"ALTER TABLE {C}.{tbl} ALTER COLUMN {col} SET TAGS ('pii' = 'pseudonymous_id', 'sensitivity' = 'internal')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Column mask: names visible only to `pii_viewers`
# MAGIC Everyone else sees a **pseudonym** (`masked:` + 8 characters of a hash). The same name always gives the same pseudonym,
# MAGIC so counts and joins still work. Gold never contains names.
# MAGIC **Masked in Silver, not Bronze:** Silver is rebuilt *from* Bronze, so masking Bronze would copy pseudonyms into
# MAGIC Silver. Bronze is protected by grants instead (only engineers can read it), and its name columns are still tagged as PII.

# COMMAND ----------

apply("mask function", f"""
CREATE OR REPLACE FUNCTION {C}.reference.mask_name(name STRING)
RETURNS STRING
COMMENT 'Column mask: full name for {G["pii"]}, deterministic pseudonym for everyone else'
RETURN CASE WHEN name IS NULL THEN NULL
            WHEN is_account_group_member('{G["pii"]}') THEN name
            ELSE concat('masked:', substr(sha2(name, 256), 1, 8)) END""")
MASKED = [(tbl, col) for tbl, col, _, _ in PII_COLUMNS if tbl.startswith("silver.")]
for tbl, col in MASKED:
    apply("column mask", f"ALTER TABLE {C}.{tbl} ALTER COLUMN {col} SET MASK {C}.reference.mask_name")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What YOU see now. Unless you are in the PII group, names come back as masked:xxxxxxxx
# MAGIC SELECT employee_id, employee_name, business_unit FROM examiner_compliance.silver.silver_employee_access
# MAGIC WHERE employee_id IN ('E1098', 'E1099') LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Row-level control: policy text by clearance group
# MAGIC Vector Search can't sync from a table with a row filter, so the rule lives in a **secure view**. People and the agent
# MAGIC are granted the view, never the base table. The Vector Search query applies the same rule through its
# MAGIC `access_classification` filter, using the caller's groups.

# COMMAND ----------

apply("clearance function", f"""
CREATE OR REPLACE FUNCTION {C}.reference.can_read_classification(cls STRING)
RETURNS BOOLEAN
COMMENT 'Row-level rule: which policy classifications the current user may read'
RETURN cls IN ('PUBLIC', 'INTERNAL')
    OR (cls = 'CONFIDENTIAL' AND (is_account_group_member('{G["confidential"]}') OR is_account_group_member('{G["restricted"]}')))
    OR (cls = 'RESTRICTED'   AND is_account_group_member('{G["restricted"]}'))""")
apply("secure view", f"""
CREATE OR REPLACE VIEW {C}.reference.policy_chunks_secure
COMMENT 'Policy chunks the current user is cleared to read. Grant this view, never the base table.' AS
SELECT * FROM {C}.reference.policy_chunks WHERE {C}.reference.can_read_classification(access_classification)""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Base table vs what you are cleared to see
# MAGIC SELECT 'base table' AS source, access_classification, count(*) AS chunks FROM examiner_compliance.reference.policy_chunks GROUP BY ALL
# MAGIC UNION ALL
# MAGIC SELECT 'secure view (you)', access_classification, count(*) FROM examiner_compliance.reference.policy_chunks_secure GROUP BY ALL
# MAGIC ORDER BY source, access_classification

# COMMAND ----------

# MAGIC %md ## 5 · Role-based, least-privilege grants

# COMMAND ----------

def grant(purpose, privs, securable, principal):
    apply(purpose, f"GRANT {privs} ON {securable} TO `{principal}`")

# everyone who works here can see the catalog exists
for p in G.values():
    if p not in (G["pii"], G["confidential"], G["restricted"]):
        grant("catalog access", "USE CATALOG", f"CATALOG {C}", p)

# data engineers: build and run the pipeline
for s in ["bronze", "silver", "gold", "controls", "features", "ml", "monitoring", "reference", "serving"]:
    grant("engineers: pipeline", "USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE FUNCTION", f"SCHEMA {C}.{s}", G["engineers"])
grant("engineers: raw files", "READ VOLUME, WRITE VOLUME", f"VOLUME {C}.reference.raw_files", G["engineers"])

# compliance analysts: read data products only (no Bronze/Silver, no raw files)
for s in ["gold", "ml"]:
    grant("analysts: data products", "USE SCHEMA, SELECT", f"SCHEMA {C}.{s}", G["analysts"])
grant("analysts: policies (secure view)", "USE SCHEMA", f"SCHEMA {C}.reference", G["analysts"])
grant("analysts: policies (secure view)", "SELECT", f"TABLE {C}.reference.policy_chunks_secure", G["analysts"])

# reviewers: analysts' access + read evidence files and the agent's audit trail
for s in ["gold", "ml", "monitoring"]:
    grant("reviewers: review", "USE SCHEMA, SELECT", f"SCHEMA {C}.{s}", G["reviewers"])
grant("reviewers: evidence files", "READ VOLUME", f"VOLUME {C}.reference.evidence", G["reviewers"])
grant("reviewers: policies", "USE SCHEMA", f"SCHEMA {C}.reference", G["reviewers"])
grant("reviewers: policies", "SELECT", f"TABLE {C}.reference.policy_chunks_secure", G["reviewers"])

# the Examiner agent: SELECT on exactly the tables its tools read; MODIFY only on its own logs
AGENT_READS = ["gold.gold_compliance_exceptions", "gold.gold_compliance_evidence", "gold.gold_control_test_results",
               "gold.gold_remediation_status", "gold.gold_examiner_summary", "gold.gold_compliance_dashboard",
               "ml.exception_risk_scores", "silver.silver_control_catalogue", "reference.policy_chunks_secure",
               # reperformance + lineage verification (read-only)
               "silver.silver_transaction", "silver.silver_employee_access", "silver.silver_customer_account",
               "silver.silver_regulatory_reporting", "reference.sod_rules", "reference.high_risk_countries",
               "reference.regulatory_report_calendar", "bronze.bronze_employee_access", "bronze.bronze_transaction",
               "bronze.bronze_customer_account", "bronze.bronze_regulatory_reporting"]
for s in sorted({t.split(".")[0] for t in AGENT_READS} | {"monitoring"}):
    grant("agent: schema", "USE SCHEMA", f"SCHEMA {C}.{s}", G["agent"])
for t in AGENT_READS:
    grant("agent: read-only table", "SELECT", f"TABLE {C}.{t}", G["agent"])
for t in ["monitoring.agent_requests", "monitoring.agent_tool_calls", "monitoring.rag_query_log"]:
    grant("agent: own logs only", "SELECT, MODIFY", f"TABLE {C}.{t}", G["agent"])
# deliberately NOT granted to the agent: MODIFY on gold/silver/bronze, CREATE anything, policy_chunks base table, raw volumes

# COMMAND ----------

# MAGIC %md ## 6 · Retention and evidence versioning (7 years of Delta history on audit evidence)

# COMMAND ----------

for t in ["gold.gold_compliance_evidence", "gold.gold_compliance_exceptions", "gold.gold_control_test_results",
          "ml.exception_risk_scores", "monitoring.agent_requests", "monitoring.agent_tool_calls", "controls.notification_outbox"]:
    apply("retention 7y", f"ALTER TABLE {C}.{t} SET TBLPROPERTIES ("
          "'delta.logRetentionDuration' = 'interval 2555 days', 'delta.deletedFileRetentionDuration' = 'interval 2555 days')")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Evidence versioning: every change is a version; any earlier state can be reproduced (time travel)
# MAGIC DESCRIBE HISTORY examiner_compliance.gold.gold_compliance_exceptions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: what the exception register looked like at its first version
# MAGIC SELECT exception_id, control_id, risk_score, risk_band, status
# MAGIC FROM examiner_compliance.gold.gold_compliance_exceptions VERSION AS OF 1
# MAGIC ORDER BY exception_id

# COMMAND ----------

# MAGIC %md ## 7 · Lineage and audit logs (system tables; skipped with a note if not enabled)

# COMMAND ----------

for title, sql in [
    ("Lineage into the evidence table", f"""
        SELECT DISTINCT source_table_full_name, target_table_full_name, entity_type, max(event_time) AS last_seen
        FROM system.access.table_lineage
        WHERE target_table_full_name LIKE '{C}.gold.%' OR target_table_full_name LIKE '{C}.silver.%'
        GROUP BY ALL ORDER BY target_table_full_name"""),
    ("Who accessed Examiner data (last 7 days)", f"""
        SELECT event_time, user_identity.email AS user, action_name, request_params.full_name_arg AS object
        FROM system.access.audit
        WHERE event_date >= current_date() - INTERVAL 7 DAYS AND request_params.full_name_arg LIKE '{C}.%'
        ORDER BY event_time DESC LIMIT 50""")]:
    try:
        print(title); display(spark.sql(sql))
    except Exception as e:
        print(f"  not available here ({str(e).splitlines()[0][:150]}). An account admin enables system schemas `access`.")
        LOG.append((title, "FAILED", "system table query", str(e).splitlines()[0][:300]))

# COMMAND ----------

# MAGIC %md ## 8 · Evidence of what was applied

# COMMAND ----------

log_df = spark.createDataFrame(
    [(datetime.now(timezone.utc), *r) for r in LOG],
    "applied_at TIMESTAMP, purpose STRING, result STRING, statement STRING, error STRING")
log_df.write.mode("append").saveAsTable(f"{C}.controls.governance_apply_log")
display(log_df.groupBy("purpose", "result").count().orderBy("purpose", "result"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- why things failed (usually: a group that doesn't exist yet)
# MAGIC SELECT purpose, error, count(*) AS n FROM examiner_compliance.controls.governance_apply_log
# MAGIC WHERE result = 'FAILED' AND applied_at = (SELECT max(applied_at) FROM examiner_compliance.controls.governance_apply_log)
# MAGIC GROUP BY ALL ORDER BY n DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the catalog now documents itself
# MAGIC SELECT schema_name, table_name, column_name, tag_name, tag_value
# MAGIC FROM examiner_compliance.information_schema.column_tags ORDER BY ALL

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Rollback (only if needed)
# MAGIC Uncomment to remove the column masks.

# COMMAND ----------

# for tbl, col in MASKED:
#     spark.sql(f"ALTER TABLE {C}.{tbl} ALTER COLUMN {col} DROP MASK")
