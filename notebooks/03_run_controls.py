# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Metadata-driven control execution  *(Implemented)*
# MAGIC
# MAGIC ```
# MAGIC silver_control_catalogue (active_flag = true)
# MAGIC     └─ for each control:  src/controls/sql/<test_type>.sql   (one file per test type)
# MAGIC            ├─ spark.sql(file, args={period_start, period_end, threshold})   ← named parameters, no string-building
# MAGIC            ├─ population_count  = all rows returned
# MAGIC            ├─ exception_count   = rows where is_exception
# MAGIC            ├─ gold_control_test_results   (APPEND: one row per control per run = history)
# MAGIC            ├─ gold_compliance_exceptions  (MERGE on control_id|entity_id: stable IDs, repeat tracking, never auto-closed)
# MAGIC            ├─ gold_compliance_evidence    (one row per exception per run + JSON file in the evidence volume)
# MAGIC            └─ controls.notification_outbox (SIMULATED alert for Critical controls with exceptions)
# MAGIC ```
# MAGIC Adding a control = one catalogue row (+ a SQL file if it is a new test type). No notebook changes.
# MAGIC
# MAGIC `risk_score` / `risk_band` are left NULL here; the risk-scoring step fills them.

# COMMAND ----------

from datetime import date, datetime, timedelta, timezone

_today = date.today()
_prev_end = _today.replace(day=1) - timedelta(days=1)
dbutils.widgets.text("period_start", _prev_end.replace(day=1).isoformat(), "Period start (yyyy-MM-dd)")
dbutils.widgets.text("period_end", _prev_end.isoformat(), "Period end (yyyy-MM-dd)")
dbutils.widgets.text("control_ids", "", "Control IDs (comma-separated, blank = all active)")

PERIOD_START = dbutils.widgets.get("period_start")
PERIOD_END = dbutils.widgets.get("period_end")
CONTROL_IDS = [c.strip() for c in dbutils.widgets.get("control_ids").split(",") if c.strip()]
date.fromisoformat(PERIOD_START); date.fromisoformat(PERIOD_END)              # validate input
assert PERIOD_START <= PERIOD_END, "period_start must be <= period_end"

# COMMAND ----------

import os, json, hashlib, uuid
import pandas as pd
from pyspark.sql import functions as F
from delta.tables import DeltaTable

C = "examiner_compliance"
S, G, CTL = f"{C}.silver", f"{C}.gold", f"{C}.controls"
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
SQL_DIR = os.path.join(REPO_ROOT, "src", "controls", "sql")
EVIDENCE_ROOT = f"/Volumes/{C}/reference/evidence"

RUN_TS = datetime.now(timezone.utc)
RUN_BATCH_ID = f"RUN-{RUN_TS:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
EXECUTED_BY = spark.sql("SELECT current_user()").first()[0]
PERIOD_LABEL = f"{PERIOD_START}..{PERIOD_END}"
print(RUN_BATCH_ID, PERIOD_LABEL, EXECUTED_BY)

REQUIRED_COLS = ["entity_id", "entity_type", "business_unit", "is_exception", "exception_description",
                 "expected_condition", "actual_condition", "variance", "financial_impact", "detection_confidence",
                 "event_timestamp", "source_table", "source_record_id", "src_record_hash", "src_source_file",
                 "src_batch_id", "related_records"]

# COMMAND ----------

# MAGIC %md ## Output tables

# COMMAND ----------

RESULTS_DDL = """test_run_id STRING, run_batch_id STRING, control_id STRING, control_name STRING, control_domain STRING,
  severity STRING, period_start DATE, period_end DATE, population_count BIGINT, exception_count BIGINT,
  exception_rate DOUBLE, threshold DOUBLE, tolerance_pct DOUBLE, test_status STRING, sql_file STRING,
  sql_sha256 STRING, execution_timestamp TIMESTAMP, executed_by STRING, error_message STRING"""
EXCEPTIONS_DDL = """exception_id STRING, exception_key STRING, control_id STRING, control_domain STRING, entity_id STRING,
  entity_type STRING, business_unit STRING, exception_description STRING, severity STRING, risk_score DOUBLE,
  risk_band STRING, evidence_reference STRING, owner STRING, status STRING, policy_reference STRING,
  remediation_action STRING, financial_impact DOUBLE, detection_confidence DOUBLE, is_repeat BOOLEAN,
  linked_issue_id STRING, detected_periods ARRAY<STRING>, first_test_run_id STRING, last_test_run_id STRING,
  creation_timestamp TIMESTAMP, last_detected_timestamp TIMESTAMP"""
EVIDENCE_DDL = """evidence_id STRING, exception_id STRING, test_run_id STRING, control_id STRING, source_table STRING,
  source_record_id STRING, evidence_summary STRING, control_logic STRING, expected_condition STRING,
  actual_condition STRING, variance STRING, event_timestamp TIMESTAMP, related_records STRING,
  policy_reference STRING, evidence_completeness DOUBLE, evidence_gaps ARRAY<STRING>, review_status STRING,
  lineage_reference STRING, bronze_record_hash STRING, source_file STRING, source_batch_id STRING,
  source_table_version BIGINT, evidence_file STRING, evidence_timestamp TIMESTAMP"""
OUTBOX_DDL = """notification_id STRING, test_run_id STRING, control_id STRING, severity STRING, recipient STRING,
  channel STRING, exception_ids ARRAY<STRING>, message STRING, status STRING, created_at TIMESTAMP"""

for table, ddl, comment in [
    (f"{G}.gold_control_test_results", RESULTS_DDL, "One row per control per test run (append-only history)"),
    (f"{G}.gold_compliance_exceptions", EXCEPTIONS_DDL, "Current exception register. Closed only by human review."),
    (f"{G}.gold_compliance_evidence", EVIDENCE_DDL, "Reproducible evidence per exception per run, with lineage"),
    (f"{CTL}.notification_outbox", OUTBOX_DDL, "SIMULATED notifications awaiting human review"),
]:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({ddl}) COMMENT '{comment}'")

def cols(ddl):
    return [part.strip().split(" ")[0] for part in ddl.replace("\n", " ").split(",") if part.strip()]

# COMMAND ----------

# MAGIC %md ## Exception-ID registry
# MAGIC IDs are stable: a (control, entity) pair keeps its ID across runs. If an earlier issue already references
# MAGIC that pair (for example ISS-3025 → EXC-10025 for `EA-0201`), the old ID is reused and the exception is marked as a repeat.

# COMMAND ----------

existing_ids = {r.exception_key: r.exception_id
                for r in spark.table(f"{G}.gold_compliance_exceptions").select("exception_key", "exception_id").collect()}
issue_links = {f"{r.control_id}|{r.entity_id}": (r.exception_id, r.issue_id)
               for r in spark.table(f"{S}.silver_compliance_issues")
                             .filter("exception_id IS NOT NULL").select("control_id", "entity_id", "exception_id", "issue_id").collect()}
known = [i for i in list(existing_ids.values()) + [v[0] for v in issue_links.values()] if i and i.startswith("EXC-")]
next_seq = max([int(i.split("-")[1]) for i in known] + [10000]) + 1

_version_cache = {}
def table_version(short_name):
    if short_name not in _version_cache:
        _version_cache[short_name] = spark.sql(f"DESCRIBE HISTORY {C}.{short_name} LIMIT 1").first()["version"]
    return _version_cache[short_name]

def py(v):
    """pandas/numpy value -> plain Python (None for NaN/NaT)."""
    if v is None or (not isinstance(v, (list, dict, str)) and pd.isna(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v.item() if hasattr(v, "item") else v

# COMMAND ----------

# MAGIC %md ## Runner

# COMMAND ----------

def run_control(ctl):
    global next_seq
    test_run_id = f"TR-{RUN_TS:%Y%m%dT%H%M%S}-{ctl.control_id}"
    sql_file = f"{ctl.test_type}.sql"
    sql_text = sql_sha = err = None
    pop_count = exc_count = 0
    status = "ERROR"
    exc_rows, evd_rows = [], []
    try:
        with open(os.path.join(SQL_DIR, sql_file)) as f:
            sql_text = f.read()
        sql_sha = hashlib.sha256(sql_text.encode()).hexdigest()
        body = "\n".join(l for l in sql_text.splitlines() if not l.strip().startswith("--"))
        args = {k: v for k, v in {"period_start": PERIOD_START, "period_end": PERIOD_END,
                                  "threshold": float(ctl.threshold)}.items() if f":{k}" in body}
        result = spark.sql(sql_text, args=args)
        missing = set(REQUIRED_COLS) - set(result.columns)
        if missing:
            raise ValueError(f"{sql_file} does not return required columns: {sorted(missing)}")

        pop_count = result.count()
        exc_pdf = result.filter("is_exception").select(*REQUIRED_COLS).toPandas()
        exc_count = len(exc_pdf)
        rate = exc_count / pop_count if pop_count else 0.0
        status = "FAIL" if rate * 100 > (ctl.tolerance_pct or 0) else "PASS"

        run_dir = f"{EVIDENCE_ROOT}/{RUN_BATCH_ID}/{ctl.control_id}"
        if exc_count:
            os.makedirs(run_dir, exist_ok=True)

        for rec in exc_pdf.to_dict("records"):
            rec = {k: py(v) for k, v in rec.items()}
            key = f"{ctl.control_id}|{rec['entity_id']}"
            linked_issue = issue_links.get(key, (None, None))[1]
            if key in existing_ids:
                exc_id = existing_ids[key]
            elif key in issue_links:
                exc_id = issue_links[key][0]
            else:
                exc_id, next_seq = f"EXC-{next_seq}", next_seq + 1
            existing_ids[key] = exc_id
            evidence_id = "EVD-" + hashlib.sha256(f"{test_run_id}|{exc_id}".encode()).hexdigest()[:12].upper()

            version = table_version(rec["source_table"])
            lineage = (f"{C}.{rec['source_table']}@v{version} <- bronze record_hash={rec['src_record_hash']} "
                       f"(file={rec['src_source_file']}, batch={rec['src_batch_id']})")
            required = {"source_record_id": rec["source_record_id"], "bronze_record_hash": rec["src_record_hash"],
                        "source_file": rec["src_source_file"], "expected_condition": rec["expected_condition"],
                        "actual_condition": rec["actual_condition"], "event_timestamp": rec["event_timestamp"],
                        "policy_reference": ctl.policy_reference}
            gaps = [k for k, v in required.items() if v in (None, "")]
            completeness = round(1 - len(gaps) / len(required), 2)
            evidence_file = f"{run_dir}/{exc_id}.json"

            evidence = {
                "evidence_id": evidence_id, "exception_id": exc_id, "test_run_id": test_run_id,
                "control_id": ctl.control_id, "source_table": rec["source_table"],
                "source_record_id": rec["source_record_id"],
                "evidence_summary": f"{ctl.control_name}: {rec['exception_description']}",
                "control_logic": f"src/controls/sql/{sql_file} sha256={sql_sha[:16]} args={json.dumps(args)}",
                "expected_condition": rec["expected_condition"], "actual_condition": rec["actual_condition"],
                "variance": rec["variance"], "event_timestamp": rec["event_timestamp"],
                "related_records": rec["related_records"], "policy_reference": ctl.policy_reference,
                "evidence_completeness": completeness, "evidence_gaps": gaps, "review_status": "PENDING_HUMAN_REVIEW",
                "lineage_reference": lineage, "bronze_record_hash": rec["src_record_hash"],
                "source_file": rec["src_source_file"], "source_batch_id": rec["src_batch_id"],
                "source_table_version": version, "evidence_file": evidence_file, "evidence_timestamp": RUN_TS,
            }
            with open(evidence_file, "w") as f:                       # immutable, versioned-by-run evidence copy
                json.dump(evidence, f, default=str, indent=2)
            evd_rows.append(evidence)

            exc_rows.append({
                "exception_id": exc_id, "exception_key": key, "control_id": ctl.control_id,
                "control_domain": ctl.control_domain, "entity_id": rec["entity_id"], "entity_type": rec["entity_type"],
                "business_unit": rec["business_unit"], "exception_description": rec["exception_description"],
                "severity": ctl.severity, "risk_score": None, "risk_band": None, "evidence_reference": evidence_id,
                "owner": ctl.control_owner, "status": "OPEN", "policy_reference": ctl.policy_reference,
                "remediation_action": ctl.remediation_action, "financial_impact": rec["financial_impact"],
                "detection_confidence": rec["detection_confidence"], "is_repeat": linked_issue is not None,
                "linked_issue_id": linked_issue, "detected_periods": [PERIOD_LABEL],
                "first_test_run_id": test_run_id, "last_test_run_id": test_run_id,
                "creation_timestamp": RUN_TS, "last_detected_timestamp": RUN_TS,
            })
    except Exception as e:                                            # one bad control must not stop the run
        err = f"{type(e).__name__}: {e}"[:2000]
        status = "ERROR"

    if exc_rows:
        src = spark.createDataFrame([tuple(r[c] for c in cols(EXCEPTIONS_DDL)) for r in exc_rows], EXCEPTIONS_DDL)
        (DeltaTable.forName(spark, f"{G}.gold_compliance_exceptions").alias("t")
            .merge(src.alias("s"), "t.exception_key = s.exception_key")
            .whenMatchedUpdate(set={
                "exception_description": "s.exception_description", "financial_impact": "s.financial_impact",
                "detection_confidence": "s.detection_confidence", "evidence_reference": "s.evidence_reference",
                "last_test_run_id": "s.last_test_run_id", "last_detected_timestamp": "s.last_detected_timestamp",
                "detected_periods": "array_union(t.detected_periods, s.detected_periods)",
                "is_repeat": "t.linked_issue_id IS NOT NULL OR size(array_union(t.detected_periods, s.detected_periods)) > 1",
                # status is deliberately NOT touched: closing is a human decision
            })
            .whenNotMatchedInsertAll()
            .execute())
        evd = spark.createDataFrame([tuple(r[c] for c in cols(EVIDENCE_DDL)) for r in evd_rows], EVIDENCE_DDL)
        (DeltaTable.forName(spark, f"{G}.gold_compliance_evidence").alias("t")
            .merge(evd.alias("s"), "t.evidence_id = s.evidence_id").whenNotMatchedInsertAll().execute())

        if ctl.severity == "CRITICAL":
            ids = [r["exception_id"] for r in exc_rows]
            spark.createDataFrame([(
                f"NTF-{uuid.uuid4().hex[:10].upper()}", test_run_id, ctl.control_id, ctl.severity, ctl.control_owner,
                "SIMULATED_EMAIL", ids,
                f"{len(ids)} critical exception(s) on {ctl.control_id} for {PERIOD_LABEL}. Human review required.",
                "PENDING_HUMAN_REVIEW", RUN_TS)], OUTBOX_DDL
            ).write.mode("append").saveAsTable(f"{CTL}.notification_outbox")

    return (test_run_id, RUN_BATCH_ID, ctl.control_id, ctl.control_name, ctl.control_domain, ctl.severity,
            date.fromisoformat(PERIOD_START), date.fromisoformat(PERIOD_END), int(pop_count), int(exc_count),
            float(exc_count / pop_count) if pop_count else 0.0, float(ctl.threshold), float(ctl.tolerance_pct or 0),
            status, sql_file, sql_sha, RUN_TS, EXECUTED_BY, err)

# COMMAND ----------

catalogue = spark.table(f"{S}.silver_control_catalogue").filter("active_flag")
if CONTROL_IDS:
    catalogue = catalogue.filter(F.col("control_id").isin(CONTROL_IDS))
controls = catalogue.orderBy("control_id").collect()
print(f"{len(controls)} active control(s) loaded")

results = []
for ctl in controls:
    row = run_control(ctl)
    results.append(row)
    print(f"{row[2]:<12} {row[13]:<6} population={row[8]:>5} exceptions={row[9]:>3} {row[18] or ''}")

spark.createDataFrame(results, RESULTS_DDL).write.mode("append").saveAsTable(f"{G}.gold_control_test_results")

# COMMAND ----------

# MAGIC %md ## Results of this run

# COMMAND ----------

display(spark.table(f"{G}.gold_control_test_results").filter(F.col("run_batch_id") == RUN_BATCH_ID)
        .select("control_id", "control_name", "population_count", "exception_count", "exception_rate",
                "threshold", "test_status", "error_message"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT e.exception_id, e.control_id, e.entity_id, e.severity, e.is_repeat, e.linked_issue_id,
# MAGIC        e.exception_description, v.evidence_completeness, v.lineage_reference
# MAGIC FROM examiner_compliance.gold.gold_compliance_exceptions e
# MAGIC JOIN examiner_compliance.gold.gold_compliance_evidence v ON v.evidence_id = e.evidence_reference
# MAGIC ORDER BY e.control_id, e.exception_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The scenario exception, traced back to Bronze
# MAGIC SELECT v.*, b.source_file AS bronze_file, b.ingestion_timestamp AS bronze_ingested_at
# MAGIC FROM examiner_compliance.gold.gold_compliance_evidence v
# MAGIC JOIN examiner_compliance.bronze.bronze_employee_access b ON b.record_hash = v.bronze_record_hash
# MAGIC WHERE v.exception_id = 'EXC-10025'
# MAGIC ORDER BY v.evidence_timestamp DESC
