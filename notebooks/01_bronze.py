# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze ingestion  *(Implemented)*
# MAGIC
# MAGIC * Raw values are kept **as strings**: nothing is cleaned here.
# MAGIC * Graded metadata columns on every row: `ingestion_timestamp, batch_id, source_file, source_system, record_hash`
# MAGIC   (plus `dup_seq` = occurrence number of an identical row inside one file).
# MAGIC * **MERGE, not append**: key = `(source_file, record_hash, dup_seq)`. Rerunning loads nothing new,
# MAGIC   but exact duplicates *inside* a file are still kept, because Bronze is the faithful raw copy.
# MAGIC   Silver decides what a duplicate is.
# MAGIC * **Incremental path**: transactions arrive through Auto Loader
# MAGIC   (`cloudFiles`, `trigger(availableNow=True)`), with a `foreachBatch` MERGE.

# COMMAND ----------

import uuid
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

CATALOG = "examiner_compliance"
RAW = f"/Volumes/{CATALOG}/reference/raw_files"
CHECKPOINTS = f"/Volumes/{CATALOG}/reference/checkpoints"
BATCH_ID = f"BATCH-{uuid.uuid4().hex[:12]}"
META_COLS = {"ingestion_timestamp", "batch_id", "source_file", "source_system", "record_hash", "dup_seq"}
print("batch_id =", BATCH_ID)

# COMMAND ----------

def add_metadata(df, source_system, batch_id):
    """df must already contain `source_file` (from _metadata.file_path)."""
    raw_has_source_system = "source_system" in df.columns          # transactions carry it as data
    business_cols = sorted(
        c for c in df.columns
        if not c.startswith("_") and (c not in META_COLS or (c == "source_system" and raw_has_source_system))
    )
    hash_input = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in business_cols])
    out = (df.withColumn("ingestion_timestamp", F.current_timestamp())
             .withColumn("batch_id", F.lit(batch_id))
             .withColumn("record_hash", F.sha2(hash_input, 256)))
    if not raw_has_source_system:
        out = out.withColumn("source_system", F.lit(source_system))
    dup_window = Window.partitionBy("source_file", "record_hash").orderBy("record_hash")
    return out.withColumn("dup_seq", F.row_number().over(dup_window))


def merge_bronze(df, table):
    """Idempotent insert-only MERGE. Works inside foreachBatch (uses the micro-batch's session)."""
    session = df.sparkSession
    target = f"{CATALOG}.bronze.{table}"
    if not session.catalog.tableExists(target):
        df.limit(0).write.format("delta").saveAsTable(target)
    (DeltaTable.forName(session, target).alias("t")
        .merge(df.alias("s"),
               "t.source_file = s.source_file AND t.record_hash = s.record_hash AND t.dup_seq = s.dup_seq")
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

# MAGIC %md ## Batch sources (CSV) → Bronze

# COMMAND ----------

BATCH_SOURCES = [
    # bronze table                 raw file                   source system
    ("bronze_customer_account",     "customer_account.csv",     "CORE_BANKING_CIF"),
    ("bronze_employee_access",      "employee_access.csv",      "IAM_PLATFORM"),
    ("bronze_regulatory_reporting", "regulatory_reporting.csv", "REG_REPORTING_HUB"),
    ("bronze_control_catalogue",    "control_catalogue.csv",    "GRC_PLATFORM"),
    ("bronze_compliance_issues",    "compliance_issues.csv",    "GRC_PLATFORM"),
]

for table, file_name, system in BATCH_SOURCES:
    df = (spark.read.format("csv")
          .option("header", True)
          .option("inferSchema", False)        # everything stays STRING in Bronze
          .option("escape", '"')
          .load(f"{RAW}/{file_name}")
          .select("*", F.col("_metadata.file_path").alias("source_file")))
    merge_bronze(add_metadata(df, system, BATCH_ID), table)
    print(f"{table:<30} {spark.table(f'{CATALOG}.bronze.{table}').count():>6} rows")

# COMMAND ----------

# MAGIC %md ## Reference data → `reference` schema (small lookup tables, full overwrite)

# COMMAND ----------

for name in ["sod_rules", "high_risk_countries", "fx_rates", "regulatory_report_calendar"]:
    (spark.read.option("header", True).option("inferSchema", True).csv(f"{RAW}/reference/{name}.csv")
         .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{CATALOG}.reference.{name}"))
    print("reference.", name, sep="")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental path: transactions with Auto Loader
# MAGIC `availableNow` processes every file not yet seen (tracked in the checkpoint) and then stops,
# MAGIC so it works on serverless and in a scheduled job. Rerun it and only new files are read.

# COMMAND ----------

def upsert_transactions(micro_df, epoch_id):
    merge_bronze(add_metadata(micro_df, "PAYMENTS_HUB", f"{BATCH_ID}-{epoch_id}"), "bronze_transaction")

stream = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/bronze_transaction/_schema")
          .option("cloudFiles.inferColumnTypes", "false")          # strings only
          .load(f"{RAW}/transactions/")
          .select("*", F.col("_metadata.file_path").alias("source_file")))

query = (stream.writeStream
         .foreachBatch(upsert_transactions)
         .option("checkpointLocation", f"{CHECKPOINTS}/bronze_transaction")
         .trigger(availableNow=True)
         .start())
query.awaitTermination()
print("bronze_transaction", spark.table(f"{CATALOG}.bronze.bronze_transaction").count(), "rows")

# COMMAND ----------

# MAGIC %md ## Checks: idempotency and metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_file, batch_id, count(*) AS rows, count(DISTINCT record_hash) AS distinct_hashes,
# MAGIC        sum(CASE WHEN dup_seq > 1 THEN 1 ELSE 0 END) AS in_file_duplicates
# MAGIC FROM examiner_compliance.bronze.bronze_transaction
# MAGIC GROUP BY ALL ORDER BY source_file

# COMMAND ----------

# MAGIC %md Rerun this whole notebook: the row counts must not change (MERGE + Auto Loader checkpoint).
