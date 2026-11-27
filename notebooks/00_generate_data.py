# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Generate synthetic data  *(Implemented)*
# MAGIC
# MAGIC Plain Python + pandas. Writes raw CSV / JSON-lines / Markdown into
# MAGIC `/Volumes/examiner_compliance/reference/raw_files/`. No Spark is used to *create* data,
# MAGIC so the generator is unit-testable (`tests/test_synthetic_data.py`).
# MAGIC
# MAGIC | Planted issue | Record | Control that should catch it |
# MAGIC |---|---|---|
# MAGIC | Bob Martin terminated 2026-09-30, admin access still ACTIVE | `EA-0201` → **EXC-10025** | CTL-ACC-001 |
# MAGIC | Bob's payment-initiator access still ACTIVE | `EA-0202` | CTL-ACC-001 |
# MAGIC | Privileged DBA not reviewed in > 90 days | `EA-0206` | CTL-ACC-001 |
# MAGIC | Alice Chen: PAYMENT_INITIATOR + PAYMENT_APPROVER | `E1098` | CTL-ACC-002 |
# MAGIC | Overdue remediation of EXC-10025 | `ISS-3025` (target 2026-10-31, OPEN) | remediation tracking |
# MAGIC | 48.5k wire, no approver / 72k wire self-approved | `TXN-2026109001`, `TXN-2026109002` | CTL-TXN-001 |
# MAGIC | 4 near-threshold cash deposits in 5 days; wire to IR | `CUST-09002`, `CUST-09003` | CTL-TXN-002 |
# MAGIC | 25k withdrawal on dormant account | `TXN-2026109008` | CTL-TXN-003 |
# MAGIC | Late / unapproved / missing reports (2026-09) | LCTR, CAR, LCR | CTL-REG-001 |
# MAGIC | Exact duplicates + malformed rows | every dataset | Silver quarantine |

# COMMAND ----------

# MAGIC %md ## 1 · Unity Catalog objects

# COMMAND ----------

CATALOG = "examiner_compliance"

# If CREATE CATALOG fails (no metastore admin rights / Free Edition), create the catalog in
# Catalog Explorer once, or point CATALOG at a catalog you own.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ["bronze", "silver", "gold", "controls", "features", "ml", "serving", "monitoring", "reference"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
for volume in ["raw_files", "checkpoints", "evidence"]:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.reference.{volume}")

RAW = f"/Volumes/{CATALOG}/reference/raw_files"

# COMMAND ----------

# MAGIC %md ## 2 · Optional reset (wipes raw files *and* streaming checkpoints)

# COMMAND ----------

dbutils.widgets.dropdown("reset", "false", ["false", "true"])
if dbutils.widgets.get("reset") == "true":
    dbutils.fs.rm(RAW, True)
    dbutils.fs.rm(f"/Volumes/{CATALOG}/reference/checkpoints", True)
    # Tables are kept: Bronze MERGE is keyed on record hash, so a reload does not duplicate.
    # For a truly clean slate: DROP SCHEMA examiner_compliance.bronze CASCADE (and silver/gold/controls).
    print("Reset done")

# COMMAND ----------

# MAGIC %md ## 3 · Generate August–October data + policy documents

# COMMAND ----------

import os, sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))     # notebook lives in /notebooks
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

import importlib
import examiner.synthetic_data as sd
importlib.reload(sd)

written = sd.build_all(RAW, policies_src=os.path.join(REPO_ROOT, "resources", "policies"))
for name, n in written.items():
    print(f"{n:>5}  {name}")

# COMMAND ----------

display(dbutils.fs.ls(RAW))
display(dbutils.fs.ls(f"{RAW}/transactions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · LATER: simulate a new extract (incremental demo)
# MAGIC Run this cell **only after** `01_bronze` has run once. Then rerun `01_bronze`:
# MAGIC Auto Loader picks up just this one new file. The file contains late-arriving October
# MAGIC transactions, a stale older version (must be ignored), a newer version (must win),
# MAGIC a cross-file duplicate and a malformed row.

# COMMAND ----------

# path = sd.write_late_batch(RAW)
# print("wrote", path)
