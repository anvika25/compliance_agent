# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Exception risk scoring  *(Implemented; training labels Simulated)*
# MAGIC
# MAGIC **Rule-plus-model hybrid** (logic in `src/examiner/risk_scoring.py`, unit-tested):
# MAGIC
# MAGIC ```
# MAGIC gold_compliance_exceptions + evidence + issues
# MAGIC        └─► features.exception_features        9 factors, each scaled 0–1
# MAGIC               ├─► rule_score   = Σ weight × factor × 100   (transparent; gives top_risk_factors)
# MAGIC               └─► model_prob   = logistic regression from MLflow / UC (alias @champion)
# MAGIC risk_score = 0.6 × rule_score + 0.4 × 100 × model_prob   → risk_band, recommended_priority
# MAGIC ```
# MAGIC * There is no real history of reviewer decisions, so the model is trained on **simulated** reviewer outcomes.
# MAGIC   This is stated in the model card. In production you'd retrain on real `confirmed / false positive` decisions.
# MAGIC * `human_review_required` is always **true**. This notebook writes only `risk_score` / `risk_band` and never `status`.
# MAGIC * Run **after** `03_run_controls`. Rerunning is safe: features are rebuilt, scores are appended as history,
# MAGIC   and the latest score is merged onto the exception.

# COMMAND ----------

import os, sys, json, importlib
from datetime import datetime, timezone
import mlflow, pandas as pd
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from delta.tables import DeltaTable

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))
import examiner.risk_scoring as rs
importlib.reload(rs)

C = "examiner_compliance"
MODEL_NAME = f"{C}.ml.exception_risk_model"
USER = spark.sql("SELECT current_user()").first()[0]
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Users/{USER}/examiner_risk_scoring")

dbutils.widgets.dropdown("retrain", "true", ["true", "false"], "Retrain model")

# COMMAND ----------

# MAGIC %md ## 1 · Features (one row per open exception)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE examiner_compliance.features.exception_features
# MAGIC COMMENT 'Risk factors per open exception, each scaled 0-1. Rebuilt on every scoring run.' AS
# MAGIC WITH open_exc AS (
# MAGIC   SELECT * FROM examiner_compliance.gold.gold_compliance_exceptions WHERE status <> 'CLOSED'
# MAGIC ),
# MAGIC bu AS (SELECT business_unit, count(*) AS n FROM open_exc GROUP BY business_unit),
# MAGIC bu_max AS (SELECT max(n) AS mx FROM bu)
# MAGIC SELECT
# MAGIC   e.exception_id, e.control_id, e.control_domain, e.business_unit, e.severity,
# MAGIC   -- severity of the breached control
# MAGIC   CASE e.severity WHEN 'CRITICAL' THEN 1.0 WHEN 'HIGH' THEN 0.75 WHEN 'MEDIUM' THEN 0.5 ELSE 0.25 END AS f_severity,
# MAGIC   -- financial impact on a log scale (CAD 1M = 1.0), with a floor for regulatory/access findings that have no amount
# MAGIC   greatest(least(log10(coalesce(e.financial_impact, 0) + 1) / 6, 1.0),
# MAGIC            CASE e.control_domain WHEN 'REGULATORY_REPORTING' THEN 0.7 WHEN 'ACCESS_CONTROL' THEN 0.5 ELSE 0.0 END) AS f_impact,
# MAGIC   CASE e.control_domain WHEN 'ACCESS_CONTROL' THEN 1.0 WHEN 'REGULATORY_REPORTING' THEN 0.9
# MAGIC                         WHEN 'AML_TRANSACTION_MONITORING' THEN 0.85 ELSE 0.5 END AS f_criticality,
# MAGIC   CASE WHEN e.is_repeat THEN 1.0 ELSE 0.0 END AS f_repeat,
# MAGIC   least(greatest(datediff(current_date(), coalesce(v.event_timestamp, e.creation_timestamp)), 0) / 90.0, 1.0) AS f_age,
# MAGIC   coalesce(b.n / m.mx, 0.0) AS f_bu_exposure,
# MAGIC   1.0 - coalesce(v.evidence_completeness, 0.0) AS f_evidence_gap,
# MAGIC   CASE WHEN i.issue_status <> 'CLOSED' AND i.target_remediation_date < current_date() THEN 1.0 ELSE 0.0 END AS f_overdue,
# MAGIC   coalesce(e.detection_confidence, 0.5) AS f_detection_confidence,
# MAGIC   -- context kept for explanations, not used as features
# MAGIC   CASE WHEN i.issue_status <> 'CLOSED' AND i.target_remediation_date < current_date()
# MAGIC        THEN datediff(current_date(), i.target_remediation_date) END AS days_overdue,
# MAGIC   e.linked_issue_id, current_timestamp() AS feature_timestamp
# MAGIC FROM open_exc e
# MAGIC LEFT JOIN examiner_compliance.gold.gold_compliance_evidence v ON v.evidence_id = e.evidence_reference
# MAGIC LEFT JOIN examiner_compliance.silver.silver_compliance_issues i ON i.issue_id = e.linked_issue_id
# MAGIC LEFT JOIN bu b ON b.business_unit = e.business_unit
# MAGIC CROSS JOIN bu_max m;
# MAGIC
# MAGIC SELECT * FROM examiner_compliance.features.exception_features ORDER BY exception_id;

# COMMAND ----------

# MAGIC %md ## 2 · Train, evaluate and register (MLflow → Unity Catalog)

# COMMAND ----------

client = MlflowClient()
if dbutils.widgets.get("retrain") == "true":
    history = rs.simulate_reviewer_history(n=3000, seed=7)
    with mlflow.start_run(run_name="exception_risk_lr") as run:
        model, metrics, coefficients, (X_te, _) = rs.train_model(history, seed=7, C=1.0)
        mlflow.log_params({"algorithm": "LogisticRegression", "C": 1.0, "seed": 7, "rule_version": rs.RULE_VERSION,
                           "rule_blend": rs.RULE_BLEND, "training_data": "SIMULATED reviewer decisions",
                           **{f"w_{f}": w for f, (w, _) in rs.FACTORS.items()}})
        mlflow.log_metrics(metrics)
        mlflow.log_input(mlflow.data.from_pandas(history, name="simulated_reviewer_history_v1"), context="training")
        mlflow.log_dict({f: {"weight": w, "label": l} for f, (w, l) in rs.FACTORS.items()}, "rule_config.json")
        mlflow.log_dict({"bands": rs.BANDS}, "risk_bands.json")
        mlflow.log_table(coefficients, "coefficients.json")
        model_card = os.path.join(REPO_ROOT, "docs", "MODEL_CARD.md")
        if os.path.exists(model_card):
            mlflow.log_artifact(model_card)
        info = mlflow.sklearn.log_model(
            model, "model",
            signature=infer_signature(X_te, model.predict_proba(X_te)[:, 1]),
            input_example=X_te.head(3), registered_model_name=MODEL_NAME)
        RUN_ID = run.info.run_id
    MODEL_VERSION = info.registered_model_version
    # Promotion rule: only make it champion if it is at least as good as rules alone
    if metrics["hybrid_roc_auc"] >= metrics["rules_roc_auc"] - 0.01:
        client.set_registered_model_alias(MODEL_NAME, "champion", MODEL_VERSION)
        print(f"v{MODEL_VERSION} promoted to @champion")
    else:
        print(f"v{MODEL_VERSION} NOT promoted (hybrid worse than rules); @champion unchanged")
    print(json.dumps(metrics, indent=2))
    display(coefficients)

# COMMAND ----------

# MAGIC %md ## 3 · Score open exceptions with `@champion`

# COMMAND ----------

champion = client.get_model_version_by_alias(MODEL_NAME, "champion")
model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@champion")
features = spark.table(f"{C}.features.exception_features").toPandas()

scores = rs.score(features, model, model_version=f"lr-v{champion.version}")
scores["scoring_timestamp"] = datetime.now(timezone.utc)
scores["mlflow_run_id"] = champion.run_id

SCORES_DDL = """exception_id STRING, risk_score DOUBLE, rule_score DOUBLE, model_probability DOUBLE, risk_band STRING,
  top_risk_factors ARRAY<STRING>, recommended_priority STRING, model_or_rule_version STRING,
  human_review_required BOOLEAN, scoring_timestamp TIMESTAMP, mlflow_run_id STRING"""
order = [c.strip().split(" ")[0] for c in SCORES_DDL.replace("\n", " ").split(",")]
def py(v):
    if isinstance(v, (list, tuple)):
        return list(v)
    if v is None or pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v.item() if hasattr(v, "item") else v

rows = [tuple(py(r[c]) for c in order) for r in scores.to_dict("records")]
scores_df = spark.createDataFrame(rows, SCORES_DDL)

# history of every scoring run (append-only)
spark.sql(f"CREATE TABLE IF NOT EXISTS {C}.ml.exception_risk_scores ({SCORES_DDL}) "
          "COMMENT 'Append-only history of risk scores. Prioritisation only; human review always required.'")
scores_df.write.mode("append").saveAsTable(f"{C}.ml.exception_risk_scores")

# latest score onto the exception register. Only risk columns change, never status.
(DeltaTable.forName(spark, f"{C}.gold.gold_compliance_exceptions").alias("t")
    .merge(scores_df.alias("s"), "t.exception_id = s.exception_id")
    .whenMatchedUpdate(set={"risk_score": "s.risk_score", "risk_band": "s.risk_band"})
    .execute())
print(f"Scored {len(scores)} exceptions with {scores.model_or_rule_version.iloc[0]}")

# COMMAND ----------

# MAGIC %md ## 4 · Result: highest-risk first

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT s.exception_id, e.control_id, e.entity_id, s.risk_score, s.rule_score, s.model_probability,
# MAGIC        s.risk_band, s.recommended_priority, s.top_risk_factors, s.human_review_required, s.model_or_rule_version
# MAGIC FROM examiner_compliance.ml.exception_risk_scores s
# MAGIC JOIN examiner_compliance.gold.gold_compliance_exceptions e USING (exception_id)
# MAGIC WHERE s.scoring_timestamp = (SELECT max(scoring_timestamp) FROM examiner_compliance.ml.exception_risk_scores)
# MAGIC ORDER BY s.risk_score DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- distribution (for the platform dashboard later)
# MAGIC SELECT risk_band, count(*) AS exceptions, round(avg(risk_score), 1) AS avg_score
# MAGIC FROM examiner_compliance.gold.gold_compliance_exceptions GROUP BY risk_band ORDER BY avg_score DESC
