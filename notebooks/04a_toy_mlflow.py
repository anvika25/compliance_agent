# Databricks notebook source
# MAGIC %md
# MAGIC # 04a · Toy exercise: MLflow in 15 minutes  *(practice, not part of the pipeline)*
# MAGIC
# MAGIC Goal: learn the five MLflow moves the real scoring notebook uses, on a dataset small enough to reason about.
# MAGIC
# MAGIC | Move | API |
# MAGIC |---|---|
# MAGIC | 1. Group work into an experiment | `mlflow.set_experiment(path)` |
# MAGIC | 2. Record one training attempt | `with mlflow.start_run():` |
# MAGIC | 3. Record settings and results | `log_param`, `log_metric`, `log_dict` / `log_artifact` |
# MAGIC | 4. Save + version the model in Unity Catalog | `mlflow.sklearn.log_model(..., registered_model_name=...)` |
# MAGIC | 5. Load the approved version by alias | `set_registered_model_alias` → `load_model("models:/name@champion")` |
# MAGIC
# MAGIC The story: predict whether a toy "alert" gets escalated from two numbers (amount, prior alerts).

# COMMAND ----------

import mlflow, numpy as np, pandas as pd
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

mlflow.set_registry_uri("databricks-uc")                 # models go to Unity Catalog, not the old workspace registry
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/examiner_toy_mlflow")  # move 1

rng = np.random.default_rng(0)
toy = pd.DataFrame({"amount_k": rng.uniform(0, 100, 400), "prior_alerts": rng.integers(0, 6, 400)})
toy["escalated"] = (rng.uniform(size=400) < 1 / (1 + np.exp(-(0.05 * toy.amount_k + 0.6 * toy.prior_alerts - 3)))).astype(int)
X_tr, X_te, y_tr, y_te = train_test_split(toy[["amount_k", "prior_alerts"]], toy.escalated, random_state=0)
display(toy.head())

# COMMAND ----------

# MAGIC %md ### Moves 2–3: two runs with different settings, so you have something to compare

# COMMAND ----------

for C in [0.01, 1.0]:
    with mlflow.start_run(run_name=f"toy_C={C}"):              # move 2
        model = LogisticRegression(C=C).fit(X_tr, y_tr)
        auc = roc_auc_score(y_te, model.predict_proba(X_te)[:, 1])
        mlflow.log_param("C", C)                              # move 3
        mlflow.log_metric("roc_auc", auc)
        mlflow.log_dict({"coef": dict(zip(X_tr.columns, model.coef_[0].round(3)))}, "coefficients.json")
        print(f"C={C}  AUC={auc:.3f}")

# COMMAND ----------

# MAGIC %md 👉 Open **Experiments** in the left sidebar → `examiner_toy_mlflow` → select both runs → **Compare**.

# COMMAND ----------

# MAGIC %md ### Moves 4–5: register the better model in UC, tag it `champion`, load it back

# COMMAND ----------

TOY_MODEL = "examiner_compliance.ml.toy_escalation_model"
with mlflow.start_run(run_name="toy_register"):
    model = LogisticRegression(C=1.0).fit(X_tr, y_tr)
    info = mlflow.sklearn.log_model(model, "model", signature=infer_signature(X_tr, model.predict_proba(X_tr)[:, 1]),
                                    input_example=X_tr.head(3), registered_model_name=TOY_MODEL)   # move 4

MlflowClient().set_registered_model_alias(TOY_MODEL, "champion", info.registered_model_version)  # move 5
loaded = mlflow.sklearn.load_model(f"models:/{TOY_MODEL}@champion")
print("version", info.registered_model_version, "->", loaded.predict_proba(pd.DataFrame({"amount_k": [90], "prior_alerts": [4]}))[:, 1])

# COMMAND ----------

# MAGIC %md
# MAGIC 👉 Open **Catalog → examiner_compliance → ml → toy_escalation_model**: versions, alias, lineage to the run.
# MAGIC
# MAGIC **Check yourself before moving on:** Why load by alias instead of by version number? *(So deployment can be
# MAGIC promoted or rolled back by moving the alias, with no code change. That's the rollback story for the interview.)*
# MAGIC
# MAGIC Clean up when done: `DROP MODEL examiner_compliance.ml.toy_escalation_model` (optional).
