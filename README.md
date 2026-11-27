# Examiner — compliance monitoring prototype on Databricks

Step 1 of the build: synthetic data → Bronze → Silver → metadata-driven controls → Gold exceptions and evidence.

Start here: **[docs/STEP_1_RUNBOOK.md](docs/STEP_1_RUNBOOK.md)**.
To rebuild with Databricks Assistant: [docs/DATABRICKS_ASSISTANT_PROMPTS.md](docs/DATABRICKS_ASSISTANT_PROMPTS.md).

```
notebooks/00_generate_data   synthetic data + UC objects          (Implemented)
notebooks/01_bronze          CSV MERGE + Auto Loader availableNow  (Implemented)
notebooks/02_silver          validate, quarantine, standardize, dedupe, late-arriving MERGE (Implemented)
notebooks/03_run_controls    catalogue-driven control runner       (Implemented; notifications Simulated)
src/examiner/                generator (pure Python)
src/controls/sql/            one SQL per test type
resources/policies/          7 synthetic policy documents
tests/                       pytest:  pytest tests -q
```
