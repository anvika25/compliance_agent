# Configuration

Examiner keeps configuration in governed places rather than loose files:

| What | Where | Changed by |
|---|---|---|
| Controls (threshold, severity, owner, policy, frequency, active flag) | `raw_files/control_catalogue.csv` → `silver.silver_control_catalogue` | Compliance (via a reviewed change) |
| Control test logic | `src/controls/sql/<test_type>.sql` | Engineers, through PR + CI |
| Risk weights, bands, blend | `src/examiner/risk_scoring.py` (`FACTORS`, `BANDS`, `RULE_BLEND`), logged to MLflow each run | Compliance + Model Risk, new `RULE_VERSION` |
| Agent policy questions, guard rules, tool allow-list | `src/examiner/agent.py` | Engineers, through PR + agent tests |
| Endpoints (LLM, embeddings, Vector Search), alert email | `databricks.yml` variables → job `base_parameters` → notebook widgets | Platform, per target |
| Groups used by grants, masks, row rules | widgets in `notebooks/08_governance.py` | Platform / IAM |
| Schedules | `resources/examiner_jobs.yml` | Platform |

Secrets (tokens, service-principal credentials) live only in GitHub environment secrets or Databricks secret scopes.
