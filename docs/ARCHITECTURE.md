# Architecture

```
 Source data (synthetic: CSV, JSON lines, Markdown policies) ── /Volumes/examiner_compliance/reference/raw_files
        │  01_bronze: CSV → MERGE on (source_file, record_hash, dup_seq); transactions → Auto Loader availableNow
        ▼
 BRONZE   bronze_customer_account · bronze_transaction · bronze_employee_access · bronze_regulatory_reporting
          bronze_control_catalogue · bronze_compliance_issues          (+ ingestion_timestamp, batch_id, source_file,
        │                                                                  source_system, record_hash)
        │  02_silver: validate → quarantine · standardise · dedupe · late-arriving MERGE · relationships · lineage
        ▼
 SILVER   silver_* (6 tables) · quarantine_records                        reference.* (SoD rules, FX, calendar, risk geo)
        │  03_run_controls: catalogue-driven, one SQL per test type, named parameters, period reruns
        ▼
 GOLD     gold_control_test_results (history) · gold_compliance_exceptions (stable IDs) · gold_compliance_evidence
        │  04_risk_scoring: features.exception_features → rules + logistic regression (MLflow, UC @champion)
        │  05_gold_products: gold_remediation_status · gold_compliance_dashboard · gold_examiner_summary
        ▼
 RAG      06: policies → section chunks + metadata → reference.policy_chunks (CDF) → Vector Search delta-sync index
          (gte-large embeddings) → clearance filter → Llama 3.3 or extractive → citation check
        ▼
 AGENT    07: guard → scope → 6 read-only tools → validated Llama recommendations → masked 11-section response
        ▼
 CONSUME  09 views → AI/BI dashboards (Controls, Platform) · Genie space (optional) · human review
 GOVERN   08: tags, PII masks, secure view, least-privilege grants, retention, lineage/audit system tables
 OPERATE  Asset Bundle (3 scheduled jobs) · GitHub Actions CI/CD · 10_integration_tests · MLflow experiments
```

## Components and status
| Component | Status | Notes |
|---|---|---|
| Synthetic data with planted violations | Implemented | `src/examiner/synthetic_data.py`, seeded, unit-tested |
| Bronze ingestion, idempotent MERGE | Implemented | |
| Incremental ingestion (Auto Loader, availableNow) | Implemented | near-real-time = run the same stream with `processingTime` on classic compute |
| Silver: quarantine, standardisation, dedupe, late-arriving | Implemented | |
| Metadata-driven control framework (6 controls) | Implemented | reruns by period/control, history, stable IDs, evidence |
| Critical-exception notifications | **Simulated** | `controls.notification_outbox`; production = job email/webhook or a Databricks SQL alert |
| Risk scoring (rules + model) + MLflow + UC registry | Implemented | training labels **Simulated** |
| Gold dashboard / examiner summary / remediation | Implemented | |
| RAG with Vector Search, citations, clearance | Implemented | BM25 fallback; LLM-judge eval Designed |
| Examiner agent (6 tools, guard, masking, logging) | Implemented | Model Serving / Agent Framework deployment Designed |
| Governance (tags, masks, secure view, grants, retention) | Implemented (grants need groups) | ABAC / governed tags Designed |
| Dashboards | Implemented (AI/BI, built in UI) | export to bundle with `databricks bundle generate dashboard` |
| Asset Bundle, CI/CD, integration tests | Implemented | prod target needs real hosts + service principal |
| Environment separation | Designed | dev/prod workspaces via bundle targets |

## Databricks platform features: how they are (or would be) used
* **Workflows (Lakeflow Jobs):** `resources/examiner_jobs.yml` defines the daily pipeline (8 tasks with dependencies,
  RAG refresh running in parallel with controls), weekly agent evaluation, monthly retrain. Failures email the platform team,
  and the last task is the integration-test gate.
* **Photon:** on by default for serverless notebooks/jobs and SQL warehouses. It speeds up the SQL-heavy Silver MERGEs, control
  SQL and dashboard queries. No code changes needed.
* **SQL Warehouses:** serve the AI/BI dashboards, the Genie space and analyst queries over the `v_dash_*` / `v_plat_*` views.
  Use serverless warehouses with auto-stop, sized separately from pipeline compute.
* **Lakehouse Federation (Designed):** in production, read source systems (core banking, IAM, GRC) in place through foreign
  catalogs (e.g. `CREATE CONNECTION … TYPE postgresql`), so Bronze could ingest via `SELECT` from federated tables, governed
  by the same Unity Catalog permissions and lineage.
* **Model Serving:** already used for Foundation Model endpoints (`databricks-gte-large-en`, Llama 3.3 70B). Designed:
  serve the risk model and the Examiner agent (MLflow `ChatAgent`) as endpoints with on-behalf-of-user auth.
* **AI Gateway (Designed):** on the LLM and agent endpoints: rate limits, PII and safety guardrails, usage tracking and
  inference tables (payload logging) for audit and the Platform dashboard.
* **Unity Catalog:** catalog/schema layout, tags, column masks, secure view, grants, lineage, model registry, volumes.
* **Vector Search:** delta-sync index with managed embeddings over `reference.policy_chunks`.
* **MLflow:** experiments for risk model, RAG eval and agent eval; UC model registry with `@champion` alias.
