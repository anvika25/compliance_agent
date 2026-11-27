# CI/CD, testing and rollback

## Test layers
| Layer | What | Where | When |
|---|---|---|---|
| Unit | generator determinism and plants, risk scoring maths, chunking, BM25, citation checks | `tests/test_synthetic_data.py`, `test_risk_scoring.py`, `test_rag.py` | every PR (GitHub Actions), no Databricks needed |
| Negative / agent | refusals (close, SQL, external, determination), injection in tool args, invented IDs in LLM output, masking, tool failure → gap | `tests/test_agent.py` | every PR |
| Evaluation | 8 RAG questions; 8 agent cases | notebooks 06 §5, 07 §7 → `ml.rag_evaluation`, `ml.agent_evaluation`, MLflow | weekly job + after changes |
| Integration | end-to-end on real tables: idempotency, metadata, quarantine, planted violations detected, evidence lineage, scoring, remediation, RAG/agent results, nothing auto-closed, masking | `notebooks/10_integration_tests.py` | last task of the daily pipeline, and after every dev deploy |

Run unit tests locally: `pip install -r requirements-dev.txt && pytest tests -q` (54 tests).

## Pipeline (`.github/workflows/ci.yml`)
1. **Pull request:** unit and negative tests → `databricks bundle validate -t dev`.
2. **Merge to `main`:** deploy to **dev** → run `examiner_daily_pipeline` (ends with integration tests). A failure blocks release.
3. **Tag `v*`:** deploy to **prod**, gated by the GitHub `production` environment (required reviewers). Runs as a service principal.

Setup: create GitHub environments `dev` and `production`, add `DATABRICKS_*` secrets (prefer workload-identity federation
over tokens), fill the hosts and service principal in `databricks.yml`.

## Rollback
| What broke | Roll back by |
|---|---|
| Code / notebooks / jobs | `git checkout v<previous>` → `databricks bundle deploy -t prod` (or revert the commit and re-tag). The bundle is declarative, so the previous job definitions come back too. |
| Risk model | `MlflowClient().set_registered_model_alias("examiner_compliance.ml.exception_risk_model", "champion", <previous version>)`. Scoring loads `@champion`, so no deploy is needed. The promotion gate already blocks a worse model. |
| Bad data written to a table | `RESTORE TABLE examiner_compliance.<schema>.<table> TO VERSION AS OF <n>` (find `n` with `DESCRIBE HISTORY`), then rerun downstream tasks. |
| Bad policy chunks / index | restore `reference.policy_chunks`, then `index.sync()` (rerun `06` §2). |
| Bad control logic | revert the SQL file. Results history is append-only, so wrong runs stay visible and are superseded by the rerun. |
| Governance change | rollback cell in `08_governance` (drop masks); `REVOKE` for grants; all changes are in `controls.governance_apply_log`. |
