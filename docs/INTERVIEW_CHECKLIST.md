# Interview checklist: the brief's "provide evidence of" list → what to show

Open these tabs before the walkthrough. Take a screenshot of each as backup.

| # | Brief asks for evidence of | Show this | Where |
|---|---|---|---|
| 1 | A successful pipeline run | Job run graph, all 8 tasks green | Workflows → `examiner-daily-pipeline` → latest run (or the notebooks' outputs) |
| 2 | An executed control test | Population / exceptions / rate / threshold / status per control, 3 periods | `SELECT * FROM gold.gold_control_test_results ORDER BY execution_timestamp DESC` |
| 3 | A detected exception | EXC-10025: Bob (E1099), terminated 2026-09-30, admin access still active; repeat; ISS-3025 | `gold.gold_compliance_exceptions WHERE exception_id='EXC-10025'` |
| 4 | Supporting evidence | expected vs actual, variance, SQL hash, lineage → Bronze row | `03_run_controls` last cell (EXC-10025 joined to Bronze) |
| 5 | Risk scoring | score ~90 CRITICAL, top factors, version, human_review_required | `04_risk_scoring` §4 + MLflow experiment `examiner_risk_scoring` + UC model `@champion` |
| 6 | Dashboard output | both published dashboards | AI/BI Dashboards |
| 7 | RAG response with citation | "What evidence is required before … closed?" → POL-REM-005 v3.0 §4 | `06_rag_policies` §4; NOT_FOUND guardrails below it |
| 8 | Examiner tool calls | 16 tool calls with args, latency, status for the scenario | `07_examiner_agent` audit-trail cell / `monitoring.agent_tool_calls` |
| 9 | Agent evaluation | 8/8 passed incl. 4 refusals + future-period rejection | `07` §7 / `ml.agent_evaluation` |
| 10 | Test execution | `pytest tests -q` → 54 passed; integration tests all pass | terminal / GitHub Actions; `monitoring.integration_test_results` |
| 11 | Deployment and governance approach | `databricks.yml` + jobs YAML + CI workflow; governance log, masked names, tags | repo + `08_governance` §3, §8 + `docs/GOVERNANCE.md` |

## Suggested 15-minute walkthrough
1. **Architecture (1 min):** `docs/ARCHITECTURE.md` diagram. Say "Implemented / Simulated / Designed" out loud.
2. **Data (2 min):** planted violations table in `00`; Bronze metadata; quarantine reasons in `02`.
3. **Controls (3 min):** catalogue row → SQL file → results → EXC-10025 → evidence → Bronze row. Rerun one control live
   with `control_ids=CTL-ACC-001` to show idempotency.
4. **Scoring (2 min):** ranking, top factors, MLflow run, `@champion`, the "simulated labels" caveat.
5. **RAG (2 min):** a cited answer; dress code → NOT_FOUND; INTERNAL user → NOT_FOUND; eval table.
6. **Agent (3 min):** run the scenario; walk the 11 sections; open the tool-call log; try "Close EXC-10025" live.
7. **Governance and ops (2 min):** masked name, secure view, grants log, dashboards, bundle + CI, rollback.

## Likely questions → short answers
* **Why not let the LLM drive the agent?** Reviews must be complete and reproducible; the fixed plan is auditable, and the LLM
  only drafts recommendations, which are validated.
* **How do you stop hallucinated policy?** Answers only from retrieved sections; citations validated against retrieved IDs;
  weak retrieval → NOT_FOUND; eval tracks citation accuracy and groundedness.
* **Isn't the model meaningless with synthetic labels?** Yes for accuracy, no for the method. Reviewer decisions from the
  review loop become the labels; the promotion gate stops a worse model.
* **What if a control's data changes after a run?** History is append-only; the agent reperforms and flags differences;
  Delta time travel reproduces the original state.
* **Can the agent leak PII or confidential policy?** Tools return IDs; names masked at Silver and in agent output;
  confidential chunks filtered before retrieval; grants give the agent SELECT on named tables only.

## Feature status (say this explicitly)
**Implemented:** everything in notebooks 00–10 and `src/`. **Simulated:** synthetic data, model training labels,
critical-exception notifications. **Designed:** environment separation by catalog, Agent/Model Serving deployment with
AI Gateway, Lakehouse Federation sources, human-review app, ABAC. Grants are implemented but need the groups to exist.
