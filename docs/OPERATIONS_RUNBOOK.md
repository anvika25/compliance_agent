# Operations runbook

## Schedules (from the bundle, Toronto time)
| Job | When | Owner on failure |
|---|---|---|
| `examiner-daily-pipeline` | daily 06:30 | Platform Engineering (email `alert_email`) |
| `examiner-agent-evaluation` | Mondays 07:00 | Compliance Analytics |
| `examiner-risk-model-retrain` | 2nd of each month 05:00 | Compliance Analytics + Model Risk sign-off |

## Daily checks (5 min, Platform dashboard)
1. **Pipeline status** table: every dataset `OK` (updated within 2 days).
2. **Failures** table empty, or each row explained.
3. Integration-test job task green (`monitoring.integration_test_results` for today's `run_id`).
4. **Unsupported-answer rate** and **agent latency** within their normal range.

## Common incidents
| Symptom | Likely cause | Action |
|---|---|---|
| `bronze` task fails on Auto Loader schema change | new field in the source JSON | rerun. Auto Loader adds the column on restart; check `_rescued_data` and quarantine |
| Quarantine count jumps | upstream file format or quality change | `SELECT explode(dq_errors), count(*) FROM silver.quarantine_records GROUP BY 1`; contact the source owner; never delete quarantine rows |
| A control shows `ERROR` | SQL error or missing table | read `error_message` in `gold_control_test_results`; fix the SQL via PR; rerun `03` for that period with `control_ids=` |
| Reperformance differs from stored result (agent gap) | data changed after the test run | rerun `03` for the period; the old result stays in history |
| RAG answers NOT_FOUND unexpectedly | Vector Search index not synced, or the threshold too high | rerun `06` §2 (`sync`); check `vs_min_score` against `ml.rag_evaluation` |
| LLM errors / timeouts | Model Serving endpoint unavailable or rate-limited | RAG falls back to extractive answers, agent to rule-based recommendations (`v_plat_failures` shows it); retry later |
| Agent latency > 60 s | many policy questions, or a cold endpoint | expected about 30 s; check `agent_tool_calls.latency_ms` per tool |
| Masked names seen by a reviewer who needs them | not in `examiner_pii_viewers` | IAM adds them to the group (access request + approval) |

## Reruns
* A whole period: run `03_run_controls` with `period_start`/`period_end`, then `04` (`retrain=false`), `05`, `09`.
* One control: same, with `control_ids=CTL-ACC-001`.
* Never delete from `gold_control_test_results` or `gold_compliance_evidence`. They are the audit trail.

## Human review workflow (Designed)
A reviewer opens an exception (dashboard or agent output), checks evidence and citations, and records
confirm / reject / false positive with their name and a timestamp, in a review table owned by `examiner_reviewers`.
Only that workflow, not the agent or pipeline, may change `status` or close an issue (POL-HRV-007, POL-REM-005 §4).
