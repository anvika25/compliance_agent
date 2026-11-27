# Examiner agent: design, tools and safety

**Status:** Implemented (deterministic orchestrator + Llama 3.3 70B for recommendation drafting).
Production serving is Designed (below).

## Why a deterministic orchestrator
A compliance review must be **complete and reproducible**. The plan (which tools, in which order) is fixed Python code
(`src/examiner/agent.py::run_examiner`), so every run performs every required step and an auditor can read the plan.
The LLM's role is limited to drafting the follow-up recommendations from tool facts. The draft is rejected (and rule-based
text is used) if it cites an ID that wasn't in the facts, or uses words like "closed", "resolved" or "compliant".

## Plan for a review request
1. **Guard** the request: refuse write actions, raw SQL/code, external actions, final determinations (no tool is called).
2. **Validate scope**: domain (`ACCESS_CONTROL`, `AML_TRANSACTION_MONITORING`, `REGULATORY_REPORTING`, `ALL_DOMAINS`),
   period (named month, `yyyy-MM`, or "previous month"; future periods rejected), optional business unit, exception IDs.
3. `compliance_scope_summary` → controls in scope, key findings, high-risk list, evidence gaps.
4. `run_control_test` for each control: **reperforms** the control SQL read-only and compares it with the stored result.
5. `exception_lookup` for the top 5 high-risk exceptions (risk score, band, top factors).
6. `exception_lookup` + `evidence_retrieval` for each requested exception (re-locates the Bronze row by record hash).
7. `remediation_status` for each control in scope (overdue items, implied SLA targets).
8. `policy_search` (RAG) for the policy questions mapped to each control's test type, plus closure evidence and escalation.
9. Draft recommendations, assemble the 11 required sections, **mask names**, log everything, mark for human review.

## Tool definitions (all read-only)
| Tool | Input (validated) | Output | Source |
|---|---|---|---|
| `run_control_test` | `control_id` `^CTL-[A-Z]{3}-\d{3}$`, `period_start`, `period_end` (ISO dates) | population, exception count, rate, threshold, status, stored result, `reperformance_matches` | control SQL file (SELECT only) + `gold_control_test_results` |
| `exception_lookup` | `exception_id` `^EXC-\d{5}$` | details, breached control, risk score/band/top factors, owner, status, linked issue | `gold_compliance_exceptions`, `ml.exception_risk_scores` |
| `evidence_retrieval` | `exception_id` | source record, control logic + SQL hash, expected vs actual, variance, lineage, `lineage_verified`, completeness | `gold_compliance_evidence`, Bronze by `record_hash` |
| `policy_search` | `question` (no `;` or backticks) | answer, cited passages (title, version, section), status | RAG (`06`), filtered by clearance |
| `remediation_status` | one of `control_id`, `issue_id`, `exception_id` | owner, status, target date, overdue, validation status | `gold_remediation_status` |
| `compliance_scope_summary` | `review_scope`, `period_label`, optional `business_unit` | controls evaluated, findings, high-risk exceptions, overdue items, gaps | `gold_examiner_summary`, `gold_compliance_dashboard` |

All queries use Spark named parameters (`:x`). Values are bound, never concatenated. Unknown tools or arguments are rejected
by `ToolRegistry` before anything runs.

## Safety requirements → where they are enforced
| Requirement | Enforcement |
|---|---|
| Read-only | Tools only `SELECT`; control SQL checked for DML before reperformance; no tool writes business tables |
| Approved tools only | `ToolRegistry` allow-list; registry refuses to load an unlisted tool |
| Reject arbitrary SQL | Request guard (`RAW_SQL`) + per-argument regex validation |
| Never close issues | Guard (`WRITE_ACTION`); no write tool exists; output check bans "closed/resolved" claims |
| No final determination | Guard (`DETERMINATION`); fixed wording in "Human Review Required" |
| Facts vs model vs recommendation | Every line is labelled `[FACT]`, `[MODEL OUTPUT]` or `[RECOMMENDATION]` |
| Cite policy | Policy text comes only from RAG citations; listed under "Sources and Citations" |
| Missing/inconclusive evidence | Tool failures, missing records, lineage misses and reperformance differences become "Data or Evidence Gaps" |
| Log prompts, tool calls, timestamps | `monitoring.agent_requests`, `monitoring.agent_tool_calls` (including the LLM prompt and response) |
| Mask PII | Names from Silver replaced by IDs in the response and every log line |
| Human approval before action | "Human Review Required: Yes" always; `review_status = PENDING_HUMAN_REVIEW`; only humans change it |

## Evaluation
`tests/test_agent.py` (fake tools, runs in CI) and notebook section 7 (`ml.agent_evaluation` + MLflow experiment
`examiner_agent_eval`): scenario completeness, EXC-10025 evidence and lineage, overdue detection, policy citations,
labelling, masking, 4 refusal cases, a future-period rejection, and an unknown-exception gap.

## Production design (Designed)
* **Tools as Unity Catalog functions** (SQL/Python UDFs) owned by a service principal with `SELECT`-only grants on Gold.
  The agent identity has no `MODIFY` anywhere.
* **Mosaic AI Agent Framework**: wrap `run_examiner` as an MLflow `ChatAgent`, log and register it in UC
  (`examiner_compliance.serving.examiner_agent`), and deploy to **Model Serving** with on-behalf-of-user auth, so row filters
  and clearance follow the caller.
* **AI Gateway** on the LLM endpoint: rate limits, PII and safety guardrails, usage tracking, inference tables for audit.
* **MLflow Tracing** in place of the custom trace table, plus MLflow GenAI evaluation with LLM judges
  (groundedness, relevance, safety) on a reviewer-labelled set.
* A **Review App** for compliance reviewers to accept or reject findings; their decisions become labels for the risk model.
