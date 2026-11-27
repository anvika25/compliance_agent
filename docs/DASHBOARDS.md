# Building the two AI/BI dashboards (about 20–30 min)

Run `notebooks/09_dashboard_views` first. Each widget below reads one view.

## How to build either dashboard
1. Left sidebar → **Dashboards** → **Create dashboard**. Rename it (top left).
2. **Data** tab → **Create from SQL** → type `SELECT * FROM examiner_compliance.<view>` → **Run** → rename the dataset
   to the view name. Repeat for each view listed below.
3. **Canvas** tab → **Add a visualization** → place it → in the right panel choose the dataset, the chart type and the fields
   in the table below.
4. Add a **filter** widget (top of the page) on `control_domain` or `period_end` where noted; link it to the datasets with that field.
5. **Publish** (top right). Choose "embed credentials" only if viewers shouldn't need their own table access.
   Otherwise each viewer's own permissions and the name mask apply.

Tip: on the Canvas, the Assistant ("Ask the Assistant" / sparkle icon) can build a chart from a sentence, such as
"bar chart of exceptions by business_unit coloured by control_domain".

## Dashboard 1 — Compliance Controls Dashboard
| # | Widget | Chart type | Dataset (view in `gold`) | Fields |
|---|---|---|---|---|
| 1 | Controls evaluated | Counter | `v_dash_kpis` | `controls_evaluated` |
| 2 | Controls failed | Counter | `v_dash_kpis` | `controls_failed` |
| 3 | Exception rate | Counter (format %) | `v_dash_kpis` | `overall_exception_rate` |
| 4 | Open high-risk exceptions | Counter | `v_dash_kpis` | `open_high_risk_exceptions` |
| 5 | Overdue remediation | Counter | `v_dash_kpis` | `overdue_remediation_items` |
| 6 | Evidence completeness | Counter (format %) | `v_dash_kpis` | `avg_evidence_completeness` |
| 7 | Exception rate by control | Bar | `v_dash_control_results` | X `control_id`, Y `exception_rate`, colour `test_status`; filter latest `period_end` |
| 8 | Trend over time | Line | `v_dash_exception_trend` | X `period_end`, Y `exceptions`, colour `control_domain` |
| 9 | Exceptions by business unit and domain | Stacked bar | `v_dash_exceptions_by_bu_domain` | X `business_unit`, Y `exceptions`, colour `control_domain` |
| 10 | Highest-risk exceptions | Table | `v_dash_high_risk_exceptions` | sort `risk_score` desc; show id, control, entity, score, band, owner, description |
| 11 | Repeat exceptions | Table | `v_dash_repeat_exceptions` | id, control, periods_detected, linked_issue_id |
| 12 | Overdue remediation | Table | `v_dash_overdue_remediation` | filter `is_overdue = true`; sort `days_overdue` desc |
| 13 | Evidence completeness by control | Bar | `v_dash_evidence_completeness` | X `control_id`, Y `avg_evidence_completeness` |
| — | Filters | Filter widgets | — | `control_domain`, `period_end` |

**Expected look:** 6 controls / 6 failed for October; trend rising 2 → 5 → about 14; EXC-10025 at the top of the high-risk
table and in the repeat table; ISS-3025 in overdue remediation; evidence completeness 100%.

## Dashboard 2 — Examiner Platform Dashboard
| # | Widget | Chart type | Dataset (view in `monitoring`) | Fields |
|---|---|---|---|---|
| 1 | Control tests run | Counter | `v_plat_kpis` | `control_tests_run` |
| 2 | Exceptions scored | Counter | `v_plat_kpis` | `exceptions_scored` |
| 3 | Agent requests | Counter | `v_plat_kpis` | `agent_requests` |
| 4 | Agent avg latency (s) | Counter | `v_plat_kpis` | `agent_avg_latency_s` |
| 5 | RAG answered rate | Counter (%) | `v_plat_kpis` | `rag_answered_rate` |
| 6 | Unsupported-answer rate | Counter (%) | `v_plat_kpis` | `unsupported_answer_rate` |
| 7 | RAG eval accuracy / agent eval pass rate | Counter ×2 | `v_plat_kpis` | `rag_eval_accuracy_latest`, `agent_eval_pass_rate_latest` |
| 8 | Control-test volume | Bar | `v_plat_control_test_volume` | X `run_date`, Y `tests`, colour `test_status` |
| 9 | Exception-scoring volume | Bar | `v_plat_scoring_volume` | X `scoring_date`, Y `exceptions_scored` |
| 10 | Risk-score distribution | Bar (histogram) | `v_plat_risk_distribution` | X `score_bucket`, Y `exceptions`, colour `risk_band` |
| 11 | Model metrics | Table or bar | `v_plat_model_metrics` | filter `component`; `metric`, `value`, `run_name` |
| 12 | Agent requests and latency | Combo / bar + line | `v_plat_agent_requests` | X `request_date`, bars `requests` by `status`, line `p95_latency_s` |
| 13 | RAG retrieval success | Stacked bar | `v_plat_rag_retrieval` | X `ask_date`, Y `questions`, colour `status` |
| 14 | Pipeline status | Table | `v_plat_pipeline_status` | layer, dataset, last_loaded, rows, freshness (conditional colour on `freshness`) |
| 15 | Model or agent failures | Table | `v_plat_failures` | event_time, component, detail, error |

Note: "unsupported-answer rate" counts NOT_FOUND + UNSUPPORTED. The deliberate refusals (dress code, INTERNAL-clearance test)
make it non-zero, which is correct behaviour. Explain that in the interview.

## Evidence for the interview
Publish both dashboards and take a screenshot of each. Optionally attach a **Genie space** to the Controls dashboard over the
`gold` tables (instructions in `docs/DATABRICKS_ASSISTANT_PROMPTS.md`, prompt 6) so reviewers can ask follow-up questions.
