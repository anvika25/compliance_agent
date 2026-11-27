# Known limitations and productionisation roadmap

## Limitations (prototype)
| Area | Limitation | Impact |
|---|---|---|
| Data | Synthetic, small, one institution, background data deliberately clean | detections prove the logic, not real-world rates |
| Dormant-account control | uses current account status, not point-in-time | could misclassify historical periods; needs SCD2 |
| Silver | re-reads all of Bronze each run | fine at this size; production needs CDF/streaming |
| Exception IDs | allocated in the driver, single runner | needs an identity column or sequence table for concurrency |
| Risk model | trained on simulated reviewer decisions; ~14 live exceptions | metrics show the method, not accuracy; bands are judgement-based |
| RAG | 7 short documents; lexical groundedness metric; Vector Search threshold tuned on 8 questions | needs a larger labelled set and an LLM judge |
| Agent | deterministic plan covers review-style requests only; ~30 s latency (sequential policy LLM calls) | parallelise or cache policy answers |
| Clearance | RAG/agent clearance is a parameter, not identity-derived | needs on-behalf-of-user auth on serving endpoints |
| Governance | grants defined but need real groups; per-table tags/masks | move to governed tags + ABAC policies |
| Catalog name | notebooks use the `examiner_compliance` catalog; environments are separated by workspace | parameterise the catalog for same-workspace dev/prod |
| Notifications | simulated outbox | wire to email, Teams or a ticketing webhook |
| Human review | review status exists, but no review UI or table writes | build a review app (next) |

## Roadmap
1. **Human review loop.** A Databricks App (or Agent Framework Review App) where reviewers confirm or reject exceptions and
   agent findings. Decisions go to a `review_decisions` table, which becomes the model's real training labels.
2. **Serve Examiner.** MLflow `ChatAgent` → Model Serving with on-behalf-of-user auth, AI Gateway (guardrails, rate limits,
   inference tables), MLflow Tracing; tools as Unity Catalog functions.
3. **Real sources.** Lakehouse Federation / Lakeflow Connect from core banking, IAM and GRC; SCD2 for accounts and access.
4. **Scale the pipeline.** Lakeflow Declarative Pipelines (expectations replace hand-written DQ rules, CDF-driven incrementals).
5. **Model maturity.** Retrain on reviewer labels, calibration monitoring (Lakehouse Monitoring on scores), bias and
   drift checks, MRM validation pack.
6. **RAG maturity.** Larger labelled eval set, LLM-judge groundedness, hybrid (BM25 + vector) retrieval with reranking,
   document versioning, and answers "as of" a policy version date.
7. **Governance at scale.** Governed tags and ABAC, catalog-per-environment with workspace bindings, automated
   access recertification of Examiner's own users.
8. **Real alerting.** Databricks SQL alerts on overdue Critical items, wired to on-call and ticketing.
