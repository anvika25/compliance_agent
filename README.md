# Examiner — AI-enabled compliance monitoring on Databricks (prototype)

Examiner turns raw compliance data into governed control results, traceable evidence, risk-prioritised exceptions,
policy-cited answers and a read-only review agent. **It is decision support:** it never closes issues, never makes final
regulatory determinations, and everything it produces is marked for human review.

Start with **[docs/INTERVIEW_CHECKLIST.md](docs/INTERVIEW_CHECKLIST.md)** (what to show) and
**[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** (how it fits together). Step-by-step run instructions and expected results:
**[docs/STEP_1_RUNBOOK.md](docs/STEP_1_RUNBOOK.md)**.

## Repository
```
/notebooks   00 generate data · 01 bronze · 02 silver · 03 run controls · 04a/04 risk scoring · 05 gold products
             06a/06 policy RAG · 07a/07 Examiner agent · 08 governance · 09 dashboard views · 10 integration tests
/src         examiner/synthetic_data.py · risk_scoring.py · rag.py · agent.py   (pure Python, unit-tested)
             controls/sql/<test_type>.sql  (one per control test type) + README (output contract)
/tests       54 unit, negative and agent tests:  pip install -r requirements-dev.txt && pytest tests -q
/config      where configuration lives (catalogue, bundle variables, widgets)
/resources   examiner_jobs.yml (Asset Bundle jobs) · policies/*.md (7 synthetic policy documents)
/docs        architecture · data dictionary · evidence & lineage · model card · agent · governance · dashboards ·
             CI/CD & rollback · operations runbook · limitations & roadmap · interview checklist · prompts
databricks.yml                Asset Bundle (dev/prod targets)
.github/workflows/ci.yml      tests → validate → deploy dev + run → deploy prod (approval)
```

## Run order (first time)
`00` → `01` → `02` → `03` (Aug, Sep, Oct) → `04a` → `04` → `05` → `06a` → `06` → `07a` → `07` → `08` → `09` → dashboards → `10`.
After that, the daily job runs `01 → 02 → 03 → 04 → 05 → 06 → 09 → 10`.

## Headline results (October 2026 review)
6 controls, all failing on planted violations. EXC-10025 (terminated employee with active admin access, repeat,
remediation ISS-3025 overdue) is ranked highest-risk and CRITICAL. RAG evaluation 8/8 for both Vector Search and BM25.
Agent evaluation 8/8, including all refusal cases.
