# Governance and access-control design

Implemented by `notebooks/08_governance.py`. Every statement's result is logged to `controls.governance_apply_log`.

## Unity Catalog structure
```
examiner_compliance                (one catalog per environment: _dev / _prod, see Environment separation)
├── bronze      raw copies + ingestion metadata            owner: Data Engineering
├── silver      conformed, deduplicated, quarantine         owner: Data Engineering
├── gold        control results, exceptions, evidence,      owner: Compliance Testing
│               remediation, dashboard, examiner summary
├── controls    notification outbox, governance log          owner: Compliance Testing
├── features    risk features                               owner: Compliance Analytics
├── ml          risk model (UC), scores, RAG/agent evals    owner: Compliance Analytics / MRM
├── serving     Vector Search index (agent endpoint later)  owner: Platform Engineering
├── monitoring  RAG query log, agent requests + tool calls  owner: Platform Engineering
└── reference   SoD rules, FX, calendar, policy chunks, mask/filter functions, volumes (raw_files, checkpoints, evidence)
```

## Personas and least privilege
| Persona (group) | Gets | Does not get |
|---|---|---|
| `examiner_engineers` | USE/SELECT/MODIFY/CREATE on all schemas, read/write raw files | names (unless also in PII group) |
| `examiner_analysts` | SELECT on `gold`, `ml`, `policy_chunks_secure` | Bronze, Silver, raw files, any MODIFY |
| `examiner_reviewers` | analysts' access + `monitoring` (agent audit trail) + evidence volume | MODIFY on data tables |
| `examiner_agent` (service principal) | SELECT on 20 named tables its tools read; SELECT/MODIFY on its own 3 log tables | MODIFY on Bronze/Silver/Gold, CREATE, base `policy_chunks`, raw volumes |
| `examiner_pii_viewers` | sees real names through the column mask | (membership only) |
| `examiner_confidential_readers` / `_restricted_readers` | CONFIDENTIAL / RESTRICTED policy chunks | (membership only) |

## Sensitive data
* **PII identified and tagged:** `customer_name`, `employee_name` (Bronze and Silver) → `pii = name`, `sensitivity = confidential`.
  IDs (`customer_id`, `employee_id`, `initiated_by`, `approved_by`, `entity_id`) → `pii = pseudonymous_id`.
* **Column mask** `reference.mask_name` on Silver names: real value for `examiner_pii_viewers`, otherwise `masked:<8-char hash>`
  (deterministic, so joins and counts still work). Bronze isn't masked, because Silver is rebuilt from it; Bronze is engineers-only.
* **Gold, evidence, agent outputs and logs contain IDs only.** The agent also masks any known name in its responses and log
  lines (defence in depth).

## Row-level control
`reference.can_read_classification(cls)` + view `reference.policy_chunks_secure`. Users and the agent are granted the
view, never the base table. Vector Search can't sync from a row-filtered table, so the index query applies the same rule through
its `access_classification` filter. In production the caller's groups come from on-behalf-of-user auth, not a parameter.

## Lineage, audit, retention, versioning
* **Record-level lineage:** every Silver/Gold row carries `record_hash`, `source_file` and `batch_id`; evidence stores the Silver
  table version + Bronze hash, and the agent re-verifies the Bronze row exists.
* **Table/column lineage:** Unity Catalog captures it automatically (`system.access.table_lineage`, Catalog Explorer → Lineage).
* **Audit logs:** `system.access.audit` (who read or changed what), plus the agent's own `monitoring.agent_requests` /
  `agent_tool_calls` and `monitoring.rag_query_log`.
* **Retention:** 7 years (POL-PRV-004 §4) of Delta history on evidence, exceptions, test results, scores, agent logs and
  notifications (`delta.logRetentionDuration` and `delta.deletedFileRetentionDuration` = 2,555 days).
* **Evidence versioning:** append-only results and evidence tables; JSON evidence files per run in the `evidence` volume;
  Delta time travel (`VERSION AS OF`) reproduces any earlier state; control SQL is hashed into every evidence record.

## Environment separation (Designed, wired up in the Asset Bundle)
Separate catalogs `examiner_compliance_dev` and `examiner_compliance_prod` (bundle variable `catalog`), bound to separate
workspaces with workspace-catalog bindings. Prod jobs run as a service principal; humans have no MODIFY in prod.
Deployment only through CI/CD.

## Known gaps (prototype)
Groups must be created by a workspace or account admin; until then GRANTs log as FAILED (expected, and visible in the log).
Tag policies (governed tags) and attribute-based access control would replace per-table tags and masks at scale.
