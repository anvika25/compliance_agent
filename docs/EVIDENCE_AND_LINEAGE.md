# Evidence and lineage design

## Requirement: each exception's evidence must be reproducible and traceable to Bronze/Silver
For every exception, every test run writes one `gold_compliance_evidence` row and an immutable JSON copy in
`/Volumes/examiner_compliance/reference/evidence/<run_batch_id>/<control_id>/<exception_id>.json`.

| Brief's evidence element | Field(s) |
|---|---|
| Source record | `source_table`, `source_record_id`, `bronze_record_hash`, `source_file`, `source_batch_id` |
| Control logic applied | `control_logic` = SQL file + SHA-256 of its text + the exact parameters used |
| Expected condition / actual condition / variance | `expected_condition`, `actual_condition`, `variance` |
| Relevant timestamps | `event_timestamp` (business event), `evidence_timestamp` (test run), Bronze `ingestion_timestamp` |
| Related records | `related_records` (JSON: account, approver, conflicting grants, report calendar, …) |
| Policy reference | `policy_reference` (from the control catalogue) + RAG citations in agent output |
| Evidence completeness | `evidence_completeness` (share of 7 required fields present) + `evidence_gaps` (which ones are missing) |
| Review status | `review_status` = `PENDING_HUMAN_REVIEW` until a reviewer acts |

## How a finding is traced back
```
gold_compliance_exceptions.evidence_reference
  → gold_compliance_evidence (source_table, source_record_id, source_table_version, bronze_record_hash)
     → silver_<table> VERSION AS OF <source_table_version> WHERE <key> = source_record_id     (exact Silver state)
        → bronze_<table> WHERE record_hash = bronze_record_hash                              (exact raw row)
           → source_file + batch_id + ingestion_timestamp                                    (exact file and load)
```
The agent's `evidence_retrieval` tool re-runs the last hop on every request and reports "Bronze record verified present",
or flags a lineage gap. Integration test `EV-02` checks the same for every access evidence row.

## Reproducibility
* **Same data:** Delta time travel on the Silver version recorded in evidence; 7-year history retention on evidence tables.
* **Same logic:** the control SQL is versioned in Git and its hash is in the evidence; the agent's `run_control_test`
  reperforms the SQL read-only and reports whether it still matches the stored result.
* **Same IDs:** exception IDs are stable per (control, entity) across reruns, and an earlier issue's ID is reused (EXC-10025).

## Table-level lineage
Unity Catalog records table and column lineage automatically for notebooks and jobs: Catalog Explorer → table → Lineage,
or `system.access.table_lineage`.
