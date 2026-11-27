# Control SQL contract

One file per **test type** (`<test_type>.sql`). The catalogue row says which test type a control uses and
supplies its `threshold`. The runner (`notebooks/03_run_controls`) passes named parameters (only those present):

| parameter | type | meaning |
|---|---|---|
| `:period_start` | `yyyy-MM-dd` string | start of review period |
| `:period_end` | `yyyy-MM-dd` string | end of review period (point-in-time tests use this) |
| `:threshold` | double | from `silver_control_catalogue.threshold` |

Each query returns **the whole population**, one row per entity, with exactly these columns:

`entity_id, entity_type, business_unit, is_exception, exception_description, expected_condition,
actual_condition, variance, financial_impact, detection_confidence, event_timestamp, source_table,
source_record_id, src_record_hash, src_source_file, src_batch_id, related_records`

`population_count = count(*)`, `exception_count = count_if(is_exception)`.
`src_*` columns carry Bronze lineage through to `gold_compliance_evidence`.

To add a control that reuses a test type, add a catalogue row only. To add a new test type, add a SQL file here.
