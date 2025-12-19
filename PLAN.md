# NanoTS 2-4 Week Plan (Detailed)

## Week 1 - Persisted Table Index & Stats
- Add a persisted table index record format.
- Record type, payload schema, and versioning in `SPEC.md`.
- Write index records on `flush()` and `pack_table()`.
- Load index records on startup; keep last record wins.
- Update stats to include index bytes.
- Tests: storage index write/read, db flush creates index record.

## Week 2 - Multi-Type Columns + NULL Support
- Extend table schema to carry column types.
- Add NULL bitmap support in segment encoding.
- Update Arrow export for new types and nulls.
- Update Python bindings (append + schema).
- Tests: mixed-type write/read, null roundtrip, Arrow export.

## Week 3 - SQL Management & Time-Bucket
- Add SQL DDL/DML (create/insert) entry points.
- Wire DataFusion catalog/provider for tables.
- Provide `time_bucket` UDF.
- Tests: SQL create/insert/query with time_bucket.

## Week 4 - Concurrency & Recovery Hardening
- Clarify concurrency model (single-writer/multi-reader).
- WAL edge-case checks and crash recovery tests.
- Add regression tests for pack+WAL and index consistency.
