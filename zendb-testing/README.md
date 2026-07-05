# zendb-testing

**Integration tests for ZeninDB simulating a document search-engine pipeline.**

This crate is `#![cfg(test)]` — it is not compiled into release builds.
It serves as both a test harness and an end-to-end usage example for the
`zendb-engine` operator runtime.

---

## What It Tests

Four integration tests exercise the full stack:

| Test | Validates |
|---|---|
| `document_indexing_pipeline` | Insert documents → verify inverted index. Reopen DB → verify all state survived. Insert more → verify incremental indexing. |
| `document_delete_removes_from_index` | Insert + index a document, delete it, verify removal from index and stats. |
| `multiple_operators_share_table_cleanly` | Two operator instances on the same table, verify shared index correctness. |
| `timers_are_evicted_on_retirement` | Timer-driven archiver produces a report, retires, and leaves no pending records. |

---

## Test Operators

### `IndexerOp`

Subscribes to the `documents` table. On `Replace` events, tokenizes text
content, builds an inverted index (`word → Set<doc_id>`) in engine state,
and tracks per-document word counts. On `Delete`, removes the document.

### `ArchiverOp`

A timer-driven operator that periodically reads `doc_stats` state and writes
summary reports to a `reports` table. Retires after `max_reports`.

---

## Structure

```
src/
├── lib.rs          # Crate root (#![cfg(test)])
├── executor.rs     # ThreadExecutor — minimal async executor for tests
├── operators.rs    # IndexerOp, ArchiverOp, helpers, define_operator_set!
└── tests.rs        # Integration test functions
```
