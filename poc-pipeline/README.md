# Pipeline POC — RAG document ingestion

A small, runnable proof of concept: a pipeline defined as a JSON DAG, plus a
minimal engine that executes it.

**Scope note:** this is platform-agnostic. It is not built on Just247Pipes — it
demonstrates the pipeline modelling and operational concerns the role calls for,
in a form that ports directly onto a visual-designer + JSON-DAG platform.

## Run it

```
python3 engine.py pipelines/doc_rag_ingest.json                     # happy path
python3 engine.py pipelines/doc_rag_ingest.json --fail extract_text # error routing
```

No dependencies beyond the Python standard library.

## The pipeline

`list_documents -> extract_text -> validate_extraction -> chunk -> embed -> upsert_index -> emit_metrics`

with `dead_letter` as an out-of-band route target.

## What it demonstrates

| Concern | Where |
|---|---|
| JSON DAG as the source of truth | `pipelines/doc_rag_ingest.json` |
| Dependency resolution, topological order, cycle detection | `topo_sort()` |
| Per-node retry with exponential backoff, overriding pipeline defaults | `execute_node()`, `embed` node (5 attempts, retry on 429/5xx) |
| Connections declared separately from logic | `connections` block |
| Secrets by reference, never inlined | `secret_ref` + `resolve_secret()` |
| Environment indirection | `${env.VAR}` in connection config |
| Validation gate that fails fast on bad extractions | `validate_extraction` node |
| Error routing to dead-letter instead of whole-run failure | `on_error: route` + `error_route` |
| Run metrics and conditional alerting | `emit_metrics` node, `alert_if` |
| Fan-out over a collection | `for_each` on `extract_text` |

## Sample output (happy path)

```
  ok    list_documents
  ok    extract_text
  ok    validate_extraction
  ok    chunk
  retry embed attempt 1 failed: 429 rate limited by embedding API -> sleeping 3s
  retry embed attempt 2 failed: 429 rate limited by embedding API -> sleeping 6s
  ok    embed (attempt 3)
  ok    upsert_index
  ok    emit_metrics

result: {"status": "success", "docs_processed": 12, "chunks_indexed": 148, "dead_lettered": 0}
```

## Sample output (injected failure)

```
  retry extract_text attempt 1 failed: injected failure -> sleeping 5s
  FAIL  extract_text after 2 attempt(s)
  route extract_text -> dead_letter
  ok    dead_letter
  halt  downstream of extract_text skipped for this item

result: {"status": "failed_with_dead_letter", "dead_lettered": 1}
ALERT: 1 item(s) dead-lettered
```

Task handlers are stubbed — the point is the orchestration contract, not the
connector implementations. Given the target platform's node schema, the same
DAG maps across node-for-node.
