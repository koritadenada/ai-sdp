# Reproducibility: prerequisites, fallbacks, troubleshooting

Everything in this repo is code: the pipeline, job and AI Search endpoint/index
deploy from the Asset Bundle, the sample documents come from
`data_generation/generate_documents.py`, and the catalog/schema/volume can come
from `infra/terraform/`. The README has the full command sequence. This page
covers what depends on the workspace.

## Workspace prerequisites

### 1. Serverless + AI Functions

Every AI function in the pipeline (`ai_parse_document`, `ai_classify`,
`ai_analyze_sentiment`, `ai_prep_search`, `ai_query`) calls a Databricks-hosted
model, so the pipeline runs on serverless compute (`serverless: true` in
`resources/ai_sdp.pipeline.yml`) in a region where Foundation Model APIs and AI
Functions are available. The pipeline uses the `PREVIEW` channel because
`ai_parse_document` needs DBR 17.3+ and `ai_prep_search` needs DBR 18.2+ on
environment version 3.

`ai_prep_search` is in Beta and must be enabled for the workspace. The other
functions are generally available.

**If missing:** the first update fails at the first AI function call.

### 2. AI Search on materialized views (preview)

The Delta Sync index sources directly from the `gold_rag_chunks` materialized
view. That needs the "AI Search on materialized views" preview on the
workspace; ask your Databricks account team to enable it. The MV side
(`pipelines.externalMetadata.enabled` and `delta.enableChangeDataFeed`) is
already configured in `src/pipelines/gold_rag_chunks.py`.

**If missing:** the index is created but its sync fails with
`[EXPECT_TABLE_NOT_VIEW] 'DESCRIBE HISTORY' expects a table but ... is a view`.

**Fallback:** point the index at the `silver_chunks` streaming table in
`resources/ai_sdp.vector_search.yml`:

```yaml
delta_sync_index_spec:
  source_table: ${var.catalog}.${var.schema}.silver_chunks
```

`silver_chunks` carries the same columns as gold (gold only filters out empty
chunks, which `silver_chunks` already drops with an expectation), and a Delta
Sync index on a streaming table syncs the same way. Nothing else changes.

### 3. Permissions

The deploying user needs to create pipelines, jobs, AI Search endpoints and
apps, and needs `USE CATALOG`, `CREATE SCHEMA` (for `seed.sh`) and write access
on the target catalog. If you can't use the default `ai_sdp_catalog`, pass
`--var catalog=<your_catalog>` or add a target in a `*.local.yml` file (see
`example.local.yml.example`).

The app runs as its own service principal, which needs `USE CATALOG`,
`USE SCHEMA` + `SELECT` on the schema and `CAN_USE` on the AI Search endpoint
(see "Deploy the app" in the README). Foundation Model API pay-per-token
endpoints are usable by default.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `bundle deploy` fails with `AI Search endpoint ... not found` | The endpoint was still provisioning when the index was created | Wait until `databricks vector-search-endpoints get-endpoint ai-sdp-vs-<target>` shows `ONLINE`, then deploy again |
| Index sync fails with `EXPECT_TABLE_NOT_VIEW` | Workspace lacks the AI Search on MVs preview | See prerequisite 2 |
| Pipeline fails on `ai_prep_search` | Beta not enabled, or not on the PREVIEW channel | See prerequisite 1 |
| A document never appears in the index | It failed to parse or had no text | `SELECT _source_file, error_status, char_count FROM <catalog>.<schema>.silver_parsed WHERE NOT is_valid` |
| New documents aren't searchable after a run | The index is `TRIGGERED` and hasn't synced | `databricks vector-search-indexes sync-index <catalog>.<schema>.gold_rag_chunks_index` |
| App answers with `Error: ... PERMISSION_DENIED` | The app's service principal lacks grants | Re-run the grants in "Deploy the app" |
| App crashes with `unexpected keyword argument 'icon'` | Old Streamlit in the Apps runtime | Keep `streamlit>=1.40` in `src/app/requirements.txt` |

## Avoid accidental full refreshes

A full refresh of `silver_parsed`, `silver_documents` or `silver_chunks` re-runs
every AI call on the whole corpus. Refresh only what you changed, for example
`databricks bundle run ai_sdp_rag --full-refresh gold_rag_chunks`.
