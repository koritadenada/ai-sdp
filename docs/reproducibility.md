# Reproducibility: prerequisites, fallbacks, troubleshooting

Everything in this repo is code: the pipeline, job and AI Search endpoint/index
deploy from the Declarative Automation Bundle, the sample documents come from
`data_generation/generate_documents.py`, and the catalog/schema/volume can come
from `infra/terraform/`. The README has the full command sequence. This page
covers what depends on the workspace.

## Workspace prerequisites

### 1. Serverless + AI Functions

Run the pipeline on serverless compute (`serverless: true` in
`resources/ai_sdp.pipeline.yml`) in a region where Foundation Model APIs are
available: AI Functions are not supported on Classic SQL warehouses,
`ai_parse_document` and `ai_prep_search` need serverless environment version 3,
and materialized views only refresh incrementally on serverless.
`ai_parse_document` needs DBR 17.3+ and `ai_prep_search` needs DBR 18.2+. The
pipeline pins the `PREVIEW` channel; channels map to runtimes and the mapping
moves (as of October 2026, CURRENT = DBR 18 and PREVIEW = DBR 19), so check the
runtime in the pipeline event log before you change it.

Statuses as of November 2026: `ai_prep_search` is in Beta and must be enabled
for the workspace; `ai_analyze_sentiment` is in Public Preview;
`ai_parse_document`, `ai_classify` and `ai_query` are generally available.

**If missing:** the first update fails at the first AI function call.

### 2. AI Search on a materialized view

The Delta Sync index sources directly from the `gold_rag_chunks` materialized
view. An MV source needs external metadata (also called Smart Clone) enabled on
the MV or its pipeline; external data access for pipeline tables is in Public
Preview (release note, June 5, 2026). The MV side
(`pipelines.externalMetadata.enabled` and `delta.enableChangeDataFeed`) is
already configured in `src/pipelines/gold_rag_chunks.py`.

**If your workspace rejects it:** the index is created but its sync fails with
`[EXPECT_TABLE_NOT_VIEW] 'DESCRIBE HISTORY' expects a table but ... is a view`.

**Fallback:** point the index at the `silver_chunks` streaming table in
`resources/ai_sdp.vector_search.yml`:

```yaml
delta_sync_index_spec:
  source_table: ${var.catalog}.${var.schema}.silver_chunks
```

A Delta Sync index on a streaming table syncs the same way, with two
differences: `silver_chunks` has no `topic`/`sentiment` columns (gold joins them
in from `silver_documents`), so topic filtering in the app won't work, and it
keeps flagged chunks (empty text or a bad embedding) that gold filters out, so
filter those in your retrieval query. The index then syncs from silver rather
than from the gold serving layer.

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
| Index sync fails with `EXPECT_TABLE_NOT_VIEW` | The workspace does not accept the MV as a source yet | See prerequisite 2 |
| Pipeline fails on `ai_prep_search` | Beta not enabled, or not on the PREVIEW channel | See prerequisite 1 |
| A document never appears in the index | It failed to parse or had no text | `SELECT _source_file, error_status, char_count FROM <catalog>.<schema>.silver_parsed WHERE NOT is_valid` |
| New documents aren't searchable after a run | The index is `TRIGGERED` and hasn't synced (the job's `sync_index` task failed, or you ran the pipeline outside the job) | Check the `sync_index` task output, or run `databricks vector-search-indexes sync-index <catalog>.<schema>.gold_rag_chunks_index` |
| An edited document isn't updated | The file-arrival trigger fires for new files only | Upload the new version under a new file name |
| App answers with `Error: ... PERMISSION_DENIED` | The app's service principal lacks grants | Re-run the grants in "Deploy the app" |
| App crashes with `unexpected keyword argument 'icon'` | Old Streamlit in the Apps runtime | Keep `streamlit>=1.40` in `src/app/requirements.txt` |

## Avoid accidental full refreshes

A full refresh of a streaming table re-runs every AI call in it on the whole
corpus, so refresh only what you changed:

- New topic labels: `databricks bundle run ai_sdp_rag --full-refresh silver_documents`
  (re-classifies only; `silver_chunks` reads `silver_parsed`, so nothing is
  re-parsed or re-embedded, and gold picks up the new labels on the next run).
- New embedding model or chunking: full-refresh `silver_chunks` (re-embeds).
- A full refresh of `silver_parsed` re-parses everything, and its downstream
  streaming tables (`silver_documents`, `silver_chunks`) then need a full
  refresh too: a streaming table can't keep reading a fully refreshed source.
