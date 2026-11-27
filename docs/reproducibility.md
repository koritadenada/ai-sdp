# Reproducibility: prerequisites, fallbacks, troubleshooting

Everything in this repo is code: the pipeline, job and AI Search endpoint/index
deploy from the Declarative Automation Bundle, and the catalog/schema/volume
can come from `infra/terraform/` (or the CLI commands in the README). The README has the full command sequence. This page
covers what depends on the workspace.

## Workspace prerequisites

### 1. Serverless + AI Functions

Run the pipeline on serverless compute (`serverless: true` in
`resources/ai_sdp.pipeline.yml`) in a region where Foundation Model APIs are
available: AI Functions are not supported on Classic SQL warehouses,
`ai_parse_document` and `ai_prep_search` need serverless environment version 3,
and materialized views only refresh incrementally on serverless.
`ai_parse_document` needs DBR 17.3 LTS+ and `ai_prep_search` needs DBR 18.2+.
DBR 18.2 reached end of support on 5 Nov 2026 and the Runtime 18 release notes
list no later 18.x, so the pipeline pins the `PREVIEW` channel (DBR 19, where it
was tested). Channels map to runtimes and the mapping moves (as of November
2026, CURRENT = DBR 18 and PREVIEW = DBR 19), so check the runtime in the
pipeline event log before you change it.

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

**No fallback table:** every candidate source (gold and the silver tables) is
a materialized view, so the index needs the workspace to accept MV sources. If
it doesn't yet, ask your account team about external data access for pipeline
tables.

### 3. Permissions

The deploying user needs to create pipelines, jobs and AI Search endpoints, and
needs `USE CATALOG`, `CREATE SCHEMA` and write access
on the target catalog. If you can't use the default `ai_sdp_catalog`, pass
`--var catalog=<your_catalog>` or add a target in a `*.local.yml` file (see
`example.local.yml.example`).

To query the index (for example with `src/agents/rag_agent.py`), a user or
service principal needs `USE CATALOG`, `USE SCHEMA` + `SELECT` on the schema and
`CAN_USE` on the AI Search endpoint. Foundation Model API pay-per-token
endpoints are usable by default.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `bundle deploy` fails with `AI Search endpoint ... not found` | The endpoint was still provisioning when the index was created | Wait until `databricks vector-search-endpoints get-endpoint ai-sdp-vs-<target>` shows `ONLINE`, then deploy again |
| Index sync fails with `EXPECT_TABLE_NOT_VIEW` | The workspace does not accept the MV as a source yet | See prerequisite 2 |
| Pipeline fails on `ai_prep_search` | Beta not enabled, or not on the PREVIEW channel | See prerequisite 1 |
| A document never appears in the index | It failed to parse or had no text | `SELECT _source_file, error_status, char_count FROM <catalog>.<schema>.silver_parsed WHERE NOT is_valid` |
| New documents aren't searchable after a run | The index is `TRIGGERED` and hasn't synced (the job's `sync_index` task failed, or you ran the pipeline outside the job) | Check the `sync_index` task output, or run `databricks vector-search-indexes sync-index <catalog>.<schema>.gold_rag_chunks_index` |
| The `sync_index` task fails with `AI Search endpoint ... is stopped` | The endpoint was stopped (for example by an idle policy); the index and its data are kept | Start the endpoint (`databricks api post /api/2.0/vector-search/endpoints/<name>/start`), wait until the index is `ONLINE`, and re-run the job |
| An update fails with `INCREMENTAL_REFRESH_UNAVAILABLE_WITH_STRICT_REFRESH_POLICY` | You changed the query of an AI MV (labels, prompt, model), which can't be applied incrementally | Full-refresh just that MV (see "Refresh policy and full refreshes") |
| An edited document isn't updated | The file-arrival trigger fires for new files only | Upload the new version under a new file name |

## Refresh policy and full refreshes

Every MV that calls a model sets `REFRESH POLICY INCREMENTAL STRICT`, so SDP
can't silently recompute it (and re-run every model call). When you change one
of those queries on purpose, the next update fails with
`INCREMENTAL_REFRESH_UNAVAILABLE_WITH_STRICT_REFRESH_POLICY`. Apply the change
with a full refresh of just that MV:

- New topic labels: `databricks bundle run ai_sdp_rag --full-refresh silver_documents`
  (re-classifies only; the chunking MVs read `silver_parsed`, so nothing is
  re-parsed or re-embedded, and gold picks up the new labels incrementally).
- New embedding model: `--full-refresh silver_chunks` (re-embeds, no re-chunking).
- New chunking: `--full-refresh silver_chunk_prep,silver_chunks`.
- Avoid `--full-refresh-all`: it re-runs every model call on the whole corpus.

Check which technique each MV used:

```sql
SELECT timestamp, origin.flow_name, message
FROM event_log(TABLE(<catalog>.<schema>.gold_rag_chunks))
WHERE event_type = 'planning_information'
ORDER BY timestamp DESC;
```
