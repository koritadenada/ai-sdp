# ai-sdp: document RAG inside one Lakeflow pipeline

A **Lakeflow Spark Declarative Pipeline (SDP)** that turns raw documents (PDF,
DOCX, PNG) into a searchable, governed RAG index, with every AI step running
inline as an AI function. A Streamlit **Databricks App** ("AI in SDP Agent")
answers questions over the index.

Built with the `pyspark.pipelines` API on **serverless** compute and deployed
with a Declarative Automation Bundle (formerly Databricks Asset Bundles).

![Architecture](docs/ai_sdp_architecture.png)

## How it works

```
raw document files (PDF/DOCX/PNG) in a UC volume
        │  Auto Loader (cloudFiles, binaryFile); file-arrival job trigger
        ▼
┌──────────────────┐
│ bronze_documents │  raw binary ingest + provenance (_ingested_at, _source_file)
│  (streaming tbl) │  no AI calls
└──────────────────┘
        │
        ▼
┌──────────────────┐
│ silver_parsed    │  ai_parse_document → text, ONCE per file
│  (streaming tbl) │  error_status · char_count · is_valid (failures kept, flagged)
└──────────────────┘
        │  WHERE is_valid (filtered BEFORE any AI call), feeds both tables below
        ├───────────────────────────────┐
        ▼                               ▼
┌──────────────────┐          ┌──────────────────┐
│ silver_documents │          │ silver_chunks    │
│  (streaming tbl) │          │  (streaming tbl) │
└──────────────────┘          └──────────────────┘
  ai_analyze_sentiment → sentiment   ai_prep_search → semantic chunks
  ai_classify (v2.1)   → topic       ai_query(gte-large-en) → 1024-dim embedding
        │                               │  ONCE per chunk
        └───────────────┬───────────────┘
                        ▼  join on doc_id
              ┌──────────────────┐
              │ gold_rag_chunks  │  chunks + topic/sentiment, valid chunks only;
              │  (mat. view)     │  no AI functions
              └──────────────────┘
        │  Delta Sync, self-managed embeddings
        ▼
┌────────────────────────┐
│ gold_rag_chunks_index  │  AI Search (formerly Vector Search) index
└────────────────────────┘
        │
        ▼
  AI in SDP Agent (Databricks App): embed question → query index → answer with an LLM
```

**The one rule:** AI function calls live only in streaming tables, which
process each row once (until you full-refresh them), so every document is
parsed and embedded once, on arrival. Gold is a materialized view with no AI
functions: a materialized view always matches its query and may fall back to
recomputing all of it, but with no model calls in the query a recompute only
re-selects columns. On serverless, SDP refreshes gold incrementally; the
technique is in the event log (`planning_information` events).

**Parse once, enrich many times:** parsing (the most expensive call) has its
own table. `silver_documents` filters to valid rows before calling a model, so
no AI call ever sees an empty or failed parse. `silver_chunks` also reads
`silver_parsed` directly, and gold joins topic and sentiment back on `doc_id`,
so changing the topic labels and fully refreshing `silver_documents` alone
re-classifies the corpus without re-parsing or re-embedding anything. Failed parses are never
dropped; list them with:

```sql
SELECT _source_file, error_status, char_count
FROM <catalog>.<schema>.silver_parsed
WHERE NOT is_valid;
```

**Index straight from the MV:** the Delta Sync index sources directly from the
`gold_rag_chunks` MV and syncs the precomputed `embedding` column, so there is
no manual push step. An MV source needs external metadata (also called Smart
Clone) enabled on the MV: `pipelines.externalMetadata.enabled=true` plus
`delta.enableChangeDataFeed=true`, both already set in
`src/pipelines/gold_rag_chunks.py`. External data access for pipeline tables is
in Public Preview.

If your workspace still rejects the MV as a source, point the index at the
`silver_chunks` streaming table instead (see `docs/reproducibility.md`).

**Sync after every run:** the index is `TRIGGERED`, so the job's second task
(`src/jobs/sync_index.py`) starts a sync once the pipeline finishes and waits
until the index has caught up. New documents are searchable when the job
succeeds.

## Repository layout

```
ai-sdp/
├── databricks.yml                 # Bundle: variables, dev/prod targets
├── example.local.yml.example      # Template for your own (gitignored) targets
├── resources/
│   ├── ai_sdp.pipeline.yml        # Serverless SDP pipeline (PREVIEW channel)
│   ├── ai_sdp.job.yml             # File-arrival job: run pipeline, then sync index
│   └── ai_sdp.vector_search.yml   # AI Search endpoint + Delta Sync index
├── src/
│   ├── pipelines/                 # One file per table
│   │   ├── bronze_documents.py
│   │   ├── silver_parsed.py
│   │   ├── silver_documents.py
│   │   ├── silver_chunks.py
│   │   └── gold_rag_chunks.py
│   ├── sql_pipeline/              # The same five tables in SQL (not deployed by default)
│   ├── jobs/sync_index.py         # Job task: sync the AI Search index and wait
│   ├── agents/rag_agent.py        # RAG agent, usable as a CLI
│   └── app/                       # Databricks App (Streamlit)
│       ├── app.py, app.yaml, requirements.txt, databricks_logo.svg
│       └── rag_agent.py           # identical copy of src/agents/rag_agent.py
├── data_generation/
│   ├── generate_documents.py      # 10 base docs + 3 "later" docs (PDF/DOCX/PNG)
│   └── seed.sh                    # schema + volume + upload of the base docs
├── infra/terraform/               # Optional: catalog, schema, volume
├── docs/                          # Architecture diagram (Mermaid source + renders)
├── BUILD.md                       # Design decisions
└── docs/reproducibility.md        # Prerequisites, fallbacks, troubleshooting
```

## Prerequisites

- A Databricks workspace with serverless compute, in a region where
  Foundation Model APIs and AI Functions are available.
- `ai_prep_search` (Beta) enabled for the workspace. Statuses as of
  November 2026: `ai_prep_search` is Beta, `ai_analyze_sentiment` is Public
  Preview, external data access for pipeline tables (needed to index the MV) is
  Public Preview; `ai_parse_document`, `ai_classify` and `ai_query` are GA.
- Databricks Runtime 18.2+ on serverless environment version 3+ for
  `ai_prep_search` (the pipeline pins the `PREVIEW` channel; see the note in
  `resources/ai_sdp.pipeline.yml`).
- Databricks CLI 1.8 or later (built and tested with v1.17), authenticated:
  ```bash
  databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile <your-profile>
  ```
- Python 3.11+ to generate the sample documents and run the agent CLI:
  ```bash
  python3 -m venv .venv
  .venv/bin/pip install databricks-sdk python-docx fpdf2 Pillow
  ```
- Terraform 1.x, only if you provision the catalog/schema/volume with
  `infra/terraform/`.

## Deploy end to end

The commands below use the `dev` target, which writes to
`ai_sdp_catalog.rag_dev`. If you can't create that catalog, pass
`--var catalog=<your_catalog>` (and `--var schema=...`) to every `bundle`
command, or define your own target in a `*.local.yml` file (see
`example.local.yml.example`).

```bash
P=<your-profile>

# 1. Schema + volume + the 10 base documents (idempotent). The catalog must
#    already exist; create it in the UI, with SQL, or with infra/terraform.
data_generation/seed.sh $P ai_sdp_catalog rag_dev

# 2. Deploy the pipeline, the file-arrival job and the AI Search endpoint + index.
databricks bundle validate --profile $P
databricks bundle deploy   --profile $P

# 3. Run the pipeline once (bronze → silver → gold).
databricks bundle run ai_sdp_rag --profile $P

# 4. Sync the index (TRIGGERED: it reads only what changed in gold). Runs
#    triggered by file arrival do this in the job's sync_index task.
databricks vector-search-indexes sync-index \
  ai_sdp_catalog.rag_dev.gold_rag_chunks_index --profile $P
```

If the first deploy fails with `AI Search endpoint ... not found`, the endpoint
was still provisioning; wait until it is `ONLINE` and run `bundle deploy` again.

**See incremental processing:** upload one file from
`data_generation/generated_docs/later/` to
`/Volumes/ai_sdp_catalog/rag_dev/raw_docs/documents/`. Within about a minute the
file-arrival trigger starts a run in which every streaming table processes
exactly one row and gold refreshes incrementally. The job then syncs the index,
and the new document is searchable. To see the refresh technique gold used:

```sql
SELECT timestamp, message
FROM event_log(TABLE(ai_sdp_catalog.rag_dev.gold_rag_chunks))
WHERE event_type = 'planning_information'
ORDER BY timestamp DESC;
```

### Deploy the app

The app is deployed with the Apps CLI (it is not part of the bundle).

```bash
P=<your-profile>
ME=$(databricks current-user me --profile $P -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['userName'])")

# 1. Point the app at your index: edit RAG_INDEX in src/app/app.yaml
#    (default: ai_sdp_catalog.rag_dev.gold_rag_chunks_index).

# 2. Create the app, upload the code, deploy.
databricks apps create ai-sdp-rag --profile $P
databricks sync src/app /Workspace/Users/$ME/apps/ai-sdp-rag --full --profile $P
databricks apps deploy ai-sdp-rag \
  --source-code-path /Workspace/Users/$ME/apps/ai-sdp-rag --profile $P

# 3. Let the app's service principal read the index and use the endpoint.
SP=$(databricks apps get ai-sdp-rag --profile $P -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['service_principal_client_id'])")
databricks grants update catalog ai_sdp_catalog --profile $P \
  --json "{\"changes\":[{\"principal\":\"$SP\",\"add\":[\"USE_CATALOG\"]}]}"
databricks grants update schema ai_sdp_catalog.rag_dev --profile $P \
  --json "{\"changes\":[{\"principal\":\"$SP\",\"add\":[\"USE_SCHEMA\",\"SELECT\"]}]}"
EP=$(databricks vector-search-endpoints get-endpoint ai-sdp-vs-dev --profile $P -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
databricks permissions update vector-search-endpoints $EP --profile $P \
  --json "{\"access_control_list\":[{\"service_principal_name\":\"$SP\",\"permission_level\":\"CAN_USE\"}]}"
```

Set `RAG_UI_ONLY=true` in `app.yaml` to preview the UI without an index.

The agent also runs from the command line:

```bash
RAG_INDEX=ai_sdp_catalog.rag_dev.gold_rag_chunks_index \
  .venv/bin/python src/agents/rag_agent.py --profile <your-profile> "How do I reset my password?"
```

## Limitations

- **New documents only.** The file-arrival trigger fires for new files only
  (overwriting a file with the same name does not start a run), and streaming
  tables append, so an edited file isn't re-processed by default and a deleted
  file's chunks stay in the index. Upload a new version under a new file name. A
  production version would use AUTO CDC keyed on `chunk_id` and remove a
  document's old chunks by `doc_id`.
- **Previews.** `ai_prep_search` is Beta, `ai_analyze_sentiment` and external
  data access for pipeline tables are Public Preview; all may change before GA.
- **Sample corpus.** The generated documents are short and produce one chunk
  each. Test chunk counts, cost and retrieval quality on your own documents.

## License

Apache 2.0, see [LICENSE](LICENSE). Databricks logos and product icons are
trademarks of Databricks, Inc. and are not covered by the license (see [NOTICE](NOTICE)).
