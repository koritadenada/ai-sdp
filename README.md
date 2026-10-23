# ai-sdp: document RAG inside one Lakeflow pipeline

A **Lakeflow Spark Declarative Pipeline (SDP)** that turns raw documents (PDF,
DOCX, PNG) into a searchable, governed RAG index, with every AI step running
inline as an AI function. A Streamlit **Databricks App** ("AI in SDP Agent")
answers questions over the index.

Built with the `pyspark.pipelines` API on **serverless** compute and deployed
with a Databricks Asset Bundle.

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
        │  WHERE is_valid (filtered BEFORE any enrichment call)
        ▼
┌──────────────────┐
│ silver_documents │  ai_analyze_sentiment(body) → sentiment
│  (streaming tbl) │  ai_classify(body, [...])   → topic
└──────────────────┘
        │
        ▼
┌──────────────────┐
│ silver_chunks    │  ai_prep_search → semantic chunks (chunk_to_embed / chunk_to_retrieve)
│  (streaming tbl) │  ai_query(databricks-gte-large-en) → 1024-dim embedding, ONCE per chunk
└──────────────────┘
        │
        ▼
┌──────────────────┐
│ gold_rag_chunks  │  projection + filter over silver_chunks; no AI functions
│  (mat. view)     │
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
process each row exactly once, so every document is parsed and embedded once,
on arrival. Gold is a materialized view with no AI functions: refreshing it
never re-invokes a model, and SDP can refresh it incrementally. Calling a
non-deterministic function such as `ai_query` inside the MV would force a full
recompute, and a re-embed of every chunk, on every refresh.

**Parse once, enrich many times:** parsing (the most expensive call) has its
own table. `silver_documents` filters to valid rows before calling a model, so
no AI call ever sees an empty or failed parse, and changing the topic labels
re-runs classification without re-parsing anything. Failed parses are never
dropped; list them with:

```sql
SELECT _source_file, error_status, char_count
FROM <catalog>.<schema>.silver_parsed
WHERE NOT is_valid;
```

**Index straight from the MV:** the Delta Sync index sources directly from the
`gold_rag_chunks` MV and syncs the precomputed `embedding` column, so there is
no manual push step. This needs:

1. The **AI Search on materialized views** preview enabled on the workspace
   (ask your Databricks account team).
2. On the MV, `pipelines.externalMetadata.enabled=true` and
   `delta.enableChangeDataFeed=true` (already set in
   `src/pipelines/gold_rag_chunks.py`).

Without the preview, point the index at the `silver_chunks` streaming table
instead (`source_table` in `resources/ai_sdp.vector_search.yml`). It carries the
same columns, still auto-syncs, and the rest of the pipeline stays the same.

## Repository layout

```
ai-sdp/
├── databricks.yml                 # Asset Bundle: variables, dev/prod targets
├── example.local.yml.example      # Template for your own (gitignored) targets
├── resources/
│   ├── ai_sdp.pipeline.yml        # Serverless SDP pipeline (PREVIEW channel)
│   ├── ai_sdp.job.yml             # File-arrival job that runs the pipeline
│   └── ai_sdp.vector_search.yml   # AI Search endpoint + Delta Sync index
├── src/
│   ├── pipelines/                 # One file per table
│   │   ├── bronze_documents.py
│   │   ├── silver_parsed.py
│   │   ├── silver_documents.py
│   │   ├── silver_chunks.py
│   │   └── gold_rag_chunks.py
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
- `ai_prep_search` (Beta) enabled for the workspace, and ideally the AI Search
  on materialized views preview (see above for the fallback).
- A recent Databricks CLI (built and tested with v1.17), authenticated:
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

# 4. Sync the index (TRIGGERED: it reads only what changed in gold).
databricks vector-search-indexes sync-index \
  ai_sdp_catalog.rag_dev.gold_rag_chunks_index --profile $P
```

If the first deploy fails with `AI Search endpoint ... not found`, the endpoint
was still provisioning; wait until it is `ONLINE` and run `bundle deploy` again.

**See incremental processing:** upload one file from
`data_generation/generated_docs/later/` to
`/Volumes/ai_sdp_catalog/rag_dev/raw_docs/documents/`. Within about a minute the
file-arrival trigger starts a run in which every streaming table processes
exactly one row and gold appends only the new chunks. Sync the index again and
the new document is searchable.

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

- **New documents only.** Streaming tables append, so an edited file isn't
  re-processed by default and a deleted file's chunks stay in the index. A
  production version would use AUTO CDC keyed on `chunk_id` and remove a
  document's old chunks by `doc_id`.
- **Previews.** `ai_prep_search` is Beta and AI Search on materialized views is
  a preview; both may change before GA.
- **Sample corpus.** The generated documents are short and produce one chunk
  each. Test chunk counts, cost and retrieval quality on your own documents.

## License

Apache 2.0, see [LICENSE](LICENSE). Databricks logos and product icons are
trademarks of Databricks, Inc. and are not covered by the license (see [NOTICE](NOTICE)).
