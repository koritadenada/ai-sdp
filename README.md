# ai-sdp: document RAG inside one Lakeflow pipeline

A **Lakeflow Spark Declarative Pipeline** that turns raw documents (PDF, DOCX,
PNG) into a governed AI Search index, with every AI step (parse, classify,
sentiment, chunk, embed) running inline as an AI function. Deployed with a
Declarative Automation Bundle on serverless compute.

![Architecture](docs/ai_sdp_architecture.png)

## How it works

- **Bronze** ingests each file once with Auto Loader. A file-arrival job runs
  the pipeline whenever a new document lands, then syncs the index.
- **Silver** materialized views parse each document, add topic and sentiment,
  and chunk and embed it. Failed parses are kept and flagged, never dropped.
- **Gold** is a materialized view with no AI functions. The AI Search index
  syncs straight from it.
- Every MV that calls a model uses `REFRESH POLICY INCREMENTAL STRICT`: a new
  document costs model calls for that document only, and the pipeline never
  silently re-runs them on the whole corpus.

Design decisions are in [BUILD.md](BUILD.md). Prerequisites, refresh policy and
troubleshooting are in [docs/reproducibility.md](docs/reproducibility.md).

## Prerequisites

- A Databricks workspace with serverless compute and AI Functions, with
  `ai_prep_search` (Beta) and external data access for pipeline tables (needed
  to index an MV) enabled.
- An existing catalog you can write to (default `ai_sdp_catalog`).
- Databricks CLI 1.8+, authenticated: `databricks auth login --profile <your-profile>`.
- Python 3.11+ in a `.venv` at the repo root, for the sample documents:
  ```bash
  python3 -m venv .venv && .venv/bin/pip install databricks-sdk python-docx fpdf2 Pillow
  ```

## Deploy

```bash
P=<your-profile>
data_generation/seed.sh $P ai_sdp_catalog rag_dev   # schema, volume, 10 sample docs
databricks bundle deploy --profile $P               # pipeline, job, AI Search index
databricks bundle run ai_sdp_ingest --profile $P    # run the pipeline, then sync the index
```

To use another catalog, pass it to `seed.sh` and add
`--var catalog=<your_catalog>` to the `bundle` commands. If the first deploy
fails with `AI Search endpoint ... not found`, the endpoint is still starting:
deploy again once it's `ONLINE`.

**See it run incrementally:** upload a file from
`data_generation/generated_docs/later/` to
`/Volumes/ai_sdp_catalog/rag_dev/raw_docs/documents/`. Within about a minute
the job processes just that document and syncs the index.

**Ask a question:**

```bash
RAG_INDEX=ai_sdp_catalog.rag_dev.gold_rag_chunks_index \
  .venv/bin/python src/agents/rag_agent.py --profile $P "How do I reset my password?"
```

## Limitations

- Only new files are processed. Upload an edited document under a new name.
- Changing a label set, prompt or model makes the next update fail until you
  full-refresh that MV on purpose (see `docs/reproducibility.md`).
- `ai_prep_search` is Beta; `ai_analyze_sentiment` and external data access for
  pipeline tables are Public Preview.

## License

Apache 2.0, see [LICENSE](LICENSE). Databricks logos and icons are trademarks of
Databricks, Inc. (see [NOTICE](NOTICE)).
