# AI in SDP: Build Document

This document captures the key architectural decisions and design rationale for the AI-in-SDP RAG pipeline.

## Problem Statement

Extracting structured insights from unstructured documents (PDFs, DOCX, images) requires a reliable pipeline that:
- Ingests raw documents at scale
- Parses and enriches them with AI-driven classification and sentiment analysis
- Chunks documents semantically (not by character count)
- Generates dense embeddings for vector search
- Makes chunks queryable via a RAG agent

## Architecture Decisions

### 1. Lakeflow SDP over Notebooks

Decision: Use `pyspark.pipelines` (Lakeflow Spark Declarative Pipelines) instead of orchestrated notebooks.

Rationale:
- **Reproducibility**: Single source of truth; no ad hoc SQL notebooks that diverge from code.
- **Streaming-native**: Auto Loader + streaming tables handle incremental ingestion elegantly.
- **Governance**: All tables, metadata, and lineage flow through Unity Catalog and Delta Lake properties.
- **One graph**: SDP works out dependencies, incremental refresh and checkpoints; each table is one Python function.

### 2. Medallion (Bronze/Silver/Gold) Architecture

Structure:
- **Bronze** (`bronze_documents`): Raw binary ingest via Auto Loader (streaming). Preserves `_ingested_at` and `_source_file` for lineage.
- **Silver** (`silver_parsed`, `silver_documents`, `silver_chunks`): parsing, enrichment and chunking as separate tables:
  - `silver_parsed`: Parses every document once (`ai_parse_document`) and flags `is_valid`. Nothing is dropped: failed parses stay here, flagged, with `error_status` and `char_count` columns (`WHERE NOT is_valid` lists the files to fix).
  - `silver_documents`: Filters to valid documents first, then analyzes sentiment and classifies topics, so no AI call sees a bad row. Changing labels means a full refresh of this table only; documents are never re-parsed or re-embedded for it.
  - `silver_chunks`: Reads valid rows of `silver_parsed` directly (not `silver_documents`), chunks via `ai_prep_search`, embeds via `ai_query(gte-large-en)`. It carries no enrichment columns, because a streaming table can't keep reading a source that was fully refreshed: if it read `silver_documents`, re-classifying would force a full refresh, and a re-embed, here too.
- **Gold** (`gold_rag_chunks` MV): valid chunks joined with topic/sentiment on `doc_id`; no AI calls, so refreshes stay cheap.

### 3. Silver_chunks is a Streaming Table (Not MV)

Decision: `silver_chunks` (which embeds chunks) is a streaming table, not a materialized view.

Rationale:
- **Expensive, non-deterministic calls**: `ai_prep_search` chunking and `ai_query` embeddings cost money per call and are non-deterministic. A streaming table processes each row once (until a full refresh), which is a guarantee. A materialized view may refresh incrementally, but it always matches its query and can fall back to a full recompute, which would re-run every model call. We want the guarantee for the expensive calls.
- **Append-only economics**: Each document's chunks are embedded exactly once when the document arrives. Later runs process only new documents.

Consequence: `gold_rag_chunks` MV only does **deterministic curation** (filters empty chunks, selects columns). This keeps it cheap to refresh.

### 4. Gold is a Materialized View, Vector Index Sources from It

Decision: `gold_rag_chunks` is a materialized view, and the Delta Sync AI Search index sources directly from the MV.

Rationale:
- **Self-managed embeddings**: The MV carries the precomputed 1024-dim `embedding` column (computed in `silver_chunks`). The AI Search index syncs this column as-is and never calls an embedding model itself.
- **No push step**: the index is `TRIGGERED`; the job's `sync_index` task syncs it after every pipeline run and waits for it to finish.
- **Clean separation**: Expensive work (embeddings) is upstream and done once; serving is fast and deterministic.

Prerequisite: an MV source needs external metadata (Smart Clone) enabled, which is part of external data access for pipeline tables (Public Preview). The MV carries:
- `pipelines.externalMetadata.enabled = true` (exposes readable CDF)
- `delta.enableChangeDataFeed = true` (the docs say row-tracked sources use CDF automatically, but index creation rejected the MV without it, so it stays)

Both are set in `src/pipelines/gold_rag_chunks.py` and are scoped only to that MV, not the entire pipeline.

### 5. Serverless Compute

Decision: All transforms run on serverless compute (required for Foundation Model APIs).

Rationale:
- **AI functions** (`ai_parse_document`, `ai_analyze_sentiment`, `ai_classify`, `ai_prep_search`, `ai_query`) call Databricks-hosted models, so the workspace must be in a Foundation Model APIs region; AI Functions are not available on Classic SQL warehouses.
- **Runtime**: `ai_parse_document` and `ai_prep_search` need serverless environment version 3.
- **Incremental MV refresh**: materialized views only refresh incrementally on serverless pipelines.
- **Cost isolation**: Serverless DBUs are paid per-second and scaled to workload; no idle clusters.
- **Simplicity**: No cluster management; jobs just run.

### 6. File-Arrival Job Trigger

Decision: Wrap the pipeline in a job with a file-arrival trigger instead of a scheduled job.

Rationale:
- **Event-driven**: Only runs when new documents land in the raw volume.
- **No unnecessary compute**: The trigger checks the volume about once a minute on a best-effort basis and starts a run only for new files (overwriting a file with the same name does not fire it).
- **Baseline isolation**: Files present when the trigger is created do not fire it (already ingested).

### 7. Databricks App for User Interface

Decision: Serve the RAG agent via a Databricks App (Streamlit-based).

Rationale:
- **No extra infrastructure**: The app embeds the question with the same model the pipeline used, queries the AI Search index through the Databricks SDK, and calls a Foundation Model API chat endpoint. No separate vector database or API layer.
- **Auth**: Users sign in with workspace SSO; the app reads the index as its own service principal, governed by Unity Catalog grants.
- **Deployment**: Deployed with the Apps CLI from `src/app/` (see the README).

### 8. Semantic Chunking via ai_prep_search

Decision: Use `ai_prep_search` for chunking instead of fixed-character or regex patterns.

Rationale:
- **Semantic coherence**: Chunks respect logical boundaries (sentences, paragraphs) not arbitrary offsets.
- **Variable-length chunks**: Avoids truncating important context or padding empty space.
- **Two outputs per chunk**: `chunk_to_embed` (with heading/title context) is embedded; `chunk_to_retrieve` (plain text) goes to the LLM.

Caveat: Embedding input length. `databricks-gte-large-en` has an 8,192-token context window, so `chunk_to_embed` must stay within it.

## Data Flow

```
Raw documents (PDF/DOCX/PNG) → Volume
  ↓
Auto Loader + File-arrival trigger
  ↓
bronze_documents (streaming table, raw binary)
  ↓
silver_parsed (streaming table, ai_parse_document once + is_valid flag)
  ↓  valid rows only, to both:
  ├─ silver_documents (streaming table, classify + sentiment)
  └─ silver_chunks (streaming table, chunk + embed via ai_prep_search + ai_query)
  ↓  join on doc_id
gold_rag_chunks (materialized view, deterministic curation)
  ↓
AI Search index (Delta Sync, syncs the precomputed embeddings)
  ↓
RAG Agent (Databricks App, queries index + calls LLM endpoint)
  ↓
User questions answered with cited sources
```


## Limitations and Future Work

### Append-only model (no document updates/deletes)

The current pipeline re-ingests a document as a new append. Chunks are re-created under the same `chunk_id`s, and deleted documents remain in the index.

Production fix: Apply an `apply_changes` CDC pattern on `chunk_id` or `doc_id` to replace/delete chunks on re-ingest or removal.

### No query expansion or re-ranking

Current approach: Embed query, retrieve top-k chunks, pass to LLM. No query expansion (synonyms, related questions) or semantic re-ranking (cross-encoder).

Production enhancement: Add a re-ranking step to re-order retrieved chunks by relevance before they go to the LLM.

### Workspace preview dependencies

- `ai_prep_search` is in Beta (API subject to change).
- An AI Search index sourcing from an MV needs external metadata (part of external data access for pipeline tables, Public Preview).

Fallback if the workspace rejects the MV as a source: point the Delta Sync index at `silver_chunks` instead (see `docs/reproducibility.md`). It has the chunk and embedding columns but not topic/sentiment, which gold joins in.

### Embedding input length

`databricks-gte-large-en` has an 8,192-token context window; longer input is truncated. `ai_prep_search` chunks are far smaller, but check `chunk_to_embed` lengths if you change the chunker or feed it very large documents.

## Layout, deployment and validation

See the README for the repository layout and the end-to-end deploy commands,
and `docs/reproducibility.md` for workspace prerequisites, fallbacks and
troubleshooting.

Read-only checks before deploying:

```bash
databricks bundle validate --profile <your-profile>
cd infra/terraform && terraform init -backend=false && terraform validate
```

Steps that are not code: enabling the previews on the workspace (see
prerequisites), and approving the Terraform plan if you use it.
