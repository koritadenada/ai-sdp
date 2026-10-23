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
  - `silver_documents`: Filters to valid documents first, then analyzes sentiment and classifies topics, so no AI call sees a bad row. Changing labels refreshes only this table; documents are never re-parsed for it.
  - `silver_chunks`: Chunks semantic chunks via `ai_prep_search`, embeds via `ai_query(gte-large-en)`.
- **Gold** (`gold_rag_chunks` MV): Curated projection for serving; no AI calls, so refreshes stay cheap.

### 3. Silver_chunks is a Streaming Table (Not MV)

Decision: `silver_chunks` (which embeds chunks) is a streaming table, not a materialized view.

Rationale:
- **Expensive, non-deterministic calls**: `ai_prep_search` chunking and `ai_query` embeddings cost money per call and are non-deterministic. Inside an MV they would rule out incremental refresh, so every refresh would recompute and **re-embed every chunk**.
- **Append-only economics**: Each document's chunks are embedded exactly once when the document arrives. Later runs process only new documents.

Consequence: `gold_rag_chunks` MV only does **deterministic curation** (filters empty chunks, selects columns). This keeps it cheap to refresh.

### 4. Gold is a Materialized View, Vector Index Sources from It

Decision: `gold_rag_chunks` is a materialized view, and the Delta Sync AI Search index sources directly from the MV.

Rationale:
- **Self-managed embeddings**: The MV carries the precomputed 1024-dim `embedding` column (computed in `silver_chunks`). The AI Search index syncs this column as-is and never calls an embedding model itself.
- **Auto-sync**: Delta Sync over an MV means the index auto-syncs whenever the MV refreshes, no manual push step.
- **Clean separation**: Expensive work (embeddings) is upstream and done once; serving is fast and deterministic.

Prerequisite: The workspace must have the AI Search on materialized views preview enabled (ask your Databricks account team). Additionally, the MV must carry:
- `pipelines.externalMetadata.enabled = true` (exposes readable CDF)
- `delta.enableChangeDataFeed = true` (lets AI Search accept the MV as a Delta Sync source)

Both are set in `src/pipelines/gold_rag_chunks.py` and are scoped only to that MV, not the entire pipeline.

### 5. Serverless Compute

Decision: All transforms run on serverless compute (required for Foundation Model APIs).

Rationale:
- **AI functions** (`ai_parse_document`, `ai_analyze_sentiment`, `ai_classify`, `ai_prep_search`, `ai_query`) call Databricks-hosted models. These run only on serverless compute, in regions where Foundation Model APIs and AI Functions are available.
- **Cost isolation**: Serverless DBUs are paid per-second and scaled to workload; no idle clusters.
- **Simplicity**: No cluster management; jobs just run.

### 6. File-Arrival Job Trigger

Decision: Wrap the pipeline in a job with a file-arrival trigger instead of a scheduled job.

Rationale:
- **Event-driven**: Only runs when new documents land in the raw volume.
- **No unnecessary compute**: The trigger checks the volume about once a minute and starts a run only when new files have arrived.
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
  ↓  valid rows only
silver_documents (streaming table, classify + sentiment)
  ↓
silver_chunks (streaming table, chunk + embed via ai_prep_search + ai_query)
  ↓
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
- An AI Search index sourcing from an MV needs the AI Search on materialized views preview.

Fallback for workspaces without the preview: point the Delta Sync index at `silver_chunks` (streaming table) instead of the MV. It carries the same columns and syncs the same way.

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
