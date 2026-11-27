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
- **Incremental by default**: Auto Loader + a streaming table ingest new files; materialized views on serverless refresh incrementally from there.
- **Governance**: All tables, metadata, and lineage flow through Unity Catalog and Delta Lake properties.
- **One graph**: SDP works out dependencies, incremental refresh and checkpoints; each table is one Python function.

### 2. Medallion (Bronze/Silver/Gold) Architecture

Structure:
- **Bronze** (`bronze_documents`): Raw binary ingest via Auto Loader (streaming). Preserves `_ingested_at` and `_source_file` for lineage.
- **Silver** (`silver_parsed`, `silver_documents`, `silver_chunk_prep`, `silver_chunks`): parsing, enrichment, chunking and embedding as separate materialized views:
  - `silver_parsed`: Parses every document (`ai_parse_document`) and flags `is_valid`. Nothing is dropped: failed parses stay here, flagged, with `error_status` and `char_count` columns (`WHERE NOT is_valid` lists the files to fix).
  - `silver_documents`: Filters to valid documents first, then analyzes sentiment and classifies topics, so no AI call sees a bad row.
  - `silver_chunk_prep`: Valid rows of `silver_parsed` → `ai_prep_search`, stored as-is.
  - `silver_chunks`: Explodes the stored chunks (`posexplode`) and embeds each via `ai_query(gte-large-en)`.
- **Gold** (`gold_rag_chunks` MV): valid chunks joined with topic/sentiment on `doc_id`; no AI calls.

### 3. AI Functions run in materialized views, with INCREMENTAL STRICT

Decision: every AI function call runs in a materialized view, and each of those MVs sets `REFRESH POLICY INCREMENTAL STRICT`. Bronze is the only streaming table (Auto Loader).

Rationale:
- **Incremental on serverless**: tested on a dogfood workspace, MVs calling `ai_parse_document`, `ai_classify`, `ai_analyze_sentiment`, `ai_prep_search` and `ai_query` are planned `APPEND_ONLY` when a new document arrives, so only that document's model calls run.
- **STRICT as a guardrail**: under the default `AUTO` policy SDP may silently fall back to a full recompute, which would re-run every model call on the corpus. STRICT makes that a failed update instead.
- **Simpler change handling than streaming tables**: an MV can read a source that was fully refreshed, so re-classifying (full refresh of `silver_documents`) doesn't break or re-embed anything downstream. With streaming tables, a fully refreshed source makes the downstream streaming table fail until it is fully refreshed too.

Query-shape pitfall: `posexplode(ai_prep_search(...))` in one MV can't be refreshed incrementally (under `AUTO` it was `COMPLETE_RECOMPUTE` on every update, even with no new data; under STRICT the update fails). Storing `ai_prep_search` output in `silver_chunk_prep` and exploding the stored column in `silver_chunks` keeps both `APPEND_ONLY`.

Consequence: a label, prompt or model change makes the next update fail until you full-refresh the affected MV on purpose. Gold has no AI calls and keeps the default policy.

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

### 7. Semantic Chunking via ai_prep_search

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
bronze_documents (streaming table, raw binary; the only streaming table)
  ↓
silver_parsed (MV, STRICT: ai_parse_document + is_valid flag)
  ↓  valid rows only, to both:
  ├─ silver_documents (MV, STRICT: classify + sentiment)
  └─ silver_chunk_prep (MV, STRICT: ai_prep_search)
       └─ silver_chunks (MV, STRICT: posexplode stored chunks + ai_query embedding)
  ↓  join on doc_id
gold_rag_chunks (materialized view, deterministic curation)
  ↓
AI Search index (Delta Sync, syncs the precomputed embeddings)
  ↓
RAG agent (src/agents/rag_agent.py: embeds the question, queries the index, calls an LLM endpoint)
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

There is no streaming-table fallback in this design: every table the index could source from is a materialized view, so the workspace must accept MV sources (external metadata).

### Embedding input length

`databricks-gte-large-en` has an 8,192-token context window; longer input is truncated. `ai_prep_search` chunks are far smaller, but check `chunk_to_embed` lengths if you change the chunker or feed it very large documents.

## Layout, deployment and validation

See the README for the deploy commands,
and `docs/reproducibility.md` for workspace prerequisites, fallbacks and
troubleshooting.

Read-only checks before deploying:

```bash
databricks bundle validate --profile <your-profile>
cd infra/terraform && terraform init -backend=false && terraform validate
```

Steps that are not code: enabling the previews on the workspace (see
prerequisites), and approving the Terraform plan if you use it.
