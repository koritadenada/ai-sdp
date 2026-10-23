"""Silver chunks: semantic chunking + self-managed embeddings (incremental).

Streaming table that expands each parsed silver document into retrieval-ready
chunks (ai_prep_search) and computes the embedding vector inline
(ai_query gte-large-en). These are the expensive, non-deterministic LLM calls,
so they live in a STREAMING table: each chunk is embedded exactly ONCE, when
its document first arrives — never re-embedded on later runs.

The downstream gold_rag_chunks materialized view then only curates/serves these
rows (no further LLM calls), so gold stays an MV while embeddings stay
incremental — the expensive work never re-runs on refresh.

ai_prep_search returns, per chunk:
  - chunk_to_embed     : context-enriched text → embedded into `embedding`
  - chunk_to_retrieve  : clean text → RETURN THIS to the LLM at query time
We assign our OWN chunk_id (doc_id + ordinal position) rather than reusing
ai_prep_search's chunk_id — see the derivation below for why (stability + global
uniqueness as the vector-index primary key).

Requires the AI Prep Search preview enabled for the workspace and
DBR 18.2+ / serverless env v3 (channel: PREVIEW).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Databricks-hosted embedding model (1024-dim output).
EMBED_MODEL = "databricks-gte-large-en"


@dp.table(
    name="silver_chunks",
    comment="Semantic chunks (ai_prep_search) with gte-large-en embeddings, computed once per document.",
    cluster_by=["topic"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
# Drop unusable chunks at the point they are produced (before gold), so the
# vector index never sees empty text or a bad vector:
#   - both text fields must be non-empty (we embed chunk_to_embed and serve
#     chunk_to_retrieve, so gate on both together);
#   - the embedding must be present and exactly 1024-dim (a transient ai_query
#     failure would otherwise store a null/short vector and break index sync).
@dp.expect_or_drop(
    "has_text",
    "length(trim(chunk_to_retrieve)) > 0 AND length(trim(chunk_to_embed)) > 0",
)
@dp.expect_or_drop("embedding_ok", "embedding IS NOT NULL AND size(embedding) = 1024")
def silver_chunks():
    # Stream from silver so ai_prep_search + the embedding call run once per
    # new document, never re-embedding chunks already in the table.
    prepped = spark.readStream.table("silver_documents").withColumn(
        "prep", F.expr("ai_prep_search(parsed)")
    )

    # Explode the chunk array, keeping the ordinal position (posexplode) so we
    # can build a stable chunk_id from it. NOTE the path is ai_prep_search's
    # output shape ('$.document.contents') — distinct from ai_parse_document's
    # upstream shape ('parsed:document:elements' in silver_documents).
    # VARIANT -> ARRAY<VARIANT> before exploding.
    chunks = prepped.select(
        "doc_id",
        "topic",
        "sentiment",
        F.col("_source_file").alias("source"),
        F.posexplode(
            F.expr("variant_get(prep, '$.document.contents', 'ARRAY<VARIANT>')")
        ).alias("chunk_pos", "chunk"),
    )

    return chunks.select(
        "doc_id",
        # Derive a STABLE, globally-unique chunk_id ourselves: doc_id + ordinal
        # position. ai_prep_search's own chunk_id ('<random-hex>_<pos>') is not
        # tied to doc_id and is regenerated on every (re)parse, so as the vector-
        # index primary key it risks cross-document collisions and changes on
        # every reprocess/full-refresh. doc_id is unique per source file (see
        # silver_documents), so doc_id + position is unique and reproducible.
        F.concat_ws("_", F.col("doc_id"), F.col("chunk_pos")).alias("chunk_id"),
        F.expr("variant_get(chunk, '$.chunk_to_embed', 'STRING')").alias("chunk_to_embed"),
        F.expr("variant_get(chunk, '$.chunk_to_retrieve', 'STRING')").alias("chunk_to_retrieve"),
        "topic",
        "sentiment",
        "source",
        # Self-managed embedding computed inline in the pipeline. The embeddings
        # endpoint returns array<double>; CAST to array<float> so the stored
        # column matches the index's declared `array<float>` schema exactly.
        F.expr(
            f"CAST(ai_query('{EMBED_MODEL}', chunk_to_embed) AS ARRAY<FLOAT>)"
        ).alias("embedding"),
    )
