"""Silver chunks: one row per chunk, with a self-managed embedding.

Materialized view over silver_chunk_prep. It explodes the STORED ai_prep_search
output into one row per chunk and computes each chunk's embedding inline
(ai_query, databricks-gte-large-en, 1024 dims).

`refresh_policy = "incremental_strict"`: when a new document arrives, only its
chunks are embedded (APPEND_ONLY), and SDP may never fall back to re-embedding
the corpus; if it can't refresh incrementally the update fails instead. The
explode works incrementally because it runs over a stored column; exploding
ai_prep_search(...) directly here would not (see silver_chunk_prep).

ai_prep_search returns, per chunk:
  - chunk_to_embed     : context-enriched text → embedded into `embedding`
  - chunk_to_retrieve  : clean text → RETURN THIS to the LLM at query time
We assign our OWN chunk_id (doc_id + ordinal position) rather than reusing
ai_prep_search's chunk_id — see the derivation below for why (stability + global
uniqueness as the vector-index primary key).

Topic and sentiment are not carried here: gold joins them from silver_documents,
so re-classifying never re-embeds.

Unusable chunks (empty text, missing or wrong-size embedding) are KEPT here and
counted by expectations, not dropped, so you can see and fix them;
gold_rag_chunks applies the same predicate as a filter, so the index only serves
valid chunks.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Databricks-hosted embedding model (1024-dim output).
EMBED_MODEL = "databricks-gte-large-en"


@dp.materialized_view(
    name="silver_chunks",
    comment="Semantic chunks (ai_prep_search) with gte-large-en embeddings, computed once per chunk.",
    refresh_policy="incremental_strict",
    cluster_by=["doc_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
# Flag (but keep) unusable chunks where they are produced. Pass rates show up in
# the pipeline UI and event log; gold filters on the same conditions so the
# vector index never sees empty text or a bad vector:
#   - both text fields must be non-empty (we embed chunk_to_embed and serve
#     chunk_to_retrieve, so gate on both together);
#   - the embedding must be present and exactly 1024-dim (a null/short vector
#     would break index sync).
# Find flagged chunks with:
#   SELECT doc_id, chunk_id FROM silver_chunks
#   WHERE NOT (coalesce(length(trim(chunk_to_retrieve)), 0) > 0
#              AND coalesce(length(trim(chunk_to_embed)), 0) > 0
#              AND embedding IS NOT NULL AND size(embedding) = 1024)
@dp.expect(
    "has_text",
    "length(trim(chunk_to_retrieve)) > 0 AND length(trim(chunk_to_embed)) > 0",
)
@dp.expect("embedding_ok", "embedding IS NOT NULL AND size(embedding) = 1024")
def silver_chunks():
    # Explode the STORED ai_prep_search output (not an AI call in this query),
    # keeping the ordinal position (posexplode) so we can build a stable
    # chunk_id. NOTE the path is ai_prep_search's output shape
    # ('$.document.contents'), distinct from ai_parse_document's upstream shape
    # ('parsed:document:elements' in silver_parsed). VARIANT -> ARRAY<VARIANT>.
    chunks = (
        spark.read.table("silver_chunk_prep")
        .select(
            "doc_id",
            "source",
            F.posexplode(F.expr("variant_get(prep, '$.document.contents', 'ARRAY<VARIANT>')")).alias(
                "chunk_pos", "chunk"
            ),
        )
        .select(
            "doc_id",
            # Derive a STABLE, globally-unique chunk_id ourselves: doc_id +
            # ordinal position. ai_prep_search's own chunk_id
            # ('<random-hex>_<pos>') is not tied to doc_id and is regenerated on
            # every re-parse, so as the vector-index primary key it risks
            # cross-document collisions. doc_id is unique per source file (see
            # silver_parsed), so doc_id + position is unique and reproducible.
            F.concat_ws("_", F.col("doc_id"), F.col("chunk_pos")).alias("chunk_id"),
            F.expr("variant_get(chunk, '$.chunk_to_embed', 'STRING')").alias("chunk_to_embed"),
            F.expr("variant_get(chunk, '$.chunk_to_retrieve', 'STRING')").alias("chunk_to_retrieve"),
            "source",
        )
    )

    # Self-managed embedding computed inline. The embeddings endpoint returns
    # array<double>; CAST to array<float> so the stored column matches the
    # index's declared `array<float>` schema exactly.
    return chunks.withColumn(
        "embedding",
        F.expr(f"CAST(ai_query('{EMBED_MODEL}', chunk_to_embed) AS ARRAY<FLOAT>)"),
    )
