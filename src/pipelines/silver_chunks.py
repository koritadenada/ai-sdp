"""Silver chunks: semantic chunking + self-managed embeddings (incremental).

Streaming table that expands each valid parsed document (silver_parsed WHERE
is_valid) into retrieval-ready chunks (ai_prep_search) and computes the
embedding vector inline
(ai_query gte-large-en). These are the expensive, non-deterministic LLM calls,
so they live in a STREAMING table: each chunk is embedded exactly ONCE, when
its document first arrives — never re-embedded on later runs.

It reads silver_parsed, NOT silver_documents, on purpose: silver_chunks carries
no enrichment columns, so re-classifying documents (a full refresh of
silver_documents after changing the topic labels) never touches this table and
never re-embeds anything. A streaming table cannot keep reading a source that
was fully refreshed (it fails with a non-append-only source error), so reading
silver_documents here would force a full refresh, and a re-embed, of this table
too. gold_rag_chunks joins topic and sentiment back on doc_id.

ai_prep_search returns, per chunk:
  - chunk_to_embed     : context-enriched text → embedded into `embedding`
  - chunk_to_retrieve  : clean text → RETURN THIS to the LLM at query time
We assign our OWN chunk_id (doc_id + ordinal position) rather than reusing
ai_prep_search's chunk_id — see the derivation below for why (stability + global
uniqueness as the vector-index primary key).

Requires ai_prep_search (Beta) enabled for the workspace and DBR 18.2+ on
serverless environment version 3+ (see the channel note in
resources/ai_sdp.pipeline.yml).

Unusable chunks (empty text, missing or wrong-size embedding) are KEPT here and
counted by expectations, not dropped: a streaming table never revisits a row it
dropped, while a kept row can be inspected and fixed. gold_rag_chunks applies
the same predicate as a filter, so the index only serves valid chunks.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Databricks-hosted embedding model (1024-dim output).
EMBED_MODEL = "databricks-gte-large-en"


@dp.table(
    name="silver_chunks",
    comment="Semantic chunks (ai_prep_search) with gte-large-en embeddings, computed once per document.",
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
    # Stream from silver_parsed (valid rows only, filtered BEFORE any AI call)
    # so ai_prep_search + the embedding call run once per new document, never
    # re-embedding chunks already in the table.
    prepped = (
        spark.readStream.table("silver_parsed")
        .where("is_valid")
        .withColumn("prep", F.expr("ai_prep_search(parsed)"))
    )

    # Explode the chunk array, keeping the ordinal position (posexplode) so we
    # can build a stable chunk_id from it. NOTE the path is ai_prep_search's
    # output shape ('$.document.contents') — distinct from ai_parse_document's
    # upstream shape ('parsed:document:elements' in silver_parsed).
    # VARIANT -> ARRAY<VARIANT> before exploding.
    chunks = prepped.select(
        "doc_id",
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
        # silver_parsed), so doc_id + position is unique and reproducible.
        F.concat_ws("_", F.col("doc_id"), F.col("chunk_pos")).alias("chunk_id"),
        F.expr("variant_get(chunk, '$.chunk_to_embed', 'STRING')").alias("chunk_to_embed"),
        F.expr("variant_get(chunk, '$.chunk_to_retrieve', 'STRING')").alias("chunk_to_retrieve"),
        "source",
        # Self-managed embedding computed inline in the pipeline. The embeddings
        # endpoint returns array<double>; CAST to array<float> so the stored
        # column matches the index's declared `array<float>` schema exactly.
        F.expr(
            f"CAST(ai_query('{EMBED_MODEL}', chunk_to_embed) AS ARRAY<FLOAT>)"
        ).alias("embedding"),
    )
