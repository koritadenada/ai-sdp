"""Gold: curated RAG-ready chunks (materialized view).

Materialized view over the silver_chunks streaming table, joined on doc_id with
the topic and sentiment from silver_documents. All expensive,
non-deterministic work (ai_prep_search + the ai_query embedding call) already
happened incrementally upstream in silver_chunks — embedded once per document.

What makes this MV cheap is that NO AI functions run in it, so refreshing it
can never re-invoke a model or re-embed anything, even if SDP falls back to a
full recompute (a materialized view always matches its query, and may recompute
the whole query to get there). On serverless, a filter + select over a streaming
table, joined to another streaming table, is also a shape SDP can refresh
incrementally; check the technique in the event log:

    SELECT timestamp, message
    FROM event_log(TABLE(<catalog>.<schema>.gold_rag_chunks))
    WHERE event_type = 'planning_information'
    ORDER BY timestamp DESC

Gold also filters out chunks that silver_chunks flagged (empty text or a bad
embedding). That filter loses nothing: the flagged rows stay in silver_chunks.

A self-managed-embeddings Delta Sync index (resources/ai_sdp.vector_search.yml)
sources directly from this MV, so the index syncs the precomputed `embedding`
with no manual push step. An MV source needs external metadata (Smart Clone)
enabled, set by the table properties below (external data access for pipeline
tables is in Public Preview).
"""

from pyspark import pipelines as dp

# Same conditions as the has_text and embedding_ok expectations on silver_chunks.
VALID_CHUNK = (
    "coalesce(length(trim(chunk_to_retrieve)), 0) > 0 "
    "AND coalesce(length(trim(chunk_to_embed)), 0) > 0 "
    "AND embedding IS NOT NULL AND size(embedding) = 1024"
)


@dp.materialized_view(
    name="gold_rag_chunks",
    comment="Curated retrieval-ready chunks + gte-large-en embeddings (MV over silver_chunks).",
    cluster_by=["topic"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        # Expose a readable Change Data Feed on this MV so the Delta Sync Vector
        # Search index can source directly from it. Equivalent to
        #   CREATE OR REFRESH MATERIALIZED VIEW ...
        #   TBLPROPERTIES('pipelines.externalMetadata.enabled' = 'true')
        # Without it the index sync fails with MATERIALIZED_VIEW_UNSUPPORTED_
        # OPERATION: CHANGE DATA FEED not supported on Materialized Views.
        # Scoped to this MV (the index source) rather than the whole pipeline.
        "pipelines.externalMetadata.enabled": "true",
        # The AI Search docs say row-tracked sources (including MVs with external
        # metadata) use change data feed automatically, so this looks redundant.
        # In practice index creation on this MV was rejected without it ("... not
        # a valid Vector Search source. Please retry after enabling change data
        # feed"), so it stays as belt-and-braces until that validation changes.
        "delta.enableChangeDataFeed": "true",
    },
)
def gold_rag_chunks():
    # Document-level enrichment, joined onto each chunk. Kept out of
    # silver_chunks so re-classifying (a full refresh of silver_documents)
    # never forces a re-embed.
    enrichment = spark.read.table("silver_documents").select("doc_id", "topic", "sentiment")
    return (
        spark.read.table("silver_chunks")
        # Deterministic curation only, no AI functions, so a refresh never
        # re-embeds. Serve only the chunks silver_chunks' expectations passed;
        # the flagged ones stay in silver_chunks for inspection.
        .where(VALID_CHUNK)
        .join(enrichment, "doc_id")
        .select(
            "doc_id",
            "chunk_id",
            "chunk_to_embed",
            "chunk_to_retrieve",
            "topic",
            "sentiment",
            "source",
            "embedding",
        )
    )
