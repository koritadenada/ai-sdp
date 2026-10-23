"""Gold: curated RAG-ready chunks (materialized view).

Materialized view over the silver_chunks streaming table. All expensive,
non-deterministic work (ai_prep_search + the ai_query embedding call) already
happened incrementally upstream in silver_chunks — embedded once per document.

What makes this MV cheap is simply that NO AI functions run in it — refreshing
it can never re-invoke an LLM or re-embed anything (a separate property from
whether the projection refreshes incrementally). That is the whole reason gold
can be an MV: put the AI calls in an MV and every refresh would fully recompute
and re-embed every chunk. Here the MV only does deterministic curation — it
selects the serving columns.

A self-managed-embeddings Delta Sync index (resources/ai_sdp.vector_search.yml)
sources directly from this MV, so the index syncs the precomputed `embedding`
with no manual push step. That needs the "AI Search on materialized views"
preview on the workspace (ask your Databricks account team) plus the two table
properties below, which give the MV a readable change data feed.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


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
        # Vector Search index-creation validation also requires the standard CDF
        # property on the source ("... not a valid Vector Search source. Please
        # retry after enabling change data feed"). external-metadata provides the
        # readable CDF; this flag makes VS accept the MV as a Delta Sync source.
        "delta.enableChangeDataFeed": "true",
    },
)
def gold_rag_chunks():
    return (
        spark.read.table("silver_chunks")
        # Deterministic curation only — no AI functions here, so the MV refresh
        # is cheap and never re-embeds. Empty text and bad embeddings are already
        # dropped by expectations on silver_chunks; this filter is defense-in-depth.
        .where(F.length(F.trim(F.col("chunk_to_retrieve"))) > 0)
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
