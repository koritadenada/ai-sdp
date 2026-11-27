"""Silver chunk prep: semantic chunking with ai_prep_search, one row per document.

Materialized view over the valid rows of silver_parsed. It only calls
ai_prep_search and stores the result; silver_chunks explodes it into one row per
chunk and embeds each chunk.

Why a separate MV: exploding an AI function's output in the SAME query
(posexplode(ai_prep_search(...))) cannot be refreshed incrementally. Under the
default refresh policy SDP then silently recomputes the MV on every update, even
with no new data, re-chunking (and, downstream, re-embedding) the whole corpus;
under INCREMENTAL STRICT the update fails. Storing ai_prep_search's output first
and exploding the stored column in the next MV keeps both incremental
(APPEND_ONLY when a new document arrives).

Requires ai_prep_search (Beta) enabled for the workspace and DBR 18.2+ on
serverless environment version 3+ (see the channel note in
resources/ai_sdp.pipeline.yml). Its output schema is versioned; silver_chunks
reads the current shape ($.document.contents).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="silver_chunk_prep",
    comment="ai_prep_search output per valid document (exploded into chunks by silver_chunks).",
    refresh_policy="incremental_strict",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def silver_chunk_prep():
    return (
        spark.read.table("silver_parsed")
        # Filter first: only valid documents reach ai_prep_search.
        .where("is_valid")
        .select(
            "doc_id",
            F.col("_source_file").alias("source"),
            F.expr("ai_prep_search(parsed)").alias("prep"),
        )
    )
