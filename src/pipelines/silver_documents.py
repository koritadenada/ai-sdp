"""Silver documents: AI-enrich valid parsed documents.

Reads silver_parsed and keeps only rows flagged is_valid, BEFORE any AI call,
then enriches each document:
  - ai_analyze_sentiment(body) → sentiment
  - ai_classify(body, [...])   → topic

Validation already happened once in silver_parsed, so no AI function here can
see an empty or failed row, and no per-call guard is needed. Failed parses stay
in silver_parsed, flagged is_valid = false, instead of being dropped.

Both enrichment calls hit Databricks-hosted Foundation Models, so this must run
serverless in a Foundation Model API region.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Candidate topic labels for ai_classify. Keep the set small (2-500 allowed).
TOPIC_LABELS = ["product", "support", "billing", "legal", "other"]


@dp.table(
    name="silver_documents",
    comment="Valid parsed documents, AI-enriched with topic and sentiment.",
    cluster_by=["topic"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def silver_documents():
    labels_sql = ", ".join(f"'{label}'" for label in TOPIC_LABELS)
    return (
        spark.readStream.table("silver_parsed")
        # Filter first: only valid documents reach the AI functions below.
        .where("is_valid")
        .withColumn("sentiment", F.expr("ai_analyze_sentiment(body)"))
        # This workspace's ai_classify returns the label as a plain STRING.
        .withColumn("topic", F.expr(f"ai_classify(body, ARRAY({labels_sql}))"))
        .withColumn("char_count", F.length(F.col("body")))
        .select(
            "doc_id",
            "body",
            "topic",
            "sentiment",
            "char_count",
            # Carried forward so silver_chunks can run ai_prep_search on the raw
            # ai_parse_document VARIANT.
            "parsed",
            "_source_file",
            "_ingested_at",
        )
    )
