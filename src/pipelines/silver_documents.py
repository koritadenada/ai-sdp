"""Silver documents: AI-enrich valid parsed documents.

Materialized view over silver_parsed. Keeps only rows flagged is_valid, BEFORE
any AI call, then enriches each document:
  - ai_analyze_sentiment(body) → sentiment
  - ai_classify(body, '[...]', map('version', '2.1')) → topic

Validation already happened once in silver_parsed, so no AI function here can
see an empty or failed row, and no per-call guard is needed. Failed parses stay
in silver_parsed, flagged is_valid = false, instead of being dropped.

ai_classify is called with version 2.1 (labels as a JSON array string), which
returns a VARIANT; the chosen label is response[0].value. The legacy 1.0 form
(ARRAY<STRING> labels, plain STRING result) is not recommended for new work.
A `topic_in_label_set` expectation counts (but keeps) rows whose label is NULL
or outside the set, so a classification problem shows up in the pipeline UI.

`refresh_policy = "incremental_strict"`: new documents are classified
incrementally (APPEND_ONLY), and SDP is never allowed to silently re-classify
the whole corpus. Changing TOPIC_LABELS changes this MV's query, which can't be
applied incrementally, so the next update FAILS with
INCREMENTAL_REFRESH_UNAVAILABLE_WITH_STRICT_REFRESH_POLICY until you re-classify
on purpose:

    databricks bundle run ai_sdp_rag --full-refresh silver_documents

That re-reads the text in silver_parsed (nothing is re-parsed), and nothing is
re-embedded because silver_chunk_prep / silver_chunks read silver_parsed, not
this table. gold joins topic and sentiment from here and picks up the new
labels incrementally on the next update.

Both enrichment calls hit Databricks-hosted Foundation Models, so this must run
serverless in a Foundation Model API region.
"""

import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Candidate topic labels for ai_classify. Keep the set small (2-500 allowed).
TOPIC_LABELS = ["product", "support", "billing", "legal", "other"]
# Version 2.1 takes the labels as a JSON array string.
LABELS_JSON = json.dumps(TOPIC_LABELS)
LABELS_SQL = ", ".join(f"'{label}'" for label in TOPIC_LABELS)


@dp.materialized_view(
    name="silver_documents",
    comment="Valid parsed documents, AI-enriched with topic and sentiment.",
    refresh_policy="incremental_strict",
    cluster_by=["topic"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
# Treat AI output as data you validate: keep the row, count the failure.
@dp.expect("topic_in_label_set", f"topic IN ({LABELS_SQL})")
def silver_documents():
    return (
        spark.read.table("silver_parsed")
        # Filter first: only valid documents reach the AI functions below.
        .where("is_valid")
        .withColumn("sentiment", F.expr("ai_analyze_sentiment(body)"))
        # ai_classify v2.1 returns a VARIANT; the chosen label is response[0].value.
        .withColumn(
            "topic",
            F.expr(
                f"variant_get(ai_classify(body, '{LABELS_JSON}', map('version', '2.1')), "
                "'$.response[0].value', 'STRING')"
            ),
        )
        .withColumn("char_count", F.length(F.col("body")))
        .select(
            "doc_id",
            "body",
            "topic",
            "sentiment",
            "char_count",
            "_source_file",
            "_ingested_at",
        )
    )
