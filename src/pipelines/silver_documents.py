"""Silver documents: AI-enrich valid parsed documents.

Reads silver_parsed and keeps only rows flagged is_valid, BEFORE any AI call,
then enriches each document:
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

Changing TOPIC_LABELS applies only to documents processed afterwards. To
re-classify the existing corpus, full-refresh silver_documents only: it re-reads
the text in silver_parsed, so no document is parsed again, and nothing is
re-embedded because silver_chunks reads silver_parsed, not this table. gold
joins topic and sentiment from here, so the next update picks up the new labels.

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


@dp.table(
    name="silver_documents",
    comment="Valid parsed documents, AI-enriched with topic and sentiment.",
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
        spark.readStream.table("silver_parsed")
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
