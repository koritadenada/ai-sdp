"""Silver parsed: parse every document and flag whether it is usable.

Materialized view over bronze_documents that:
  1. ai_parse_document(content) → structured VARIANT (pages, elements)
  2. flattens the parsed elements into a single `body` text string
  3. records `error_status` (NULL when the parse succeeded) and `char_count`
  4. computes `is_valid` (no parse error AND some text); never NULL

AI functions run in materialized views throughout this pipeline. On serverless,
SDP refreshes these MVs incrementally: when a new file lands in bronze, only that
row is parsed (the event log reports APPEND_ONLY). `refresh_policy =
"incremental_strict"` makes that a requirement: if SDP ever cannot refresh this
MV incrementally, the update FAILS instead of silently re-parsing the corpus.

Nothing is dropped here: every file is kept with its validity flag, and the
expectation counts the failures. List the files to fix and re-upload with:

    SELECT _source_file, error_status, char_count
    FROM silver_parsed WHERE NOT is_valid

silver_documents and silver_chunk_prep both read the valid rows of this table,
side by side, so changing the enrichment never re-parses or re-chunks anything.

ai_parse_document requires DBR 17.3+ on serverless and must run in a Foundation
Model API region.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ai_parse_document sets error_status to a JSON value on failure; on success it
# is VARIANT-null, which to_json renders as 'null'. nullif() turns that into a
# real SQL NULL, so error_status is NULL exactly when there was no error (also
# when the key is missing).
ERROR_STATUS = "nullif(to_json(parsed:error_status), 'null')"
# Usable = no parse error AND some text. Both sides are NULL-safe, so is_valid is
# always TRUE or FALSE and a row can't fall through both `WHERE is_valid` and
# `WHERE NOT is_valid`.
IS_VALID = "error_status IS NULL AND char_count > 0"


@dp.materialized_view(
    name="silver_parsed",
    comment="Every ingested document parsed once (ai_parse_document), with an is_valid flag.",
    refresh_policy="incremental_strict",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
# Track (but keep) invalid rows: the pass rate shows up in the pipeline UI and
# event log, and the rows themselves stay in this table, flagged.
@dp.expect("is_valid", "is_valid")
def silver_parsed():
    return (
        spark.read.table("bronze_documents")
        # 1. Parse the binary document into a structured VARIANT.
        .withColumn("parsed", F.expr("ai_parse_document(content)"))
        # 2. Flatten parsed elements into one text body, normalizing whitespace.
        .withColumn(
            "body",
            F.trim(
                F.regexp_replace(
                    F.expr(
                        "concat_ws('\\n', transform(parsed:document:elements::ARRAY<VARIANT>, e -> e:content::STRING))"
                    ),
                    r"\s+",
                    " ",
                )
            ),
        )
        # A stable doc_id: readable file stem + short hash of the FULL path.
        # The hash suffix keeps doc_id (and therefore the downstream chunk_id /
        # vector-index primary key) unique when two files share a basename in
        # different folders — e.g. a/report.pdf vs b/report.pdf.
        .withColumn(
            "doc_id",
            F.concat_ws(
                "_",
                F.regexp_replace(F.regexp_extract(F.col("_source_file"), r"([^/]+)$", 1), r"\.[^.]+$", ""),
                F.substring(F.sha2(F.col("_source_file"), 256), 1, 8),
            ),
        )
        # 3. Diagnostics, kept as plain columns so failure review is a cheap scan.
        .withColumn("error_status", F.expr(ERROR_STATUS))
        .withColumn("char_count", F.coalesce(F.length("body"), F.lit(0)))
        # 4. One validity flag, evaluated once, used by every downstream table.
        .withColumn("is_valid", F.expr(IS_VALID))
        .select(
            "doc_id",
            "body",
            # Carried forward so silver_chunk_prep can run ai_prep_search on the
            # raw ai_parse_document VARIANT (its schema is document.elements[]).
            "parsed",
            "error_status",
            "char_count",
            "is_valid",
            "_source_file",
            "_ingested_at",
        )
    )
