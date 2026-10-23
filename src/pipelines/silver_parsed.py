"""Silver parsed: parse every document once and flag whether it is usable.

Reads the bronze binary stream and:
  1. ai_parse_document(content) → structured VARIANT (pages, elements)
  2. flattens the parsed elements into a single `body` text string
  3. records `error_status` (NULL when the parse succeeded) and `char_count`
  4. computes `is_valid` (no parse error AND some text); never NULL

Parsing is the most expensive step, so it lives in its own streaming table and
runs exactly once per file. Nothing is dropped here: every file is kept with its
validity flag. silver_documents enriches valid rows only (no AI call ever sees a
bad row), and failed parses stay right here with is_valid = false, so a
streaming table never silently loses a document. To list the files to fix and
re-upload (error_status set = parse error; NULL with char_count 0 = no text,
e.g. a scan with no readable content):

    SELECT _source_file, error_status, char_count
    FROM silver_parsed WHERE NOT is_valid

Keeping parsing separate from enrichment also means changing the enrichment
(topic labels, sentiment) only refreshes silver_documents; documents are never
re-parsed for it.

ai_parse_document requires DBR 17.3+ on serverless (pinned in the pipeline
resource) and must run in a Foundation Model API region.
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


@dp.table(
    name="silver_parsed",
    comment="Every ingested document parsed once (ai_parse_document), with an is_valid flag.",
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
        spark.readStream.table("bronze_documents")
        # 1. Parse the binary document into a structured VARIANT.
        .withColumn("parsed", F.expr("ai_parse_document(content)"))
        # 2. Flatten parsed elements into one text body, normalizing whitespace.
        .withColumn(
            "body",
            F.trim(
                F.regexp_replace(
                    F.expr(
                        "concat_ws('\\n', "
                        "transform(parsed:document:elements::ARRAY<VARIANT>, "
                        "e -> e:content::STRING))"
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
                F.regexp_replace(
                    F.regexp_extract(F.col("_source_file"), r"([^/]+)$", 1), r"\.[^.]+$", ""
                ),
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
            # Carried forward so silver_chunks can run ai_prep_search on the raw
            # ai_parse_document VARIANT (its schema is document.elements[]; note
            # ai_prep_search downstream emits a *different* shape, document.contents[]).
            "parsed",
            "error_status",
            "char_count",
            "is_valid",
            "_source_file",
            "_ingested_at",
        )
    )
