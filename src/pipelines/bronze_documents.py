"""Bronze: raw document ingest (binary).

Streams raw document *files* (PDF, DOCX, PPTX, PNG, JPEG, TIFF, ...) from a
Unity Catalog volume using Auto Loader in binaryFile mode. Append-only: we keep
the raw bytes plus provenance so silver can parse them with ai_parse_document.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Path to the raw document files, supplied via pipeline configuration.
RAW_DOCS_PATH = spark.conf.get("raw_docs_path")


@dp.table(
    name="bronze_documents",
    comment="Raw ingested document files (binary) with provenance metadata.",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def bronze_documents():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .load(RAW_DOCS_PATH)
        # `content` (BINARY) + `path` come from binaryFile; add ingest metadata.
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("path"))
    )
