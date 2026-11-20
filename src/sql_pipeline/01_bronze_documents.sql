-- Bronze: raw document ingest (binary). No AI calls.
-- The pipeline's only streaming table (Auto Loader needs one); every AI call
-- downstream runs in a materialized view.
-- Auto Loader (read_files in a streaming query) in binaryFile mode keeps the raw
-- bytes in `content` plus `path`, `length` and `modificationTime`.
CREATE OR REFRESH STREAMING TABLE bronze_documents
COMMENT 'Raw ingested document files (binary) with provenance metadata.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.enableRowTracking' = 'true'   -- lets downstream MVs refresh incrementally
)
AS SELECT
  *,
  current_timestamp() AS _ingested_at,
  path AS _source_file
FROM STREAM read_files('${raw_docs_path}', format => 'binaryFile');
