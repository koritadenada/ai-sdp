-- Silver chunk prep: ai_prep_search per valid document, stored as-is.
-- Kept separate from the explode: posexplode(ai_prep_search(...)) in one query
-- can't be refreshed incrementally (under the default policy SDP silently
-- recomputes it on every update; under INCREMENTAL STRICT the update fails).
CREATE OR REFRESH MATERIALIZED VIEW silver_chunk_prep
COMMENT 'ai_prep_search output per valid document (exploded into chunks by silver_chunks).'
REFRESH POLICY INCREMENTAL STRICT
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS SELECT
  doc_id,
  _source_file AS source,
  ai_prep_search(parsed) AS prep
FROM silver_parsed
WHERE is_valid;   -- filter BEFORE any AI call
