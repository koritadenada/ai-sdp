-- Silver chunks: explode the STORED ai_prep_search output into one row per
-- chunk, then embed each chunk (ai_query, databricks-gte-large-en, 1024 dims).
-- INCREMENTAL STRICT: only new chunks are embedded, never the corpus again.
-- Unusable chunks are kept and counted by the expectations; gold filters them.
CREATE OR REFRESH MATERIALIZED VIEW silver_chunks (
  CONSTRAINT has_text
    EXPECT (length(trim(chunk_to_retrieve)) > 0 AND length(trim(chunk_to_embed)) > 0),
  CONSTRAINT embedding_ok
    EXPECT (embedding IS NOT NULL AND size(embedding) = 1024)
)
COMMENT 'Semantic chunks (ai_prep_search) with gte-large-en embeddings, computed once per chunk.'
REFRESH POLICY INCREMENTAL STRICT
CLUSTER BY (doc_id)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS SELECT
  doc_id,
  chunk_id,
  chunk_to_embed,
  chunk_to_retrieve,
  source,
  CAST(ai_query('databricks-gte-large-en', chunk_to_embed) AS ARRAY<FLOAT>) AS embedding
FROM (
  SELECT
    doc_id,
    -- Stable primary key for the index: doc_id + chunk position.
    concat_ws('_', doc_id, CAST(chunk_pos AS STRING)) AS chunk_id,
    variant_get(chunk, '$.chunk_to_embed', 'STRING') AS chunk_to_embed,
    variant_get(chunk, '$.chunk_to_retrieve', 'STRING') AS chunk_to_retrieve,
    source
  FROM (
    SELECT
      doc_id,
      source,
      posexplode(variant_get(prep, '$.document.contents', 'ARRAY<VARIANT>'))
        AS (chunk_pos, chunk)
    FROM silver_chunk_prep
  )
);
