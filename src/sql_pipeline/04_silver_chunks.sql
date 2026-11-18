-- Silver chunks: semantic chunking (ai_prep_search) + self-managed embeddings
-- (ai_query, databricks-gte-large-en, 1024 dims), once per document.
-- Unusable chunks are kept and counted by the expectations; gold filters them.
-- Reads silver_parsed (not silver_documents) so re-classifying never re-embeds.
CREATE OR REFRESH STREAMING TABLE silver_chunks (
  CONSTRAINT has_text
    EXPECT (length(trim(chunk_to_retrieve)) > 0 AND length(trim(chunk_to_embed)) > 0),
  CONSTRAINT embedding_ok
    EXPECT (embedding IS NOT NULL AND size(embedding) = 1024)
)
COMMENT 'Semantic chunks (ai_prep_search) with gte-large-en embeddings, computed once per document.'
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
      _source_file AS source,
      posexplode(variant_get(ai_prep_search(parsed), '$.document.contents',
                             'ARRAY<VARIANT>')) AS (chunk_pos, chunk)
    FROM STREAM(silver_parsed)
    WHERE is_valid   -- filter BEFORE any AI call
  )
);
