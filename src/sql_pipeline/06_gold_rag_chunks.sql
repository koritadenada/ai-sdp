-- Gold: curated, retrieval-ready chunks. No AI functions, so it keeps the
-- default refresh policy, and a refresh (even a
-- full recompute) never re-embeds. Serves only chunks that passed the
-- silver_chunks expectations; flagged chunks stay in silver_chunks. Topic and
-- sentiment are joined from silver_documents, so re-classifying never re-embeds.
-- External metadata makes the MV usable as an AI Search Delta Sync source.
CREATE OR REFRESH MATERIALIZED VIEW gold_rag_chunks
COMMENT 'Curated retrieval-ready chunks + gte-large-en embeddings (MV over silver_chunks).'
CLUSTER BY (topic)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'pipelines.externalMetadata.enabled' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
AS SELECT
  c.doc_id,
  c.chunk_id,
  c.chunk_to_embed,
  c.chunk_to_retrieve,
  d.topic,
  d.sentiment,
  c.source,
  c.embedding
FROM silver_chunks c
JOIN silver_documents d USING (doc_id)
WHERE coalesce(length(trim(c.chunk_to_retrieve)), 0) > 0
  AND coalesce(length(trim(c.chunk_to_embed)), 0) > 0
  AND c.embedding IS NOT NULL AND size(c.embedding) = 1024;
