-- Silver documents: enrich valid documents only.
-- Filter BEFORE any AI call, then classify (ai_classify v2.1) and score sentiment.
-- The expectation counts (but keeps) rows with a NULL or unexpected label.
-- INCREMENTAL STRICT: changing the labels makes the next update fail until you
-- re-classify on purpose with a full refresh of this MV only. The chunking MVs
-- read from silver_parsed, not this MV, so nothing is re-parsed or re-embedded.
CREATE OR REFRESH MATERIALIZED VIEW silver_documents (
  CONSTRAINT topic_in_label_set
    EXPECT (topic IN ('product', 'support', 'billing', 'legal', 'other'))
)
COMMENT 'Valid parsed documents, AI-enriched with topic and sentiment.'
REFRESH POLICY INCREMENTAL STRICT
CLUSTER BY (topic)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS SELECT
  doc_id,
  body,
  variant_get(
    ai_classify(body, '["product", "support", "billing", "legal", "other"]',
                map('version', '2.1')),
    '$.response[0].value', 'STRING') AS topic,
  ai_analyze_sentiment(body) AS sentiment,
  length(body) AS char_count,
  _source_file,
  _ingested_at
FROM silver_parsed
WHERE is_valid;   -- filter BEFORE any AI call
