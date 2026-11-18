-- Silver parsed: parse every document once and flag whether it is usable.
-- Nothing is dropped: the expectation counts failures, failed parses stay here
-- with is_valid = false. List the files to fix with:
--   SELECT _source_file, error_status, char_count FROM silver_parsed WHERE NOT is_valid
CREATE OR REFRESH STREAMING TABLE silver_parsed (
  CONSTRAINT is_valid EXPECT (is_valid)   -- count failures, drop nothing
)
COMMENT 'Every ingested document parsed once (ai_parse_document), with an is_valid flag.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS SELECT
  doc_id,
  body,
  parsed,
  error_status,
  char_count,
  error_status IS NULL AND char_count > 0 AS is_valid,
  _source_file,
  _ingested_at
FROM (
  SELECT
    *,
    -- VARIANT-null on success; nullif turns it into a real SQL NULL.
    nullif(to_json(parsed:error_status), 'null') AS error_status,
    coalesce(length(body), 0) AS char_count
  FROM (
    SELECT
      *,
      -- Flatten the parsed elements into one text body.
      trim(regexp_replace(
        concat_ws('\n', transform(parsed:document:elements::ARRAY<VARIANT>,
                                  e -> e:content::STRING)),
        '\\s+', ' ')) AS body,
      -- Stable doc_id: file stem + short hash of the full path.
      concat_ws('_',
        regexp_replace(regexp_extract(_source_file, '([^/]+)$', 1), '\\.[^.]+$', ''),
        substring(sha2(_source_file, 256), 1, 8)) AS doc_id
    FROM (
      SELECT *, ai_parse_document(content) AS parsed
      FROM STREAM(bronze_documents)
    )
  )
);
