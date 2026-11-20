# SQL version of the pipeline

The same six tables as `src/pipelines/` (Python), written in SQL: one streaming
table (bronze, Auto Loader) and five materialized views. The blog post
shows these snippets; the Python files remain what the bundle deploys.

To deploy the SQL version instead, change the library glob in
`resources/ai_sdp.pipeline.yml` from `../src/pipelines/**` to
`../src/sql_pipeline/*.sql` (keep only one of the two: both define the same table
names). `${raw_docs_path}` is substituted from the pipeline `configuration`.
