#!/usr/bin/env bash
# Reproducible source-data seeding for the ai-sdp pipeline.
#
# Creates the schema + raw volume and uploads the sample documents so the
# pipeline has data to ingest. Idempotent: safe to re-run.
#
# Usage:
#   data_generation/seed.sh [PROFILE] [CATALOG] [SCHEMA]
# Defaults: PROFILE=DEFAULT  CATALOG=ai_sdp_catalog  SCHEMA=rag_dev
set -euo pipefail

PROFILE="${1:-DEFAULT}"
CATALOG="${2:-ai_sdp_catalog}"
SCHEMA="${3:-rag_dev}"
VOLUME="raw_docs"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo ">> Creating schema ${CATALOG}.${SCHEMA} and volume ${VOLUME} ..."
databricks schemas create "${SCHEMA}" "${CATALOG}" --profile "${PROFILE}" 2>/dev/null \
  || echo "   schema already exists"
databricks volumes create "${CATALOG}" "${SCHEMA}" "${VOLUME}" MANAGED --profile "${PROFILE}" 2>/dev/null \
  || echo "   volume already exists"

# Generate the sample documents (PDF / DOCX / PNG) if not already present.
if [ ! -d "${HERE}/generated_docs" ] || [ -z "$(ls -A "${HERE}/generated_docs" 2>/dev/null)" ]; then
  echo ">> Generating sample documents ..."
  "${HERE}/../.venv/bin/python" "${HERE}/generate_documents.py"
fi

DOCS_DIR="dbfs:/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}/documents"
databricks fs mkdir "${DOCS_DIR}" --profile "${PROFILE}" 2>/dev/null || true
echo ">> Uploading sample documents to ${DOCS_DIR} ..."
# Top-level files only: generated_docs/later/ holds the documents you drop in
# one at a time afterwards to show an incremental run.
for f in "${HERE}"/generated_docs/*.*; do
  databricks fs cp --overwrite "${f}" "${DOCS_DIR}/" --profile "${PROFILE}"
done

echo ">> Done. Raw documents are ready to ingest."
echo "   To show incremental processing later, upload one file from"
echo "   ${HERE}/generated_docs/later/ to ${DOCS_DIR}/"
