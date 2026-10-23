# Unity Catalog for all ai-sdp data assets
resource "databricks_catalog" "ai_sdp" {
  name           = var.catalog_name
  comment        = "AI in SDP: RAG-ready document chunks, embeddings, and related assets"
  isolation_mode = "OPEN"
  force_destroy  = var.environment == "dev" ? true : false
}

# Schema for organizing bronze/silver/gold tables
resource "databricks_schema" "rag" {
  name              = var.schema_name
  catalog_name      = databricks_catalog.ai_sdp.name
  comment           = "Bronze/silver/gold medallion tables for the RAG pipeline"
  force_destroy     = var.environment == "dev" ? true : false
  depends_on        = [databricks_catalog.ai_sdp]
}

# UC Volume to hold raw source documents (PDFs, DOCX, PNGs, etc.)
resource "databricks_volume" "raw_docs" {
  name           = var.raw_volume_name
  catalog_name   = databricks_catalog.ai_sdp.name
  schema_name    = databricks_schema.rag.name
  volume_type    = "MANAGED"
  comment        = "Raw source documents for ingestion (PDF/DOCX/PNG)"
  depends_on     = [databricks_schema.rag]
}

# Optional: metastore grants for administrators
# (Granular permissions on tables/volumes are typically applied at the job/app level)
# resource "databricks_grants" "catalog_admin" {
#   catalog = databricks_catalog.ai_sdp.name
#   grant {
#     principal  = "admins@example.com"
#     permission = "OWNER"
#   }
# }

# Optional: create a secret scope for LLM API keys or service credentials
# (Uncomment if needed for production deployments)
# resource "databricks_secret_scope" "ai_sdp" {
#   name                     = "ai-sdp-scope"
#   initial_manage_principal = "users"
# }
