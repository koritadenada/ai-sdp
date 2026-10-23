output "catalog_name" {
  description = "The Unity Catalog name"
  value       = databricks_catalog.ai_sdp.name
}

output "catalog_id" {
  description = "The Unity Catalog ID"
  value       = databricks_catalog.ai_sdp.id
}

output "schema_name" {
  description = "The schema name"
  value       = databricks_schema.rag.name
}

output "schema_id" {
  description = "The schema ID"
  value       = databricks_schema.rag.id
}

output "volume_name" {
  description = "The raw documents volume name"
  value       = databricks_volume.raw_docs.name
}

output "volume_id" {
  description = "The raw documents volume ID"
  value       = databricks_volume.raw_docs.id
}
