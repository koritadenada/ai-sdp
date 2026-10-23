variable "databricks_host" {
  description = "Databricks workspace host (e.g., https://<your-workspace>.cloud.databricks.com)"
  type        = string
  default     = ""
}

variable "catalog_name" {
  description = "Unity Catalog to create (if not already present)"
  type        = string
  default     = "ai_sdp_catalog"
}

variable "schema_name" {
  description = "Schema to create within the catalog"
  type        = string
  default     = "rag"
}

variable "raw_volume_name" {
  description = "Volume name for raw source documents"
  type        = string
  default     = "raw_docs"
}

variable "environment" {
  description = "Environment name (dev/prod)"
  type        = string
  default     = "dev"
}
