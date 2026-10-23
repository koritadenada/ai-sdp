terraform {
  # Local state for this demo. For production, use:
  # backend "s3" {
  #   bucket         = "my-tf-state-bucket"
  #   key            = "ai-sdp/terraform.tfstate"
  #   region         = "us-west-2"
  #   encrypt        = true
  # }
  # OR:
  # backend "azurerm" {
  #   resource_group_name  = "my-rg"
  #   storage_account_name = "mystgacct"
  #   container_name       = "tfstate"
  #   key                  = "ai-sdp.tfstate"
  # }
}

provider "databricks" {
  # Host and token are read from environment variables:
  # DATABRICKS_HOST and DATABRICKS_TOKEN
  # Or pass --var "databricks_host=https://..." if using tfvars.
}
