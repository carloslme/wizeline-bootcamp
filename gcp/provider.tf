terraform {
    required_providers {
        google = {
            version = "= 3.54.0"
        }
    }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Create a GCS Bucket
resource "google_storage_bucket" "wizeline-bootcamp-330020" {
name     = var.bucket_name
location = var.region
}