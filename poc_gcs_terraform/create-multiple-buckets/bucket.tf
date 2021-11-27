# Specify the GCP Provider
provider "google" {
    project = var.project_id
    region  = var.region
}

# Create a GCS Bucket
resource "google_storage_bucket" "raw-layer-330021" {
    name     = var.bucket_name_raw
    location = var.region
}

resource "google_storage_bucket" "staging-layer-330021" {
    name     = var.bucket_name_staging
    location = var.region
}