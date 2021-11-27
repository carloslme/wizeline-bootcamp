terraform {
backend "gcs" {
  bucket = "backend-330020"   # GCS bucket name to store terraform tfstate
  prefix = "gcs-tfstate"           # Update to desired prefix name. Prefix name should be unique for each Terraform project having same remote state bucket.
  }
}