terraform {
backend "gcs" {
  bucket = "wizeline-bootcamp-330020"   # GCS bucket name to store terraform tfstate
  prefix = "wizeline-bucket"            # Update to desired prefix name. Prefix name should be unique for each Terraform project having same remote state bucket.
  }
}