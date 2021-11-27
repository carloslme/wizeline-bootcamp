variable "project_id" {
description = "Google Project ID."
type        = string
}

variable "bucket_name_raw" {
description = "GCS Bucket name. Value should be unique ."
type        = string
}

variable "bucket_name_staging" {
description = "GCS Bucket name. Value should be unique ."
type        = string
}

variable "region" {
description = "Google Cloud region"
type        = string
default     = "us-central1"
}