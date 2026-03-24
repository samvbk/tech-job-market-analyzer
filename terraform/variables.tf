variable "project" {
  description = "Project ID"
  type        = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "us-central1"
  type        = string
}

variable "gcs_bucket_name_suffix" {
  description = "My Storage Bucket Name Suffix"
  default     = "raw-data-lake"
  type        = string
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "tech_job_market_data"
  type        = string
}
