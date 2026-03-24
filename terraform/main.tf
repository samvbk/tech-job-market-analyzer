terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

# Data Lake Bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${var.project}-${var.gcs_bucket_name_suffix}"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# Data Warehouse Dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id                 = var.bq_dataset_name
  location                   = var.region
  project                    = var.project
  delete_contents_on_destroy = true
}
