#https://registry\.terraform.io/providers/hashicorp/google/latest/docs

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.16.0"
    }
  }
}

provider "google" {
  credentials = file("/home/gabri/brazil-rural-credit/credentials_dezoomcamp.json")
  #used set (export in unix) and the authentication is done by the gcloud command
  project = var.project
  region  = var.region

}

#defining a resource
resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

#
resource "google_bigquery_dataset" "brazil-rural-credit" {
  dataset_id = var.bq_dataset_name
  location      = var.location
}