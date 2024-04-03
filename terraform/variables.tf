variable "bq_dataset_name" {
    description = "The name of the dataset"
    default     = "brazil_rural_credit"
}


variable "gcs_storage_class" {
    description = "The storage class of the bucket"
    default     = "STANDARD"
}

variable "location" {
    description = "value of the location"
    default     = "US"
}

variable "gcs_bucket_name" {
    description = "The name of the bucket"
    default     = "de-zoomcamp-2k24"
}

variable "project" {
    description = "project"
    default     = "de-zoomcamp-2k24"
  
}

variable "region" {
    description = "region"
    default     = "us-central1"
  
}
  