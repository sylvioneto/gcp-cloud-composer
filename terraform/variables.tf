locals {
  airflow_conn_dvdrental = "gcpcloudsql://airflow:${var.db_password}@${google_sql_database_instance.instance.private_ip_address}:5432/dvdrental"

  // Datastream us-central1
  datastream_ips = [
    "34.72.28.29",
    "34.67.234.134",
    "34.67.6.157",
    "34.72.239.218",
    "34.71.242.81"
  ]
}

variable "project_id" {
  description = "GCP Project ID"
  default     = null
}

variable "region" {
  type        = string
  description = "GCP region"
  default     = "us-central1"
}

variable "composer_env_name" {
  type        = string
  description = "Cloud Composer environment name"
  default     = "composer-af2"
}

variable "db_password" {
  type        = string
  description = "Postgres root password"
  default     = "supersecret"
}

variable "composer_ip_ranges" {
  type        = map(string)
  description = "Composer 2 runs on GKE, so inform here the IP ranges you want to use"
  default = {
    pods     = "10.0.0.0/22"
    services = "10.0.4.0/24"
    nodes    = "10.0.6.0/24"
    master   = "10.0.7.0/28"
  }
}

variable "resource_labels" {
  type        = map(string)
  description = "Resource labels"
  default = {
    terraform = "true"
    app       = "cloud-composer"
    purpose   = "test"
    env       = "sandbox"
    repo      = "gcp-cloud-composer"
  }
}
