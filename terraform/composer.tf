module "composer" {
  source  = "terraform-google-modules/composer/google//modules/create_environment_v2"
  version = "3.2.0"

  project_id               = var.project_id
  region                   = var.region
  composer_env_name        = var.composer_env_name
  composer_service_account = google_service_account.service_account.email
  image_version            = "composer-2.0.30-airflow-2.3.3"
  environment_size         = "ENVIRONMENT_SIZE_SMALL"
  labels                   = var.resource_labels

  network                          = module.vpc.network_name
  subnetwork                       = var.composer_env_name
  master_ipv4_cidr                 = var.composer_ip_ranges.master
  service_ip_allocation_range_name = "services"
  pod_ip_allocation_range_name     = "pods"
  enable_private_endpoint          = true

  env_variables = {
    GCS_DATA_LAKE_BUCKET      = google_storage_bucket.data_lake.name
    GCS_SQL_BACKUP_BUCKET     = google_storage_bucket.sql_backup.name
    DVDRENTAL_INSTANCE_NAME   = google_sql_database_instance.instance.name
    AIRFLOW_CONN_DVDRENTAL_DB = local.airflow_conn_dvdrental
  }

  # Pre-installed packages https://cloud.google.com/composer/docs/concepts/versioning/composer-versions
  pypi_packages = {
    # add custom packages here 
  }

  # Airflow components configuration
  scheduler = {
    "count" : 1,
    "cpu" : 1,
    "memory_gb" : 2,
    "storage_gb" : 1
  }

  web_server = {
    "cpu" : 1,
    "memory_gb" : 2,
    "storage_gb" : 1
  }

  worker = {
    cpu        = 1
    memory_gb  = 2
    storage_gb = 1
    min_count  = 1
    max_count  = 4
  }

  depends_on = [
    module.vpc
  ]
}
