# Data Analytics

## Description

This example demonstrates how to use Cloud Composer to:
- Restore a Postgres backup
- Extract data from Postgres to Cloud Storage
- Load data from Cloud Storage to BigQuery
- Transform data on BigQuery (to-do)

Resources created:
- VPC with firewall rules
- Cloud Composer
- Cloud SQL for Postgres
- Cloud Storage Buckets

Check more operators available in [Airflow Google Operators doc](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/index.html).


## Deploy

1. Create a new project and select it
2. Open Cloud Shell and ensure the env var below is set, otherwise set it with `gcloud config set project` command
```
echo $GOOGLE_CLOUD_PROJECT
```

3. Create a bucket to store your project's Terraform state
```
gsutil mb gs://$GOOGLE_CLOUD_PROJECT-tf-state
```

4. Enable the necessary APIs
```
gcloud services enable compute.googleapis.com \
    container.googleapis.com \
    containerregistry.googleapis.com\
    composer.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com \
    cloudfunctions.googleapis.com \
    pubsub.googleapis.com \
    sqladmin.googleapis.com 
```

5. Give permissions to Cloud Build for creating the resources
```
PROJECT_NUMBER=$(gcloud projects describe $GOOGLE_CLOUD_PROJECT --format='value(projectNumber)')
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member=serviceAccount:$PROJECT_NUMBER@cloudbuild.gserviceaccount.com --role=roles/editor
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member=serviceAccount:$PROJECT_NUMBER@cloudbuild.gserviceaccount.com --role=roles/iam.securityAdmin
```

6. Clone this repo
```
git clone https://github.com/sylvioneto/gcp-cloud-composer.git
cd gcp-cloud-composer
```

7. Execute Terraform using Cloud Build
```
gcloud builds submit . --config cloudbuild.yaml
```

8. Go to [Cloud Composer](https://console.cloud.google.com/composer) and check out the dags


## Destroy
1. Execute Terraform using Cloud Build
```
gcloud builds submit . --config cloudbuild_destroy.yaml
```
