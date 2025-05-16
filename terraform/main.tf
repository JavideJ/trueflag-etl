provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_bigquery_dataset" "tweet" {
  dataset_id                  = "tweet"
  location                    = var.bq_location
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "raw_tweet" {
  dataset_id = google_bigquery_dataset.tweet.dataset_id
  table_id   = "raw_tweet"

  schema = file("schema.json")

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["sentiment"]

  deletion_protection = false
}

resource "google_project_service" "required_apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "bigquerydatatransfer.googleapis.com",
    "cloudbuild.googleapis.com",
    "dataform.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "secretmanager.googleapis.com"
  ])
  project = var.project_id
  service = each.key

  disable_on_destroy = false
}
