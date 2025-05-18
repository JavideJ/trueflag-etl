provider "google" {
  credentials = file("cred.json")
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  credentials = file("cred.json")
  project     = var.project_id
  region      = var.region
}

resource "google_bigquery_dataset" "tweet" {
  dataset_id                  = "tweet"
  location                    = var.bq_location
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "youtubecomment" {
  dataset_id                  = "youtubecomment"
  location                    = var.bq_location
  delete_contents_on_destroy = true
}

resource "google_project_service" "required_apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "bigquerydatatransfer.googleapis.com",
    "cloudbuild.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "secretmanager.googleapis.com",
    "cloudfunctions.googleapis.com",
    "eventarc.googleapis.com",
    "run.googleapis.com",
    "cloudscheduler.googleapis.com"

  ])
  project = var.project_id
  service = each.key

  disable_on_destroy = false
}

resource "google_bigquery_table" "raw_youtube_comment" {
  dataset_id = google_bigquery_dataset.youtubecomment.dataset_id
  table_id   = "raw_youtube_comment"

  schema = file("schemas/schema_yt_comment.json")

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["sentiment"]

  deletion_protection = false
}

resource "google_bigquery_table" "raw_tweet" {
  dataset_id = google_bigquery_dataset.tweet.dataset_id
  table_id   = "raw_tweet"

  schema = file("schemas/schema_tweet.json")

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["sentiment"]

  deletion_protection = false
}

resource "google_storage_bucket" "cloud_function" {
  name     = "cloud_function_trueflag_etl"
  location = var.region
  force_destroy = true
}

resource "google_storage_bucket_object" "cloud_function_zip" {
  name   = "cloud_function.zip"
  bucket = google_storage_bucket.cloud_function.name
  source = "../cloud_function/cloud_function.zip"
}

resource "google_cloudfunctions2_function" "trueflag-etl" {
  name        = "trueflag-etl"
  description = "ETL archivos bucket s3"
  location    = var.region

  build_config {
    runtime     = "python310"
    entry_point = "main"
    source {
      storage_source {
        bucket = google_storage_bucket.cloud_function.name
        object = google_storage_bucket_object.cloud_function_zip.name
      }
    }
  }

  service_config {
    available_memory = "1Gi"
    available_cpu    = "1"
    service_account_email = var.service_account
    max_instance_count = 3
    min_instance_count = 1
    timeout_seconds = 1000
  }

}

resource "google_cloud_scheduler_job" "invoke_trueflag_etl" {
  name        = "invoke-trueflag-etl"
  description = "Invoca la función trueflag-etl cada 30 minutos"
  schedule    = "*/30 * * * *"
  time_zone   = "Europe/Madrid"
  paused      = true
  attempt_deadline = "1500s"

  retry_config {
    retry_count = 1
  }

  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions2_function.trueflag-etl.service_config[0].uri

    oidc_token {
      service_account_email = var.service_account
    }
  }
  depends_on = [google_project_service.required_apis]
}
