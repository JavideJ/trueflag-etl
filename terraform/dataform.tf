resource "google_dataform_repository" "trueflag_repo" {
  provider = google-beta
  project = var.project_id
  region  = var.region
  name    = "trueflag_repo"
}

# resource "google_dataform_repository_workspace" "trueflag_workspace" {
#   provider = google-beta
#   project      = var.project_id
#   region       = var.region
#   repository   = google_dataform_repository.trueflag_repo.name
#   workspace_id = "default_workspace"
# }