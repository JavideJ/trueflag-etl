variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "EU"
}

variable "bq_location" {
  type    = string
  default = "EU"          
}

variable "service_account" {
  type    = string        
}
