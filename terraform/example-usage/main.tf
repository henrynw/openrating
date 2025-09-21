terraform {
  required_version = ">= 1.6.0"
  required_providers { aws = { source = "hashicorp/aws", version = ">= 5.30.0" } }
}
provider "aws" { region = var.aws_region }
variable "aws_region" { type = string }
variable "env" { type = string default = "prod" }
variable "container_image" { type = string }
variable "db_password" { type = string }

module "openrating" {
  source = "../openrating-aws-module"
  aws_region     = var.aws_region
  project_name   = "openrating"
  env            = var.env
  container_image = var.container_image
  db_password     = var.db_password
}

output "api_invoke_url" { value = module.openrating.api_invoke_url }
output "db_endpoint"   { value = module.openrating.db_endpoint }
output "ledger_bucket" { value = module.openrating.ledger_bucket }
