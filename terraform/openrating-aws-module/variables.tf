variable "aws_region" { type = string }
variable "project_name" { type = string  default = "openrating" }
variable "env" { type = string  default = "prod" }
variable "tags" { type = map(string) default = {} }
variable "vpc_cidr" { type = string default = "10.40.0.0/16" }
variable "rds_engine_version" { type = string default = "8.0.mysql_aurora.3.06.0" }
variable "db_name" { type = string default = "ratings" }
variable "db_username" { type = string default = "ratings_admin" }
variable "db_password" { type = string } # recommend Secrets Manager
variable "rds_min_acu" { type = number default = 0.5 }
variable "rds_max_acu" { type = number default = 4 }
variable "container_image" { type = string }
variable "container_port"  { type = number default = 8080 }
variable "container_cpu"   { type = number default = 512 }
variable "container_memory"{ type = number default = 1024 }
variable "desired_count"   { type = number default = 2 }
variable "enable_api_gateway" { type = bool default = true }
variable "enable_cognito" { type = bool default = true }
