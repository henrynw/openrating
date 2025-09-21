terraform {
  required_version = ">= 1.6.0"
  required_providers { aws = { source = "hashicorp/aws", version = ">= 5.30.0" } }
}
provider "aws" { region = var.aws_region }
locals { name_prefix = "${var.project_name}-${var.env}"; tags = merge({ Project = var.project_name, Env = var.env }, var.tags) }
resource "aws_vpc" "this" { cidr_block = var.vpc_cidr  enable_dns_support=true enable_dns_hostnames=true tags = merge(local.tags,{Name="${local.name_prefix}-vpc"}) }
data "aws_availability_zones" "available" {}
resource "aws_subnet" "public" { for_each={ for idx, az in slice(data.aws_availability_zones.available.names,0,2): idx=>az }
  vpc_id=aws_vpc.this.id cidr_block=cidrsubnet(var.vpc_cidr,4,each.key) availability_zone=each.value map_public_ip_on_launch=true
  tags = merge(local.tags,{Name="${local.name_prefix}-public-${each.value}",Tier="public"}) }
resource "aws_subnet" "private" { for_each={ for idx, az in slice(data.aws_availability_zones.available.names,0,2): idx=>az }
  vpc_id=aws_vpc.this.id cidr_block=cidrsubnet(var.vpc_cidr,4,each.key+8) availability_zone=each.value
  tags = merge(local.tags,{Name="${local.name_prefix}-private-${each.value}",Tier="private"}) }
resource "aws_internet_gateway" "igw" { vpc_id=aws_vpc.this.id tags=merge(local.tags,{Name="${local.name_prefix}-igw"}) }
resource "aws_eip" "nat" { count=1 domain="vpc" tags=merge(local.tags,{Name="${local.name_prefix}-nat-eip"}) }
resource "aws_nat_gateway" "nat" { allocation_id=aws_eip.nat[0].id subnet_id=values(aws_subnet.public)[0].id depends_on=[aws_internet_gateway.igw] tags=merge(local.tags,{Name="${local.name_prefix}-nat"}) }
resource "aws_route_table" "public" { vpc_id=aws_vpc.this.id tags=merge(local.tags,{Name="${local.name_prefix}-public-rt"}) }
resource "aws_route" "public_internet" { route_table_id=aws_route_table.public.id destination_cidr_block="0.0.0.0/0" gateway_id=aws_internet_gateway.igw.id }
resource "aws_route_table_association" "public_assoc" { for_each=aws_subnet.public subnet_id=each.value.id route_table_id=aws_route_table.public.id }
resource "aws_route_table" "private" { vpc_id=aws_vpc.this.id tags=merge(local.tags,{Name="${local.name_prefix}-private-rt"}) }
resource "aws_route" "private_nat" { route_table_id=aws_route_table.private.id destination_cidr_block="0.0.0.0/0" nat_gateway_id=aws_nat_gateway.nat.id }
resource "aws_route_table_association" "private_assoc" { for_each=aws_subnet.private subnet_id=each.value.id route_table_id=aws_route_table.private.id }
resource "random_id" "suffix" { byte_length=3 }
resource "aws_kms_key" "s3" { description="${local.name_prefix} s3 cmk" deletion_window_in_days=7 enable_key_rotation=true tags=local.tags }
resource "aws_s3_bucket" "ledger" { bucket="${local.name_prefix}-ledger-${random_id.suffix.hex}" force_destroy=false tags=merge(local.tags,{Name="${local.name_prefix}-ledger"}) }
resource "aws_s3_bucket_versioning" "ledger" { bucket=aws_s3_bucket.ledger.id versioning_configuration { status="Enabled" } }
resource "aws_s3_bucket_server_side_encryption_configuration" "ledger" { bucket=aws_s3_bucket.ledger.id rule { apply_server_side_encryption_by_default { sse_algorithm="aws:kms" kms_master_key_id=aws_kms_key.s3.arn } } }
resource "aws_s3_bucket_lifecycle_configuration" "ledger" { bucket=aws_s3_bucket.ledger.id rule { id="retain-forever" status="Enabled" noncurrent_version_expiration { noncurrent_days=36500 } } }
resource "aws_kms_key" "rds" { description="${local.name_prefix} rds cmk" deletion_window_in_days=7 enable_key_rotation=true tags=local.tags }
resource "aws_db_subnet_group" "this" { name="${local.name_prefix}-db-subnets" subnet_ids=[for s in aws_subnet.private: s.id] tags=local.tags }
resource "aws_rds_cluster" "this" {
  engine="aurora-mysql" engine_version=var.rds_engine_version database_name=var.db_name master_username=var.db_username master_password=var.db_password
  db_subnet_group_name=aws_db_subnet_group.this.name storage_encrypted=true kms_key_id=aws_kms_key.rds.arn backup_retention_period=7 preferred_backup_window="03:00-04:00"
  deletion_protection=true copy_tags_to_snapshot=true
  serverlessv2_scaling_configuration { min_capacity=var.rds_min_acu max_capacity=var.rds_max_acu }
  tags = merge(local.tags,{Name="${local.name_prefix}-aurora-cluster"})
}
resource "aws_rds_cluster_instance" "this" {
  count=2 identifier="${local.name_prefix}-aurora-${count.index}" cluster_identifier=aws_rds_cluster.this.id instance_class="db.serverless"
  engine=aws_rds_cluster.this.engine engine_version=aws_rds_cluster.this.engine_version publicly_accessible=false db_subnet_group_name=aws_db_subnet_group.this.name tags=local.tags
}
resource "aws_ecs_cluster" "this" { name="${local.name_prefix}-ecs" setting { name="containerInsights" value="enabled" } tags=local.tags }
data "aws_iam_policy_document" "ecs_tasks_assume" { statement { actions=["sts:AssumeRole"] principals { type="Service" identifiers=["ecs-tasks.amazonaws.com"] } } }
resource "aws_iam_role" "task_execution" { name="${local.name_prefix}-ecs-task-exec" assume_role_policy=data.aws_iam_policy_document.ecs_tasks_assume.json tags=local.tags }
resource "aws_iam_role_policy_attachment" "task_execution" { role=aws_iam_role.task_execution.name policy_arn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy" }
resource "aws_cloudwatch_log_group" "api" { name="/${local.name_prefix}/api" retention_in_days=30 tags=local.tags }
resource "aws_ecs_task_definition" "api" {
  family="${local.name_prefix}-api" network_mode="awsvpc" requires_compatibilities=["FARGATE"] cpu=var.container_cpu memory=var.container_memory
  execution_role_arn=aws_iam_role.task_execution.arn task_role_arn=aws_iam_role.task_execution.arn
  container_definitions = jsonencode([{
    name="openrating-api", image=var.container_image, essential=true,
    portMappings=[{containerPort=var.container_port, protocol="tcp"}],
    environment=[
      {name="DB_HOST", value=aws_rds_cluster.this.endpoint},
      {name="DB_NAME", value=var.db_name},
      {name="DB_USER", value=var.db_username},
      {name="DB_PASS", value=var.db_password},
      {name="AWS_REGION", value=var.aws_region},
      {name="LEDGER_BUCKET", value=aws_s3_bucket.ledger.bucket}
    ],
    logConfiguration={ logDriver="awslogs", options={
      "awslogs-group"=aws_cloudwatch_log_group.api.name, "awslogs-region"=var.aws_region, "awslogs-stream-prefix"="ecs"
    }},
    healthCheck={ command=["CMD-SHELL","curl -f http://localhost:${var.container_port}/health || exit 1"], interval=30, timeout=5, retries=3, startPeriod=30 }
  }])
  runtime_platform { operating_system_family="LINUX" cpu_architecture="X86_64" }
  tags=local.tags
}
resource "aws_security_group" "alb" { name="${local.name_prefix}-alb-sg" vpc_id=aws_vpc.this.id
  ingress { from_port=80 to_port=80 protocol="tcp" cidr_blocks=["0.0.0.0/0"] }
  egress { from_port=0 to_port=0 protocol="-1" cidr_blocks=["0.0.0.0/0"] }
  tags=local.tags
}
resource "aws_security_group" "service" { name="${local.name_prefix}-svc-sg" vpc_id=aws_vpc.this.id
  ingress { from_port=var.container_port to_port=var.container_port protocol="tcp" security_groups=[aws_security_group.alb.id] }
  egress  { from_port=0 to_port=0 protocol="-1" cidr_blocks=["0.0.0.0/0"] }
  tags=local.tags
}
resource "aws_lb" "internal" { name=substr("${local.name_prefix}-alb",0,32) internal=true load_balancer_type="application" security_groups=[aws_security_group.alb.id] subnets=[for s in aws_subnet.private: s.id] tags=local.tags }
resource "aws_lb_target_group" "api" { name=substr("${local.name_prefix}-tg",0,32) port=var.container_port protocol="HTTP" vpc_id=aws_vpc.this.id target_type="ip"
  health_check { path="/health" interval=30 healthy_threshold=2 unhealthy_threshold=3 timeout=5 matcher="200-399" } tags=local.tags }
resource "aws_lb_listener" "http" { load_balancer_arn=aws_lb.internal.arn port=80 protocol="HTTP" default_action { type="forward" target_group_arn=aws_lb_target_group.api.arn } }
resource "aws_ecs_service" "api" {
  name="${local.name_prefix}-api" cluster=aws_ecs_cluster.this.id task_definition=aws_ecs_task_definition.api.arn desired_count=var.desired_count launch_type="FARGATE"
  network_configuration { subnets=[for s in aws_subnet.private: s.id] security_groups=[aws_security_group.service.id] assign_public_ip=false }
  load_balancer { target_group_arn=aws_lb_target_group.api.arn container_name="openrating-api" container_port=var.container_port }
  deployment_minimum_healthy_percent=50 deployment_maximum_percent=200 force_new_deployment=true tags=local.tags
}
resource "aws_apigatewayv2_vpc_link" "this" { count=var.enable_api_gateway?1:0 name="${local.name_prefix}-vpclink" subnet_ids=[for s in aws_subnet.private: s.id] security_group_ids=[aws_security_group.alb.id] tags=local.tags }
resource "aws_cognito_user_pool" "this" { count=var.enable_cognito?1:0 name="${local.name_prefix}-users" tags=local.tags }
resource "aws_cognito_user_pool_client" "this" {
  count=var.enable_cognito?1:0 name="${local.name_prefix}-appclient" user_pool_id=aws_cognito_user_pool.this[0].id generate_secret=true
  allowed_oauth_flows_user_pool_client=true allowed_oauth_flows=["client_credentials"] allowed_oauth_scopes=["aws.cognito.signin.user.admin","email","openid","profile"]
  explicit_auth_flows=["ALLOW_ADMIN_NO_SRP_AUTH","ALLOW_REFRESH_TOKEN_AUTH"] prevent_user_existence_errors="ENABLED" supported_identity_providers=["COGNITO"]
}
resource "aws_apigatewayv2_api" "http" { count=var.enable_api_gateway?1:0 name="${local.name_prefix}-api" protocol_type="HTTP" tags=local.tags }
resource "aws_apigatewayv2_authorizer" "jwt" {
  count=var.enable_api_gateway && var.enable_cognito ? 1 : 0
  api_id=aws_apigatewayv2_api.http[0].id authorizer_type="JWT" identity_sources=["$request.header.Authorization"] name="${local.name_prefix}-jwt"
  jwt_configuration { audience=[aws_cognito_user_pool_client.this[0].id] issuer="https://${aws_cognito_user_pool.this[0].endpoint}" }
}
resource "aws_apigatewayv2_integration" "alb" {
  count=var.enable_api_gateway?1:0 api_id=aws_apigatewayv2_api.http[0].id integration_type="HTTP_PROXY" integration_method="ANY"
  connection_type="VPC_LINK" connection_id=aws_apigatewayv2_vpc_link.this[0].id integration_uri=aws_lb_listener.http.arn payload_format_version="1.0" timeout_milliseconds=29000
}
resource "aws_apigatewayv2_route" "proxy" {
  count=var.enable_api_gateway?1:0 api_id=aws_apigatewayv2_api.http[0].id route_key="ANY /{proxy+}"
  target="integrations/${aws_apigatewayv2_integration.alb[0].id}" authorizer_id=var.enable_cognito ? aws_apigatewayv2_authorizer.jwt[0].id : null
  authorization_type=var.enable_cognito ? "JWT" : "NONE"
}
resource "aws_apigatewayv2_stage" "prod" { count=var.enable_api_gateway?1:0 api_id=aws_apigatewayv2_api.http[0].id name="$default" auto_deploy=true tags=local.tags }
resource "aws_sqs_queue" "ingest" { name="${local.name_prefix}-ingest" message_retention_seconds=1209600 visibility_timeout_seconds=60 tags=local.tags }
resource "aws_eventbridge_bus" "this" { name="${local.name_prefix}-bus" tags=local.tags }
output "api_invoke_url" { value = var.enable_api_gateway ? aws_apigatewayv2_api.http[0].api_endpoint : null }
output "db_endpoint" { value = aws_rds_cluster.this.endpoint }
output "cluster_name" { value = aws_ecs_cluster.this.name }
output "ecs_service_name" { value = aws_ecs_service.api.name }
output "ledger_bucket" { value = aws_s3_bucket.ledger.bucket }
output "vpc_id" { value = aws_vpc.this.id }
output "private_subnet_ids" { value = [for s in aws_subnet.private : s.id] }
