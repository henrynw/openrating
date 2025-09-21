output "api_invoke_url" { value = var.enable_api_gateway ? aws_apigatewayv2_api.http[0].api_endpoint : null }
output "db_endpoint" { value = aws_rds_cluster.this.endpoint }
output "cluster_name" { value = aws_ecs_cluster.this.name }
output "ecs_service_name" { value = aws_ecs_service.api.name }
output "ledger_bucket" { value = aws_s3_bucket.ledger.bucket }
output "vpc_id" { value = aws_vpc.this.id }
output "private_subnet_ids" { value = [for s in aws_subnet.private : s.id] }
