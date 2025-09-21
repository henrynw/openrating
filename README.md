# OpenRating
Open infrastructure for sport ratings and rankings. Open source. Federation‑friendly. BWF will be our first production adopter.

## Quickstart
### Run locally (Node 20+)
```bash
cd service/ts
cp .env.example .env
npm i
npm run dev
# http://localhost:8080/health
```

### Deploy on AWS (Terraform)
```bash
cd terraform/example-usage
terraform init
terraform apply
```
Provide `aws_region`, `container_image`, and `db_password` (or wire Secrets Manager).

## Project layout
```
openapi/        # OpenAPI spec (API-first)
service/ts/     # Reference API (Express + Zod) — swap for Go if preferred
sdk/ts/         # Minimal TypeScript client
terraform/      # Terraform module + example usage
docs/           # MkDocs site (optional)
.github/        # CI workflows & issue templates
```
License: Apache-2.0
