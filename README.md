# OpenRating
Open infrastructure for sport ratings and rankings. Open source. Federation‑friendly. BWF will be our first production adopter.

## Quickstart
### Run locally (Node 20+)
```bash
# 1. Start Postgres (example uses Docker; adjust as needed)
docker run --name openrating-db \
  -e POSTGRES_PASSWORD=openrating \
  -e POSTGRES_USER=openrating \
  -e POSTGRES_DB=openrating \
  -p 5432:5432 -d postgres:16

# 2. Install deps & configure env
cd service/ts
npm install
cat > .env <<'EOF'
DATABASE_URL=postgres://openrating:openrating@localhost:5432/openrating
PORT=8080
EOF

# 3. Apply database migrations
npm run db:migrate

# 4. Run the API
npm run dev
# http://localhost:8080/health
```

> Tip: if `DATABASE_URL` is omitted the service falls back to the in-memory store (handy for demos, no persistence).

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
