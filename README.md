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
# Auth (optional – leave unset to run with auth disabled)
# AUTH0_DOMAIN=your-tenant.us.auth0.com
# AUTH0_AUDIENCE=https://api.openrating.app
# AUTH_PROVIDER=AUTH0
# AUTH_DISABLE=1
EOF

# 3. Apply database migrations
npm run db:migrate

# 4. Run the API
npm run dev
# http://localhost:8080/health
```

> Tip: if `DATABASE_URL` is omitted the service falls back to the in-memory store (handy for demos, no persistence).

### Deploy on Render (blueprint)
1. [Create a Render account](https://render.com) and connect this repo.
2. Accept the detected `render.yaml` blueprint — it provisions a Node web service (`openrating-api`) and a managed Postgres instance (`openrating-db`).
3. Render runs `npm install`, `npm run build`, executes migrations (`npm run db:migrate`), then starts the API.
4. Visit the generated URL; `/health` should respond with `{ ok: true }`.

> Tip: edit `render.yaml` if you need a different plan, region, or environment settings.

### Auth0 integration
- Define an API in Auth0 with identifier `https://api.openrating.app` (or your own audience).
- Create machine-to-machine apps for providers and assign scopes like `matches:write`.
- Add these environment variables to the service (Render dashboard or `.env`):
  - `AUTH0_DOMAIN=your-tenant.us.auth0.com`
  - `AUTH0_AUDIENCE=https://api.openrating.app`
  - `AUTH_PROVIDER=AUTH0`
  - Leave `AUTH_DISABLE` unset in production; set `AUTH_DISABLE=1` locally to bypass auth.
- Seed `subjects` and `subject_grants` tables with the org/sport permissions your callers need.  The API auto-creates a `subjects` row the first time a new Auth0 `sub` appears.

### Grant management CLI
```bash
cd service/ts
# Add/ensure a grant (example for BWF)
npm run grants -- add \
  --subject bwf-provider \
  --name "BWF Provider" \
  --org BWF \
  --sport BADMINTON \
  --permission matches:write

# Use --region ANY (or "*") to allow all regions for the organization.
npm run grants -- add \
  --subject bwf-provider \
  --org BWF \
  --sport BADMINTON \
  --region ANY \
  --permission matches:write

# List grants
npm run grants -- list --subject bwf-provider
```

The CLI reads credentials from environment variables so provider secrets stay out of git.

### Listing data
- `POST /v1/organizations`: create a new organization (returns the canonical UUID).
- `GET /v1/organizations`: search/paginate organizations by name or slug.
- `GET /v1/players`: paginated list filtered by `organization_id` or `organization_slug`, optional `q`, `limit`, and `cursor`. Returns `next_cursor` for continue tokens.
- `GET /v1/matches`: paginated list filtered by `organization_id` or `organization_slug` with optional `sport`, `player_id`, `event_id`, `start_after`, `start_before`, `cursor`, and `limit`.
- `GET /v1/organizations/{slug}/leaderboard`: read-optimized leaderboard for the current ladder, including rank, ratings, and latest deltas (requires `ratings:read`, `matches:read`, or `matches:write`). Tier and region filters are optional; omit them to aggregate across all ladders.
- `GET /v1/organizations/{slug}/leaderboard/movers`: highlights players with the largest rating changes since a supplied timestamp (requires `ratings:read`, `matches:read`, or `matches:write`). Tier and region filters are optional.
- `POST /v1/events`: create a tournament/league container for matches (requires `matches:write`).
- `GET /v1/events`: list events for an organization with optional type filters and pagination (requires `matches:read`, `matches:write`, or `ratings:read`).
- `GET /v1/events/{event_id}` / `PATCH /v1/events/{event_id}`: fetch or update event metadata.
- `GET /v1/events/{event_id}/participants` & `POST /v1/events/{event_id}/participants`: view or ensure event participants. Matches linked to an event automatically enroll the players.

Players must be registered in advance—match submissions referencing unknown or cross-organization player IDs return `invalid_players`.


### Player lifecycle
- Create organizations first via `POST /v1/organizations` to receive the canonical `organization_id` (UUID). Use `GET /v1/organizations` to search by name/slug.
- All match and player requests now accept either `organization_id` or `organization_slug`; the API resolves slugs to IDs internally.
- Register players via `POST /v1/players` before submitting matches. The response returns the `player_id` you must use in match payloads.
- Each player belongs to a single organization; posting a match with a player from another organization results in `invalid_players`.
- The API no longer auto-creates placeholder players—unknown IDs will be rejected.

## Project layout
```
openapi/        # OpenAPI spec (API-first)
service/ts/     # Reference API (Express + Zod) — swap for Go if preferred
sdk/ts/         # Minimal TypeScript client
docs/           # MkDocs site (optional)
.github/        # CI workflows & issue templates
```
License: Apache-2.0
