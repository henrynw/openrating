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

# 5. (optional) Run the insights worker in another terminal to build player
# insight snapshots and keep them fresh
npm run insights:worker

# 6. (optional) Run the AI narrative worker to generate on-demand summaries
npm run ai-insights:worker
```

> Tip: if `DATABASE_URL` is omitted the service falls back to the in-memory store (handy for demos, no persistence).

## Player profile photos

Cloudflare Images is the default integration for serving profile photos. The API remains optional—if you skip the environment variables below, the photo routes stay disabled and existing clients continue to work without images.

Set these environment variables on every service that issues upload URLs (`web`, `worker`, `cron`):

- `CF_IMAGES_ACCOUNT_ID` – Cloudflare account ID.
- `CF_IMAGES_ACCOUNT_HASH` – Images delivery hash (found in the Images dashboard).
- `CF_IMAGES_API_TOKEN` – API token with the *Cloudflare Images: Edit* permission.
- *(optional)* `CF_IMAGES_DEFAULT_VARIANT` – Variant name to use when returning `profile_photo_url` (defaults to `public`).
- *(optional)* `CF_IMAGES_VARIANT_ALIASES` – Comma-separated alias map (e.g. `default:avatarcard,thumb:avatarthumb`) so you can keep friendly keys while Cloudflare stores the actual variant names.

> Render blueprint users can store these as secrets named `cf-images-account-id`, `cf-images-account-hash`, and `cf-images-api-token`.

Flow:

1. `POST /v1/players/{player_id}/profile-photo/upload` – returns a Cloudflare direct-upload URL plus the provisional `image_id`.
2. The client uploads the binary to that URL.
3. `POST /v1/players/{player_id}/profile-photo/finalize` – the API confirms Cloudflare finished processing, stores the `image_id`, and returns the public URLs/variants.
4. `DELETE /v1/players/{player_id}/profile-photo` removes the image and clears the player metadata.

You control the variants (resize, format, etc.) inside Cloudflare Images—define them once in the dashboard and reference the names via `CF_IMAGES_DEFAULT_VARIANT`/`CF_IMAGES_VARIANT_ALIASES`. The default blueprint expects two variants: `avatarcard` (≈256×256) for detail views and `avatarthumb` (≈96×96) for leaderboards.

### Deploy on Render (blueprint)
1. [Create a Render account](https://render.com) and connect this repo.
2. Accept the detected `render.yaml` blueprint — it provisions:
   - `openrating-api` (web) with auto deploy and automated migrations.
   - `openrating-insights-worker` (worker) that continuously refreshes player insights.
   - `openrating-ai-insights-worker` (worker) that generates AI narratives on demand (requires `OPENAI_API_KEY`).
   - `openrating-db` (Postgres).
3. Render runs the web build `npm install && npm run build && npm run db:migrate` and each worker build `npm install && npm run build`. All services auto-deploy on each push (toggle `autoDeploy` in Render if you prefer manual approvals).
4. Verify the API at `/health`, confirm the insights worker is processing jobs, and point the AI worker at a valid `OPENAI_API_KEY` before setting `include_ai=1` in clients.

> Tip: edit `render.yaml` if you need different plans, regions, or environment settings.

### Auth0 integration
- Define an API in Auth0 with identifier `https://api.openrating.app` (or your own audience).
- Create machine-to-machine apps for providers and assign scopes like `matches:write`.
- Add these environment variables to the service (Render dashboard or `.env`):
  - `AUTH0_DOMAIN=your-tenant.us.auth0.com`
  - `AUTH0_AUDIENCE=https://api.openrating.app`
  - `AUTH_PROVIDER=AUTH0`
  - Leave `AUTH_DISABLE` unset in production; set `AUTH_DISABLE=1` locally to bypass auth.
- Seed `subjects` and `subject_grants` tables with the org/sport permissions your callers need.  The API auto-creates a `subjects` row the first time a new Auth0 `sub` appears.
- Using the hosted OpenRating API? Request client credentials via the [contact form](https://www.openrating.app/contact) and we’ll provision access for you.

### Dev token minting (skip Auth0)
- For local/dev environments you can swap to a shared-secret signer by setting `AUTH_PROVIDER=DEV` and supplying `AUTH_DEV_SHARED_SECRET` (optionally `AUTH_DEV_AUDIENCE`/`AUTH_DEV_ISSUER`). The API will validate HS256 bearer tokens using those values.
- Generate compatible tokens with `npm run token:mint:dev -- --subject dev-client --scope "matches:write ratings:read"`. The script prints the token plus its claims so you can copy it into tooling or an `Authorization: Bearer` header.
- The CLI uses the same environment variables, so keep `AUTH_DEV_SHARED_SECRET` consistent wherever you mint tokens. Rotate the secret when you need to invalidate previously issued dev tokens.

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

### Background insights job queue
- `/v1/players/{player_id}/insights` serves precomputed snapshots backed by the `player_insights` table and honours `If-None-Match` headers.
- Run `npm run insights:worker` (locally or in production) to process `player_insight_jobs`, rebuild snapshots, and write them back atomically.
- Tweak throughput with env vars: `INSIGHTS_WORKER_POLL_MS` (idle poll delay, default `1000`), `INSIGHTS_WORKER_BATCH_SIZE` (jobs claimed per poll, default `25`), `INSIGHTS_WORKER_LOOP_COOLDOWN_MS` (sleep after each batch, default `25`), and `INSIGHTS_WORKER_LOG_EVERY` (print a progress line every N jobs, default `250`).
- Enqueue refreshes by calling `store.enqueuePlayerInsightsRefresh({ organizationId, playerId, sport, discipline })` wherever you ingest matches or rating changes. Jobs are deduped by scope, and multiple workers can run concurrently.
- For backfills, enqueue jobs for every player (or wipe the table) and let the worker regenerate snapshots; it’s safe to replay history.
- To pause processing, stop the worker. Pending jobs remain in the queue until the worker resumes.

### AI insight narratives
- Set `OPENAI_API_KEY` anywhere you run `npm run ai-insights:worker` (and on the API service in production). Optional overrides: `AI_INSIGHTS_MODEL` (defaults to `gpt-4o-mini`), `AI_INSIGHTS_TEMPERATURE`, `AI_INSIGHTS_MAX_OUTPUT_TOKENS`, and `AI_INSIGHTS_TTL_HOURS`.
- The AI worker consumes `player_insight_ai_jobs`, reads the latest snapshot, calls the OpenAI Responses API, and stores the narrative with token accounting and expiry metadata.
- Clients request a narrative with `/v1/players/{player_id}/insights?include_ai=1`. The API immediately returns the numeric snapshot; if the narrative is missing it enqueues a job and responds with `ai.status = "pending"` plus polling hints.
- Narratives are cached by snapshot digest—when the underlying stats change, the next `include_ai=1` call triggers a fresh job while existing cache entries keep serving until expiry.

### Rating replay & backfills
- Match ingestion records the current ladder state. If you later insert a match with a `start_time` earlier than what has already been processed, the ladder is marked dirty and a replay entry is queued automatically.
- Run `npm run replay -- queue` to process queued ladders. The command re-sorts matches by `start_time`, truncates rating history, and rebuilds player ratings + pair synergies inside a transaction. Use `--dry-run` to preview the work without mutating state. Any players touched during the replay automatically receive fresh insight jobs so the worker catches their new rating history.
- To rebuild a specific ladder on demand (for example after bulk edits), run `npm run replay -- ladder BADMINTON:SINGLES`. You can optionally pass `--from <ISO timestamp>` to hint at the earliest affected match and `--dry-run` to inspect the plan first.
- The CLI works in both Postgres and in-memory modes; in-memory runs are useful for local validation. Replays clear the corresponding queue entry on success so repeated runs are idempotent.

### Listing data
- `POST /v1/organizations`: create a new organization (returns the canonical UUID).
- `GET /v1/organizations`: search/paginate organizations by name or slug.
- `GET /v1/players`: paginated list filtered by `organization_id` or `organization_slug`, optional `q`, `limit`, and `cursor`. Returns `next_cursor` for continue tokens.
- `GET /v1/matches`: paginated list filtered by `organization_id` or `organization_slug` with optional `sport`, `player_id`, `event_id`, `start_after`, `start_before`, `cursor`, and `limit`.
- `GET /v1/organizations/{slug}/leaderboard`: read-optimized leaderboard for the current ladder, including rank, ratings, and latest deltas (requires `ratings:read`, `matches:read`, or `matches:write`). Supports cursor pagination via `limit` + `cursor` and returns `total_players`, `total_pages`, and `next_cursor` metadata. Tier and region filters are optional; omit them to aggregate across all ladders. Players without any rated matches are automatically excluded—if you upgraded from an older build, run `npm run leaderboard:backfill` (with `DATABASE_URL` set) to resync historical `matches_count` values. Additional filters are available for `sex`, `country_code`, `region_id`, and age buckets (`age_group` plus `age_from`/`age_to` with an optional `age_cutoff`). Players missing birth data are omitted whenever an age filter is applied.
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
