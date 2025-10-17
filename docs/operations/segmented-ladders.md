# Segmented ladders (PARA / Junior / Masters)

The ladder identifier already encodes segment + class code information (see
`buildLadderId`), but older database snapshots still had a unique index on
`rating_ladders (sport, discipline)`. That index prevented us from creating
additional ladders such as `BADMINTON:SINGLES:segment=PARA:class=WH1`.

Run `npm run db:migrate` to apply the migration in `0022_allow_segmented_ladders.sql`.
The migration simply drops the legacy unique index so `ensureLadder` can create as
many specialized ladders as needed.

Once migrated you can seed individual ladders cheaply by hitting the leaderboard
endpoint with the appropriate query parameters, for example:

```bash
TOKEN=$(curl -s https://auth.openrating.app/oauth2/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=client_credentials' \
  -d "client_id=$YOUR_CLIENT_ID" \
  -d "client_secret=$YOUR_CLIENT_SECRET" \
  -d 'scope=openrating/ratings:read openrating/matches:read openrating/organizations:read openrating/players:read' \
  | jq -r '.access_token')

curl -s -H "Authorization: Bearer $TOKEN" \
  'https://api.openrating.app/v1/ratings/BADMINTON/SINGLES?segment=PARA&class_codes=WH1&limit=1'
```

Follow up with the normal replay/backfill workflow so para / junior / masters
results migrate into their segmented ladders:

```bash
npm run replay -- ladder 'BADMINTON:SINGLES:segment=PARA:class=WH1'
```

After the replay, the player listings fetched with `segment=PARA` (and optional
`class_codes`) will start returning data for the new ladders.
