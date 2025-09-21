#!/usr/bin/env bash
set -euo pipefail
BASE_URL=${BASE_URL:-http://localhost:8080}
curl -sS -X POST "$BASE_URL/v1/matches"   -H 'Content-Type: application/json'   -H "Idempotency-Key: test-1"   -d '{
    "provider_id":"demo",
    "organization_id":"EGY",
    "discipline":"SINGLES",
    "format":"BO3_21RALLY",
    "start_time":"2025-09-21T08:00:00Z",
    "sides":{"A":{"players":["p1"]},"B":{"players":["p2"]}},
    "games":[{"game_no":1,"a":21,"b":18},{"game_no":2,"a":21,"b":15}]
  }' | jq .
