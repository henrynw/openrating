# OpenRating Docs

Welcome to OpenRating — the open infrastructure for transparent sports ratings.

## Competition Taxonomy

OpenRating exposes a structured taxonomy on every competition payload so downstream consumers can reliably segment events without reverse-engineering provider-specific strings.

- `classification.segment` captures the headline marketing bucket (`STANDARD`, `PARA`, `JUNIOR`, `MASTERS`, `COLLEGIATE`, `EXHIBITION`, `OTHER`).
- `classification.profile` breaks the segment into orthogonal axes:
  - `participation`: `STANDARD`, `PARA`, `MIXED`, `UNKNOWN`.
  - `age_bracket`: `OPEN`, `JUNIOR`, `MASTERS`, `UNKNOWN`.
  - `skill_level`: `PROFESSIONAL`, `AMATEUR`, `COLLEGIATE`, `RECREATIONAL`, `UNKNOWN`.
- `classification.class_code` holds the primary division code when a single value applies; `classification.class_codes` preserves additional division codes (e.g. multiple para classes or weight bands).

### Canonical division codes (Badminton)

| Code | Notes |
| ---- | ----- |
| SL3 | Standing lower-limb players; half court. |
| SL4 | Standing lower-limb players; full court. |
| SU5 | Standing upper-limb players; full court. |
| WH1 | Wheelchair users with limited trunk control. |
| WH2 | Wheelchair users with normal trunk control. |
| SS6 | Short stature classification. |

When new feeds introduce additional codes, add them to this table and propagate through the registry that drives payload validation.
