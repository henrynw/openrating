-- Add external_ref field to matches table for provider-specific match identifiers
-- This enables idempotency: providers can submit the same match multiple times without creating duplicates

ALTER TABLE matches ADD COLUMN IF NOT EXISTS external_ref TEXT;

-- Create partial unique index on (provider_id, external_ref) where external_ref is not null
-- This ensures that each provider can only submit a match with a given external_ref once
CREATE UNIQUE INDEX IF NOT EXISTS matches_provider_ref_idx
  ON matches (provider_id, external_ref)
  WHERE external_ref IS NOT NULL;
