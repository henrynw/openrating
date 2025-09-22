ALTER TABLE organizations ADD COLUMN IF NOT EXISTS slug TEXT;
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS description TEXT;

UPDATE organizations
SET slug = lower(organization_id)
WHERE slug IS NULL OR slug = '';

ALTER TABLE organizations ADD CONSTRAINT organizations_slug_key UNIQUE (slug);

ALTER TABLE organizations ALTER COLUMN slug SET NOT NULL;
