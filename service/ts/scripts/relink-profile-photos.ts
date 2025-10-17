#!/usr/bin/env tsx
import 'dotenv/config';
import { eq, and } from 'drizzle-orm';
import { getDb, getPool } from '../src/db/client.js';
import { players } from '../src/db/schema.js';

const accountId = process.env.CF_IMAGES_ACCOUNT_ID;
const apiToken = process.env.CF_IMAGES_API_TOKEN;

if (!accountId || !apiToken) {
  console.error('Cloudflare Images credentials missing. Set CF_IMAGES_ACCOUNT_ID and CF_IMAGES_API_TOKEN.');
  process.exit(1);
}

const dryRun = process.argv.includes('--dry-run');

interface CloudflareImage {
  id: string;
  uploaded?: string;
  meta?: Record<string, any>;
}

interface ImageLink {
  imageId: string;
  uploadedAt: string | null;
}

interface PlayerKey {
  organizationId: string;
  playerId: string;
}

const fetchImages = async (): Promise<Map<string, ImageLink>> => {
  const perPage = 100;
  let page = 1;
  const links = new Map<string, ImageLink>();

  while (true) {
    const url = new URL(`https://api.cloudflare.com/client/v4/accounts/${accountId}/images/v1`);
    url.searchParams.set('page', String(page));
    url.searchParams.set('per_page', String(perPage));
    url.searchParams.set('order', 'desc');
    url.searchParams.set('sort', 'created');

    const response = await fetch(url, {
      headers: { Authorization: `Bearer ${apiToken}` },
    });

    if (!response.ok) {
      const body = await response.text();
      throw new Error(`Cloudflare API error (${response.status}): ${body}`);
    }

    const payload = await response.json();
    const results = (payload.result ?? []) as CloudflareImage[];

    for (const image of results) {
      const meta = image.meta ?? {};
      const organizationId = meta.organizationId ?? meta.organisationId; // tolerate spelling variations
      const playerId = meta.playerId ?? meta.player_id;
      if (!organizationId || !playerId) continue;

      const key = `${organizationId}:${playerId}`;
      const uploadedAt = image.uploaded ?? null;

      const existing = links.get(key);
      if (!existing) {
        links.set(key, { imageId: image.id, uploadedAt });
        continue;
      }

      if (!uploadedAt) continue;
      if (!existing.uploadedAt || new Date(uploadedAt) > new Date(existing.uploadedAt)) {
        links.set(key, { imageId: image.id, uploadedAt });
      }
    }

    const info = payload.result_info ?? {};
    const totalPages = info.total_pages ?? info.totalPages ?? page;
    if (page >= totalPages) break;
    page += 1;
  }

  return links;
};

const main = async () => {
  const links = await fetchImages();
  console.log(`Fetched ${links.size} player->image mappings from Cloudflare.`);

  const db = getDb();
  const pool = getPool();

  let updated = 0;
  let skippedMissingPlayer = 0;
  let skippedUnchanged = 0;

  for (const [key, link] of links.entries()) {
    const [organizationId, playerId] = key.split(':');

    const rows = await db
      .select({ profilePhotoId: players.profilePhotoId, profilePhotoUploadedAt: players.profilePhotoUploadedAt })
      .from(players)
      .where(and(eq(players.organizationId, organizationId), eq(players.playerId, playerId)))
      .limit(1);

    const row = rows.at(0);
    if (!row) {
      skippedMissingPlayer += 1;
      continue;
    }

    const currentId = row.profilePhotoId ?? null;
    const currentUploadedAt = row.profilePhotoUploadedAt?.toISOString() ?? null;

    if (currentId === link.imageId && currentUploadedAt === link.uploadedAt) {
      skippedUnchanged += 1;
      continue;
    }

    if (dryRun) {
      console.log('[dry-run] would update', { organizationId, playerId, imageId: link.imageId, uploadedAt: link.uploadedAt });
      updated += 1;
      continue;
    }

    await db
      .update(players)
      .set({
        profilePhotoId: link.imageId,
        profilePhotoUploadedAt: link.uploadedAt ? new Date(link.uploadedAt) : row.profilePhotoUploadedAt ?? new Date(),
      })
      .where(and(eq(players.organizationId, organizationId), eq(players.playerId, playerId)));

    updated += 1;
  }

  if (!dryRun) {
    await pool.end();
  }

  console.log(
    JSON.stringify(
      {
        updated,
        skippedMissingPlayer,
        skippedUnchanged,
        dryRun,
      },
      null,
      2
    )
  );
};

main().catch((err) => {
  console.error('relink_profile_photos_failed', err);
  process.exitCode = 1;
});
