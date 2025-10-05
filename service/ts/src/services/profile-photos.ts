const DEFAULT_VARIANT = 'public';

export interface DirectUploadResult {
  imageId: string;
  uploadUrl: string;
  expiresAt: string | null;
}

export interface ProfilePhotoFinalizeResult {
  imageId: string;
  uploadedAt: string | null;
  variants: Record<string, string>;
}

export class ProfilePhotoServiceDisabledError extends Error {
  constructor(message = 'Profile photo service is disabled') {
    super(message);
    this.name = 'ProfilePhotoServiceDisabledError';
  }
}

export class ProfilePhotoNotReadyError extends Error {
  constructor(message = 'Profile photo is not yet ready') {
    super(message);
    this.name = 'ProfilePhotoNotReadyError';
  }
}

export class CloudflareImagesError extends Error {
  constructor(message: string, readonly status?: number) {
    super(message);
    this.name = 'CloudflareImagesError';
  }
}

export class ProfilePhotoService {
  private readonly enabled: boolean;
  private readonly accountId?: string;
  private readonly apiToken?: string;
  private readonly accountHash?: string;
  private readonly defaultVariant: string;
  private readonly variantAliases: Record<string, string>;

  constructor(options: {
    accountId?: string;
    apiToken?: string;
    accountHash?: string;
    defaultVariant?: string;
    variantAliases?: Record<string, string>;
  } = {}) {
    this.accountId = options.accountId ?? process.env.CF_IMAGES_ACCOUNT_ID;
    this.apiToken = options.apiToken ?? process.env.CF_IMAGES_API_TOKEN;
    this.accountHash = options.accountHash ?? process.env.CF_IMAGES_ACCOUNT_HASH;
    this.defaultVariant = options.defaultVariant ?? process.env.CF_IMAGES_DEFAULT_VARIANT ?? DEFAULT_VARIANT;
    this.variantAliases = options.variantAliases ?? this.parseVariantAliases(process.env.CF_IMAGES_VARIANT_ALIASES);

    this.enabled = Boolean(this.accountId && this.apiToken && this.accountHash);
  }

  isEnabled() {
    return this.enabled;
  }

  getPublicUrl(imageId?: string | null, variant?: string | null) {
    if (!imageId || !this.accountHash) return null;
    const variantName = variant
      ? this.variantAliases[variant] ?? variant
      : this.defaultVariant;
    return `https://imagedelivery.net/${this.accountHash}/${imageId}/${variantName}`;
  }

  async createDirectUpload(params: {
    organizationId: string;
    playerId: string;
    contentType?: string;
    requireSignedUrl?: boolean;
  }): Promise<DirectUploadResult> {
    if (!this.enabled) {
      throw new ProfilePhotoServiceDisabledError();
    }

    const body = {
      requireSignedURLs: Boolean(params.requireSignedUrl ?? false),
      metadata: {
        organizationId: params.organizationId,
        playerId: params.playerId,
        ...(params.contentType ? { contentType: params.contentType } : {}),
      },
    } satisfies Record<string, unknown>;

    const response = await this.request('/images/v2/direct_upload', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    const { result } = await response.json();

    return {
      imageId: result.id,
      uploadUrl: result.uploadURL,
      expiresAt: result.expiration ? new Date(result.expiration).toISOString() : null,
    } satisfies DirectUploadResult;
  }

  async finalize(imageId: string): Promise<ProfilePhotoFinalizeResult> {
    if (!this.enabled) {
      throw new ProfilePhotoServiceDisabledError();
    }

    const response = await this.request(`/images/v1/${encodeURIComponent(imageId)}`, {
      method: 'GET',
    });

    const { result } = await response.json();

    if (!result) {
      throw new CloudflareImagesError('Profile photo lookup failed');
    }

    if (result.status !== 'active') {
      throw new ProfilePhotoNotReadyError();
    }

    const variants = this.buildVariantMap(result.variants as string[] | undefined, imageId);

    return {
      imageId,
      uploadedAt: result.uploaded ? new Date(result.uploaded).toISOString() : null,
      variants,
    } satisfies ProfilePhotoFinalizeResult;
  }

  async delete(imageId: string | null | undefined) {
    if (!this.enabled) return;
    if (!imageId) return;

    await this.request(`/images/v1/${encodeURIComponent(imageId)}`, {
      method: 'DELETE',
    }, true);
  }

  private async request(path: string, init: RequestInit = {}, swallowNotFound = false) {
    if (!this.accountId || !this.apiToken) {
      throw new ProfilePhotoServiceDisabledError();
    }

    const url = `https://api.cloudflare.com/client/v4/accounts/${this.accountId}${path}`;
    const response = await globalThis.fetch(url, {
      ...init,
      headers: {
        Authorization: `Bearer ${this.apiToken}`,
        ...(init.headers || {}),
      },
    });

    if (swallowNotFound && response.status === 404) {
      return response;
    }

    if (!response.ok) {
      let message = `Cloudflare API error: ${response.status}`;
      try {
        const body = await response.json();
        if (body?.errors?.length) {
          message = body.errors.map((err: any) => err.message ?? message).join('; ');
        }
      } catch (err) {
        // ignore parse errors
      }
      throw new CloudflareImagesError(message, response.status);
    }

    return response;
  }

  private buildVariantMap(variants: string[] | undefined, imageId: string) {
    const map: Record<string, string> = {};

    if (!variants?.length) {
      const fallback = this.getPublicUrl(imageId);
      if (fallback) {
        map.default = fallback;
      }
      return map;
    }

    for (const variant of variants) {
      const name = this.extractVariantName(variant);
      if (name) {
        map[name] = variant;
      }
    }

    for (const [alias, variantName] of Object.entries(this.variantAliases)) {
      if (!map[alias] && map[variantName]) {
        map[alias] = map[variantName];
      }
    }

    if (!map.default) {
      const alias = this.variantAliases.default ?? this.defaultVariant;
      const aliasUrl = map[alias];
      if (aliasUrl) {
        map.default = aliasUrl;
      } else {
        const fallback = this.getPublicUrl(imageId);
        if (fallback) {
          map.default = fallback;
        }
      }
    }

    return map;
  }

  private extractVariantName(url: string) {
    try {
      const parsed = new URL(url);
      const parts = parsed.pathname.split('/').filter(Boolean);
      return parts.at(-1) ?? null;
    } catch (err) {
      return null;
    }
  }

  private parseVariantAliases(raw: string | undefined) {
    if (!raw) return {};
    return raw.split(',').reduce<Record<string, string>>((acc, pair) => {
      const [alias, variant] = pair.split(':').map((value) => value.trim()).filter(Boolean);
      if (alias && variant) {
        acc[alias] = variant;
      }
      return acc;
    }, {});
  }
}

export const profilePhotoService = new ProfilePhotoService();
