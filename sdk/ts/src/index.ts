export interface MatchSubmitSide {
  players: string[];
}

export interface MatchSubmitGame {
  game_no: number;
  a: number;
  b: number;
}

export type Discipline = 'SINGLES' | 'DOUBLES' | 'MIXED';
export type Tier = 'SANCTIONED' | 'LEAGUE' | 'SOCIAL' | 'EXHIBITION';

export interface MatchSubmit {
  provider_id: string;
  organization_id: string;
  discipline: Discipline;
  format: string;
  start_time: string;
  venue_region_id?: string;
  tier?: Tier;
  sides: { A: MatchSubmitSide; B: MatchSubmitSide };
  games: MatchSubmitGame[];
}

export interface HealthResponse {
  ok: boolean;
  version?: string;
}

export interface MatchSubmissionResponse {
  match_id: string;
  rating_status: 'QUEUED' | 'RATED' | 'PENDING';
}

export interface RetryPolicy {
  attempts?: number;
  backoffMs?: number;
  retryOnStatuses?: number[];
}

export interface OpenRatingClientOptions {
  baseUrl: string;
  token?: string;
  fetchImpl?: typeof fetch;
  retry?: RetryPolicy;
  defaultHeaders?: Record<string, string>;
}

export class OpenRatingError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly body: unknown
  ) {
    super(message);
    this.name = 'OpenRatingError';
  }
}

export class OpenRatingClient {
  private readonly baseUrl: URL;
  private readonly token?: string;
  private readonly fetchImpl: typeof fetch;
  private readonly retry: Required<RetryPolicy>;
  private readonly defaultHeaders: Record<string, string>;

  constructor(options: OpenRatingClientOptions) {
    this.baseUrl = new URL(options.baseUrl);
    this.token = options.token;
    this.fetchImpl = options.fetchImpl ?? globalThis.fetch?.bind(globalThis);
    if (!this.fetchImpl) {
      throw new Error('Global fetch implementation not found. Pass options.fetchImpl explicitly.');
    }

    this.retry = {
      attempts: Math.max(1, options.retry?.attempts ?? 1),
      backoffMs: Math.max(0, options.retry?.backoffMs ?? 250),
      retryOnStatuses: options.retry?.retryOnStatuses ?? [408, 429, 500, 502, 503, 504],
    };

    this.defaultHeaders = {
      'Content-Type': 'application/json',
      ...options.defaultHeaders,
    };
  }

  async health(): Promise<HealthResponse> {
    return this.request<HealthResponse>('/health', { method: 'GET' });
  }

  async submitMatch(match: MatchSubmit): Promise<MatchSubmissionResponse> {
    return this.request<MatchSubmissionResponse>('/v1/matches', {
      method: 'POST',
      body: JSON.stringify(match),
    });
  }

  private async request<T>(path: string, init: RequestInit): Promise<T> {
    const url = new URL(path, this.baseUrl);
    const headers = new Headers(this.defaultHeaders);
    if (init.headers) {
      new Headers(init.headers).forEach((value, key) => headers.set(key, value));
    }
    if (this.token) {
      headers.set('Authorization', `Bearer ${this.token}`);
    }

    const attemptRequest = async (attempt: number): Promise<T> => {
      const response = await this.fetchImpl(url, { ...init, headers });

      if (!response.ok) {
        const body = await this.safeParseBody(response);
        const shouldRetry =
          attempt + 1 < this.retry.attempts &&
          this.retry.retryOnStatuses.includes(response.status);

        if (shouldRetry) {
          await this.delay(this.retry.backoffMs * Math.pow(2, attempt));
          return attemptRequest(attempt + 1);
        }

        throw new OpenRatingError(
          `Request to ${url.pathname} failed with status ${response.status}`,
          response.status,
          body
        );
      }

      return (await this.safeParseBody(response)) as T;
    };

    return attemptRequest(0);
  }

  private async safeParseBody(response: Response): Promise<unknown> {
    if (response.status === 204) {
      return null;
    }

    const contentType = response.headers.get('content-type') ?? '';
    if (contentType.includes('application/json')) {
      return response.json();
    }

    return response.text();
  }

  private async delay(ms: number): Promise<void> {
    if (ms <= 0) return;
    await new Promise((resolve) => setTimeout(resolve, ms));
  }
}
