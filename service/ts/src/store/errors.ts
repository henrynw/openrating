export class PlayerLookupError extends Error {
  constructor(
    message: string,
    public readonly context: {
      missing?: string[];
      wrongOrganization?: string[];
    } = {}
  ) {
    super(message);
    this.name = 'PlayerLookupError';
  }
}

export class OrganizationLookupError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'OrganizationLookupError';
  }
}

export class EventLookupError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EventLookupError';
  }
}

export class MatchLookupError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MatchLookupError';
  }
}

export class InvalidLeaderboardFilterError extends Error {
  constructor(
    message: string,
    public readonly code: 'invalid_age_group' | 'invalid_age_range' | 'invalid_age_cutoff'
  ) {
    super(message);
    this.name = 'InvalidLeaderboardFilterError';
  }
}
