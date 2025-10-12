export class InvalidBirthInputError extends Error {
  constructor(
    message: string,
    public readonly code: 'invalid_birth_date' | 'birth_year_mismatch' | 'invalid_birth_year'
  ) {
    super(message);
    this.name = 'InvalidBirthInputError';
  }
}

interface BirthState {
  birthYear?: number | null;
  birthDate?: string | null;
}

const parseDate = (value: string): Date => {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(value.trim());
  if (!match) {
    throw new InvalidBirthInputError(`Invalid birth_date format: ${value}`, 'invalid_birth_date');
  }
  const year = Number.parseInt(match[1], 10);
  const month = Number.parseInt(match[2], 10);
  const day = Number.parseInt(match[3], 10);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    Number.isNaN(date.getTime()) ||
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    throw new InvalidBirthInputError(`Invalid birth_date value: ${value}`, 'invalid_birth_date');
  }
  return date;
};

const formatDate = (date: Date): string => {
  const year = date.getUTCFullYear();
  const month = `${date.getUTCMonth() + 1}`.padStart(2, '0');
  const day = `${date.getUTCDate()}`.padStart(2, '0');
  return `${year}-${month}-${day}`;
};

const normalizeBirthYear = (value: number | null | undefined): number | null => {
  if (value === null || value === undefined) return null;
  if (!Number.isInteger(value) || value < 1900 || value > 2100) {
    throw new InvalidBirthInputError(`Invalid birth_year value: ${value}`, 'invalid_birth_year');
  }
  return value;
};

export const reconcileBirthDetails = (
  current: BirthState,
  patch: BirthState
): { birthYear: number | null; birthDate: string | null } => {
  let birthYear = normalizeBirthYear(patch.birthYear ?? current.birthYear ?? null);
  let birthDate = patch.birthDate !== undefined ? patch.birthDate : current.birthDate ?? null;

  if (birthDate === undefined) {
    birthDate = current.birthDate ?? null;
  }

  if (birthDate !== null && birthDate !== undefined) {
    const parsed = parseDate(birthDate);
    const derivedYear = parsed.getUTCFullYear();
    if (birthYear !== null && birthYear !== derivedYear) {
      throw new InvalidBirthInputError('birth_year must match birth_date year when both are provided', 'birth_year_mismatch');
    }
    birthYear = derivedYear;
    birthDate = formatDate(parsed);
  }

  if (birthDate === null && patch.birthDate === null) {
    // Explicitly cleared birth date; keep existing birth year if set explicitly, otherwise maintain current
    if (patch.birthYear === undefined) {
      birthYear = birthYear ?? null;
    }
  }

  if (patch.birthYear === null) {
    birthYear = null;
    if (patch.birthDate === undefined) {
      // Clearing birth year without explicit birth date change should also clear birth date
      birthDate = null;
    }
  }

  // Ensure null instead of undefined in final payload
  return {
    birthYear: birthYear ?? null,
    birthDate: birthDate ?? null,
  };
};
