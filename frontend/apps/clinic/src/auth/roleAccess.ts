import { ROLE_HOME_PATHS, UserRole } from './roleTypes';

/** Shared analytics/report paths used by the clinic app. */
export const ALL_REPORT_PATHS = [
    '/dashboard',
    '/revenue',
    '/providers',
    '/patients',
    '/appointments',
    '/ar-aging',
    '/treatment-acceptance',
    '/hygiene-retention',
    '/referral-sources',
    '/kpi-definitions',
] as const;

export type ClinicReportPath = (typeof ALL_REPORT_PATHS)[number];

/**
 * Paths each role may open (own home + allowed reports).
 * `admin` and `owner` currently see everything (may diverge later).
 */
export const ROLE_ALLOWED_PATHS: Record<UserRole, readonly string[]> = {
    admin: [
        ...Object.values(ROLE_HOME_PATHS),
        ...ALL_REPORT_PATHS,
    ],
    owner: [
        ...Object.values(ROLE_HOME_PATHS),
        ...ALL_REPORT_PATHS,
    ],
    'practice-manager': [
        ROLE_HOME_PATHS['practice-manager'],
        ...ALL_REPORT_PATHS,
    ],
    'front-desk': [
        ROLE_HOME_PATHS['front-desk'],
        '/appointments',
        '/patients',
        '/hygiene-retention',
        '/treatment-acceptance',
    ],
    insurance: [
        ROLE_HOME_PATHS.insurance,
        '/ar-aging',
        '/revenue',
        '/treatment-acceptance',
        '/patients',
    ],
};

export function canAccessPath(role: UserRole, path: string): boolean {
    const normalized = path.replace(/\/$/, '') || '/';
    if (normalized === '/' || normalized === '/login') {
        return true;
    }
    return ROLE_ALLOWED_PATHS[role].includes(normalized);
}

export function filterNavPaths<T extends { path: string }>(
    role: UserRole,
    items: readonly T[],
): T[] {
    return items.filter((item) => canAccessPath(role, item.path));
}
