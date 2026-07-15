import { ROLE_HOME_PATHS, UserRole } from '../auth/roleTypes';
import { canAccessPath } from '../auth/roleAccess';

const ENTRY_PATHS = new Set(['/', '/login']);
const ALL_ROLE_HOME_PATHS = new Set(Object.values(ROLE_HOME_PATHS));

export function roleHomePath(role: UserRole): string {
    return ROLE_HOME_PATHS[role];
}

export function isRoleHomePath(path: string): boolean {
    return ALL_ROLE_HOME_PATHS.has(path);
}

/**
 * After login, land on the role home unless returning to an allowed report page.
 * Foreign / forbidden paths fall back to the role home.
 */
export function resolvePostLoginPath(role: UserRole, from?: string): string {
    const roleHome = ROLE_HOME_PATHS[role];
    if (!from || ENTRY_PATHS.has(from)) {
        return roleHome;
    }
    if (isRoleHomePath(from) && from !== roleHome && role !== 'admin' && role !== 'owner') {
        return roleHome;
    }
    if (!canAccessPath(role, from)) {
        return roleHome;
    }
    return from;
}
