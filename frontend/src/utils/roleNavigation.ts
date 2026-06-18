import { ROLE_HOME_PATHS, UserRole } from '../context/roleTypes';

const ENTRY_PATHS = new Set(['/', '/login']);
const ALL_ROLE_HOME_PATHS = new Set(Object.values(ROLE_HOME_PATHS));

export function roleHomePath(role: UserRole): string {
    return ROLE_HOME_PATHS[role];
}

export function isRoleHomePath(path: string): boolean {
    return ALL_ROLE_HOME_PATHS.has(path);
}

/** After login, always land on the signed-in role's home unless returning to a report page. */
export function resolvePostLoginPath(role: UserRole, from?: string): string {
    const roleHome = ROLE_HOME_PATHS[role];
    if (!from || ENTRY_PATHS.has(from) || isRoleHomePath(from)) {
        return roleHome;
    }
    return from;
}
