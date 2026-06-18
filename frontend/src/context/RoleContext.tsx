import { useAuth } from './AuthContext';
import { ROLE_HOME_PATHS, ROLE_LABELS, UserRole } from './roleTypes';

export type { UserRole };
export { ROLE_LABELS, ROLE_HOME_PATHS };

/** Role comes from portal login (AuthContext) on clinic builds. */
export function useRole() {
    const { role, homePath, isAuthenticated } = useAuth();
    return {
        role,
        homePath,
        hasSelectedRole: isAuthenticated,
    };
}
