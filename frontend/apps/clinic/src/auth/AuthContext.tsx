import React, {
    createContext,
    useCallback,
    useContext,
    useEffect,
    useMemo,
    useState,
} from 'react';
import { authApi } from '@mdc/analytics-api/clinic';
import { ROLE_HOME_PATHS, UserRole } from './roleTypes';

const SESSION_STORAGE_KEY = 'clinic_portal_session';
const LEGACY_ROLE_STORAGE_KEY = 'clinic_user_role';

export interface PortalSession {
    token: string;
    username: string;
    displayName: string;
    role: UserRole;
    expiresAt: number;
}

interface AuthContextValue {
    session: PortalSession | null;
    isAuthenticated: boolean;
    isClinicBuild: boolean;
    role: UserRole;
    homePath: string;
    displayName: string;
    login: (username: string, password: string) => Promise<PortalSession>;
    logout: () => void;
    authLoading: boolean;
}

const AuthContext = createContext<AuthContextValue | null>(null);

function isUserRole(value: string): value is UserRole {
    return (
        value === 'practice-manager' ||
        value === 'owner' ||
        value === 'front-desk' ||
        value === 'insurance'
    );
}

function clearLegacyClinicRoleStorage(): void {
    try {
        localStorage.removeItem(LEGACY_ROLE_STORAGE_KEY);
    } catch {
        // ignore private browsing / storage errors
    }
}

function readStoredSession(): PortalSession | null {
    try {
        const raw = sessionStorage.getItem(SESSION_STORAGE_KEY);
        if (!raw) {
            return null;
        }
        const parsed = JSON.parse(raw) as PortalSession;
        if (
            !parsed?.token ||
            !parsed.username ||
            !isUserRole(parsed.role) ||
            typeof parsed.expiresAt !== 'number'
        ) {
            sessionStorage.removeItem(SESSION_STORAGE_KEY);
            return null;
        }
        if (parsed.expiresAt < Date.now()) {
            sessionStorage.removeItem(SESSION_STORAGE_KEY);
            return null;
        }
        return parsed;
    } catch {
        sessionStorage.removeItem(SESSION_STORAGE_KEY);
        return null;
    }
}

function persistSession(session: PortalSession | null) {
    if (!session) {
        sessionStorage.removeItem(SESSION_STORAGE_KEY);
        return;
    }
    sessionStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(session));
}

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
    const [session, setSession] = useState<PortalSession | null>(() => readStoredSession());
    const [authLoading, setAuthLoading] = useState(!!readStoredSession());

    useEffect(() => {
        clearLegacyClinicRoleStorage();
    }, []);

    const logout = useCallback(() => {
        persistSession(null);
        setSession(null);
    }, []);

    const login = useCallback(async (username: string, password: string): Promise<PortalSession> => {
        const result = await authApi.login(username, password);
        if (!isUserRole(result.role)) {
            throw new Error(`Unknown role returned from server: ${result.role}`);
        }
        const next: PortalSession = {
            token: result.token,
            username: result.username,
            displayName: result.display_name,
            role: result.role,
            expiresAt: Date.now() + result.expires_in * 1000,
        };
        persistSession(next);
        setSession(next);
        return next;
    }, []);

    useEffect(() => {
        const stored = readStoredSession();
        if (!stored?.token) {
            setAuthLoading(false);
            return;
        }

        let cancelled = false;
        setAuthLoading(true);
        authApi
            .me(stored.token)
            .then((me) => {
                if (cancelled) {
                    return;
                }
                if (!isUserRole(me.role)) {
                    logout();
                    return;
                }
                const refreshed: PortalSession = {
                    ...stored,
                    username: me.username,
                    displayName: me.display_name,
                    role: me.role,
                };
                persistSession(refreshed);
                setSession(refreshed);
            })
            .catch(() => {
                if (!cancelled) {
                    logout();
                }
            })
            .finally(() => {
                if (!cancelled) {
                    setAuthLoading(false);
                }
            });

        return () => {
            cancelled = true;
        };
    }, [logout]);

    const role: UserRole = session?.role ?? 'practice-manager';
    const homePath = ROLE_HOME_PATHS[role];

    const value = useMemo(
        () => ({
            session,
            isAuthenticated: !!session && !authLoading,
            isClinicBuild: true,
            role,
            homePath,
            displayName: session?.displayName ?? '',
            login,
            logout,
            authLoading,
        }),
        [session, role, homePath, login, logout, authLoading]
    );

    return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

export function useAuth(): AuthContextValue {
    const ctx = useContext(AuthContext);
    if (!ctx) {
        throw new Error('useAuth must be used within AuthProvider');
    }
    return ctx;
}
