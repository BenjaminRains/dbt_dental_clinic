/**
 * Clinic vs demo/portfolio — hostname wins over build-time VITE_IS_DEMO so
 * dbtdentalclinic.com never requires portal login even if the wrong build ships.
 */
const PORTFOLIO_HOSTS = new Set(['dbtdentalclinic.com', 'www.dbtdentalclinic.com']);
const CLINIC_HOSTS = new Set(['clinic.dbtdentalclinic.com']);
const LOCAL_DEV_HOSTS = new Set(['localhost', '127.0.0.1']);

export function isClinicSite(): boolean {
    if (typeof window !== 'undefined') {
        const host = window.location.hostname;
        if (CLINIC_HOSTS.has(host)) {
            return true;
        }
        if (PORTFOLIO_HOSTS.has(host)) {
            return false;
        }
        if (LOCAL_DEV_HOSTS.has(host)) {
            // Portfolio local dev by default; set VITE_IS_DEMO=false to test clinic login locally.
            return import.meta.env.VITE_IS_DEMO === 'false';
        }
    }

    if (import.meta.env.VITE_IS_DEMO === 'true') {
        return false;
    }

    // Clinic CloudFront / artifact hosts without a known portfolio hostname.
    return import.meta.env.VITE_IS_DEMO !== 'true';
}

/** @deprecated use isClinicSite */
export function isDemoSite(): boolean {
    return !isClinicSite();
}

const LEGACY_ROLE_STORAGE_KEY = 'clinic_user_role';

/** Old role picker stored role here without login — remove on clinic site. */
export function clearLegacyClinicRoleStorage(): void {
    try {
        localStorage.removeItem(LEGACY_ROLE_STORAGE_KEY);
    } catch {
        // ignore private browsing / storage errors
    }
}
