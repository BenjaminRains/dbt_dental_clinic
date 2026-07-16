export function resolveApiBaseUrl(): string {
    const envUrl = (import.meta.env.VITE_API_URL as string | undefined)?.trim();
    const localHostPattern = /^https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?\/?$/i;

    if (import.meta.env.DEV) {
        if (envUrl && !localHostPattern.test(envUrl)) {
            return envUrl.replace(/\/$/, '');
        }
        return '/api';
    }
    return envUrl?.replace(/\/$/, '') || 'http://localhost:8000';
}
