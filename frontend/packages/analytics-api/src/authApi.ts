import axios from 'axios';

function resolveApiBaseUrl(): string {
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

const API_BASE_URL = resolveApiBaseUrl();

export interface LoginResult {
    token: string;
    username: string;
    display_name: string;
    role: string;
    expires_in: number;
}

export interface SessionInfo {
    username: string;
    display_name: string;
    role: string;
}

export const authApi = {
    async login(username: string, password: string): Promise<LoginResult> {
        const response = await axios.post<LoginResult>(
            `${API_BASE_URL}/auth/login`,
            { username, password },
            { timeout: 15000 }
        );
        return response.data;
    },

    async me(token: string): Promise<SessionInfo> {
        const response = await axios.get<SessionInfo>(`${API_BASE_URL}/auth/me`, {
            headers: { Authorization: `Bearer ${token}` },
            timeout: 15000,
        });
        return response.data;
    },
};
