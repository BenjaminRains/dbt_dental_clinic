import axios from 'axios';
import { resolveApiBaseUrl } from './baseUrl';

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

export interface PortalConfiguredInfo {
    portal_login_enabled: boolean;
    users_loaded: boolean;
    session_secret_configured: boolean;
    users_file: string;
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

    async configured(): Promise<PortalConfiguredInfo> {
        const response = await axios.get<PortalConfiguredInfo>(
            `${API_BASE_URL}/auth/configured`,
            { timeout: 15000 }
        );
        return response.data;
    },
};
