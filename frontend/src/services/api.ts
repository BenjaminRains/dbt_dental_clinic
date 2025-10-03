// API service layer for dental analytics dashboard
import axios, { AxiosResponse } from 'axios';
import {
    Patient,
    RevenueTrend,
    RevenueKPISummary,
    RevenueOpportunity,
    RevenueOpportunitySummary,
    RevenueRecoveryPlan,
    ProviderPerformance,
    ProviderSummary,
    ARSummary,
    DashboardKPIs,
    DateRange,
    ProviderFilter,
    ApiResponse,
    AppointmentSummary,
    AppointmentDetail,
    MetricLineageInfo,
    DBTModelMetadata
} from '../types/api';

// Configure axios base URL
// Use proxy in development, direct URL in production
const API_BASE_URL = import.meta.env.VITE_API_URL ||
    (import.meta.env.DEV ? '/api' : 'http://localhost:8000');

const api = axios.create({
    baseURL: API_BASE_URL,
    timeout: 10000,
    headers: {
        'Content-Type': 'application/json',
    },
});

// Request interceptor for logging
api.interceptors.request.use(
    (config) => {
        console.log(`API Request: ${config.method?.toUpperCase()} ${config.url}`);
        return config;
    },
    (error) => {
        console.error('API Request Error:', error);
        return Promise.reject(error);
    }
);

// Response interceptor for error handling
api.interceptors.response.use(
    (response) => {
        console.log(`API Response: ${response.status} ${response.config.url}`);
        return response;
    },
    (error) => {
        console.error('API Response Error:', error.response?.data || error.message);
        return Promise.reject(error);
    }
);

// Generic API call wrapper with error handling
async function apiCall<T>(
    apiCall: () => Promise<AxiosResponse<T>>
): Promise<ApiResponse<T>> {
    try {
        const response = await apiCall();
        return {
            data: response.data,
            loading: false,
        };
    } catch (error: any) {
        let errorMessage = 'An error occurred';

        if (error.response?.data?.detail) {
            if (Array.isArray(error.response.data.detail)) {
                errorMessage = error.response.data.detail.map((d: any) => d.msg || d).join(', ');
            } else {
                errorMessage = error.response.data.detail;
            }
        } else if (error.message) {
            errorMessage = error.message;
        }

        return {
            error: errorMessage,
            loading: false,
        };
    }
}

// Standardized API response helper for fetch-based requests
export async function toApiResponse<T>(p: Promise<Response>): Promise<ApiResponse<T>> {
    try {
        const res = await p;
        if (!res.ok) return { loading: false, error: `${res.status}: ${res.statusText}` };
        const data = await res.json();
        return { loading: false, data };
    } catch (e: any) {
        return { loading: false, error: e?.message ?? 'Network error' };
    }
}

// Revenue Analytics API calls
export const revenueApi = {
    getTrends: async (params: ProviderFilter = {}): Promise<ApiResponse<RevenueTrend[]>> => {
        return apiCall(() => api.get('/revenue/trends', { params }));
    },

    getKPISummary: async (params: DateRange = {}): Promise<ApiResponse<RevenueKPISummary>> => {
        return apiCall(() => api.get('/revenue/kpi-summary', { params }));
    },

    getOpportunities: async (
        skip: number = 0,
        limit: number = 100,
        params: ProviderFilter & {
            opportunity_type?: string;
            recovery_potential?: string;
            min_priority_score?: number;
        } = {}
    ): Promise<ApiResponse<RevenueOpportunity[]>> => {
        return apiCall(() => api.get('/revenue/opportunities', {
            params: { skip, limit, ...params }
        }));
    },

    getOpportunitySummary: async (params: DateRange = {}): Promise<ApiResponse<RevenueOpportunitySummary[]>> => {
        return apiCall(() => api.get('/revenue/opportunities/summary', { params }));
    },

    getRecoveryPlan: async (
        min_priority_score: number = 50,
        params: DateRange = {}
    ): Promise<ApiResponse<RevenueRecoveryPlan[]>> => {
        return apiCall(() => api.get('/revenue/recovery-plan', {
            params: { min_priority_score, ...params }
        }));
    },

    getOpportunityById: async (opportunityId: number): Promise<ApiResponse<RevenueOpportunity>> => {
        return apiCall(() => api.get(`/revenue/opportunities/${opportunityId}`));
    },
};

// Provider Performance API calls
export const providerApi = {
    getPerformance: async (params: ProviderFilter = {}): Promise<ApiResponse<ProviderPerformance[]>> => {
        return apiCall(() => api.get('/reports/providers/performance', { params }));
    },

    getSummary: async (params: DateRange = {}): Promise<ApiResponse<ProviderSummary[]>> => {
        return apiCall(() => api.get('/providers/summary', { params }));
    },
};

// Accounts Receivable API calls
export const arApi = {
    getSummary: async (params: DateRange = {}): Promise<ApiResponse<ARSummary[]>> => {
        return apiCall(() => api.get('/reports/ar/summary', { params }));
    },
};

// Dashboard API calls
export const dashboardApi = {
    getKPIs: async (params: DateRange = {}): Promise<ApiResponse<DashboardKPIs>> => {
        return apiCall(() => api.get('/reports/dashboard/kpis', { params }));
    },
};

// Patient API calls
export const patientApi = {
    getPatients: async (skip: number = 0, limit: number = 100): Promise<ApiResponse<{ patients: Patient[], total: number }>> => {
        return apiCall(() => api.get('/patients/', { params: { skip, limit } }));
    },

    getPatientById: async (patientId: number): Promise<ApiResponse<Patient>> => {
        return apiCall(() => api.get(`/patients/${patientId}`));
    },
};

// Appointment API calls
export const appointmentApi = {
    getSummary: async (params: ProviderFilter = {}): Promise<ApiResponse<AppointmentSummary[]>> => {
        return apiCall(() => api.get('/appointments/summary', { params }));
    },

    getAppointments: async (
        skip: number = 0,
        limit: number = 100,
        params: ProviderFilter = {}
    ): Promise<ApiResponse<AppointmentDetail[]>> => {
        return apiCall(() => api.get('/appointments/', {
            params: { skip, limit, ...params }
        }));
    },

    getAppointmentById: async (appointmentId: number): Promise<ApiResponse<AppointmentDetail>> => {
        return apiCall(() => api.get(`/appointments/${appointmentId}`));
    },

    getTodayAppointments: async (providerId?: number): Promise<ApiResponse<AppointmentDetail[]>> => {
        const params = providerId ? { provider_id: providerId } : {};
        return apiCall(() => api.get('/appointments/today', { params }));
    },

    getUpcomingAppointments: async (
        days: number = 7,
        providerId?: number
    ): Promise<ApiResponse<AppointmentDetail[]>> => {
        const params: any = { days };
        if (providerId) params.provider_id = providerId;
        return apiCall(() => api.get('/appointments/upcoming', { params }));
    },
};

// Utility functions for date formatting
export const dateUtils = {
    formatDate: (date: Date): string => {
        return date.toISOString().split('T')[0];
    },

    getDateRange: (days: number): DateRange => {
        const endDate = new Date();
        const startDate = new Date();
        startDate.setDate(startDate.getDate() - days);

        return {
            start_date: dateUtils.formatDate(startDate),
            end_date: dateUtils.formatDate(endDate),
        };
    },

    getLast30Days: (): DateRange => dateUtils.getDateRange(30),
    getLast90Days: (): DateRange => dateUtils.getDateRange(90),
    getLastYear: (): DateRange => dateUtils.getDateRange(365),
};

// DBT Metadata API calls
export const dbtMetadataApi = {
    getMetricLineage: async (metricName: string): Promise<ApiResponse<MetricLineageInfo>> => {
        return apiCall(() => api.get(`/dbt/metric-lineage/${metricName}`));
    },

    getAllMetricLineage: async (): Promise<ApiResponse<MetricLineageInfo[]>> => {
        return apiCall(() => api.get('/dbt/metric-lineage'));
    },

    getModelMetadata: async (modelName: string): Promise<ApiResponse<DBTModelMetadata>> => {
        return apiCall(() => api.get(`/dbt/model-metadata/${modelName}`));
    },

    getAllModels: async (params: {
        model_type?: string;
        schema_name?: string;
    } = {}): Promise<ApiResponse<DBTModelMetadata[]>> => {
        return apiCall(() => api.get('/dbt/models', { params }));
    },
};

// Export all API functions
export const apiService = {
    patient: patientApi,
    revenue: revenueApi,
    provider: providerApi,
    ar: arApi,
    dashboard: dashboardApi,
    appointment: appointmentApi,
    dbt: dbtMetadataApi,
    utils: dateUtils,
};

export default apiService;
