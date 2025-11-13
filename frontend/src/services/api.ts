// API service layer for dental analytics dashboard
import axios, { AxiosResponse } from 'axios';
import {
    Patient,
    RevenueTrend,
    RevenueKPISummary,
    RevenueOpportunity,
    RevenueOpportunitySummary,
    RevenueRecoveryPlan,
    RevenueLostSummary,
    RevenueLostOpportunity,
    LostAppointmentDetail,
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
    DBTModelMetadata,
    ARKPISummary,
    ARAgingSummary,
    ARPriorityQueueItem,
    ARRiskDistribution,
    ARAgingTrend,
    TreatmentAcceptanceKPISummary,
    TreatmentAcceptanceSummary,
    TreatmentAcceptanceTrend,
    TreatmentAcceptanceProviderPerformance
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

    // PBN-style Revenue Lost endpoints
    getLostSummary: async (params: DateRange = {}): Promise<ApiResponse<RevenueLostSummary>> => {
        return apiCall(() => api.get('/revenue/lost-summary', { params }));
    },

    getLostOpportunity: async (params: DateRange = {}): Promise<ApiResponse<RevenueLostOpportunity>> => {
        return apiCall(() => api.get('/revenue/lost-opportunity', { params }));
    },

    getLostAppointments: async (
        skip: number = 0,
        limit: number = 100,
        params: DateRange & { status?: string } = {}
    ): Promise<ApiResponse<LostAppointmentDetail[]>> => {
        return apiCall(() => api.get('/revenue/lost-appointments', {
            params: { skip, limit, ...params }
        }));
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

    // AR Aging Dashboard API calls
    getKPISummary: async (params: DateRange = {}): Promise<ApiResponse<ARKPISummary>> => {
        return apiCall(() => api.get('/ar/kpi-summary', { params }));
    },

    getAgingSummary: async (params: DateRange & { snapshot_date?: string } = {}): Promise<ApiResponse<ARAgingSummary[]>> => {
        return apiCall(() => api.get('/ar/aging-summary', { params }));
    },

    getPriorityQueue: async (
        skip: number = 0,
        limit: number = 100,
        filters: {
            min_priority_score?: number;
            risk_category?: string;
            min_balance?: number;
            provider_id?: number;
        } = {}
    ): Promise<ApiResponse<ARPriorityQueueItem[]>> => {
        return apiCall(() => api.get('/ar/priority-queue', {
            params: { skip, limit, ...filters }
        }));
    },

    getRiskDistribution: async (params: DateRange & { snapshot_date?: string } = {}): Promise<ApiResponse<ARRiskDistribution[]>> => {
        return apiCall(() => api.get('/ar/risk-distribution', { params }));
    },

    getSnapshotDates: async (): Promise<ApiResponse<Array<{ snapshot_date: string }>>> => {
        return apiCall(() => api.get('/ar/snapshot-dates'));
    },

    getAgingTrends: async (params: DateRange = {}): Promise<ApiResponse<ARAgingTrend[]>> => {
        return apiCall(() => api.get('/ar/aging-trends', { params }));
    },
};

// Treatment Acceptance API calls
export const treatmentAcceptanceApi = {
    getKPISummary: async (params?: {
        start_date?: string;
        end_date?: string;
        provider_id?: number;
        clinic_id?: number;
    }): Promise<ApiResponse<TreatmentAcceptanceKPISummary>> => {
        return apiCall(() => api.get('/treatment-acceptance/kpi-summary', { params }));
    },

    getSummary: async (params?: {
        start_date?: string;
        end_date?: string;
        provider_id?: number;
        clinic_id?: number;
    }): Promise<ApiResponse<TreatmentAcceptanceSummary[]>> => {
        return apiCall(() => api.get('/treatment-acceptance/summary', { params }));
    },

    getTrends: async (params?: {
        start_date?: string;
        end_date?: string;
        provider_id?: number;
        clinic_id?: number;
        group_by?: string;
    }): Promise<ApiResponse<TreatmentAcceptanceTrend[]>> => {
        return apiCall(() => api.get('/treatment-acceptance/trends', { params }));
    },

    getProviderPerformance: async (params?: {
        start_date?: string;
        end_date?: string;
        clinic_id?: number;
    }): Promise<ApiResponse<TreatmentAcceptanceProviderPerformance[]>> => {
        return apiCall(() => api.get('/treatment-acceptance/provider-performance', { params }));
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
    treatmentAcceptance: treatmentAcceptanceApi,
    utils: dateUtils,
};

export default apiService;
