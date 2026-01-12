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
    TreatmentAcceptanceProviderPerformance,
    HygieneRetentionSummary,
    TopPatientBalance
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

// Request interceptor for API key and logging
api.interceptors.request.use(
    (config) => {
        // Add API key from environment variable
        const apiKey = import.meta.env.VITE_API_KEY;
        if (apiKey) {
            config.headers['X-API-Key'] = apiKey;
        } else {
            console.warn('VITE_API_KEY not set - API requests may fail authentication');
        }

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
        // Log full error for debugging (only in development)
        if (import.meta.env.DEV) {
            console.error('API Response Error:', error.response?.data || error.message);
            console.error('Full error object:', error);
        }

        // Don't sanitize in development - let the apiCall function handle it
        // This allows us to see the actual error messages during debugging
        if (!import.meta.env.DEV && error.response?.data?.detail) {
            const detail = error.response.data.detail;
            // Only show generic messages to users in production
            if (detail.includes('SQL') || detail.includes('database') || detail.includes('column') ||
                detail.includes('psycopg2') || detail.includes('UndefinedColumn') ||
                detail.includes('syntax error') || detail.includes('connection')) {
                error.response.data.detail = 'A network error occurred. Please try again later.';
            }
        }

        return Promise.reject(error);
    }
);

// Helper function to convert error details to a string
function errorDetailToString(detail: any): string {
    if (typeof detail === 'string') {
        return detail;
    }
    if (Array.isArray(detail)) {
        // Handle FastAPI/Pydantic validation errors array
        return detail.map((err: any) => {
            if (typeof err === 'string') {
                return err;
            }
            if (err && typeof err === 'object') {
                const loc = err.loc ? err.loc.join('.') : '';
                const msg = err.msg || err.message || 'Validation error';
                return loc ? `${loc}: ${msg}` : msg;
            }
            return String(err);
        }).join('; ');
    }
    if (detail && typeof detail === 'object') {
        // Try to extract a message from the object
        if (detail.message) {
            return String(detail.message);
        }
        if (detail.msg) {
            return String(detail.msg);
        }
        // Fallback to JSON string
        return JSON.stringify(detail);
    }
    return String(detail);
}

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
        // Sanitize error messages for security - show generic messages only
        let errorMessage = 'A network error occurred. Please try again later.';

        // Only show user-friendly error messages
        if (error.response?.status === 401) {
            errorMessage = 'Authentication required. Please check your API key.';
        } else if (error.response?.status === 403) {
            errorMessage = 'Access denied.';
        } else if (error.response?.status === 404) {
            errorMessage = 'Resource not found.';
        } else if (error.response?.status === 422) {
            // Handle validation errors (422)
            if (error.response?.data?.detail) {
                errorMessage = `Validation error: ${errorDetailToString(error.response.data.detail)}`;
            } else {
                errorMessage = 'Invalid request parameters.';
            }
        } else if (error.response?.status === 429) {
            errorMessage = 'Too many requests. Please try again later.';
        } else if (error.response?.status >= 500) {
            // For 500 errors, always show the API's error message if available
            // Don't sanitize 500 errors - they're already sanitized by the backend
            if (error.response?.data?.detail) {
                errorMessage = errorDetailToString(error.response.data.detail);
            } else if (error.response?.data?.message) {
                errorMessage = errorDetailToString(error.response.data.message);
            } else {
                errorMessage = 'A server error occurred. Please try again later.';
            }
            // Always log full error details for debugging
            console.error('Server error details:', error.response?.data);
            console.error('Error status:', error.response?.status);
            console.error('Full error response:', error.response);
        } else if (error.response?.data?.detail) {
            // Use sanitized detail from API (already sanitized by backend)
            const detailStr = errorDetailToString(error.response.data.detail);
            // Only use if it's a generic message (not technical details)
            if (!detailStr.includes('SQL') && !detailStr.includes('database') &&
                !detailStr.includes('column') && !detailStr.includes('psycopg2') &&
                !detailStr.includes('UndefinedColumn') && !detailStr.includes('syntax error')) {
                errorMessage = detailStr;
            }
        } else if (error.response?.data?.message) {
            errorMessage = errorDetailToString(error.response.data.message);
        } else if (error.code === 'ECONNABORTED' || error.message?.includes('timeout')) {
            errorMessage = 'Request timed out. Please try again.';
        } else if (error.message?.includes('Network Error') || !error.response) {
            errorMessage = 'Network error. Please check your connection.';
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

// Hygiene Retention API calls
export const hygieneApi = {
    getRetentionSummary: async (
        params: DateRange = {}
    ): Promise<ApiResponse<HygieneRetentionSummary>> => {
        return apiCall(() => api.get('/hygiene/retention-summary', { params }));
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

    getTopBalances: async (limit: number = 10): Promise<ApiResponse<TopPatientBalance[]>> => {
        return apiCall(() => api.get('/patients/top-balances', { params: { limit } }));
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
    getThisWeek: (): DateRange => {
        const today = new Date();
        const dayOfWeek = today.getDay(); // 0 = Sunday, 1 = Monday, etc.
        const startOfWeek = new Date(today);
        startOfWeek.setDate(today.getDate() - dayOfWeek); // Go back to Sunday
        startOfWeek.setHours(0, 0, 0, 0);

        return {
            start_date: dateUtils.formatDate(startOfWeek),
            end_date: dateUtils.formatDate(today),
        };
    },

    getPreviousBusinessWeek: (): DateRange => {
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        const dayOfWeek = today.getDay(); // 0 = Sunday, 1 = Monday, etc.

        // Find the most recent Friday (end of previous business week)
        // Friday is day 5 (0=Sunday, 5=Friday)
        let daysSinceFriday;
        if (dayOfWeek === 0) {
            // Sunday - go back 2 days to Friday
            daysSinceFriday = 2;
        } else if (dayOfWeek === 6) {
            // Saturday - go back 1 day to Friday
            daysSinceFriday = 1;
        } else if (dayOfWeek <= 5) {
            // Monday (1) through Friday (5)
            // If it's Monday-Thursday, go back to last Friday
            // If it's Friday, use today as the end date
            if (dayOfWeek === 5) {
                daysSinceFriday = 0; // Today is Friday, use it
            } else {
                // Monday (1) - Thursday (4): go back to last Friday
                daysSinceFriday = dayOfWeek + 2; // e.g., Monday: 1+2=3 days back
            }
        } else {
            daysSinceFriday = 1;
        }

        const endOfPreviousWeek = new Date(today);
        endOfPreviousWeek.setDate(today.getDate() - daysSinceFriday);

        // Monday is 4 days before Friday
        const startOfPreviousWeek = new Date(endOfPreviousWeek);
        startOfPreviousWeek.setDate(endOfPreviousWeek.getDate() - 4);

        return {
            start_date: dateUtils.formatDate(startOfPreviousWeek),
            end_date: dateUtils.formatDate(endOfPreviousWeek),
        };
    },
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
    hygiene: hygieneApi,
    utils: dateUtils,
};

export default apiService;
