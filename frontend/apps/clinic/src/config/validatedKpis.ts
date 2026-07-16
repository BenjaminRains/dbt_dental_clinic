/**
 * KPIs with within_tolerance status in dbt_dental_models/validation/kpi/KPI_VALIDATION_REGISTRY.md
 */
export const VALIDATED_KPIS = {
    dailyNetCollections: {
        id: 'payments',
        name: 'Daily net collections',
        model: 'mart_daily_payments',
        field: 'net_collections_amount',
        odReport: 'Daily Payments',
        odMenuPath: 'Reports → Standard → Daily Payments',
        registryStatus: 'within_tolerance' as const,
    },
    dailyProductionByProcedure: {
        id: 'daily-production-by-procedure',
        name: 'Daily production by procedure',
        model: 'mart_daily_production_by_procedure',
        field: 'total_fees',
        odReport: 'Production by Procedure',
        odMenuPath: 'Reports → Standard → Daily → Production by Procedure',
        registryStatus: 'within_tolerance' as const,
    },
} as const;

export type ValidatedKpiId = keyof typeof VALIDATED_KPIS;
