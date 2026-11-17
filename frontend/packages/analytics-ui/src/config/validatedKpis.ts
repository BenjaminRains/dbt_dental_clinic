/**
 * KPIs with within_tolerance status in
 * dbt_dental_models/validation/kpi/KPI_VALIDATION_REGISTRY.md.
 *
 * Clinic Practice Manager Home and the executive dashboard both read this list.
 * Portfolio KPI Validation reads the same module so the sign-off links cannot drift.
 */
const GITHUB_MAIN = 'https://github.com/BenjaminRains/dbt_dental_clinic/blob/main';

export const VALIDATED_KPIS = {
    dailyNetCollections: {
        id: 'payments',
        name: 'Daily net collections',
        model: 'mart_daily_payments',
        field: 'net_collections_amount',
        odReport: 'Daily Payments',
        odMenuPath: 'Reports → Standard → Daily Payments',
        registryStatus: 'within_tolerance' as const,
        signedOff: '2026-06-26',
        reportHref: `${GITHUB_MAIN}/dbt_dental_models/validation/kpi/daily-payments/VALIDATION_REPORT.md`,
        walkthroughTo: '/daily-net-collections',
    },
    dailyProductionByProcedure: {
        id: 'daily-production-by-procedure',
        name: 'Daily production by procedure',
        model: 'mart_daily_production_by_procedure',
        field: 'total_fees',
        odReport: 'Production by Procedure',
        odMenuPath: 'Reports → Standard → Daily → Production by Procedure',
        registryStatus: 'within_tolerance' as const,
        signedOff: '2026-07-16',
        reportHref: `${GITHUB_MAIN}/dbt_dental_models/validation/kpi/daily-production-by-procedure/VALIDATION_REPORT.md`,
    },
} as const;

export type ValidatedKpiId = keyof typeof VALIDATED_KPIS;
