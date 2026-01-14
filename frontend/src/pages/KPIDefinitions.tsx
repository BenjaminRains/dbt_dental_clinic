import React, { useState } from 'react';
import {
    Box,
    Typography,
    Card,
    CardContent,
    Tabs,
    Tab,
    Divider,
    Chip,
    Paper,
} from '@mui/material';
import {
    HelpOutline as HelpIcon,
} from '@mui/icons-material';

interface TabPanelProps {
    children?: React.ReactNode;
    index: number;
    value: number;
}

function TabPanel(props: TabPanelProps) {
    const { children, value, index, ...other } = props;

    return (
        <div
            role="tabpanel"
            hidden={value !== index}
            id={`kpi-tabpanel-${index}`}
            aria-labelledby={`kpi-tab-${index}`}
            {...other}
        >
            {value === index && <Box sx={{ p: 3 }}>{children}</Box>}
        </div>
    );
}

interface Calculation {
    formula?: string;           // Mathematical formula: "(A / B) × 100"
    source?: string;            // SQL-level: "Sum of fact_payment.payment_amount"
    sourceModel?: string;       // dbt model: "mart_provider_performance"
    sourceField?: string;       // Field: "total_collections"
    sqlDetail?: string;         // Detailed SQL calculation description
    includes?: string[];        // What's included
    excludes?: string[];        // What's excluded
    timePeriod?: string;        // Date range constraints
    filter?: string;            // Filter conditions
    subComponents?: {           // Breakdowns (like recovery potential percentages)
        [key: string]: string;
    };
}

interface KPIDefinition {
    name: string;
    description: string;
    calculation?: Calculation;
    dataSource?: string;
    businessContext?: string;
}

interface DashboardKPIs {
    dashboardName: string;
    dashboardPath: string;
    kpis: KPIDefinition[];
}

// KPI definitions extracted from dbt_dental_models/models/marts/exposures.yml
// All KPIs documented with structured Calculation format (Phase 2 complete)
const dashboardKPIs: DashboardKPIs[] = [
    {
        dashboardName: 'Executive Dashboard',
        dashboardPath: '/dashboard',
        kpis: [
            {
                name: 'Revenue Lost',
                description: 'Total revenue from missed appointments, no-shows, and cancellations.',
                calculation: {
                    formula: 'Sum of scheduled_production_amount',
                    source: 'fact_appointment.scheduled_production_amount',
                    sourceModel: 'mart_revenue_lost',
                    sqlDetail: 'Sum of procedure fees (procedure_fee) for procedures linked to the appointment',
                    timePeriod: 'Last 12 months (appointment_date >= current_date - interval \'1 year\')',
                    filter: 'Only includes appointments with scheduled_production_amount > 0',
                },
                dataSource: 'mart_revenue_lost',
                businessContext: 'Represents potential revenue that was scheduled but not realized.',
            },
            {
                name: 'Recovery Potential',
                description: 'Estimated recoverable revenue based on opportunity analysis.',
                calculation: {
                    formula: 'Sum of estimated_recoverable_amount',
                    source: 'mart_revenue_lost.estimated_recoverable_amount',
                    sourceModel: 'mart_revenue_lost',
                    sqlDetail: 'Estimated recoverable amount based on opportunity type and recovery potential category',
                    subComponents: {
                        'High Recovery Potential': '80% of lost revenue',
                        'Medium Recovery Potential': '50% of lost revenue',
                        'Low Recovery Potential': '20% of lost revenue',
                        'None Recovery Potential': '0% of lost revenue',
                    },
                },
                dataSource: 'mart_revenue_lost',
                businessContext: 'Indicates revenue that can potentially be recovered through follow-up actions.',
            },
            {
                name: 'Active Providers',
                description: 'Count of providers with scheduled appointments.',
                calculation: {
                    formula: 'COUNT(DISTINCT provider_id)',
                    source: 'mart_provider_performance',
                    sourceModel: 'mart_provider_performance',
                    sqlDetail: 'Distinct count of providers with appointments in date range',
                    timePeriod: 'Filtered by selected date range',
                },
                dataSource: 'mart_provider_performance',
                businessContext: 'Measures provider utilization and activity levels.',
            },
            {
                name: 'Collection Rate',
                description: 'Average collection rate across all providers.',
                calculation: {
                    formula: '(Total Collections / Total Production) × 100',
                    source: 'Sum of fact_payment.payment_amount where payment_direction = \'Income\'',
                    sourceModel: 'mart_provider_performance',
                    sourceField: 'total_collections',
                    includes: ['All payments received (patient direct payments and insurance carrier payments)'],
                    excludes: ['Refunds (payments where payment_direction = \'Outcome\')', 'Adjustments and write-offs (handled separately)'],
                    timePeriod: 'Filtered by selected date range (default: current week)',
                },
                dataSource: 'mart_provider_performance',
                businessContext: 'Key financial metric indicating efficiency of revenue collection.',
            },
            {
                name: 'Total Production',
                description: 'Total billed revenue for services provided (completed procedures) within the selected date range.',
                calculation: {
                    formula: 'Sum of all fact_procedure.actual_fee values for completed procedures',
                    source: 'mart_provider_performance.total_production',
                    sourceModel: 'mart_provider_performance',
                    sqlDetail: 'Sum of all fact_procedure.actual_fee values for completed procedures',
                    includes: ['All procedure types (hygiene, restorative, surgical, etc.)', 'Both insurance-billed and direct-pay procedures'],
                    timePeriod: 'Filtered by selected date range (default: current week)',
                },
                dataSource: 'mart_provider_performance',
                businessContext: 'Production represents the total value of services delivered to patients. This is the primary revenue metric for dental practices.',
            },
            {
                name: 'Total Collection',
                description: 'Total cash received from all payment sources within the selected date range.',
                calculation: {
                    formula: 'Sum of all fact_payment.payment_amount where payment_direction = \'Income\'',
                    source: 'mart_provider_performance.total_collections',
                    sourceModel: 'mart_provider_performance',
                    sqlDetail: 'Sum of all fact_payment.payment_amount where payment_direction = \'Income\'',
                    includes: ['All payments received (patient direct payments and insurance carrier payments)'],
                    excludes: ['Refunds (payments where payment_direction = \'Outcome\')', 'Adjustments and write-offs (handled separately)'],
                    timePeriod: 'Filtered by selected date range (default: current week)',
                },
                dataSource: 'mart_provider_performance',
                businessContext: 'Collections represent actual cash flow into the practice. The ratio of collections to production indicates collection efficiency.',
            },
            {
                name: 'Recovery Rate',
                description: 'Percentage of lost revenue that is estimated to be recoverable through follow-up actions.',
                calculation: {
                    formula: '(Total Recovery Potential / Total Revenue Lost) × 100',
                    source: 'mart_revenue_lost.total_recovery_potential and mart_revenue_lost.total_revenue_lost',
                    sourceModel: 'mart_revenue_lost',
                    sqlDetail: 'Estimated recoverable amount based on opportunity type and recovery potential category',
                    timePeriod: 'Filtered by selected date range (default: current week)',
                    subComponents: {
                        'High Recovery Potential': '80% of lost revenue',
                        'Medium Recovery Potential': '50% of lost revenue',
                        'Low Recovery Potential': '20% of lost revenue',
                        'None Recovery Potential': '0% of lost revenue',
                    },
                },
                dataSource: 'mart_revenue_lost',
                businessContext: 'Recovery rate helps executives understand the potential for recovering lost revenue through systematic follow-up activities.',
            },
        ],
    },
    {
        dashboardName: 'Revenue Analytics',
        dashboardPath: '/revenue',
        kpis: [
            {
                name: 'Total Revenue Lost',
                description: 'Total revenue lost across all opportunity types.',
                calculation: {
                    formula: 'Sum of lost_revenue across all opportunities',
                    source: 'mart_revenue_lost.lost_revenue',
                    sourceModel: 'mart_revenue_lost',
                    sqlDetail: 'Aggregated by opportunity type: Missed Appointments (scheduled_production_amount), Claim Rejections (billed_amount - paid_amount), Write Offs (abs(adjustment_amount))',
                    timePeriod: 'Last 12 months',
                },
                dataSource: 'mart_revenue_lost',
            },
            {
                name: 'Total Recovery Potential',
                description: 'Total estimated recoverable revenue.',
                calculation: {
                    formula: 'Sum of estimated_recoverable_amount',
                    source: 'mart_revenue_lost.estimated_recoverable_amount',
                    sourceModel: 'mart_revenue_lost',
                    sqlDetail: 'Estimated based on opportunity type and recovery potential category',
                    subComponents: {
                        'High Recovery Potential': '80% of lost revenue',
                        'Medium Recovery Potential': '50% of lost revenue',
                        'Low Recovery Potential': '20% of lost revenue',
                    },
                    timePeriod: 'Last 12 months',
                },
                dataSource: 'mart_revenue_lost',
            },
            {
                name: 'Total Opportunities',
                description: 'Count of all revenue recovery opportunities.',
                calculation: {
                    formula: 'COUNT(DISTINCT opportunity_id)',
                    source: 'mart_revenue_lost',
                    sourceModel: 'mart_revenue_lost',
                    sqlDetail: 'Distinct count of opportunities across all types (Missed Appointment, Claim Rejection, Treatment Plan Delay, Write Off)',
                    timePeriod: 'Last 12 months',
                },
                dataSource: 'mart_revenue_lost',
            },
            {
                name: 'Affected Patients',
                description: 'Number of patients affected by revenue opportunities.',
                calculation: {
                    formula: 'COUNT(DISTINCT patient_id)',
                    source: 'mart_revenue_lost.patient_id',
                    sourceModel: 'mart_revenue_lost',
                    sqlDetail: 'Distinct count of patients with revenue opportunities',
                    timePeriod: 'Last 12 months',
                },
                dataSource: 'mart_revenue_lost',
            },
        ],
    },
    {
        dashboardName: 'Provider Performance',
        dashboardPath: '/providers',
        kpis: [
            {
                name: 'Total Providers',
                description: 'Count of active providers in the system.',
                calculation: {
                    formula: 'COUNT(DISTINCT provider_id)',
                    source: 'mart_provider_performance',
                    sourceModel: 'mart_provider_performance',
                    sqlDetail: 'Distinct count of providers with activity in the selected date range',
                    timePeriod: 'Filtered by selected date range',
                },
                dataSource: 'mart_provider_performance',
            },
            {
                name: 'Total Appointments',
                description: 'Aggregate scheduled appointments across all providers.',
                calculation: {
                    formula: 'Sum of total_appointments',
                    source: 'mart_appointment_summary.total_appointments',
                    sourceModel: 'mart_appointment_summary',
                    sqlDetail: 'Aggregated scheduled appointments across all providers',
                    timePeriod: 'Filtered by selected date range',
                },
                dataSource: 'mart_appointment_summary',
            },
            {
                name: 'Completion Rate',
                description: 'Percentage of appointments completed (Target: >80%).',
                calculation: {
                    formula: '(Completed Appointments / Total Appointments) × 100',
                    source: 'mart_appointment_summary.appointment_completion_rate',
                    sourceModel: 'mart_appointment_summary',
                    sqlDetail: 'Completed appointments divided by total appointments, multiplied by 100',
                    timePeriod: 'Filtered by selected date range',
                    subComponents: {
                        'Target': '>80%',
                        'Excellent': '≥95%',
                        'Good': '≥90%',
                        'Fair': '≥85%',
                        'Poor': '<85%',
                    },
                },
                dataSource: 'mart_appointment_summary',
            },
            {
                name: 'No-Show Rate',
                description: 'Percentage of missed appointments (Target: <10%).',
                calculation: {
                    formula: '(No-Show Appointments / Total Appointments) × 100',
                    source: 'mart_appointment_summary.no_show_rate',
                    sourceModel: 'mart_appointment_summary',
                    sqlDetail: 'No-show appointments divided by total appointments, multiplied by 100',
                    timePeriod: 'Filtered by selected date range',
                    subComponents: {
                        'Target': '<10%',
                        'Excellent': '≤5%',
                        'Good': '≤10%',
                        'Fair': '≤15%',
                        'Poor': '>15%',
                    },
                },
                dataSource: 'mart_appointment_summary',
            },
            {
                name: 'Production per Appointment',
                description: 'Average production amount per appointment.',
                calculation: {
                    formula: 'Total Production / Completed Appointments',
                    source: 'mart_provider_performance.total_production, completed_appointments',
                    sourceModel: 'mart_provider_performance',
                    sqlDetail: 'Total production divided by number of completed appointments',
                    timePeriod: 'Filtered by selected date range',
                },
                dataSource: 'mart_provider_performance',
            },
        ],
    },
    {
        dashboardName: 'Appointment Analytics',
        dashboardPath: '/appointments',
        kpis: [
            {
                name: 'Completion Rate',
                description: 'Percentage of appointments that were completed.',
                calculation: {
                    formula: '(Completed Appointments / Total Appointments) × 100',
                    source: 'mart_appointment_summary.appointment_completion_rate',
                    sourceModel: 'mart_appointment_summary',
                    sqlDetail: 'Sum of completed_appointments divided by sum of total_appointments, multiplied by 100',
                    timePeriod: 'Filtered by selected date range (default: last 30 days)',
                },
                dataSource: 'mart_appointment_summary',
            },
            {
                name: 'No-Show Rate',
                description: 'Percentage of appointments where patient did not show.',
                calculation: {
                    formula: '(No-Show Appointments / Total Appointments) × 100',
                    source: 'mart_appointment_summary.no_show_rate',
                    sourceModel: 'mart_appointment_summary',
                    sqlDetail: 'Sum of no_show_appointments divided by sum of total_appointments, multiplied by 100',
                    timePeriod: 'Filtered by selected date range (default: last 30 days)',
                },
                dataSource: 'mart_appointment_summary',
            },
            {
                name: 'Cancellation Rate',
                description: 'Percentage of appointments that were cancelled.',
                calculation: {
                    formula: '(Cancelled Appointments / Total Appointments) × 100',
                    source: 'mart_appointment_summary.cancellation_rate',
                    sourceModel: 'mart_appointment_summary',
                    sqlDetail: 'Sum of broken_appointments divided by sum of total_appointments, multiplied by 100',
                    timePeriod: 'Filtered by selected date range (default: last 30 days)',
                },
                dataSource: 'mart_appointment_summary',
            },
            {
                name: 'Utilization Rate',
                description: 'Time-based capacity usage metric.',
                calculation: {
                    formula: '(Productive Hours / Total Scheduled Hours) × 100',
                    source: 'mart_appointment_summary.time_utilization_rate',
                    sourceModel: 'mart_appointment_summary',
                    sqlDetail: 'Sum of productive_minutes divided by sum of total_scheduled_minutes, converted to hours, multiplied by 100',
                    timePeriod: 'Filtered by selected date range (default: last 30 days)',
                },
                dataSource: 'mart_appointment_summary',
            },
        ],
    },
    {
        dashboardName: 'Patient Management',
        dashboardPath: '/patients',
        kpis: [
            {
                name: 'Patient Counts',
                description: 'Total number of patients by status (Active, Inactive, etc.).',
                calculation: {
                    formula: 'COUNT(DISTINCT patient_id)',
                    source: 'dim_patient.patient_id',
                    sourceModel: 'dim_patient',
                    sqlDetail: 'Distinct count of patients, optionally filtered by patient_status',
                },
                dataSource: 'dim_patient',
            },
            {
                name: 'Status Tracking',
                description: 'Distribution of patients across status categories.',
                calculation: {
                    formula: 'GROUP BY patient_status, COUNT(*)',
                    source: 'dim_patient.patient_status',
                    sourceModel: 'dim_patient',
                    sqlDetail: 'Count of patients grouped by status (Active, Inactive, Deceased, Deleted)',
                },
                dataSource: 'dim_patient',
            },
            {
                name: 'Demographics',
                description: 'Patient demographic information (age, gender, etc.).',
                calculation: {
                    source: 'dim_patient',
                    sourceModel: 'dim_patient',
                    sqlDetail: 'Patient demographic fields: age, age_category, gender, first_visit_date',
                    includes: ['Age calculation from birthdate', 'Age categories (Minor/Adult/Senior)', 'Gender distribution', 'Patient tenure from first visit'],
                },
                dataSource: 'dim_patient',
            },
            {
                name: 'Financial Summaries',
                description: 'Total balance and financial status by patient.',
                calculation: {
                    formula: 'Sum of total_balance by patient',
                    source: 'mart_ar_summary.total_balance',
                    sourceModel: 'mart_ar_summary',
                    sqlDetail: 'Aggregated accounts receivable balance per patient, includes patient_responsibility and insurance_estimate',
                    includes: ['Patient AR (patient_responsibility)', 'Insurance AR (insurance_estimate)'],
                },
                dataSource: 'mart_ar_summary',
            },
        ],
    },
    {
        dashboardName: 'AR Aging',
        dashboardPath: '/ar-aging',
        kpis: [
            {
                name: 'Total AR Outstanding',
                description: 'Complete outstanding balance with patient vs insurance breakdown.',
                calculation: {
                    formula: 'Sum of total_balance where total_balance > 0',
                    source: 'mart_ar_summary.total_balance',
                    sourceModel: 'mart_ar_summary',
                    sqlDetail: 'Sum of all total_balance values, excludes zero balances and credits',
                    filter: 'Only includes accounts where total_balance > 0',
                    subComponents: {
                        'Patient AR': 'Sum of patient_responsibility',
                        'Insurance AR': 'Sum of insurance_estimate',
                    },
                },
                dataSource: 'mart_ar_summary',
            },
            {
                name: 'AR Aging Buckets',
                description: 'Distribution of AR by aging categories (Current, 30 Day, 60 Day, 90 Day).',
                calculation: {
                    formula: '(bucket_amount / total_balance) × 100',
                    source: 'mart_ar_summary aging bucket fields',
                    sourceModel: 'mart_ar_summary',
                    sqlDetail: 'Each bucket calculated as percentage of total balance',
                    subComponents: {
                        'Current (0-30 days)': 'balance_0_30_days',
                        '30 Day (31-60 days)': 'balance_31_60_days',
                        '60 Day (61-90 days)': 'balance_61_90_days',
                        '90 Day (90+ days)': 'balance_over_90_days',
                    },
                },
                dataSource: 'mart_ar_summary',
            },
            {
                name: 'Days Sales Outstanding (DSO)',
                description: 'Average number of days to collect payment on accounts receivable.',
                calculation: {
                    formula: '((Balance Over 90 Days / Total AR) × 90) + 30',
                    source: 'mart_ar_summary.balance_over_90_days, total_balance',
                    sourceModel: 'mart_ar_summary',
                    sqlDetail: 'Simplified DSO calculation: percentage of AR over 90 days multiplied by 90, plus 30',
                    timePeriod: 'Current snapshot',
                    subComponents: {
                        'Target DSO': '<45 days (industry benchmark)',
                        'Limitation': 'Simplified formula; standard DSO = (AR Balance / Average Daily Sales) × Days',
                    },
                },
                dataSource: 'mart_ar_summary',
            },
            {
                name: 'Collection Rate',
                description: 'Percentage of production that has been collected in payments.',
                calculation: {
                    formula: '(Total Collections / Total Production) × 100',
                    source: 'fact_payment.payment_amount, fact_procedure.actual_fee',
                    sourceModel: 'mart_ar_summary (derived)',
                    sqlDetail: 'Collections: Sum of fact_payment.payment_amount where payment_direction = \'Income\'; Production: Sum of fact_procedure.actual_fee for completed procedures',
                    timePeriod: 'Rolling 365-day window (last year)',
                    includes: ['All payments received (patient and insurance)', 'All completed procedures'],
                    excludes: ['Refunds (payment_direction = \'Outcome\')', 'Adjustments and write-offs'],
                    subComponents: {
                        'Excellent': '>95%',
                        'Good': '90-95% (industry standard)',
                        'Needs Attention': '<90%',
                    },
                },
                dataSource: 'mart_ar_summary',
            },
            {
                name: 'High Risk Accounts',
                description: 'Accounts with significant portion of balance aged over 90 days.',
                calculation: {
                    formula: 'COUNT(DISTINCT patient_id, provider_id) where aging_risk_category = \'High Risk\'',
                    source: 'mart_ar_summary.aging_risk_category',
                    sourceModel: 'mart_ar_summary',
                    sqlDetail: 'Distinct count of patient-provider combinations where >50% of balance is over 90 days old',
                    subComponents: {
                        'High Risk': '>50% of balance over 90 days',
                        'Medium Risk': '25-50% of balance over 90 days',
                        'Moderate Risk': '>50% of balance 61-90 days',
                        'Low Risk': '<25% of balance over 90 days',
                    },
                },
                dataSource: 'mart_ar_summary',
            },
            {
                name: 'AR Ratio',
                description: 'Current month collections compared to current month production (PBN-style metric).',
                calculation: {
                    formula: '(Monthly Collections / Monthly Production) × 100',
                    source: 'fact_procedure.actual_fee, fact_payment.payment_amount',
                    sourceModel: 'mart_ar_summary (derived)',
                    sqlDetail: 'Monthly Production: Sum of fact_procedure.actual_fee for current calendar month; Monthly Collections: Sum of fact_payment.payment_amount (where payment_direction = \'Income\') for current calendar month',
                    timePeriod: 'Current calendar month only (from first day of month to today)',
                    subComponents: {
                        '>100%': 'Collecting more than current month production (collecting on past months\' AR)',
                        '80-100%': 'Healthy collection pace relative to production',
                        '<80%': 'Collections lagging behind current month production',
                    },
                },
                dataSource: 'mart_ar_summary',
                businessContext: 'Short-term indicator of collection efficiency for current period',
            },
            {
                name: 'Collection Priority Score',
                description: 'Composite score (0-100 scale) ranking accounts by collection urgency and importance.',
                calculation: {
                    formula: 'Balance Factor + Aging Factor + Payment Recency Factor + Insurance Factor',
                    source: 'mart_ar_summary.collection_priority_score',
                    sourceModel: 'mart_ar_summary',
                    sqlDetail: 'Multi-factor algorithm summing four components (each up to 25 points): Balance Factor (25 if >$1,000, otherwise balance/40), Aging Factor (25 if >50% over 90 days), Payment Recency Factor (25 if no payment in 90+ days), Insurance Factor (25 if no insurance)',
                    subComponents: {
                        '76-100 (Critical)': 'Highest priority, immediate action required',
                        '51-75 (High)': 'High priority, aggressive follow-up needed',
                        '26-50 (Medium)': 'Moderate priority, routine collection activities',
                        '0-25 (Low)': 'Low priority, standard follow-up sufficient',
                    },
                },
                dataSource: 'mart_ar_summary',
                businessContext: 'Enables data-driven collection workflow prioritization and resource allocation',
            },
            {
                name: 'Payment Recency',
                description: 'Categorization of how recently a patient has made a payment, indicating payment behavior patterns.',
                calculation: {
                    source: 'mart_ar_summary.days_since_last_payment',
                    sourceModel: 'mart_ar_summary',
                    sqlDetail: 'Calculated from most recent payment date in fact_payment table (last 365 days)',
                    subComponents: {
                        'Recent': 'Last payment within 30 days',
                        'Moderate': 'Last payment 31-90 days ago',
                        'Old': 'Last payment 91-180 days ago',
                        'Very Old': 'Last payment over 180 days ago (or no payment in last 365 days)',
                    },
                    timePeriod: 'Based on last 365 days of payment history',
                },
                dataSource: 'mart_ar_summary',
                businessContext: 'Informs collection strategy - recent payers may need gentle reminders, very old payers may require aggressive collection or write-off consideration',
            },
            {
                name: 'Risk Categories',
                description: 'Automated risk assessment based on AR aging patterns and balance distribution.',
                calculation: {
                    source: 'mart_ar_summary.aging_risk_category',
                    sourceModel: 'mart_ar_summary',
                    sqlDetail: 'Determined in mart_ar_summary model based on aging bucket percentages',
                    subComponents: {
                        'High Risk': '>50% of balance over 90 days old - requires immediate aggressive collection',
                        'Medium Risk': '25-50% of balance over 90 days old - requires focused follow-up',
                        'Moderate Risk': '>50% of balance 61-90 days old - requires proactive attention',
                        'Low Risk': '<25% of balance over 90 days old - routine collection activities sufficient',
                        'Credit Balance': 'Total balance is negative (patient overpaid) - refund or credit application needed',
                        'No Balance': 'Total balance is zero - no collection action needed',
                    },
                },
                dataSource: 'mart_ar_summary',
                businessContext: 'Enables risk-based collection workflow management and resource prioritization',
            },
        ],
    },
];

const KPIDefinitions: React.FC = () => {
    const [tabValue, setTabValue] = useState(0);

    const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
        setTabValue(newValue);
    };

    const renderKPIDefinition = (kpi: KPIDefinition, index: number) => {
        return (
            <Card key={index} sx={{ mb: 2 }}>
                <CardContent>
                    <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                        <Typography variant="h6" component="h3" sx={{ fontWeight: 'bold', flexGrow: 1 }}>
                            {kpi.name}
                        </Typography>
                    </Box>
                    <Divider sx={{ my: 1 }} />
                    <Typography variant="body1" paragraph>
                        {kpi.description}
                    </Typography>
                    {kpi.calculation && (
                        <Box sx={{ mb: 2 }}>
                            <Typography variant="subtitle2" color="text.secondary" gutterBottom sx={{ fontWeight: 'medium', mb: 1 }}>
                                Calculation:
                            </Typography>
                            <Paper
                                variant="outlined"
                                sx={{
                                    p: 2,
                                    backgroundColor: 'background.default',
                                }}
                            >
                                {kpi.calculation.formula && (
                                    <Box sx={{ mb: 1.5 }}>
                                        <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 'medium', display: 'block', mb: 0.5 }}>
                                            Formula:
                                        </Typography>
                                        <Typography variant="body2" sx={{ fontFamily: 'monospace', fontWeight: 'medium' }}>
                                            {kpi.calculation.formula}
                                        </Typography>
                                    </Box>
                                )}
                                {kpi.calculation.source && (
                                    <Box sx={{ mb: 1.5 }}>
                                        <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 'medium', display: 'block', mb: 0.5 }}>
                                            Source:
                                        </Typography>
                                        <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                                            {kpi.calculation.source}
                                        </Typography>
                                    </Box>
                                )}
                                {kpi.calculation.sourceModel && (
                                    <Box sx={{ mb: 1.5 }}>
                                        <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 'medium', display: 'block', mb: 0.5 }}>
                                            Source Model:
                                        </Typography>
                                        <Chip
                                            label={kpi.calculation.sourceModel}
                                            size="small"
                                            color="primary"
                                            variant="outlined"
                                        />
                                    </Box>
                                )}
                                {kpi.calculation.sqlDetail && (
                                    <Box sx={{ mb: 1.5 }}>
                                        <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 'medium', display: 'block', mb: 0.5 }}>
                                            SQL Detail:
                                        </Typography>
                                        <Typography variant="body2">
                                            {kpi.calculation.sqlDetail}
                                        </Typography>
                                    </Box>
                                )}
                                {kpi.calculation.timePeriod && (
                                    <Box sx={{ mb: 1.5 }}>
                                        <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 'medium', display: 'block', mb: 0.5 }}>
                                            Time Period:
                                        </Typography>
                                        <Typography variant="body2">
                                            {kpi.calculation.timePeriod}
                                        </Typography>
                                    </Box>
                                )}
                                {kpi.calculation.filter && (
                                    <Box sx={{ mb: 1.5 }}>
                                        <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 'medium', display: 'block', mb: 0.5 }}>
                                            Filter:
                                        </Typography>
                                        <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                                            {kpi.calculation.filter}
                                        </Typography>
                                    </Box>
                                )}
                                {kpi.calculation.includes && kpi.calculation.includes.length > 0 && (
                                    <Box sx={{ mb: 1.5 }}>
                                        <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 'medium', display: 'block', mb: 0.5 }}>
                                            Includes:
                                        </Typography>
                                        <Box component="ul" sx={{ m: 0, pl: 2 }}>
                                            {kpi.calculation.includes.map((item, idx) => (
                                                <Typography key={idx} variant="body2" component="li">
                                                    {item}
                                                </Typography>
                                            ))}
                                        </Box>
                                    </Box>
                                )}
                                {kpi.calculation.excludes && kpi.calculation.excludes.length > 0 && (
                                    <Box sx={{ mb: 1.5 }}>
                                        <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 'medium', display: 'block', mb: 0.5 }}>
                                            Excludes:
                                        </Typography>
                                        <Box component="ul" sx={{ m: 0, pl: 2 }}>
                                            {kpi.calculation.excludes.map((item, idx) => (
                                                <Typography key={idx} variant="body2" component="li">
                                                    {item}
                                                </Typography>
                                            ))}
                                        </Box>
                                    </Box>
                                )}
                                {kpi.calculation.subComponents && Object.keys(kpi.calculation.subComponents).length > 0 && (
                                    <Box>
                                        <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 'medium', display: 'block', mb: 0.5 }}>
                                            Sub-components:
                                        </Typography>
                                        <Box>
                                            {Object.entries(kpi.calculation.subComponents).map(([key, value]) => (
                                                <Box key={key} sx={{ mb: 0.5 }}>
                                                    <Typography variant="body2" component="span" sx={{ fontWeight: 'medium' }}>
                                                        {key}:
                                                    </Typography>
                                                    <Typography variant="body2" component="span" sx={{ ml: 1 }}>
                                                        {value}
                                                    </Typography>
                                                </Box>
                                            ))}
                                        </Box>
                                    </Box>
                                )}
                            </Paper>
                        </Box>
                    )}
                    {kpi.dataSource && (
                        <Box sx={{ mb: 2 }}>
                            <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                                Data Source:
                            </Typography>
                            <Chip
                                label={kpi.dataSource}
                                size="small"
                                color="primary"
                                variant="outlined"
                            />
                        </Box>
                    )}
                    {kpi.businessContext && (
                        <Box>
                            <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                                Business Context:
                            </Typography>
                            <Typography variant="body2" color="text.secondary">
                                {kpi.businessContext}
                            </Typography>
                        </Box>
                    )}
                </CardContent>
            </Card>
        );
    };

    return (
        <Box>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                <HelpIcon sx={{ mr: 1, fontSize: 32, color: 'primary.main' }} />
                <Box>
                    <Typography variant="h4" component="h1" gutterBottom>
                        KPI Definitions
                    </Typography>
                    <Typography variant="subtitle1" color="text.secondary">
                        Comprehensive documentation of all Key Performance Indicators across dashboards
                    </Typography>
                </Box>
            </Box>

            <Card sx={{ mt: 3 }}>
                <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
                    <Tabs
                        value={tabValue}
                        onChange={handleTabChange}
                        aria-label="KPI definitions tabs"
                        variant="scrollable"
                        scrollButtons="auto"
                    >
                        {dashboardKPIs.map((dashboard, index) => (
                            <Tab
                                key={index}
                                label={dashboard.dashboardName}
                                id={`kpi-tab-${index}`}
                                aria-controls={`kpi-tabpanel-${index}`}
                            />
                        ))}
                    </Tabs>
                </Box>

                {dashboardKPIs.map((dashboard, index) => (
                    <TabPanel key={index} value={tabValue} index={index}>
                        <Box sx={{ mb: 3 }}>
                            <Typography variant="h5" component="h2" gutterBottom>
                                {dashboard.dashboardName}
                            </Typography>
                            <Typography variant="body2" color="text.secondary" gutterBottom>
                                Dashboard Path: {dashboard.dashboardPath}
                            </Typography>
                            <Divider sx={{ mt: 2, mb: 3 }} />
                        </Box>

                        {dashboard.kpis.length > 0 ? (
                            dashboard.kpis.map((kpi, kpiIndex) => renderKPIDefinition(kpi, kpiIndex))
                        ) : (
                            <Typography variant="body2" color="text.secondary">
                                KPI definitions coming soon...
                            </Typography>
                        )}
                    </TabPanel>
                ))}
            </Card>

            <Card sx={{ mt: 3, p: 2, backgroundColor: 'success.light', color: 'success.contrastText' }}>
                <Typography variant="body2">
                    <strong>Note:</strong> All KPI definitions have been extracted from the dbt exposures documentation
                    and are now available in this centralized reference. Definitions are organized by dashboard and
                    include detailed calculation formulas, data sources, and business context.
                </Typography>
            </Card>
        </Box>
    );
};

export default KPIDefinitions;
