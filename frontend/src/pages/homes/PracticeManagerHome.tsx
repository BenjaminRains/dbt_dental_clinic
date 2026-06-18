import React, { useMemo } from 'react';
import {
    Box,
    Typography,
    Grid,
    Alert,
    CircularProgress,
    Paper,
    Chip,
} from '@mui/material';
import {
    arApi,
    revenueApi,
    appointmentApi,
    hygieneApi,
    dashboardApi,
} from '../../services/api';
import { useApiQuery } from '../../hooks/useApiQuery';
import InsightCard, { InsightCardProps } from '../../components/insights/InsightCard';
import WorkQueue from '../../components/queues/WorkQueue';
import {
    formatCurrency,
    formatPercent,
    getPriorityScoreColor,
    getRiskCategoryColor,
} from '../../utils/format';
import { INSIGHT_THRESHOLDS } from '../../config/insights';
import {
    ARPriorityQueueItem,
    RevenueRecoveryPlan,
    AppointmentDetail,
    ARKPISummary,
    HygieneRetentionSummary,
    DashboardKPIs,
} from '../../types/api';

const QUEUE_LIMIT = 5;
const MIN_PRIORITY_SCORE = 50;

function buildInsights(
    arKpi: ARKPISummary | undefined,
    hygiene: HygieneRetentionSummary | undefined,
    dashboard: DashboardKPIs | undefined,
    recoveryCount: number
): InsightCardProps[] {
    const insights: InsightCardProps[] = [];

    if (arKpi) {
        if (arKpi.over_90_percentage >= INSIGHT_THRESHOLDS.over90ArPercentWarning) {
            insights.push({
                metric: 'AR over 90 days',
                severity: arKpi.over_90_percentage >= 35 ? 'error' : 'warning',
                narrative: `AR over 90 days is ${formatPercent(arKpi.over_90_percentage)} of outstanding balances (${formatCurrency(arKpi.over_90_amount)}).`,
                recommendedAction: 'Prioritize high-score accounts in the collections queue.',
                drillDownLink: '/ar-aging',
            });
        }

        if (arKpi.collection_rate < INSIGHT_THRESHOLDS.collectionRateTarget) {
            insights.push({
                metric: 'Collection rate',
                severity: 'warning',
                narrative: `Collection rate is ${formatPercent(arKpi.collection_rate)}, below the ${INSIGHT_THRESHOLDS.collectionRateTarget}% target.`,
                recommendedAction: 'Review overdue balances and follow up on recent production.',
                drillDownLink: '/ar-aging',
            });
        }

        if (arKpi.high_risk_count >= INSIGHT_THRESHOLDS.highRiskAccountCountWarning) {
            insights.push({
                metric: 'High-risk accounts',
                severity: 'error',
                narrative: `${arKpi.high_risk_count} high-risk accounts represent ${formatCurrency(arKpi.high_risk_amount)} in outstanding AR.`,
                recommendedAction: 'Work the collections queue starting with highest priority scores.',
                drillDownLink: '/ar-aging',
            });
        }
    }

    if (hygiene) {
        if (hygiene.recall_overdue_percent >= INSIGHT_THRESHOLDS.recallOverdueWarning) {
            insights.push({
                metric: 'Hygiene recall',
                severity: 'warning',
                narrative: `${formatPercent(hygiene.recall_overdue_percent)} of hygiene patients are overdue for recall.`,
                recommendedAction: 'Schedule outreach for overdue hygiene patients.',
                drillDownLink: '/hygiene-retention',
            });
        }

        if (hygiene.hyg_pre_appointment_percent < INSIGHT_THRESHOLDS.hygienePreAppointmentTarget) {
            insights.push({
                metric: 'Hygiene pre-appointment',
                severity: 'info',
                narrative: `Only ${formatPercent(hygiene.hyg_pre_appointment_percent)} of hygiene patients have a future appointment booked.`,
                recommendedAction: 'Book next hygiene visits before patients leave the office.',
                drillDownLink: '/appointments',
            });
        }
    }

    if (dashboard && dashboard.revenue.total_recovery_potential > 0) {
        insights.push({
            metric: 'Revenue recovery',
            severity: recoveryCount >= INSIGHT_THRESHOLDS.revenueRecoveryOpportunityWarning ? 'warning' : 'info',
            narrative: `${formatCurrency(dashboard.revenue.total_recovery_potential)} in recovery potential across active opportunities.`,
            recommendedAction: 'Work missed appointments and treatment delays from the recovery queue.',
            drillDownLink: '/revenue',
        });
    }

    return insights.slice(0, 6);
}

const PracticeManagerHome: React.FC = () => {
    const arKpiQuery = useApiQuery(() => arApi.getKPISummary(), []);
    const collectionsQuery = useApiQuery(
        () =>
            arApi.getPriorityQueue(0, QUEUE_LIMIT, {
                min_priority_score: MIN_PRIORITY_SCORE,
            }),
        []
    );
    const recoveryQuery = useApiQuery(
        () => revenueApi.getRecoveryPlan(MIN_PRIORITY_SCORE),
        []
    );
    const todayQuery = useApiQuery(() => appointmentApi.getTodayAppointments(), []);
    const upcomingQuery = useApiQuery(() => appointmentApi.getUpcomingAppointments(7), []);
    const hygieneQuery = useApiQuery(() => hygieneApi.getRetentionSummary(), []);
    const dashboardQuery = useApiQuery(() => dashboardApi.getKPIs(), []);

    const loading =
        arKpiQuery.loading ||
        collectionsQuery.loading ||
        recoveryQuery.loading ||
        todayQuery.loading ||
        upcomingQuery.loading ||
        hygieneQuery.loading ||
        dashboardQuery.loading;

    const errors = [
        arKpiQuery.error,
        collectionsQuery.error,
        recoveryQuery.error,
        todayQuery.error,
        upcomingQuery.error,
        hygieneQuery.error,
        dashboardQuery.error,
    ].filter(Boolean);

    const recoveryPlan = useMemo(
        () => (recoveryQuery.data ?? []).slice(0, QUEUE_LIMIT),
        [recoveryQuery.data]
    );

    const todayAppointments = useMemo(
        () => (todayQuery.data ?? []).slice(0, QUEUE_LIMIT),
        [todayQuery.data]
    );

    const insights = useMemo(
        () =>
            buildInsights(
                arKpiQuery.data,
                hygieneQuery.data,
                dashboardQuery.data,
                recoveryPlan.length
            ),
        [arKpiQuery.data, hygieneQuery.data, dashboardQuery.data, recoveryPlan.length]
    );

    const briefLines = useMemo(() => {
        const lines: string[] = [];
        const ar = arKpiQuery.data;
        const hygiene = hygieneQuery.data;

        if (ar) {
            lines.push(
                `Total AR outstanding: ${formatCurrency(ar.total_ar_outstanding)} (${formatPercent(ar.collection_rate)} collection rate).`
            );
        }
        if (recoveryPlan.length > 0) {
            lines.push(`${recoveryPlan.length} high-priority revenue recovery opportunities need follow-up.`);
        }
        if (collectionsQuery.data && collectionsQuery.data.length > 0) {
            lines.push(`${collectionsQuery.data.length} accounts in today's collections queue.`);
        }
        if (hygiene) {
            lines.push(
                `Hygiene recall: ${formatPercent(hygiene.recall_current_percent)} current, ${formatPercent(hygiene.recall_overdue_percent)} overdue.`
            );
        }
        if (todayAppointments.length > 0) {
            lines.push(`${todayAppointments.length} appointments scheduled today.`);
        }

        return lines;
    }, [arKpiQuery.data, recoveryPlan.length, collectionsQuery.data, hygieneQuery.data, todayAppointments.length]);

    if (loading) {
        return (
            <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: 400 }}>
                <CircularProgress />
            </Box>
        );
    }

    return (
        <Box>
            <Typography variant="h4" component="h1" gutterBottom>
                Practice Manager Home
            </Typography>
            <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                What needs attention today
            </Typography>

            {errors.length > 0 && (
                <Alert severity="warning" sx={{ mt: 2, mb: 2 }}>
                    Some sections could not be loaded. Partial data is shown below.
                </Alert>
            )}

            {briefLines.length > 0 && (
                <Paper variant="outlined" sx={{ p: 2, mt: 2, mb: 3, bgcolor: 'background.paper' }}>
                    <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
                        Morning brief
                    </Typography>
                    {briefLines.map((line, i) => (
                        <Typography key={i} variant="body2" color="text.secondary" sx={{ mb: 0.5 }}>
                            • {line}
                        </Typography>
                    ))}
                </Paper>
            )}

            {insights.length > 0 ? (
                <Grid container spacing={2} sx={{ mb: 3 }}>
                    {insights.map((insight) => (
                        <Grid item xs={12} sm={6} md={4} key={insight.metric}>
                            <InsightCard {...insight} />
                        </Grid>
                    ))}
                </Grid>
            ) : (
                <Alert severity="success" sx={{ mb: 3 }}>
                    No exceptions detected against current thresholds. Review queues below for proactive follow-up.
                </Alert>
            )}

            <Grid container spacing={3}>
                <Grid item xs={12} lg={4}>
                    <WorkQueue<ARPriorityQueueItem>
                        title="Collections"
                        rows={collectionsQuery.data ?? []}
                        loading={collectionsQuery.loading}
                        error={collectionsQuery.error}
                        severity="error"
                        viewAllLink="/ar-aging"
                        getRowKey={(row) => `${row.patient_id}-${row.provider_id}`}
                        columns={[
                            {
                                key: 'patient',
                                label: 'Patient',
                                render: (row) => row.patient_id,
                            },
                            {
                                key: 'balance',
                                label: 'Balance',
                                align: 'right',
                                render: (row) => (
                                    <Typography variant="body2" fontWeight="bold">
                                        {formatCurrency(row.total_balance)}
                                    </Typography>
                                ),
                            },
                            {
                                key: 'risk',
                                label: 'Risk',
                                render: (row) => (
                                    <Chip
                                        label={row.aging_risk_category}
                                        size="small"
                                        color={getRiskCategoryColor(row.aging_risk_category)}
                                    />
                                ),
                            },
                            {
                                key: 'score',
                                label: 'Score',
                                align: 'right',
                                render: (row) => (
                                    <Chip
                                        label={row.collection_priority_score}
                                        size="small"
                                        color={getPriorityScoreColor(row.collection_priority_score)}
                                    />
                                ),
                            },
                        ]}
                    />
                </Grid>

                <Grid item xs={12} lg={4}>
                    <WorkQueue<RevenueRecoveryPlan>
                        title="Revenue recovery"
                        rows={recoveryPlan}
                        loading={recoveryQuery.loading}
                        error={recoveryQuery.error}
                        severity="warning"
                        viewAllLink="/revenue"
                        getRowKey={(row) => row.opportunity_id}
                        columns={[
                            {
                                key: 'patient',
                                label: 'Patient',
                                render: (row) => row.patient_id,
                            },
                            {
                                key: 'type',
                                label: 'Type',
                                render: (row) => (
                                    <Chip label={row.opportunity_type} size="small" variant="outlined" />
                                ),
                            },
                            {
                                key: 'priority',
                                label: 'Priority',
                                align: 'right',
                                render: (row) => (
                                    <Chip
                                        label={row.priority_score}
                                        size="small"
                                        color={getPriorityScoreColor(row.priority_score)}
                                    />
                                ),
                            },
                            {
                                key: 'actions',
                                label: 'Action',
                                render: (row) => (
                                    <Typography variant="caption" color="text.secondary">
                                        {row.recommended_actions[0] ?? '—'}
                                    </Typography>
                                ),
                            },
                        ]}
                    />
                </Grid>

                <Grid item xs={12} lg={4}>
                    <WorkQueue<AppointmentDetail>
                        title="Today's schedule"
                        rows={todayAppointments}
                        loading={todayQuery.loading}
                        error={todayQuery.error}
                        severity="info"
                        viewAllLink="/appointments"
                        emptyMessage="No appointments scheduled today"
                        getRowKey={(row) => row.appointment_id}
                        columns={[
                            {
                                key: 'time',
                                label: 'Time',
                                render: (row) => row.appointment_time,
                            },
                            {
                                key: 'patient',
                                label: 'Patient',
                                render: (row) => row.patient_id,
                            },
                            {
                                key: 'type',
                                label: 'Type',
                                render: (row) => row.appointment_type,
                            },
                            {
                                key: 'status',
                                label: 'Status',
                                render: (row) => (
                                    <Chip
                                        label={row.appointment_status}
                                        size="small"
                                        color={row.is_no_show ? 'error' : row.is_completed ? 'success' : 'default'}
                                    />
                                ),
                            },
                        ]}
                    />
                </Grid>
            </Grid>

            {(upcomingQuery.data?.length ?? 0) > 0 && (
                <Box sx={{ mt: 3 }}>
                    <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                        Upcoming week: {upcomingQuery.data?.length} appointments in the next 7 days
                    </Typography>
                </Box>
            )}
        </Box>
    );
};

export default PracticeManagerHome;
