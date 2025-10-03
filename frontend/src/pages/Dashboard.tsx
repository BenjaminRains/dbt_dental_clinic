import React, { useEffect, useState } from 'react';
import {
    Box,
    Grid,
    Card,
    CardContent,
    Typography,
    CircularProgress,
    Alert,
} from '@mui/material';
import {
    AttachMoney,
    People,
    TrendingUp,
    Assessment,
} from '@mui/icons-material';
import { dashboardApi, revenueApi } from '../services/api';
import { DashboardKPIs, ApiResponse, RevenueTrend } from '../types/api';
import KPICard from '../components/charts/KPICard';
import RevenueTrendChart from '../components/charts/RevenueTrendChart';

const Dashboard: React.FC = () => {
    const [kpis, setKpis] = useState<ApiResponse<DashboardKPIs>>({ loading: true });
    const [revenueTrends, setRevenueTrends] = useState<ApiResponse<RevenueTrend[]>>({ loading: true });

    useEffect(() => {
        const fetchData = async () => {
            const [kpisResult, trendsResult] = await Promise.all([
                dashboardApi.getKPIs(),
                revenueApi.getTrends()
            ]);
            setKpis(kpisResult);
            setRevenueTrends(trendsResult);
        };

        fetchData();
    }, []);

    if (kpis.loading || revenueTrends.loading) {
        return (
            <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
                <CircularProgress />
            </Box>
        );
    }

    if (kpis.error) {
        return (
            <Alert severity="error" sx={{ mb: 2 }}>
                Error loading dashboard data: {kpis.error}
            </Alert>
        );
    }

    if (revenueTrends.error) {
        return (
            <Alert severity="error" sx={{ mb: 2 }}>
                Error loading revenue trends: {revenueTrends.error}
            </Alert>
        );
    }

    const data = kpis.data!;
    const trendsData = revenueTrends.data || [];

    return (
        <Box>
            <Typography variant="h4" component="h1" gutterBottom>
                Executive Dashboard
            </Typography>
            <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                Key performance indicators and analytics overview
            </Typography>

            <Grid container spacing={3} sx={{ mt: 2 }}>
                {/* KPI Cards */}
                <Grid item xs={12} sm={6} md={3}>
                    <KPICard
                        title="Revenue Lost"
                        value={data.revenue.total_revenue_lost}
                        format="currency"
                        color="error"
                        icon={<AttachMoney />}
                        metricName="revenue_lost"
                        showLineageTooltip={true}
                        trend={{
                            value: -5.2,
                            label: "vs last month",
                            isPositive: false
                        }}
                    />
                </Grid>
                <Grid item xs={12} sm={6} md={3}>
                    <KPICard
                        title="Recovery Potential"
                        value={data.revenue.total_recovery_potential}
                        format="currency"
                        color="success"
                        icon={<TrendingUp />}
                        metricName="recovery_potential"
                        showLineageTooltip={true}
                        trend={{
                            value: 12.5,
                            label: "vs last month",
                            isPositive: true
                        }}
                    />
                </Grid>
                <Grid item xs={12} sm={6} md={3}>
                    <KPICard
                        title="Active Providers"
                        value={data.providers.active_providers}
                        format="number"
                        color="primary"
                        icon={<People />}
                        metricName="active_providers"
                        showLineageTooltip={true}
                    />
                </Grid>
                <Grid item xs={12} sm={6} md={3}>
                    <KPICard
                        title="Collection Rate"
                        value={data.providers.avg_collection_rate * 100}
                        format="percentage"
                        color="info"
                        icon={<Assessment />}
                        metricName="collection_rate"
                        showLineageTooltip={true}
                        trend={{
                            value: 2.1,
                            label: "vs last month",
                            isPositive: true
                        }}
                    />
                </Grid>

                {/* Revenue Trend Chart */}
                <Grid item xs={12} lg={8}>
                    <Card>
                        <CardContent>
                            <RevenueTrendChart
                                data={trendsData}
                                title="Revenue Trend Analysis"
                                height={400}
                            />
                        </CardContent>
                    </Card>
                </Grid>

                {/* Quick Stats */}
                <Grid item xs={12} lg={4}>
                    <Card>
                        <CardContent>
                            <Typography variant="h6" gutterBottom>
                                Quick Statistics
                            </Typography>
                            <Box sx={{ mt: 2 }}>
                                <Typography variant="body2" color="text.secondary">
                                    Total Production
                                </Typography>
                                <Typography variant="h5" color="primary">
                                    ${data.providers.total_production.toLocaleString()}
                                </Typography>
                            </Box>
                            <Box sx={{ mt: 2 }}>
                                <Typography variant="body2" color="text.secondary">
                                    Total Collection
                                </Typography>
                                <Typography variant="h5" color="success.main">
                                    ${data.providers.total_collection.toLocaleString()}
                                </Typography>
                            </Box>
                            <Box sx={{ mt: 2 }}>
                                <Typography variant="body2" color="text.secondary">
                                    Recovery Rate
                                </Typography>
                                <Typography variant="h5" color="info.main">
                                    {data.revenue.total_recovery_potential > 0
                                        ? ((data.revenue.total_recovery_potential / data.revenue.total_revenue_lost) * 100).toFixed(1)
                                        : 0}%
                                </Typography>
                            </Box>
                        </CardContent>
                    </Card>
                </Grid>
            </Grid>
        </Box>
    );
};

export default Dashboard;
