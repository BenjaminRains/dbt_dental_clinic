import React, { useEffect, useState } from 'react';
import {
    Box,
    Grid,
    Card,
    CardContent,
    Typography,
    CircularProgress,
    Alert,
    TextField,
    Stack,
    Chip,
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableHead,
    TableRow,
    Paper,
} from '@mui/material';
import {
    AttachMoney,
    People,
    TrendingUp,
    Assessment,
} from '@mui/icons-material';
import { dashboardApi, revenueApi, patientApi, dateUtils } from '../services/api';
import { DashboardKPIs, ApiResponse, RevenueTrend, TopPatientBalance } from '../types/api';
import KPICard from '../components/charts/KPICard';
import RevenueTrendChart from '../components/charts/RevenueTrendChart';

const Dashboard: React.FC = () => {
    // Initialize with previous business week's date range
    const previousBusinessWeek = dateUtils.getPreviousBusinessWeek();
    const [startDate, setStartDate] = useState<string>(previousBusinessWeek.start_date || '');
    const [endDate, setEndDate] = useState<string>(previousBusinessWeek.end_date || '');

    const [kpis, setKpis] = useState<ApiResponse<DashboardKPIs>>({ loading: true });
    const [revenueTrends, setRevenueTrends] = useState<ApiResponse<RevenueTrend[]>>({ loading: true });
    const [topBalances, setTopBalances] = useState<ApiResponse<TopPatientBalance[]>>({ loading: true });

    // Helper function to safely convert error to string
    const errorToString = (error: any): string => {
        if (typeof error === 'string') {
            return error;
        }
        if (error && typeof error === 'object') {
            if (error.message) {
                return String(error.message);
            }
            if (error.msg) {
                return String(error.msg);
            }
            return JSON.stringify(error);
        }
        return String(error);
    };

    useEffect(() => {
        const fetchData = async () => {
            try {
                const dateRange = {
                    start_date: startDate || undefined,
                    end_date: endDate || undefined,
                };

                console.log('Dashboard: Fetching data with date range:', dateRange);
                const [kpisResult, trendsResult, balancesResult] = await Promise.all([
                    dashboardApi.getKPIs(dateRange),
                    revenueApi.getTrends(dateRange),
                    patientApi.getTopBalances(10)
                ]);

                console.log('Dashboard: API Results:', {
                    kpis: { hasError: !!kpisResult.error, hasData: !!kpisResult.data, loading: kpisResult.loading },
                    trends: { hasError: !!trendsResult.error, hasData: !!trendsResult.data, loading: trendsResult.loading },
                    balances: { hasError: !!balancesResult.error, hasData: !!balancesResult.data, loading: balancesResult.loading }
                });

                if (kpisResult.error) {
                    console.error('Dashboard: KPIs error:', kpisResult.error);
                } else if (kpisResult.data) {
                    console.log('Dashboard: KPIs data structure:', {
                        hasRevenue: !!kpisResult.data.revenue,
                        hasProviders: !!kpisResult.data.providers,
                        revenueKeys: kpisResult.data.revenue ? Object.keys(kpisResult.data.revenue) : [],
                        providersKeys: kpisResult.data.providers ? Object.keys(kpisResult.data.providers) : []
                    });
                }

                setKpis(kpisResult);
                setRevenueTrends(trendsResult);
                setTopBalances(balancesResult);
            } catch (error) {
                console.error('Dashboard: Error fetching dashboard data:', error);
                // Set error states for all failed requests
                setKpis({
                    loading: false,
                    error: error instanceof Error ? error.message : 'Failed to load dashboard data'
                });
                setRevenueTrends({
                    loading: false,
                    error: error instanceof Error ? error.message : 'Failed to load revenue trends'
                });
                setTopBalances({
                    loading: false,
                    error: error instanceof Error ? error.message : 'Failed to load patient balances'
                });
            }
        };

        fetchData();
    }, [startDate, endDate]);

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
                Error loading dashboard data: {errorToString(kpis.error)}
            </Alert>
        );
    }

    if (revenueTrends.error) {
        return (
            <Alert severity="error" sx={{ mb: 2 }}>
                Error loading revenue trends: {errorToString(revenueTrends.error)}
            </Alert>
        );
    }

    // Safety check: ensure data exists before accessing it
    if (!kpis.data) {
        console.warn('Dashboard: kpis.data is missing', { kpis });
        return (
            <Alert severity="error" sx={{ mb: 2 }}>
                No data available. Please try refreshing the page.
            </Alert>
        );
    }

    const data = kpis.data;
    const trendsData = revenueTrends.data || [];

    // Safety check: ensure nested properties exist
    if (!data.revenue || !data.providers) {
        console.warn('Dashboard: Invalid data structure', {
            hasRevenue: !!data.revenue,
            hasProviders: !!data.providers,
            dataKeys: Object.keys(data)
        });
        return (
            <Alert severity="error" sx={{ mb: 2 }}>
                Invalid data structure. Please try refreshing the page.
            </Alert>
        );
    }

    const formatCurrency = (value: number | null | undefined) => {
        if (value === null || value === undefined || isNaN(value)) {
            return '$0';
        }
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
            minimumFractionDigits: 0,
            maximumFractionDigits: 0,
        }).format(value);
    };

    const formatDateRange = () => {
        if (!startDate || !endDate) return 'All Time';
        const start = new Date(startDate).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        const end = new Date(endDate).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
        return `${start} - ${end}`;
    };

    return (
        <Box>
            <Typography variant="h4" component="h1" gutterBottom>
                Executive Dashboard
            </Typography>
            <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                Key performance indicators and analytics overview
            </Typography>

            {/* Date Range Filters */}
            <Card sx={{ mt: 2, mb: 2 }}>
                <CardContent>
                    <Grid container spacing={2} alignItems="center">
                        <Grid item xs={12} sm={6} md={3}>
                            <TextField
                                label="Start Date"
                                type="date"
                                value={startDate}
                                onChange={(e) => setStartDate(e.target.value)}
                                InputLabelProps={{ shrink: true }}
                                fullWidth
                            />
                        </Grid>
                        <Grid item xs={12} sm={6} md={3}>
                            <TextField
                                label="End Date"
                                type="date"
                                value={endDate}
                                onChange={(e) => setEndDate(e.target.value)}
                                InputLabelProps={{ shrink: true }}
                                fullWidth
                            />
                        </Grid>
                        <Grid item xs={12} sm={12} md={6}>
                            <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                                <Chip
                                    label="Previous Business Week"
                                    onClick={() => {
                                        const range = dateUtils.getPreviousBusinessWeek();
                                        if (range.start_date) setStartDate(range.start_date);
                                        if (range.end_date) setEndDate(range.end_date);
                                    }}
                                    variant="outlined"
                                    clickable
                                    color={startDate === previousBusinessWeek.start_date ? 'primary' : 'default'}
                                />
                                <Chip
                                    label="Last 30 Days"
                                    onClick={() => {
                                        const range = dateUtils.getLast30Days();
                                        if (range.start_date) setStartDate(range.start_date);
                                        if (range.end_date) setEndDate(range.end_date);
                                    }}
                                    variant="outlined"
                                    clickable
                                />
                                <Chip
                                    label="Last 90 Days"
                                    onClick={() => {
                                        const range = dateUtils.getLast90Days();
                                        if (range.start_date) setStartDate(range.start_date);
                                        if (range.end_date) setEndDate(range.end_date);
                                    }}
                                    variant="outlined"
                                    clickable
                                />
                                <Chip
                                    label="Last Year"
                                    onClick={() => {
                                        const range = dateUtils.getLastYear();
                                        if (range.start_date) setStartDate(range.start_date);
                                        if (range.end_date) setEndDate(range.end_date);
                                    }}
                                    variant="outlined"
                                    clickable
                                />
                            </Stack>
                        </Grid>
                    </Grid>
                </CardContent>
            </Card>

            {/* Previous Business Week Summary Section */}
            <Card sx={{ mt: 2, mb: 2, bgcolor: 'primary.light', color: 'primary.contrastText' }}>
                <CardContent>
                    <Typography variant="h5" component="h2" gutterBottom>
                        Previous Business Week Summary
                    </Typography>
                    <Typography variant="body2" sx={{ mb: 2, opacity: 0.9 }}>
                        {formatDateRange()}
                    </Typography>
                    <Grid container spacing={3}>
                        <Grid item xs={12} sm={6} md={3}>
                            <Box>
                                <Typography variant="body2" sx={{ opacity: 0.8 }}>
                                    Revenue Lost
                                </Typography>
                                <Typography variant="h4">
                                    {formatCurrency(data.revenue.total_revenue_lost)}
                                </Typography>
                            </Box>
                        </Grid>
                        <Grid item xs={12} sm={6} md={3}>
                            <Box>
                                <Typography variant="body2" sx={{ opacity: 0.8 }}>
                                    Recovery Potential
                                </Typography>
                                <Typography variant="h4">
                                    {formatCurrency(data.revenue.total_recovery_potential)}
                                </Typography>
                            </Box>
                        </Grid>
                        <Grid item xs={12} sm={6} md={3}>
                            <Box>
                                <Typography variant="body2" sx={{ opacity: 0.8 }}>
                                    Recovery Rate
                                </Typography>
                                <Typography variant="h4">
                                    {data.revenue.total_revenue_lost > 0
                                        ? ((data.revenue.total_recovery_potential / data.revenue.total_revenue_lost) * 100).toFixed(1)
                                        : 0}%
                                </Typography>
                            </Box>
                        </Grid>
                        <Grid item xs={12} sm={6} md={3}>
                            <Box>
                                <Typography variant="body2" sx={{ opacity: 0.8 }}>
                                    Active Providers
                                </Typography>
                                <Typography variant="h4">
                                    {data.providers.active_providers}
                                </Typography>
                            </Box>
                        </Grid>
                    </Grid>
                </CardContent>
            </Card>

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
                        value={data.providers.avg_collection_rate ?? 0}
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
                            {trendsData && trendsData.length > 0 ? (
                                <RevenueTrendChart
                                    data={trendsData}
                                    title="Revenue Trend Analysis"
                                    height={400}
                                />
                            ) : (
                                <Typography variant="body2" color="text.secondary">
                                    No trend data available for the selected date range.
                                </Typography>
                            )}
                        </CardContent>
                    </Card>
                </Grid>

                {/* Quick Statistics */}
                <Grid item xs={12} lg={4}>
                    <Card>
                        <CardContent>
                            <Typography variant="h6" gutterBottom>
                                Quick Statistics
                            </Typography>
                            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                                {formatDateRange()}
                            </Typography>
                            <Box sx={{ mt: 2 }}>
                                <Typography variant="body2" color="text.secondary">
                                    Total Production
                                </Typography>
                                <Typography variant="h5" color="primary">
                                    {formatCurrency(data.providers.total_production)}
                                </Typography>
                                <Typography variant="caption" color="text.secondary">
                                    Total billed revenue for services provided
                                </Typography>
                            </Box>
                            <Box sx={{ mt: 2 }}>
                                <Typography variant="body2" color="text.secondary">
                                    Total Collection
                                </Typography>
                                <Typography variant="h5" color="success.main">
                                    {formatCurrency(data.providers.total_collection)}
                                </Typography>
                                <Typography variant="caption" color="text.secondary">
                                    Total cash received from all sources
                                </Typography>
                            </Box>
                            <Box sx={{ mt: 2 }}>
                                <Typography variant="body2" color="text.secondary">
                                    Recovery Rate
                                </Typography>
                                <Typography variant="h5" color="info.main">
                                    {data.revenue.total_revenue_lost > 0
                                        ? ((data.revenue.total_recovery_potential / data.revenue.total_revenue_lost) * 100).toFixed(1)
                                        : 0}%
                                </Typography>
                                <Typography variant="caption" color="text.secondary">
                                    Estimated recoverable revenue percentage
                                </Typography>
                            </Box>
                        </CardContent>
                    </Card>
                </Grid>

                {/* Top 10 Patient Balances */}
                <Grid item xs={12} lg={8}>
                    <Card>
                        <CardContent>
                            <Typography variant="h6" gutterBottom>
                                Top 10 Patient Balances
                            </Typography>
                            {topBalances.loading ? (
                                <Box display="flex" justifyContent="center" alignItems="center" minHeight="200px">
                                    <CircularProgress />
                                </Box>
                            ) : topBalances.error ? (
                                <Alert severity="error" sx={{ mb: 2 }}>
                                    Error loading patient balances: {errorToString(topBalances.error)}
                                </Alert>
                            ) : topBalances.data && topBalances.data.length > 0 ? (
                                <TableContainer component={Paper} variant="outlined">
                                    <Table size="small">
                                        <TableHead>
                                            <TableRow>
                                                <TableCell><strong>Patient ID</strong></TableCell>
                                                <TableCell align="right"><strong>Total Balance</strong></TableCell>
                                                <TableCell align="right"><strong>0-30 Days</strong></TableCell>
                                                <TableCell align="right"><strong>31-60 Days</strong></TableCell>
                                                <TableCell align="right"><strong>61-90 Days</strong></TableCell>
                                                <TableCell align="right"><strong>90+ Days</strong></TableCell>
                                                <TableCell><strong>Risk Category</strong></TableCell>
                                            </TableRow>
                                        </TableHead>
                                        <TableBody>
                                            {topBalances.data.map((patient) => (
                                                <TableRow key={patient.patient_id} hover>
                                                    <TableCell>{patient.patient_id}</TableCell>
                                                    <TableCell align="right">
                                                        <strong>{formatCurrency(patient.total_balance)}</strong>
                                                    </TableCell>
                                                    <TableCell align="right">
                                                        {formatCurrency(patient.balance_0_30_days)}
                                                    </TableCell>
                                                    <TableCell align="right">
                                                        {formatCurrency(patient.balance_31_60_days)}
                                                    </TableCell>
                                                    <TableCell align="right">
                                                        {formatCurrency(patient.balance_61_90_days)}
                                                    </TableCell>
                                                    <TableCell align="right">
                                                        {formatCurrency(patient.balance_over_90_days)}
                                                    </TableCell>
                                                    <TableCell>
                                                        <Chip
                                                            label={patient.aging_risk_category || 'Unknown'}
                                                            size="small"
                                                            color={
                                                                patient.aging_risk_category === 'High Risk' ? 'error' :
                                                                    patient.aging_risk_category === 'Medium Risk' ? 'warning' :
                                                                        patient.aging_risk_category === 'Moderate Risk' ? 'info' :
                                                                            'default'
                                                            }
                                                        />
                                                    </TableCell>
                                                </TableRow>
                                            ))}
                                        </TableBody>
                                    </Table>
                                </TableContainer>
                            ) : (
                                <Typography variant="body2" color="text.secondary">
                                    No patient balances found.
                                </Typography>
                            )}
                        </CardContent>
                    </Card>
                </Grid>
            </Grid>
        </Box>
    );
};

export default Dashboard;
