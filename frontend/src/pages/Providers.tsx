import React, { useState, useEffect } from 'react';
import {
    Box,
    Typography,
    Grid,
    Card,
    CardContent,
    CircularProgress,
    Alert,
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
    People,
    CalendarToday,
    CheckCircle,
    Cancel,
} from '@mui/icons-material';
import KPICard from '../components/charts/KPICard';
import { apiService } from '../services/api';
import { ProviderSummary, ApiResponse } from '../types/api';

const Providers: React.FC = () => {
    const [providersData, setProvidersData] = useState<ApiResponse<ProviderSummary[]>>({ loading: true });

    useEffect(() => {
        const fetchProviders = async () => {
            const response = await apiService.provider.getSummary();
            setProvidersData(response);
        };

        fetchProviders();
    }, []);

    const formatCurrency = (amount: number): string => {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
        }).format(amount);
    };

    const getCompletionRateColor = (rate: number): 'success' | 'warning' | 'error' => {
        if (rate >= 80) return 'success';
        if (rate >= 60) return 'warning';
        return 'error';
    };

    const getNoShowRateColor = (rate: number): 'success' | 'warning' | 'error' => {
        if (rate <= 5) return 'success';
        if (rate <= 10) return 'warning';
        return 'error';
    };

    if (providersData.loading) {
        return (
            <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
                <CircularProgress />
            </Box>
        );
    }

    if (providersData.error) {
        return (
            <Box>
                <Typography variant="h4" component="h1" gutterBottom>
                    Provider Performance
                </Typography>
                <Alert severity="error" sx={{ mt: 2 }}>
                    {providersData.error}
                </Alert>
            </Box>
        );
    }

    const providers = providersData.data || [];

    // Calculate aggregate metrics
    const totalAppointments = providers.reduce((sum, p) => sum + p.total_appointments, 0);
    const totalCompleted = providers.reduce((sum, p) => sum + p.completed_appointments, 0);
    const totalNoShows = providers.reduce((sum, p) => sum + p.no_show_appointments, 0);
    const avgCompletionRate = totalAppointments > 0 ? (totalCompleted / totalAppointments) * 100 : 0;
    const avgNoShowRate = totalAppointments > 0 ? (totalNoShows / totalAppointments) * 100 : 0;

    return (
        <Box>
            <Typography variant="h4" component="h1" gutterBottom>
                Provider Performance
            </Typography>
            <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                Individual provider metrics and performance comparisons
            </Typography>

            {/* KPI Cards */}
            <Grid container spacing={3} sx={{ mt: 2 }}>
                <Grid item xs={12} sm={6} md={3}>
                    <KPICard
                        title="Total Providers"
                        value={providers.length}
                        color="primary"
                        icon={<People />}
                    />
                </Grid>
                <Grid item xs={12} sm={6} md={3}>
                    <KPICard
                        title="Total Appointments"
                        value={totalAppointments.toLocaleString()}
                        color="info"
                        icon={<CalendarToday />}
                        metricName="total_appointments"
                        showLineageTooltip={true}
                    />
                </Grid>
                <Grid item xs={12} sm={6} md={3}>
                    <KPICard
                        title="Completion Rate"
                        value={`${avgCompletionRate.toFixed(1)}%`}
                        color={getCompletionRateColor(avgCompletionRate)}
                        icon={<CheckCircle />}
                        metricName="completion_rate"
                        showLineageTooltip={true}
                    />
                </Grid>
                <Grid item xs={12} sm={6} md={3}>
                    <KPICard
                        title="No-Show Rate"
                        value={`${avgNoShowRate.toFixed(1)}%`}
                        color={getNoShowRateColor(avgNoShowRate)}
                        icon={<Cancel />}
                        metricName="no_show_rate"
                        showLineageTooltip={true}
                    />
                </Grid>
            </Grid>

            {/* Provider Performance Table */}
            <Card sx={{ mt: 3 }}>
                <CardContent>
                    <Typography variant="h6" gutterBottom>
                        Provider Performance Details
                    </Typography>
                    <TableContainer component={Paper} variant="outlined">
                        <Table>
                            <TableHead>
                                <TableRow>
                                    <TableCell>Provider ID</TableCell>
                                    <TableCell align="right">Total Appointments</TableCell>
                                    <TableCell align="right">Completed</TableCell>
                                    <TableCell align="right">Completion Rate</TableCell>
                                    <TableCell align="right">No-Show Rate</TableCell>
                                    <TableCell align="right">Unique Patients</TableCell>
                                    <TableCell align="right">Avg Production</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {providers.map((provider) => (
                                    <TableRow key={provider.provider_id}>
                                        <TableCell>
                                            <Typography variant="body2" fontWeight="medium">
                                                {provider.provider_id}
                                            </Typography>
                                        </TableCell>
                                        <TableCell align="right">
                                            {provider.total_appointments.toLocaleString()}
                                        </TableCell>
                                        <TableCell align="right">
                                            {provider.completed_appointments.toLocaleString()}
                                        </TableCell>
                                        <TableCell align="right">
                                            <Chip
                                                label={`${provider.completion_rate.toFixed(1)}%`}
                                                color={getCompletionRateColor(provider.completion_rate)}
                                                size="small"
                                            />
                                        </TableCell>
                                        <TableCell align="right">
                                            <Chip
                                                label={`${provider.no_show_rate.toFixed(1)}%`}
                                                color={getNoShowRateColor(provider.no_show_rate)}
                                                size="small"
                                            />
                                        </TableCell>
                                        <TableCell align="right">
                                            {provider.unique_patients.toLocaleString()}
                                        </TableCell>
                                        <TableCell align="right">
                                            {formatCurrency(provider.avg_production_per_appointment)}
                                        </TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </TableContainer>
                </CardContent>
            </Card>
        </Box>
    );
};

export default Providers;
