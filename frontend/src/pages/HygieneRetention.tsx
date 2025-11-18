import React, { useState, useEffect } from 'react';
import {
    Box,
    Container,
    Typography,
    Grid,
    Paper,
    Alert,
    CircularProgress,
    TextField,
    Button,
} from '@mui/material';
import HygieneKPICard from '../components/charts/HygieneKPICard';
import MetricsTable from '../components/common/MetricsTable';
import { hygieneApi } from '../services/api';
import { HygieneRetentionSummary } from '../types/api';

const HygieneRetention: React.FC = () => {
    // Initialize with past 30 days as default
    const getDefaultDateRange = () => {
        const end = new Date();
        const start = new Date();
        start.setDate(start.getDate() - 30);
        return { start, end };
    };

    const defaultRange = getDefaultDateRange();
    const [data, setData] = useState<HygieneRetentionSummary | null>(null);
    const [loading, setLoading] = useState<boolean>(true);
    const [error, setError] = useState<string | null>(null);
    const [startDate, setStartDate] = useState<Date | null>(defaultRange.start);
    const [endDate, setEndDate] = useState<Date | null>(defaultRange.end);

    const fetchData = async () => {
        setLoading(true);
        setError(null);

        const params: { start_date?: string; end_date?: string } = {};
        if (startDate) {
            params.start_date = formatDate(startDate);
        }
        if (endDate) {
            params.end_date = formatDate(endDate);
        }

        const response = await hygieneApi.getRetentionSummary(params);

        if (response.error) {
            setError(response.error);
            setData(null);
        } else if (response.data) {
            setData(response.data);
        }

        setLoading(false);
    };

    useEffect(() => {
        fetchData();
    }, []);

    const handleDateRangeChange = () => {
        fetchData();
    };

    const handleResetDates = () => {
        const defaultRange = getDefaultDateRange();
        setStartDate(defaultRange.start);
        setEndDate(defaultRange.end);
        // Fetch with default dates (last 30 days)
        setTimeout(() => {
            fetchData();
        }, 0);
    };

    const formatDate = (date: Date | null): string => {
        if (!date) return '';
        return date.toISOString().split('T')[0];
    };

    const formatDisplayDate = (date: Date | null): string => {
        if (!date) return '';
        return date.toLocaleDateString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric'
        });
    };

    const metrics = data
        ? [
            {
                label: 'Hygiene Patients Seen',
                value: data.hyg_patients_seen,
                isPercentage: false,
            },
            {
                label: 'Hygiene Patients Re-appointed',
                value: data.hyg_pts_reappntd,
                isPercentage: false,
            },
            {
                label: 'Patients with Overdue Recall',
                value: data.recall_overdue_percent,
                isPercentage: true,
            },
            {
                label: 'Patients Not on Recall',
                value: data.not_on_recall_percent,
                isPercentage: true,
            },
        ]
        : [];

    return (
        <Container maxWidth="xl" sx={{ py: 4 }}>
            <Box sx={{ mb: 4 }}>
                <Typography
                    variant="h4"
                    component="h1"
                    sx={{
                        fontWeight: 'bold',
                        color: '#1a237e',
                        mb: 2,
                    }}
                >
                    Hygiene Retention
                </Typography>

                {/* Date Range Filters */}
                <Paper
                    sx={{
                        p: 2,
                        mb: 3,
                        display: 'flex',
                        gap: 2,
                        alignItems: 'center',
                        flexWrap: 'wrap',
                    }}
                >
                    <TextField
                        label="Start Date"
                        type="date"
                        value={formatDate(startDate)}
                        onChange={(e) => setStartDate(e.target.value ? new Date(e.target.value) : null)}
                        InputLabelProps={{ shrink: true }}
                        size="small"
                    />
                    <TextField
                        label="End Date"
                        type="date"
                        value={formatDate(endDate)}
                        onChange={(e) => setEndDate(e.target.value ? new Date(e.target.value) : null)}
                        InputLabelProps={{ shrink: true }}
                        size="small"
                    />
                    <Button
                        variant="contained"
                        onClick={handleDateRangeChange}
                        disabled={loading}
                    >
                        Apply
                    </Button>
                    <Button
                        variant="outlined"
                        onClick={handleResetDates}
                        disabled={loading}
                    >
                        Reset
                    </Button>
                    {startDate && endDate && (
                        <Typography
                            variant="body2"
                            sx={{
                                ml: 'auto',
                                color: '#666',
                                fontWeight: 500,
                            }}
                        >
                            Showing data from {formatDisplayDate(startDate)} to {formatDisplayDate(endDate)}
                        </Typography>
                    )}
                </Paper>
            </Box>

            {loading && (
                <Box
                    sx={{
                        display: 'flex',
                        justifyContent: 'center',
                        alignItems: 'center',
                        minHeight: '400px',
                    }}
                >
                    <CircularProgress />
                </Box>
            )}

            {error && (
                <Alert severity="error" sx={{ mb: 3 }}>
                    {error}
                </Alert>
            )}

            {data && !loading && (
                <>
                    {/* KPI Cards */}
                    <Grid container spacing={3} sx={{ mb: 4 }}>
                        <Grid item xs={12} md={6}>
                            <HygieneKPICard
                                title="Recall Current"
                                value={data.recall_current_percent}
                                isPercentage={true}
                                color="#26a69a"
                            />
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <HygieneKPICard
                                title="Hyg Pre-appmt"
                                value={data.hyg_pre_appointment_percent}
                                isPercentage={true}
                                color="#26a69a"
                            />
                        </Grid>
                    </Grid>

                    {/* Metrics Table */}
                    <Grid container spacing={3}>
                        <Grid item xs={12} md={8}>
                            <Paper
                                sx={{
                                    p: 3,
                                    borderRadius: 2,
                                    boxShadow: 2,
                                }}
                            >
                                <Typography
                                    variant="h6"
                                    sx={{
                                        fontWeight: 'bold',
                                        color: '#1a237e',
                                        mb: 2,
                                    }}
                                >
                                    Metrics
                                </Typography>
                                <MetricsTable metrics={metrics} />
                            </Paper>
                        </Grid>
                    </Grid>
                </>
            )}
        </Container>
    );
};

export default HygieneRetention;

