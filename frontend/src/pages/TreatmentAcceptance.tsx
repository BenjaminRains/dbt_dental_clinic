import React, { useState, useEffect } from 'react';
import {
    Box,
    Typography,
    Card,
    CardContent,
    Grid,
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableHead,
    TableRow,
    Paper,
    CircularProgress,
    Alert,
    FormControl,
    InputLabel,
    Select,
    MenuItem,
    TextField,
    Stack,
    Chip,
} from '@mui/material';
import {
    LineChart,
    Line,
    BarChart,
    Bar,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer,
} from 'recharts';
import { treatmentAcceptanceApi, dateUtils } from '../services/api';
import {
    TreatmentAcceptanceKPISummary,
    TreatmentAcceptanceTrend,
    TreatmentAcceptanceProviderPerformance,
} from '../types/api';

const TreatmentAcceptance: React.FC = () => {
    // State management
    const [kpiSummary, setKpiSummary] = useState<TreatmentAcceptanceKPISummary | null>(null);
    const [trends, setTrends] = useState<TreatmentAcceptanceTrend[]>([]);
    const [providerPerformance, setProviderPerformance] = useState<TreatmentAcceptanceProviderPerformance[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    // Filter state
    // Default to "Last 30 Days" to ensure data is available (synthetic data: 2023-01-01 to now)
    const [startDate, setStartDate] = useState<string>(() => {
        const range = dateUtils.getLast30Days();
        return range.start_date || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    });
    const [endDate, setEndDate] = useState<string>(() => {
        const range = dateUtils.getLast30Days();
        return range.end_date || new Date().toISOString().split('T')[0];
    });
    const [providerId, setProviderId] = useState<number | ''>('');
    const [clinicId, setClinicId] = useState<number | ''>('');
    const [groupBy, setGroupBy] = useState<string>('month');

    // Load data when filters change
    useEffect(() => {
        loadTreatmentAcceptanceData();
    }, [startDate, endDate, providerId, clinicId, groupBy]);

    const loadTreatmentAcceptanceData = async () => {
        setLoading(true);
        setError(null);

        const params: {
            start_date?: string;
            end_date?: string;
            provider_id?: number;
            clinic_id?: number;
            group_by?: string;
        } = {};

        if (startDate) params.start_date = startDate;
        if (endDate) params.end_date = endDate;
        if (providerId) params.provider_id = providerId as number;
        if (clinicId) params.clinic_id = clinicId as number;
        if (groupBy) params.group_by = groupBy;

        try {
            const [kpiResponse, trendsResponse, providerResponse] = await Promise.all([
                treatmentAcceptanceApi.getKPISummary(params),
                treatmentAcceptanceApi.getTrends(params),
                treatmentAcceptanceApi.getProviderPerformance(params),
            ]);

            if (kpiResponse.error) {
                setError(kpiResponse.error);
            } else {
                setKpiSummary(kpiResponse.data || null);
            }

            if (trendsResponse.error) {
                setError(trendsResponse.error);
            } else {
                setTrends(trendsResponse.data || []);
            }

            if (providerResponse.error) {
                setError(providerResponse.error);
            } else {
                setProviderPerformance(providerResponse.data || []);
            }
        } catch (err) {
            setError('Failed to load Treatment Acceptance data');
        } finally {
            setLoading(false);
        }
    };

    const formatCurrency = (amount: number) => {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
            minimumFractionDigits: 0,
            maximumFractionDigits: 0,
        }).format(amount);
    };

    const formatPercentage = (value: number | null) => {
        if (value === null || value === undefined) return 'N/A';
        return `${value.toFixed(1)}%`;
    };

    // Format diagnosis rate with special handling for unreliable rates (> 150%)
    // Diagnosis rate > 100% can occur if patients with exams are presented with treatment multiple times
    const formatDiagnosisRate = (rate: number | null, patientsWithExams: number) => {
        if (rate === null || rate === undefined) return 'N/A';
        // If patients_with_exams is very low (< 10) or rate is extremely high (> 150%), it's unreliable
        if (patientsWithExams < 10 || rate > 150) {
            return 'N/A';
        }
        return `${rate.toFixed(1)}%`;
    };

    const getRateColor = (rate: number | null, thresholds: { good: number; fair: number } = { good: 80, fair: 60 }) => {
        if (rate === null || rate === undefined) return 'default';
        if (rate >= thresholds.good) return 'success';
        if (rate >= thresholds.fair) return 'warning';
        return 'error';
    };

    // Get diagnosis rate color - treat rates > 150% as unreliable (data quality issue)
    const getDiagnosisRateColor = (rate: number | null, patientsWithExams: number) => {
        if (rate === null || rate === undefined) return 'default';
        // If patients_with_exams is very low or rate is extremely high, it's unreliable
        if (patientsWithExams < 10 || rate > 150) {
            return 'default'; // Grey for unreliable data
        }
        // Normal thresholds for reliable diagnosis rates (PBN target: ~50-60%)
        if (rate >= 50) return 'success';
        if (rate >= 40) return 'warning';
        return 'error';
    };

    if (loading) {
        return (
            <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
                <CircularProgress />
            </Box>
        );
    }

    return (
        <Box>
            <Typography variant="h4" component="h1" gutterBottom>
                Treatment Acceptance Dashboard
            </Typography>
            <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                Track treatment acceptance rates and patient engagement metrics
            </Typography>

            {error && (
                <Alert severity="error" sx={{ mt: 2 }} onClose={() => setError(null)}>
                    {error}
                </Alert>
            )}

            {/* Filters */}
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
                        <Grid item xs={12} sm={6} md={2}>
                            <FormControl fullWidth>
                                <InputLabel>Group By</InputLabel>
                                <Select
                                    value={groupBy}
                                    onChange={(e) => setGroupBy(e.target.value)}
                                    label="Group By"
                                >
                                    <MenuItem value="day">Day</MenuItem>
                                    <MenuItem value="week">Week</MenuItem>
                                    <MenuItem value="month">Month</MenuItem>
                                </Select>
                            </FormControl>
                        </Grid>
                        <Grid item xs={12} sm={6} md={2}>
                            <TextField
                                label="Provider ID"
                                type="number"
                                value={providerId}
                                onChange={(e) => setProviderId(e.target.value ? Number(e.target.value) : '')}
                                fullWidth
                            />
                        </Grid>
                        <Grid item xs={12} sm={6} md={2}>
                            <TextField
                                label="Clinic ID"
                                type="number"
                                value={clinicId}
                                onChange={(e) => setClinicId(e.target.value ? Number(e.target.value) : '')}
                                fullWidth
                            />
                        </Grid>
                        <Grid item xs={12}>
                            <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
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
                                <Chip
                                    label="Current Month"
                                    onClick={() => {
                                        const now = new Date();
                                        setStartDate(new Date(now.getFullYear(), now.getMonth(), 1).toISOString().split('T')[0]);
                                        setEndDate(new Date(now.getFullYear(), now.getMonth() + 1, 0).toISOString().split('T')[0]);
                                    }}
                                    variant="outlined"
                                    clickable
                                />
                            </Stack>
                        </Grid>
                    </Grid>
                </CardContent>
            </Card>

            {/* KPI Summary Cards */}
            {kpiSummary && (
                <Grid container spacing={3} sx={{ mt: 2 }}>
                    {/* Primary KPIs (matching PBN) */}
                    <Grid item xs={12} sm={6} md={4}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    Tx Accept %
                                </Typography>
                                <Typography variant="h4" component="div" color={getRateColor(kpiSummary.tx_acceptance_rate, { good: 40, fair: 30 })}>
                                    {formatPercentage(kpiSummary.tx_acceptance_rate)}
                                </Typography>
                                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                                    {formatCurrency(kpiSummary.tx_accepted_amount)} / {formatCurrency(kpiSummary.tx_presented_amount)}
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={4}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    Pt Accept %
                                </Typography>
                                <Typography variant="h4" component="div" color={getRateColor(kpiSummary.patient_acceptance_rate, { good: 80, fair: 70 })}>
                                    {formatPercentage(kpiSummary.patient_acceptance_rate)}
                                </Typography>
                                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                                    {kpiSummary.patients_accepted} / {kpiSummary.patients_presented} patients
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={4}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    Diag %
                                    {(kpiSummary.diagnosis_rate !== null && kpiSummary.diagnosis_rate !== undefined &&
                                        (kpiSummary.patients_seen < 10 || kpiSummary.diagnosis_rate > 150)) && (
                                            <Chip
                                                label="Unreliable"
                                                size="small"
                                                color="default"
                                                sx={{ ml: 1, height: 20, fontSize: '0.65rem' }}
                                            />
                                        )}
                                </Typography>
                                <Typography
                                    variant="h4"
                                    component="div"
                                    color={getDiagnosisRateColor(kpiSummary.diagnosis_rate, kpiSummary.patients_with_exams || 0)}
                                >
                                    {formatDiagnosisRate(kpiSummary.diagnosis_rate, kpiSummary.patients_with_exams || 0)}
                                </Typography>
                                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                                    {kpiSummary.patients_with_exams_and_presented || 0} / {kpiSummary.patients_with_exams || 0} patients with exams
                                </Typography>
                                {(kpiSummary.diagnosis_rate !== null && kpiSummary.diagnosis_rate !== undefined &&
                                    ((kpiSummary.patients_with_exams || 0) < 10 || kpiSummary.diagnosis_rate > 150)) && (
                                        <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
                                            Rate {'>'} 150% or low patient count indicates data quality issues
                                        </Typography>
                                    )}
                            </CardContent>
                        </Card>
                    </Grid>

                    {/* Supporting Metrics */}
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    Patients Seen
                                </Typography>
                                <Typography variant="h5" component="div">
                                    {kpiSummary.patients_seen.toLocaleString()}
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    Tx Presented
                                </Typography>
                                <Typography variant="h5" component="div">
                                    {formatCurrency(kpiSummary.tx_presented_amount)}
                                </Typography>
                                <Typography variant="body2" color="text.secondary">
                                    {kpiSummary.procedures_presented.toLocaleString()} procedures
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    Tx Accepted
                                </Typography>
                                <Typography variant="h5" component="div" color="success.main">
                                    {formatCurrency(kpiSummary.tx_accepted_amount)}
                                </Typography>
                                <Typography variant="body2" color="text.secondary">
                                    {kpiSummary.procedures_accepted.toLocaleString()} procedures
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    Pts Presented
                                </Typography>
                                <Typography variant="h5" component="div">
                                    {kpiSummary.patients_presented.toLocaleString()}
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    Pts Accepted
                                </Typography>
                                <Typography variant="h5" component="div" color="success.main">
                                    {kpiSummary.patients_accepted.toLocaleString()}
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    Same Day Treatment
                                </Typography>
                                <Typography variant="h5" component="div">
                                    {formatCurrency(kpiSummary.same_day_treatment_amount)}
                                </Typography>
                                {kpiSummary.same_day_treatment_rate !== null && (
                                    <Typography variant="body2" color="text.secondary">
                                        {formatPercentage(kpiSummary.same_day_treatment_rate)} of presented
                                    </Typography>
                                )}
                            </CardContent>
                        </Card>
                    </Grid>
                </Grid>
            )}

            {/* Trending Charts */}
            {trends.length > 0 && (
                <Grid container spacing={3} sx={{ mt: 2 }}>
                    <Grid item xs={12} md={6}>
                        <Card>
                            <CardContent>
                                <Typography variant="h6" gutterBottom>
                                    Treatment Acceptance Rates Over Time
                                </Typography>
                                <ResponsiveContainer width="100%" height={300}>
                                    <LineChart data={trends}>
                                        <CartesianGrid strokeDasharray="3 3" />
                                        <XAxis
                                            dataKey="date"
                                            tick={{ fontSize: 12 }}
                                        />
                                        <YAxis
                                            tick={{ fontSize: 12 }}
                                            domain={[0, 100]}
                                        />
                                        <Tooltip
                                            formatter={(value: any) => {
                                                if (value === null || value === undefined) return 'N/A';
                                                return formatPercentage(value as number);
                                            }}
                                        />
                                        <Legend />
                                        <Line
                                            type="monotone"
                                            dataKey="tx_acceptance_rate"
                                            stroke="#1976d2"
                                            name="Tx Accept %"
                                            strokeWidth={2}
                                        />
                                        <Line
                                            type="monotone"
                                            dataKey="patient_acceptance_rate"
                                            stroke="#388e3c"
                                            name="Pt Accept %"
                                            strokeWidth={2}
                                        />
                                    </LineChart>
                                </ResponsiveContainer>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} md={6}>
                        <Card>
                            <CardContent>
                                <Typography variant="h6" gutterBottom>
                                    Treatment Amounts Over Time
                                </Typography>
                                <ResponsiveContainer width="100%" height={300}>
                                    <BarChart data={trends}>
                                        <CartesianGrid strokeDasharray="3 3" />
                                        <XAxis
                                            dataKey="date"
                                            tick={{ fontSize: 12 }}
                                        />
                                        <YAxis
                                            tickFormatter={(value) => formatCurrency(value)}
                                            tick={{ fontSize: 12 }}
                                        />
                                        <Tooltip
                                            formatter={(value: number) => formatCurrency(value)}
                                        />
                                        <Legend />
                                        <Bar
                                            dataKey="tx_presented_amount"
                                            fill="#1976d2"
                                            name="Tx Presented"
                                            radius={[4, 4, 0, 0]}
                                        />
                                        <Bar
                                            dataKey="tx_accepted_amount"
                                            fill="#388e3c"
                                            name="Tx Accepted"
                                            radius={[4, 4, 0, 0]}
                                        />
                                    </BarChart>
                                </ResponsiveContainer>
                            </CardContent>
                        </Card>
                    </Grid>
                </Grid>
            )}

            {/* Provider Performance Table */}
            {providerPerformance.length > 0 && (
                <Card sx={{ mt: 3 }}>
                    <CardContent>
                        <Typography variant="h6" gutterBottom>
                            Provider Performance
                        </Typography>
                        <TableContainer component={Paper}>
                            <Table>
                                <TableHead>
                                    <TableRow>
                                        <TableCell>Provider ID</TableCell>
                                        <TableCell align="right">Tx Accept %</TableCell>
                                        <TableCell align="right">Pt Accept %</TableCell>
                                        <TableCell align="right">Diag %</TableCell>
                                        <TableCell align="right">Tx Presented</TableCell>
                                        <TableCell align="right">Tx Accepted</TableCell>
                                        <TableCell align="right">Patients Seen</TableCell>
                                        <TableCell align="right">Patients Presented</TableCell>
                                        <TableCell align="right">Patients Accepted</TableCell>
                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {providerPerformance.map((provider) => (
                                        <TableRow key={provider.provider_id}>
                                            <TableCell>{provider.provider_id}</TableCell>
                                            <TableCell align="right">
                                                <Chip
                                                    label={formatPercentage(provider.tx_acceptance_rate)}
                                                    color={getRateColor(provider.tx_acceptance_rate, { good: 40, fair: 30 })}
                                                    size="small"
                                                />
                                            </TableCell>
                                            <TableCell align="right">
                                                <Chip
                                                    label={formatPercentage(provider.patient_acceptance_rate)}
                                                    color={getRateColor(provider.patient_acceptance_rate, { good: 80, fair: 70 })}
                                                    size="small"
                                                />
                                            </TableCell>
                                            <TableCell align="right">
                                                <Chip
                                                    label={formatDiagnosisRate(provider.diagnosis_rate, provider.patients_with_exams || 0)}
                                                    color={getDiagnosisRateColor(provider.diagnosis_rate, provider.patients_with_exams || 0)}
                                                    size="small"
                                                    title={
                                                        provider.diagnosis_rate !== null && provider.diagnosis_rate !== undefined &&
                                                            ((provider.patients_with_exams || 0) < 10 || provider.diagnosis_rate > 150)
                                                            ? 'Unreliable: Rate > 150% or low patient count indicates data quality issues'
                                                            : undefined
                                                    }
                                                />
                                            </TableCell>
                                            <TableCell align="right">{formatCurrency(provider.tx_presented_amount)}</TableCell>
                                            <TableCell align="right">{formatCurrency(provider.tx_accepted_amount)}</TableCell>
                                            <TableCell align="right">{provider.patients_seen.toLocaleString()}</TableCell>
                                            <TableCell align="right">{provider.patients_presented.toLocaleString()}</TableCell>
                                            <TableCell align="right">{provider.patients_accepted.toLocaleString()}</TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </TableContainer>
                    </CardContent>
                </Card>
            )}
        </Box>
    );
};

export default TreatmentAcceptance;

