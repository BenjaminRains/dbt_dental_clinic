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
    Chip,
    CircularProgress,
    Alert,
    FormControl,
    InputLabel,
    Select,
    MenuItem,
    TextField,
    Slider,
    Stack,
    Pagination,
    Switch,
    FormControlLabel,
    Collapse
} from '@mui/material';
import {
    BarChart,
    Bar,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer,
    PieChart,
    Pie,
    Cell,
    LineChart,
    Line
} from 'recharts';
import { arApi, dateUtils } from '../services/api';
import {
    ARKPISummary,
    ARAgingSummary,
    ARPriorityQueueItem,
    ARRiskDistribution,
    ARAgingTrend,
    DateRange
} from '../types/api';

const AR: React.FC = () => {
    // State management
    const [kpiSummary, setKpiSummary] = useState<ARKPISummary | null>(null);
    const [agingSummary, setAgingSummary] = useState<ARAgingSummary[]>([]);
    const [priorityQueue, setPriorityQueue] = useState<ARPriorityQueueItem[]>([]);
    const [riskDistribution, setRiskDistribution] = useState<ARRiskDistribution[]>([]);
    const [agingTrends, setAgingTrends] = useState<ARAgingTrend[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    // Filter state
    const [minPriorityScore, setMinPriorityScore] = useState<number>(50);
    const [riskCategory, setRiskCategory] = useState<string>('');
    const [minBalance, setMinBalance] = useState<number>(0);
    const [selectedProvider, setSelectedProvider] = useState<number | ''>('');
    const [currentPage, setCurrentPage] = useState(1);
    const [pageSize] = useState(20);

    // Date range state
    const [useDateRange, setUseDateRange] = useState<boolean>(false);
    const [dateRange, setDateRange] = useState<DateRange>({});

    // Load available snapshot dates on mount
    useEffect(() => {
        const loadSnapshotDates = async () => {
            const response = await arApi.getSnapshotDates();
            if (!response.error && response.data) {
                // Snapshot dates loaded (available for future use if needed)
                // const availableDates = response.data.map(d => d.snapshot_date).filter(Boolean);
            }
        };
        loadSnapshotDates();
    }, []);

    useEffect(() => {
        loadARData();
    }, [minPriorityScore, riskCategory, minBalance, selectedProvider, currentPage, useDateRange, dateRange]);

    const loadARData = async () => {
        setLoading(true);
        setError(null);

        // Build date range params if toggle is enabled
        const dateParams: DateRange = useDateRange && dateRange.start_date && dateRange.end_date
            ? { start_date: dateRange.start_date, end_date: dateRange.end_date }
            : {};

        try {
            const [kpiResponse, agingResponse, priorityResponse, riskResponse, trendsResponse] = await Promise.all([
                arApi.getKPISummary(dateParams),
                arApi.getAgingSummary(dateParams),
                arApi.getPriorityQueue((currentPage - 1) * pageSize, pageSize, {
                    min_priority_score: minPriorityScore,
                    risk_category: riskCategory || undefined,
                    min_balance: minBalance || undefined,
                    provider_id: selectedProvider || undefined
                }),
                arApi.getRiskDistribution(dateParams),
                arApi.getAgingTrends(dateParams)
            ]);

            if (kpiResponse.error) {
                setError(kpiResponse.error);
            } else {
                setKpiSummary(kpiResponse.data || null);
            }

            if (agingResponse.error) {
                setError(agingResponse.error);
                console.error('AR Aging Summary Error:', agingResponse.error);
            } else {
                console.log('AR Aging Summary Data:', agingResponse.data);
                setAgingSummary(agingResponse.data || []);
            }

            if (priorityResponse.error) {
                setError(priorityResponse.error);
            } else {
                setPriorityQueue(priorityResponse.data || []);
            }

            if (riskResponse.error) {
                setError(riskResponse.error);
            } else {
                setRiskDistribution(riskResponse.data || []);
            }

            if (trendsResponse.error) {
                setError(trendsResponse.error);
            } else {
                setAgingTrends(trendsResponse.data || []);
            }
        } catch (err) {
            setError('Failed to load AR data');
        } finally {
            setLoading(false);
        }
    };

    const formatCurrency = (amount: number) => {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD'
        }).format(amount);
    };

    const getRiskCategoryColor = (category: string) => {
        switch (category.toLowerCase()) {
            case 'high risk': return 'error';
            case 'medium risk': return 'warning';
            case 'moderate risk': return 'info';
            case 'low risk': return 'success';
            default: return 'default';
        }
    };

    const getPriorityScoreColor = (score: number) => {
        if (score >= 80) return 'error';
        if (score >= 60) return 'warning';
        if (score >= 40) return 'info';
        return 'default';
    };

    // Pie chart colors for risk distribution
    const COLORS = ['#d32f2f', '#f57c00', '#fbc02d', '#388e3c', '#1976d2'];

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
                AR Aging Dashboard
            </Typography>
            <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                Accounts Receivable analysis and collection prioritization
            </Typography>

            {error && (
                <Alert severity="error" sx={{ mt: 2 }} onClose={() => setError(null)}>
                    {error}
                </Alert>
            )}

            {/* Date Range Filter */}
            <Card sx={{ mt: 2, mb: 2 }}>
                <CardContent>
                    <Grid container spacing={2} alignItems="center">
                        <Grid item xs={12} sm={6} md={3}>
                            <FormControlLabel
                                control={
                                    <Switch
                                        checked={useDateRange}
                                        onChange={(e) => {
                                            setUseDateRange(e.target.checked);
                                            if (!e.target.checked) {
                                                setDateRange({});
                                            } else if (!dateRange.start_date || !dateRange.end_date) {
                                                // Set default to last 30 days if toggling on
                                                const defaultRange = dateUtils.getLast30Days();
                                                setDateRange(defaultRange);
                                            }
                                        }}
                                    />
                                }
                                label="Use Date Range Filter"
                            />
                        </Grid>
                        <Grid item xs={12} sm={6} md={9}>
                            <Collapse in={useDateRange}>
                                <Grid container spacing={2}>
                                    <Grid item xs={12} sm={6} md={4}>
                                        <TextField
                                            label="Start Date"
                                            type="date"
                                            value={dateRange.start_date || ''}
                                            onChange={(e) => setDateRange({ ...dateRange, start_date: e.target.value })}
                                            InputLabelProps={{ shrink: true }}
                                            fullWidth
                                            disabled={!useDateRange}
                                        />
                                    </Grid>
                                    <Grid item xs={12} sm={6} md={4}>
                                        <TextField
                                            label="End Date"
                                            type="date"
                                            value={dateRange.end_date || ''}
                                            onChange={(e) => setDateRange({ ...dateRange, end_date: e.target.value })}
                                            InputLabelProps={{ shrink: true }}
                                            fullWidth
                                            disabled={!useDateRange}
                                        />
                                    </Grid>
                                    <Grid item xs={12} sm={6} md={4}>
                                        <Stack direction="row" spacing={1}>
                                            <FormControl fullWidth size="small">
                                                <InputLabel>Quick Select</InputLabel>
                                                <Select
                                                    value=""
                                                    onChange={(e) => {
                                                        const value = e.target.value;
                                                        if (value === 'getLast30Days') {
                                                            setDateRange(dateUtils.getLast30Days());
                                                            setUseDateRange(true);
                                                        } else if (value === 'getLast90Days') {
                                                            setDateRange(dateUtils.getLast90Days());
                                                            setUseDateRange(true);
                                                        } else if (value === 'getLastYear') {
                                                            setDateRange(dateUtils.getLastYear());
                                                            setUseDateRange(true);
                                                        }
                                                    }}
                                                    label="Quick Select"
                                                    disabled={!useDateRange}
                                                >
                                                    <MenuItem value="getLast30Days">Last 30 Days</MenuItem>
                                                    <MenuItem value="getLast90Days">Last 90 Days</MenuItem>
                                                    <MenuItem value="getLastYear">Last Year</MenuItem>
                                                </Select>
                                            </FormControl>
                                        </Stack>
                                    </Grid>
                                </Grid>
                            </Collapse>
                        </Grid>
                    </Grid>
                </CardContent>
            </Card>

            {/* KPI Summary Cards */}
            {kpiSummary && (
                <Grid container spacing={3} sx={{ mt: 2 }}>
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    Total AR Outstanding
                                </Typography>
                                <Typography variant="h5" component="div">
                                    {formatCurrency(kpiSummary.total_ar_outstanding)}
                                </Typography>
                                {kpiSummary.patient_ar !== undefined && kpiSummary.insurance_ar !== undefined && (
                                    <>
                                        <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                                            Patient: {formatCurrency(kpiSummary.patient_ar)}
                                        </Typography>
                                        <Typography variant="body2" color="text.secondary">
                                            Insurance: {formatCurrency(kpiSummary.insurance_ar)}
                                        </Typography>
                                    </>
                                )}
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    Current
                                </Typography>
                                <Typography variant="h5" component="div">
                                    {formatCurrency(kpiSummary.current_amount)}
                                </Typography>
                                <Typography variant="body2" color="text.secondary">
                                    {kpiSummary.current_percentage.toFixed(1)}%
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    90 Day
                                </Typography>
                                <Typography variant="h5" component="div" color="error">
                                    {formatCurrency(kpiSummary.over_90_amount)}
                                </Typography>
                                <Typography variant="body2" color="text.secondary">
                                    {kpiSummary.over_90_percentage.toFixed(1)}%
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    AR Days
                                </Typography>
                                <Typography variant="h5" component="div">
                                    {kpiSummary.dso_days.toFixed(1)} days
                                </Typography>
                                <Typography variant="body2" color="text.secondary">
                                    Collection Rate: {kpiSummary.collection_rate.toFixed(1)}%
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    AR Ratio
                                </Typography>
                                <Typography variant="h5" component="div">
                                    {(kpiSummary.ar_ratio ?? 0).toFixed(1)}%
                                </Typography>
                                <Typography variant="body2" color="text.secondary">
                                    Current Month: Collections / Production
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    High Risk Count
                                </Typography>
                                <Typography variant="h5" component="div" color="error">
                                    {kpiSummary.high_risk_count}
                                </Typography>
                                <Typography variant="body2" color="text.secondary">
                                    {formatCurrency(kpiSummary.high_risk_amount)}
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                </Grid>
            )}

            {/* Charts Row */}
            <Grid container spacing={3} sx={{ mt: 2 }}>
                {/* AR Aging Chart */}
                <Grid item xs={12} md={8}>
                    <Card>
                        <CardContent>
                            <Typography variant="h6" gutterBottom>
                                AR Aging Summary
                            </Typography>
                            <ResponsiveContainer width="100%" height={300}>
                                <BarChart
                                    data={agingSummary.length > 0 ? agingSummary : [
                                        { aging_bucket: "Current", amount: 0 },
                                        { aging_bucket: "30 Day", amount: 0 },
                                        { aging_bucket: "60 Day", amount: 0 },
                                        { aging_bucket: "90 Day", amount: 0 }
                                    ]}
                                    margin={{ left: 20, right: 20, top: 20, bottom: 20 }}
                                >
                                    <CartesianGrid strokeDasharray="3 3" />
                                    <XAxis
                                        dataKey="aging_bucket"
                                        tick={{ fontSize: 12 }}
                                    />
                                    <YAxis
                                        tickFormatter={(value) => formatCurrency(value)}
                                        domain={[
                                            0,
                                            (dataMax: number) => {
                                                // Add 10% padding to the top so bars don't touch the top
                                                return Math.ceil(dataMax * 1.1);
                                            }
                                        ]}
                                        width={80}
                                        tick={{ fontSize: 12 }}
                                    />
                                    <Tooltip
                                        formatter={(value: number) => formatCurrency(value)}
                                        labelFormatter={(label) => `Bucket: ${label}`}
                                    />
                                    <Legend />
                                    <Bar
                                        dataKey="amount"
                                        fill="#1976d2"
                                        name="Amount ($)"
                                        radius={[4, 4, 0, 0]}
                                    />
                                </BarChart>
                            </ResponsiveContainer>
                            {/* Debug: Show raw data */}
                            {import.meta.env.DEV && agingSummary.length === 0 && (
                                <Typography variant="caption" color="error" sx={{ mt: 1 }}>
                                    No data available. Check console for API response.
                                </Typography>
                            )}
                        </CardContent>
                    </Card>
                </Grid>

                {/* Risk Distribution Pie Chart */}
                <Grid item xs={12} md={4}>
                    <Card>
                        <CardContent>
                            <Typography variant="h6" gutterBottom>
                                Risk Distribution
                            </Typography>
                            <ResponsiveContainer width="100%" height={300}>
                                <PieChart>
                                    <Pie
                                        data={riskDistribution}
                                        cx="50%"
                                        cy="50%"
                                        labelLine={false}
                                        label={({ name, percentage }: { name: string; percentage: number }) => `${name}: ${percentage.toFixed(1)}%`}
                                        outerRadius={80}
                                        fill="#8884d8"
                                        dataKey="total_amount"
                                    >
                                        {riskDistribution.map((_, index) => (
                                            <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                                        ))}
                                    </Pie>
                                    <Tooltip formatter={(value: number) => formatCurrency(value)} />
                                </PieChart>
                            </ResponsiveContainer>
                        </CardContent>
                    </Card>
                </Grid>
            </Grid>

            {/* Filters and Priority Queue */}
            <Grid container spacing={3} sx={{ mt: 2 }}>
                {/* Filters */}
                <Grid item xs={12}>
                    <Card>
                        <CardContent>
                            <Typography variant="h6" gutterBottom>
                                Filters
                            </Typography>
                            <Grid container spacing={2}>
                                <Grid item xs={12} sm={6} md={3}>
                                    <FormControl fullWidth>
                                        <InputLabel>Risk Category</InputLabel>
                                        <Select
                                            value={riskCategory}
                                            onChange={(e) => setRiskCategory(e.target.value)}
                                            label="Risk Category"
                                        >
                                            <MenuItem value="">All</MenuItem>
                                            <MenuItem value="High Risk">High Risk</MenuItem>
                                            <MenuItem value="Medium Risk">Medium Risk</MenuItem>
                                            <MenuItem value="Moderate Risk">Moderate Risk</MenuItem>
                                            <MenuItem value="Low Risk">Low Risk</MenuItem>
                                        </Select>
                                    </FormControl>
                                </Grid>
                                <Grid item xs={12} sm={6} md={3}>
                                    <Typography gutterBottom>
                                        Min Priority Score: {minPriorityScore}
                                    </Typography>
                                    <Slider
                                        value={minPriorityScore}
                                        onChange={(_, value) => setMinPriorityScore(value as number)}
                                        min={0}
                                        max={100}
                                        step={5}
                                        marks={[
                                            { value: 0, label: '0' },
                                            { value: 50, label: '50' },
                                            { value: 100, label: '100' }
                                        ]}
                                    />
                                </Grid>
                                <Grid item xs={12} sm={6} md={3}>
                                    <TextField
                                        fullWidth
                                        label="Min Balance ($)"
                                        type="number"
                                        value={minBalance}
                                        onChange={(e) => setMinBalance(parseFloat(e.target.value) || 0)}
                                    />
                                </Grid>
                                <Grid item xs={12} sm={6} md={3}>
                                    <TextField
                                        fullWidth
                                        label="Provider ID"
                                        type="number"
                                        value={selectedProvider}
                                        onChange={(e) => setSelectedProvider(e.target.value ? parseInt(e.target.value) : '')}
                                    />
                                </Grid>
                            </Grid>
                        </CardContent>
                    </Card>
                </Grid>

                {/* Priority Queue Table */}
                <Grid item xs={12}>
                    <Card>
                        <CardContent>
                            <Typography variant="h6" gutterBottom>
                                Collection Priority Queue
                            </Typography>
                            <TableContainer component={Paper} variant="outlined">
                                <Table>
                                    <TableHead>
                                        <TableRow>
                                            <TableCell>Patient ID</TableCell>
                                            <TableCell>Provider ID</TableCell>
                                            <TableCell align="right">Total Balance</TableCell>
                                            <TableCell align="right">Current</TableCell>
                                            <TableCell align="right">30 Day</TableCell>
                                            <TableCell align="right">60 Day</TableCell>
                                            <TableCell align="right">90 Day</TableCell>
                                            <TableCell>Risk Category</TableCell>
                                            <TableCell align="right">Priority Score</TableCell>
                                            <TableCell>Payment Recency</TableCell>
                                        </TableRow>
                                    </TableHead>
                                    <TableBody>
                                        {priorityQueue.length === 0 ? (
                                            <TableRow>
                                                <TableCell colSpan={10} align="center">
                                                    No records found
                                                </TableCell>
                                            </TableRow>
                                        ) : (
                                            priorityQueue.map((item) => (
                                                <TableRow key={`${item.patient_id}-${item.provider_id}`} hover>
                                                    <TableCell>{item.patient_id}</TableCell>
                                                    <TableCell>{item.provider_id}</TableCell>
                                                    <TableCell align="right">
                                                        <Typography variant="body2" fontWeight="bold">
                                                            {formatCurrency(item.total_balance)}
                                                        </Typography>
                                                    </TableCell>
                                                    <TableCell align="right">
                                                        {formatCurrency(item.balance_0_30_days)}
                                                    </TableCell>
                                                    <TableCell align="right">
                                                        {formatCurrency(item.balance_31_60_days)}
                                                    </TableCell>
                                                    <TableCell align="right">
                                                        {formatCurrency(item.balance_61_90_days)}
                                                    </TableCell>
                                                    <TableCell align="right" sx={{ color: 'error.main' }}>
                                                        {formatCurrency(item.balance_over_90_days)}
                                                    </TableCell>
                                                    <TableCell>
                                                        <Chip
                                                            label={item.aging_risk_category}
                                                            color={getRiskCategoryColor(item.aging_risk_category)}
                                                            size="small"
                                                        />
                                                    </TableCell>
                                                    <TableCell align="right">
                                                        <Chip
                                                            label={item.collection_priority_score}
                                                            color={getPriorityScoreColor(item.collection_priority_score)}
                                                            size="small"
                                                        />
                                                    </TableCell>
                                                    <TableCell>{item.payment_recency}</TableCell>
                                                </TableRow>
                                            ))
                                        )}
                                    </TableBody>
                                </Table>
                            </TableContainer>
                            <Stack spacing={2} sx={{ mt: 2, alignItems: 'center' }}>
                                <Pagination
                                    count={Math.ceil(priorityQueue.length / pageSize) || 1}
                                    page={currentPage}
                                    onChange={(_, page) => setCurrentPage(page)}
                                    color="primary"
                                />
                            </Stack>
                        </CardContent>
                    </Card>
                </Grid>

                {/* Aging Trends Chart */}
                {agingTrends.length > 0 && (
                    <Grid item xs={12}>
                        <Card>
                            <CardContent>
                                <Typography variant="h6" gutterBottom>
                                    AR Aging Trends
                                </Typography>
                                <ResponsiveContainer width="100%" height={400}>
                                    <LineChart data={agingTrends}>
                                        <CartesianGrid strokeDasharray="3 3" />
                                        <XAxis dataKey="date" />
                                        <YAxis />
                                        <Tooltip formatter={(value: number) => formatCurrency(value)} />
                                        <Legend />
                                        <Line
                                            type="monotone"
                                            dataKey="current_amount"
                                            stroke="#388e3c"
                                            name="Current"
                                            strokeWidth={2}
                                        />
                                        <Line
                                            type="monotone"
                                            dataKey="over_30_amount"
                                            stroke="#fbc02d"
                                            name="Over 30 Days"
                                            strokeWidth={2}
                                        />
                                        <Line
                                            type="monotone"
                                            dataKey="over_60_amount"
                                            stroke="#f57c00"
                                            name="Over 60 Days"
                                            strokeWidth={2}
                                        />
                                        <Line
                                            type="monotone"
                                            dataKey="over_90_amount"
                                            stroke="#d32f2f"
                                            name="90 Day"
                                            strokeWidth={2}
                                        />
                                    </LineChart>
                                </ResponsiveContainer>
                            </CardContent>
                        </Card>
                    </Grid>
                )}
            </Grid>
        </Box>
    );
};

export default AR;

