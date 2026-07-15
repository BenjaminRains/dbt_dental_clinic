import React, { useCallback, useEffect, useMemo, useState } from 'react';
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
    Stack,
    MenuItem,
    FormControlLabel,
    Switch,
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableHead,
    TableRow,
    TableSortLabel,
    Link as MuiLink,
} from '@mui/material';
import { Link } from 'react-router-dom';
import {
    LineChart,
    Line,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer,
} from 'recharts';
import { referralApi } from '@mdc/analytics-api';
import { ReferralSourceKPIRow, ReferralSourceMonthlySummary } from '@mdc/analytics-api';
import KPICard from '../charts/KPICard';

const PERIOD_BASIS_OPTIONS = [
    { value: '', label: 'All bases' },
    { value: 'referral_link', label: 'Referral link in month' },
    { value: 'new_patient_first_visit', label: 'New patient first visit' },
    { value: 'production_in_period', label: 'Production in month' },
];

const SEGMENT_OPTIONS = [
    { value: '', label: 'All segments' },
    { value: 'physician_or_clinical', label: 'Physician / clinical' },
    { value: 'organization', label: 'Organization' },
    { value: 'individual', label: 'Individual' },
];

const CHART_COLORS = ['#1976d2', '#2e7d32', '#ed6c02'];

type OrderDir = 'asc' | 'desc';

function monthInputToFirstOfMonth(ym: string): string | undefined {
    if (!ym || ym.length < 7) return undefined;
    return `${ym}-01`;
}

const ReferralSources: React.FC = () => {
    const defaultEnd = new Date();
    const defaultStart = new Date();
    defaultStart.setMonth(defaultStart.getMonth() - 11);
    const toMonthStr = (d: Date) => d.toISOString().slice(0, 7);

    const [startMonth, setStartMonth] = useState(toMonthStr(defaultStart));
    const [endMonth, setEndMonth] = useState(toMonthStr(defaultEnd));
    const [periodBasis, setPeriodBasis] = useState('');
    const [segment, setSegment] = useState('');
    const [referralSearch, setReferralSearch] = useState('');
    const [referralId, setReferralId] = useState('');
    const [isDoctorOnly, setIsDoctorOnly] = useState(false);
    const [metric, setMetric] = useState<'collections' | 'production'>('collections');

    const [summaryRows, setSummaryRows] = useState<ReferralSourceMonthlySummary[]>([]);
    const [patientNote, setPatientNote] = useState('');
    const [detailRows, setDetailRows] = useState<ReferralSourceKPIRow[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    const [orderBy, setOrderBy] = useState<keyof ReferralSourceKPIRow>('net_collections_in_period');
    const [orderDir, setOrderDir] = useState<OrderDir>('desc');

    const buildParams = useCallback(() => {
        const p: Record<string, string | number | boolean> = {};
        const sm = monthInputToFirstOfMonth(startMonth);
        const em = monthInputToFirstOfMonth(endMonth);
        if (sm) p.start_month = sm;
        if (em) p.end_month = em;
        if (periodBasis) p.period_basis = periodBasis;
        if (segment) p.referral_source_segment = segment;
        if (referralSearch.trim()) p.referral_search = referralSearch.trim();
        const rid = referralId.trim() ? parseInt(referralId.trim(), 10) : NaN;
        if (!Number.isNaN(rid)) p.referral_id = rid;
        if (isDoctorOnly) p.is_doctor_only = true;
        return p;
    }, [startMonth, endMonth, periodBasis, segment, referralSearch, referralId, isDoctorOnly]);

    const fetchData = async () => {
        setLoading(true);
        setError(null);
        const params = buildParams();
        const [sumRes, kpiRes] = await Promise.all([
            referralApi.getSummary(params),
            referralApi.getKpis({ ...params, limit: 5000, offset: 0 }),
        ]);
        const errMsg = sumRes.error || kpiRes.error || null;
        if (sumRes.data) {
            setSummaryRows(sumRes.data.rows);
            setPatientNote(sumRes.data.patient_count_note);
        } else {
            setSummaryRows([]);
            setPatientNote('');
        }
        if (kpiRes.data) {
            setDetailRows(kpiRes.data);
        } else {
            setDetailRows([]);
        }
        setError(errMsg);
        setLoading(false);
    };

    useEffect(() => {
        fetchData();
    }, []);

    const { chartData, chartLines } = useMemo(() => {
        const orderMap = new Map<string, number>();
        summaryRows.forEach((r) => {
            if (!orderMap.has(r.period_basis)) {
                orderMap.set(r.period_basis, r.period_basis_sort_order);
            }
        });
        const lines = [...orderMap.entries()]
            .sort((a, b) => a[1] - b[1])
            .map(([basis]) => basis);
        const valueKey: 'total_net_collections' | 'total_production_value' =
            metric === 'collections' ? 'total_net_collections' : 'total_production_value';
        const months = [...new Set(summaryRows.map((r) => r.reporting_year_month))].sort();
        const data = months.map((m) => {
            const point: Record<string, string | number> = { month: m };
            for (const b of lines) {
                const row = summaryRows.find((r) => r.reporting_year_month === m && r.period_basis === b);
                point[b] = row ? row[valueKey] : 0;
            }
            return point;
        });
        return { chartData: data, chartLines: lines };
    }, [summaryRows, metric]);

    const kpiTotals = useMemo(() => {
        let prod = 0;
        let coll = 0;
        let summedPatients = 0;
        summaryRows.forEach((r) => {
            prod += r.total_production_value;
            coll += r.total_net_collections;
            summedPatients += r.summed_distinct_patient_count;
        });
        const singleReferrer = Boolean(referralId.trim() && !Number.isNaN(parseInt(referralId.trim(), 10)));
        return { prod, coll, summedPatients, singleReferrer };
    }, [summaryRows, referralId]);

    const sortedDetails = useMemo(() => {
        const copy = [...detailRows];
        copy.sort((a, b) => {
            const av = a[orderBy];
            const bv = b[orderBy];
            const c = typeof av === 'number' && typeof bv === 'number' ? av - bv : String(av).localeCompare(String(bv));
            return orderDir === 'asc' ? c : -c;
        });
        return copy;
    }, [detailRows, orderBy, orderDir]);

    const handleSort = (col: keyof ReferralSourceKPIRow) => {
        if (orderBy === col) {
            setOrderDir((d) => (d === 'asc' ? 'desc' : 'asc'));
        } else {
            setOrderBy(col);
            setOrderDir('desc');
        }
    };

    const exportCsv = () => {
        const headers = [
            'reporting_year_month',
            'referral_id',
            'referral_display_name',
            'period_basis',
            'distinct_patient_count',
            'production_value_in_period',
            'net_collections_in_period',
        ];
        const lines = [
            headers.join(','),
            ...sortedDetails.map((r) =>
                [
                    r.reporting_year_month,
                    r.referral_id,
                    `"${(r.referral_display_name || '').replace(/"/g, '""')}"`,
                    r.period_basis,
                    r.distinct_patient_count,
                    r.production_value_in_period,
                    r.net_collections_in_period,
                ].join(',')
            ),
        ];
        const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'referral_source_kpis.csv';
        a.click();
        URL.revokeObjectURL(url);
    };

    return (
        <Container maxWidth="xl" sx={{ py: 4 }}>
            <Box sx={{ mb: 2 }}>
                <Typography variant="h4" component="h1" gutterBottom>
                    Referral sources
                </Typography>
                <Typography variant="body2" color="text.secondary">
                    Monthly KPIs by referring source (OpenDental referral list).{' '}
                    <MuiLink component={Link} to="/kpi-definitions">
                        KPI Definitions
                    </MuiLink>{' '}
                    includes a <strong>Referral sources</strong> tab with measure definitions.
                </Typography>
            </Box>

            <Alert severity="info" sx={{ mb: 2 }}>
                <strong>Collections</strong> is net cash (Income + Refund payment rows) in the month;{' '}
                <strong>production</strong> is posted procedure fees in the month. Summing patient counts across multiple
                referrers in a headline over-counts people—filter to one referral or use row-level detail.
            </Alert>

            {patientNote && (
                <Alert severity="warning" sx={{ mb: 2 }}>
                    {patientNote}
                </Alert>
            )}

            <Paper sx={{ p: 2, mb: 3 }}>
                <Stack direction={{ xs: 'column', md: 'row' }} spacing={2} flexWrap="wrap" alignItems="center">
                    <TextField
                        label="Start month"
                        type="month"
                        value={startMonth}
                        onChange={(e) => setStartMonth(e.target.value)}
                        InputLabelProps={{ shrink: true }}
                        size="small"
                    />
                    <TextField
                        label="End month"
                        type="month"
                        value={endMonth}
                        onChange={(e) => setEndMonth(e.target.value)}
                        InputLabelProps={{ shrink: true }}
                        size="small"
                    />
                    <TextField
                        select
                        label="Period basis"
                        value={periodBasis}
                        onChange={(e) => setPeriodBasis(e.target.value)}
                        size="small"
                        sx={{ minWidth: 220 }}
                    >
                        {PERIOD_BASIS_OPTIONS.map((o) => (
                            <MenuItem key={o.value || 'all'} value={o.value}>
                                {o.label}
                            </MenuItem>
                        ))}
                    </TextField>
                    <TextField
                        select
                        label="Referral segment"
                        value={segment}
                        onChange={(e) => setSegment(e.target.value)}
                        size="small"
                        sx={{ minWidth: 180 }}
                    >
                        {SEGMENT_OPTIONS.map((o) => (
                            <MenuItem key={o.value || 'all'} value={o.value}>
                                {o.label}
                            </MenuItem>
                        ))}
                    </TextField>
                    <TextField
                        label="Search referrer name"
                        value={referralSearch}
                        onChange={(e) => setReferralSearch(e.target.value)}
                        size="small"
                        sx={{ minWidth: 200 }}
                    />
                    <TextField
                        label="Referral ID"
                        value={referralId}
                        onChange={(e) => setReferralId(e.target.value)}
                        size="small"
                        sx={{ width: 120 }}
                        helperText="optional"
                    />
                    <FormControlLabel
                        control={<Switch checked={isDoctorOnly} onChange={(e) => setIsDoctorOnly(e.target.checked)} />}
                        label="Doctor referrers only"
                    />
                    <TextField
                        select
                        label="Chart metric"
                        value={metric}
                        onChange={(e) => setMetric(e.target.value as 'collections' | 'production')}
                        size="small"
                        sx={{ minWidth: 160 }}
                    >
                        <MenuItem value="collections">Net collections</MenuItem>
                        <MenuItem value="production">Production</MenuItem>
                    </TextField>
                    <Button variant="contained" onClick={fetchData} disabled={loading}>
                        Apply
                    </Button>
                    <Button variant="outlined" onClick={exportCsv} disabled={!sortedDetails.length}>
                        Export CSV
                    </Button>
                </Stack>
            </Paper>

            {loading ? (
                <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
                    <CircularProgress />
                </Box>
            ) : (
                <>
                    {error && (
                        <Alert severity="error" sx={{ mb: 2 }}>
                            {error}
                        </Alert>
                    )}
                    <Grid container spacing={2} sx={{ mb: 3 }}>
                        <Grid item xs={12} md={4}>
                            <KPICard
                                title="Total net collections (filtered)"
                                value={`$${kpiTotals.coll.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
                            />
                        </Grid>
                        <Grid item xs={12} md={4}>
                            <KPICard
                                title="Total production (filtered)"
                                value={`$${kpiTotals.prod.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
                            />
                        </Grid>
                        <Grid item xs={12} md={4}>
                            <KPICard
                                title={
                                    kpiTotals.singleReferrer
                                        ? 'Summed patient counts (single referrer)'
                                        : 'Patient counts (see warning)'
                                }
                                value={
                                    kpiTotals.singleReferrer
                                        ? kpiTotals.summedPatients.toLocaleString()
                                        : '—'
                                }
                            />
                        </Grid>
                    </Grid>

                    <Paper sx={{ p: 2, mb: 3 }}>
                        <Typography variant="h6" gutterBottom>
                            Trend by month and period basis
                        </Typography>
                        {chartData.length === 0 ? (
                            <Typography variant="body2" color="text.secondary" sx={{ py: 4 }}>
                                No summary rows for the current filters.
                            </Typography>
                        ) : (
                            <ResponsiveContainer width="100%" height={360}>
                                <LineChart data={chartData} margin={{ top: 8, right: 16, left: 8, bottom: 8 }}>
                                    <CartesianGrid strokeDasharray="3 3" />
                                    <XAxis dataKey="month" tick={{ fontSize: 12 }} />
                                    <YAxis tick={{ fontSize: 12 }} />
                                    <Tooltip formatter={(v: number) => `$${Number(v).toFixed(2)}`} />
                                    <Legend />
                                    {chartLines.map((b, i) => (
                                        <Line
                                            key={b}
                                            type="monotone"
                                            dataKey={b}
                                            stroke={CHART_COLORS[i % CHART_COLORS.length]}
                                            dot={false}
                                            name={b.replace(/_/g, ' ')}
                                        />
                                    ))}
                                </LineChart>
                            </ResponsiveContainer>
                        )}
                    </Paper>

                    <Paper sx={{ p: 2 }}>
                        <Typography variant="h6" gutterBottom>
                            Detail rows ({sortedDetails.length})
                        </Typography>
                        <TableContainer>
                            <Table size="small" stickyHeader>
                                <TableHead>
                                    <TableRow>
                                        <TableCell>
                                            <TableSortLabel
                                                active={orderBy === 'reporting_year_month'}
                                                direction={orderBy === 'reporting_year_month' ? orderDir : 'asc'}
                                                onClick={() => handleSort('reporting_year_month')}
                                            >
                                                Month
                                            </TableSortLabel>
                                        </TableCell>
                                        <TableCell>
                                            <TableSortLabel
                                                active={orderBy === 'referral_display_name'}
                                                direction={orderBy === 'referral_display_name' ? orderDir : 'asc'}
                                                onClick={() => handleSort('referral_display_name')}
                                            >
                                                Referrer
                                            </TableSortLabel>
                                        </TableCell>
                                        <TableCell>Segment</TableCell>
                                        <TableCell>
                                            <TableSortLabel
                                                active={orderBy === 'period_basis'}
                                                direction={orderBy === 'period_basis' ? orderDir : 'asc'}
                                                onClick={() => handleSort('period_basis')}
                                            >
                                                Basis
                                            </TableSortLabel>
                                        </TableCell>
                                        <TableCell align="right">
                                            <TableSortLabel
                                                active={orderBy === 'distinct_patient_count'}
                                                direction={orderBy === 'distinct_patient_count' ? orderDir : 'desc'}
                                                onClick={() => handleSort('distinct_patient_count')}
                                            >
                                                Patients
                                            </TableSortLabel>
                                        </TableCell>
                                        <TableCell align="right">
                                            <TableSortLabel
                                                active={orderBy === 'production_value_in_period'}
                                                direction={orderBy === 'production_value_in_period' ? orderDir : 'desc'}
                                                onClick={() => handleSort('production_value_in_period')}
                                            >
                                                Production
                                            </TableSortLabel>
                                        </TableCell>
                                        <TableCell align="right">
                                            <TableSortLabel
                                                active={orderBy === 'net_collections_in_period'}
                                                direction={orderBy === 'net_collections_in_period' ? orderDir : 'desc'}
                                                onClick={() => handleSort('net_collections_in_period')}
                                            >
                                                Collections
                                            </TableSortLabel>
                                        </TableCell>
                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {sortedDetails.slice(0, 500).map((r) => (
                                        <TableRow key={`${r.reporting_month}-${r.referral_id}-${r.period_basis}`}>
                                            <TableCell>{r.reporting_year_month}</TableCell>
                                            <TableCell>
                                                {r.referral_display_name}
                                                <Typography variant="caption" display="block" color="text.secondary">
                                                    ID {r.referral_id}
                                                </Typography>
                                            </TableCell>
                                            <TableCell>{r.referral_source_segment}</TableCell>
                                            <TableCell>{r.period_basis.replace(/_/g, ' ')}</TableCell>
                                            <TableCell align="right">{r.distinct_patient_count}</TableCell>
                                            <TableCell align="right">
                                                ${r.production_value_in_period.toFixed(2)}
                                            </TableCell>
                                            <TableCell align="right">
                                                ${r.net_collections_in_period.toFixed(2)}
                                            </TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </TableContainer>
                        {sortedDetails.length > 500 && (
                            <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 1 }}>
                                Showing first 500 rows. Export CSV for full result set (up to API limit).
                            </Typography>
                        )}
                    </Paper>
                </>
            )}
        </Container>
    );
};

export default ReferralSources;
