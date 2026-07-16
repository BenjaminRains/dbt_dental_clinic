import React, { useMemo, useState } from 'react';
import {
    Box,
    Typography,
    Alert,
    CircularProgress,
    Chip,
    Paper,
    ToggleButton,
    ToggleButtonGroup,
    Divider,
    TextField,
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableRow,
} from '@mui/material';
import VerifiedIcon from '@mui/icons-material/Verified';
import { dateUtils, kpiApi, useApiQuery } from '@mdc/analytics-api';
import { formatCurrencyPrecise } from '@mdc/ui-common';
import { VALIDATED_KPIS } from '../../config/validatedKpis';

const collectionsMeta = VALIDATED_KPIS.dailyNetCollections;
const productionMeta = VALIDATED_KPIS.dailyProductionByProcedure;

type DatePickerMode = 'today' | 'yesterday' | 'latest-business-day' | 'custom';

const DATE_MODE_LABELS: Record<Exclude<DatePickerMode, 'custom'>, string> = {
    today: 'Today (Central)',
    yesterday: 'Yesterday (Central)',
    'latest-business-day': 'Latest with activity',
};

function formatDisplayDate(isoDate: string): string {
    const [year, month, day] = isoDate.split('-').map(Number);
    return new Date(year, month - 1, day).toLocaleDateString('en-US', {
        weekday: 'long',
        month: 'long',
        day: 'numeric',
        year: 'numeric',
    });
}

/** Prefer the later of collections vs production latest dates for the shared picker. */
function maxIsoDate(a: string | null | undefined, b: string | null | undefined): string | null {
    if (a && b) {
        return a >= b ? a : b;
    }
    return a ?? b ?? null;
}

const PracticeManagerHome: React.FC = () => {
    const [dateMode, setDateMode] = useState<DatePickerMode>('latest-business-day');
    const [customDate, setCustomDate] = useState<string>('');

    const clinicToday = dateUtils.getClinicToday();
    const clinicYesterday = dateUtils.getClinicYesterday();

    const latestCollectionsQuery = useApiQuery(() => kpiApi.getLatestCollectionsDate(), []);
    const latestProductionQuery = useApiQuery(() => kpiApi.getLatestProductionDate(), []);

    const resolvedReportDate = useMemo(() => {
        if (dateMode === 'custom' && customDate) {
            return customDate;
        }
        if (dateMode === 'today') {
            return clinicToday;
        }
        if (dateMode === 'yesterday') {
            return clinicYesterday;
        }
        if (dateMode === 'latest-business-day') {
            return maxIsoDate(
                latestCollectionsQuery.data?.payment_date,
                latestProductionQuery.data?.production_date
            );
        }
        return null;
    }, [
        dateMode,
        customDate,
        clinicToday,
        clinicYesterday,
        latestCollectionsQuery.data?.payment_date,
        latestProductionQuery.data?.production_date,
    ]);

    const dailyCollectionsQuery = useApiQuery(
        () => {
            if (!resolvedReportDate) {
                return Promise.resolve({ loading: false, data: undefined });
            }
            return kpiApi.getDailyCollections(resolvedReportDate);
        },
        [resolvedReportDate]
    );

    const dailyProductionQuery = useApiQuery(
        () => {
            if (!resolvedReportDate) {
                return Promise.resolve({ loading: false, data: undefined });
            }
            return kpiApi.getDailyProduction(resolvedReportDate);
        },
        [resolvedReportDate]
    );

    const productionByCodeQuery = useApiQuery(
        () => {
            if (!resolvedReportDate) {
                return Promise.resolve({ loading: false, data: undefined });
            }
            return kpiApi.getDailyProductionByCode(resolvedReportDate);
        },
        [resolvedReportDate]
    );

    const waitingForLatestDate =
        dateMode === 'latest-business-day' &&
        (latestCollectionsQuery.loading || latestProductionQuery.loading) &&
        !resolvedReportDate;

    const loading =
        waitingForLatestDate ||
        dailyCollectionsQuery.loading ||
        dailyProductionQuery.loading ||
        productionByCodeQuery.loading;

    const handleDateModeChange = (
        _event: React.MouseEvent<HTMLElement>,
        nextMode: DatePickerMode | null
    ) => {
        if (nextMode !== null) {
            setDateMode(nextMode);
        }
    };

    const handleCustomDateChange = (value: string) => {
        setCustomDate(value);
        if (value) {
            setDateMode('custom');
        }
    };

    if (loading) {
        return (
            <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: 400 }}>
                <CircularProgress />
            </Box>
        );
    }

    const collections = dailyCollectionsQuery.data;
    const production = dailyProductionQuery.data;
    const byCode = productionByCodeQuery.data;
    const queryError =
        latestCollectionsQuery.error ||
        latestProductionQuery.error ||
        dailyCollectionsQuery.error ||
        dailyProductionQuery.error ||
        productionByCodeQuery.error;
    const modeLabel =
        dateMode === 'custom'
            ? 'Custom date'
            : DATE_MODE_LABELS[dateMode as Exclude<DatePickerMode, 'custom'>];

    return (
        <Box sx={{ maxWidth: 900 }}>
            <Typography variant="h4" component="h1" gutterBottom>
                Practice Manager Home
            </Typography>
            <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                Validated KPIs — reconciled against OpenDental Daily Payments and Production by
                Procedure (Central US dates)
            </Typography>

            <Paper variant="outlined" sx={{ p: 2.5, mt: 2, mb: 2 }}>
                <Typography variant="overline" color="text.secondary" display="block" gutterBottom>
                    Report date
                </Typography>

                <Box
                    sx={{
                        display: 'flex',
                        flexDirection: { xs: 'column', sm: 'row' },
                        flexWrap: 'wrap',
                        alignItems: { xs: 'stretch', sm: 'center' },
                        gap: 2,
                        mb: 2,
                    }}
                >
                    <ToggleButtonGroup
                        value={dateMode === 'custom' ? null : dateMode}
                        exclusive
                        onChange={handleDateModeChange}
                        size="small"
                        aria-label="Report date"
                    >
                        <ToggleButton value="latest-business-day">Latest</ToggleButton>
                        <ToggleButton value="yesterday">Yesterday</ToggleButton>
                        <ToggleButton value="today">Today</ToggleButton>
                    </ToggleButtonGroup>

                    <TextField
                        type="date"
                        label="Pick date"
                        size="small"
                        value={dateMode === 'custom' ? customDate : resolvedReportDate ?? ''}
                        onChange={(e) => handleCustomDateChange(e.target.value)}
                        InputLabelProps={{ shrink: true }}
                        sx={{ minWidth: 160 }}
                    />
                </Box>

                {resolvedReportDate ? (
                    <>
                        <Typography variant="h5" component="p" fontWeight="bold">
                            {formatDisplayDate(resolvedReportDate)}
                        </Typography>
                        <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                            {modeLabel} · <strong>{resolvedReportDate}</strong>
                        </Typography>
                    </>
                ) : (
                    <Alert severity="info" sx={{ mt: 1 }}>
                        No dates in payment or production marts yet. Publish analytics, or pick a
                        date manually.
                    </Alert>
                )}

                <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 1.5 }}>
                    Central calendar now: {clinicToday} ({dateUtils.formatCalendarWeekday(clinicToday)}
                    ) · yesterday: {clinicYesterday}
                </Typography>
            </Paper>

            {queryError && (
                <Alert severity="error" sx={{ mb: 2 }}>
                    {queryError}
                </Alert>
            )}

            {!queryError && collections && resolvedReportDate && (
                <Paper variant="outlined" sx={{ p: 3, mb: 2 }}>
                    <Typography variant="overline" color="text.secondary">
                        {collectionsMeta.name}
                    </Typography>
                    <Typography
                        variant="h3"
                        component="p"
                        fontWeight="bold"
                        color="primary.main"
                        sx={{ mt: 0.5, mb: 1 }}
                    >
                        {formatCurrencyPrecise(collections.net_collections_amount)}
                    </Typography>

                    {collections.has_data ? (
                        <Typography variant="body2" color="text.secondary">
                            Patient {formatCurrencyPrecise(collections.patient_payment_amount)}
                            {' · '}
                            Insurance {formatCurrencyPrecise(collections.insurance_payment_amount)}
                            {' · '}
                            {collections.payment_count} payments
                        </Typography>
                    ) : (
                        <Alert severity="info" sx={{ mt: 1 }}>
                            No row in <code>mart_daily_payments</code> for{' '}
                            <strong>{resolvedReportDate}</strong>.
                        </Alert>
                    )}

                    <Divider sx={{ my: 2 }} />

                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                        <Chip
                            size="small"
                            color="success"
                            icon={<VerifiedIcon />}
                            label="Validated vs OD Daily Payments"
                        />
                        <Chip size="small" variant="outlined" label={collectionsMeta.model} />
                        <Chip size="small" variant="outlined" label={collectionsMeta.odReport} />
                    </Box>
                </Paper>
            )}

            {!queryError && production && resolvedReportDate && (
                <Paper variant="outlined" sx={{ p: 3 }}>
                    <Typography variant="overline" color="text.secondary">
                        {productionMeta.name}
                    </Typography>
                    <Typography
                        variant="h3"
                        component="p"
                        fontWeight="bold"
                        color="primary.main"
                        sx={{ mt: 0.5, mb: 1 }}
                    >
                        {formatCurrencyPrecise(production.total_fees)}
                    </Typography>

                    {production.has_data ? (
                        <Typography variant="body2" color="text.secondary">
                            {production.procedure_quantity} procedures
                            {' · '}
                            {production.procedure_code_count} codes
                        </Typography>
                    ) : (
                        <Alert severity="info" sx={{ mt: 1 }}>
                            No rows in <code>mart_daily_production_by_procedure</code> for{' '}
                            <strong>{resolvedReportDate}</strong>. Try golden dates{' '}
                            <strong>2026-06-10</strong>, <strong>2025-11-18</strong>, or{' '}
                            <strong>2026-02-07</strong>.
                        </Alert>
                    )}

                    <Divider sx={{ my: 2 }} />

                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mb: 2 }}>
                        <Chip
                            size="small"
                            color="success"
                            icon={<VerifiedIcon />}
                            label="Validated vs OD Production by Procedure"
                        />
                        <Chip size="small" variant="outlined" label={productionMeta.model} />
                        <Chip size="small" variant="outlined" label={productionMeta.odReport} />
                    </Box>

                    {byCode?.has_data && byCode.rows.length > 0 && (
                        <Table size="small" aria-label="Production by procedure code">
                            <TableHead>
                                <TableRow>
                                    <TableCell>Code</TableCell>
                                    <TableCell>Category</TableCell>
                                    <TableCell align="right">Qty</TableCell>
                                    <TableCell align="right">Avg fee</TableCell>
                                    <TableCell align="right">Total fees</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {byCode.rows.map((row) => (
                                    <TableRow key={row.procedure_code}>
                                        <TableCell>{row.procedure_code}</TableCell>
                                        <TableCell>{row.procedure_category ?? '—'}</TableCell>
                                        <TableCell align="right">{row.procedure_quantity}</TableCell>
                                        <TableCell align="right">
                                            {formatCurrencyPrecise(row.average_fee)}
                                        </TableCell>
                                        <TableCell align="right">
                                            {formatCurrencyPrecise(row.total_fees)}
                                        </TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    )}
                </Paper>
            )}
        </Box>
    );
};

export default PracticeManagerHome;
