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
} from '@mui/material';
import VerifiedIcon from '@mui/icons-material/Verified';
import { dateUtils, kpiApi } from '../../services/api';
import { useApiQuery } from '../../hooks/useApiQuery';
import { formatCurrencyPrecise } from '../../utils/format';
import { VALIDATED_KPIS } from '../../config/validatedKpis';

const dailyKpiMeta = VALIDATED_KPIS.dailyNetCollections;

type DatePickerMode = 'today' | 'yesterday' | 'latest-business-day' | 'custom';

const DATE_MODE_LABELS: Record<Exclude<DatePickerMode, 'custom'>, string> = {
    today: 'Today (Central)',
    yesterday: 'Yesterday (Central)',
    'latest-business-day': 'Latest with payments',
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

const PracticeManagerHome: React.FC = () => {
    const [dateMode, setDateMode] = useState<DatePickerMode>('latest-business-day');
    const [customDate, setCustomDate] = useState<string>('');

    const clinicToday = dateUtils.getClinicToday();
    const clinicYesterday = dateUtils.getClinicYesterday();

    const latestDateQuery = useApiQuery(() => kpiApi.getLatestCollectionsDate(), []);

    const resolvedPaymentDate = useMemo(() => {
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
            return latestDateQuery.data?.payment_date ?? null;
        }
        return null;
    }, [
        dateMode,
        customDate,
        clinicToday,
        clinicYesterday,
        latestDateQuery.data?.payment_date,
    ]);

    const dailyCollectionsQuery = useApiQuery(
        () => {
            if (!resolvedPaymentDate) {
                return Promise.resolve({ loading: false, data: undefined });
            }
            return kpiApi.getDailyCollections(resolvedPaymentDate);
        },
        [resolvedPaymentDate]
    );

    const waitingForLatestDate =
        dateMode === 'latest-business-day' &&
        latestDateQuery.loading &&
        !latestDateQuery.data?.payment_date;

    const loading = waitingForLatestDate || dailyCollectionsQuery.loading;

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

    const data = dailyCollectionsQuery.data;
    const queryError = latestDateQuery.error || dailyCollectionsQuery.error;
    const modeLabel =
        dateMode === 'custom'
            ? 'Custom date'
            : DATE_MODE_LABELS[dateMode as Exclude<DatePickerMode, 'custom'>];

    return (
        <Box sx={{ maxWidth: 720 }}>
            <Typography variant="h4" component="h1" gutterBottom>
                Practice Manager Home
            </Typography>
            <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                Validated KPIs — reconciled against OpenDental Daily Payments (Central US dates)
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
                        aria-label="Collections report date"
                    >
                        <ToggleButton value="latest-business-day">Latest</ToggleButton>
                        <ToggleButton value="yesterday">Yesterday</ToggleButton>
                        <ToggleButton value="today">Today</ToggleButton>
                    </ToggleButtonGroup>

                    <TextField
                        type="date"
                        label="Pick date"
                        size="small"
                        value={dateMode === 'custom' ? customDate : resolvedPaymentDate ?? ''}
                        onChange={(e) => handleCustomDateChange(e.target.value)}
                        InputLabelProps={{ shrink: true }}
                        sx={{ minWidth: 160 }}
                    />
                </Box>

                {resolvedPaymentDate ? (
                    <>
                        <Typography variant="h5" component="p" fontWeight="bold">
                            {formatDisplayDate(resolvedPaymentDate)}
                        </Typography>
                        <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                            {modeLabel} · <strong>{resolvedPaymentDate}</strong>
                        </Typography>
                    </>
                ) : (
                    <Alert severity="info" sx={{ mt: 1 }}>
                        No payment dates in mart_daily_payments yet. Publish analytics to RDS, or
                        pick a date manually.
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

            {!queryError && data && resolvedPaymentDate && (
                <Paper variant="outlined" sx={{ p: 3 }}>
                    <Typography variant="overline" color="text.secondary">
                        {dailyKpiMeta.name}
                    </Typography>
                    <Typography
                        variant="h3"
                        component="p"
                        fontWeight="bold"
                        color="primary.main"
                        sx={{ mt: 0.5, mb: 1 }}
                    >
                        {formatCurrencyPrecise(data.net_collections_amount)}
                    </Typography>

                    {data.has_data ? (
                        <Typography variant="body2" color="text.secondary">
                            Patient {formatCurrencyPrecise(data.patient_payment_amount)}
                            {' · '}
                            Insurance {formatCurrencyPrecise(data.insurance_payment_amount)}
                            {' · '}
                            {data.payment_count} payments
                        </Typography>
                    ) : (
                        <Alert severity="info" sx={{ mt: 1 }}>
                            No row in <code>mart_daily_payments</code> for{' '}
                            <strong>{resolvedPaymentDate}</strong>. Try <strong>Latest</strong>, pick{' '}
                            <strong>2026-06-24</strong> (validated golden day), or run{' '}
                            <code>mdc publish analytics --env clinic</code>.
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
                        <Chip size="small" variant="outlined" label={dailyKpiMeta.model} />
                        <Chip size="small" variant="outlined" label={dailyKpiMeta.odReport} />
                    </Box>
                </Paper>
            )}
        </Box>
    );
};

export default PracticeManagerHome;
