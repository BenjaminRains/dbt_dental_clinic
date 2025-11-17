import React, { useEffect, useState } from 'react';
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
    Button,
    Tabs,
    Tab,
} from '@mui/material';
import {
    CalendarToday,
    TrendingUp,
    People,
    AttachMoney,
} from '@mui/icons-material';
import { appointmentApi, dateUtils } from '../services/api';
import { AppointmentSummary, AppointmentDetail, ApiResponse, ProviderFilter } from '../types/api';

interface TabPanelProps {
    children?: React.ReactNode;
    index: number;
    value: number;
}

function TabPanel(props: TabPanelProps) {
    const { children, value, index, ...other } = props;

    return (
        <div
            role="tabpanel"
            hidden={value !== index}
            id={`appointment-tabpanel-${index}`}
            aria-labelledby={`appointment-tab-${index}`}
            {...other}
        >
            {value === index && <Box sx={{ p: 3 }}>{children}</Box>}
        </div>
    );
}

const Appointments: React.FC = () => {
    const [tabValue, setTabValue] = useState(0);
    const [summaryData, setSummaryData] = useState<ApiResponse<AppointmentSummary[]>>({ loading: true });
    const [todayAppointments, setTodayAppointments] = useState<ApiResponse<AppointmentDetail[]>>({ loading: true });
    const [upcomingAppointments, setUpcomingAppointments] = useState<ApiResponse<AppointmentDetail[]>>({ loading: true });
    const [providerFilter, setProviderFilter] = useState<number | undefined>(undefined);
    const [dateRange, setDateRange] = useState<ProviderFilter>(dateUtils.getLast30Days());

    const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
        setTabValue(newValue);
    };

    const loadSummaryData = async () => {
        const result = await appointmentApi.getSummary(dateRange);
        setSummaryData(result);
    };

    const loadTodayAppointments = async () => {
        const result = await appointmentApi.getTodayAppointments(providerFilter);
        setTodayAppointments(result);
    };

    const loadUpcomingAppointments = async () => {
        const result = await appointmentApi.getUpcomingAppointments(7, providerFilter);
        setUpcomingAppointments(result);
    };

    useEffect(() => {
        loadSummaryData();
    }, [dateRange]);

    useEffect(() => {
        loadTodayAppointments();
        loadUpcomingAppointments();
    }, [providerFilter]);

    const getStatusChip = (appointment: AppointmentDetail) => {
        if (appointment.is_completed) {
            return <Chip label="Completed" color="success" size="small" />;
        } else if (appointment.is_no_show) {
            return <Chip label="No Show" color="error" size="small" />;
        } else if (appointment.is_broken) {
            return <Chip label="Cancelled" color="warning" size="small" />;
        } else {
            return <Chip label="Scheduled" color="primary" size="small" />;
        }
    };

    const formatCurrency = (amount: number) => {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
        }).format(amount);
    };

    const formatDate = (dateString: string) => {
        return new Date(dateString).toLocaleDateString();
    };

    return (
        <Box>
            <Typography variant="h4" component="h1" gutterBottom>
                Appointment Analytics
            </Typography>
            <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                Scheduling efficiency, utilization, and capacity planning
            </Typography>

            {/* Filters */}
            <Card sx={{ mt: 3, mb: 3 }}>
                <CardContent>
                    <Grid container spacing={2} alignItems="center">
                        <Grid item xs={12} sm={3}>
                            <TextField
                                label="Start Date"
                                type="date"
                                value={dateRange.start_date || ''}
                                onChange={(e) => setDateRange({ ...dateRange, start_date: e.target.value })}
                                InputLabelProps={{ shrink: true }}
                                fullWidth
                            />
                        </Grid>
                        <Grid item xs={12} sm={3}>
                            <TextField
                                label="End Date"
                                type="date"
                                value={dateRange.end_date || ''}
                                onChange={(e) => setDateRange({ ...dateRange, end_date: e.target.value })}
                                InputLabelProps={{ shrink: true }}
                                fullWidth
                            />
                        </Grid>
                        <Grid item xs={12} sm={3}>
                            <FormControl fullWidth>
                                <InputLabel>Provider</InputLabel>
                                <Select
                                    value={providerFilter || ''}
                                    onChange={(e) => setProviderFilter(e.target.value ? Number(e.target.value) : undefined)}
                                    label="Provider"
                                >
                                    <MenuItem value="">All Providers</MenuItem>
                                    {/* TODO: Add provider options from API */}
                                </Select>
                            </FormControl>
                        </Grid>
                        <Grid item xs={12} sm={3}>
                            <Button
                                variant="contained"
                                onClick={loadSummaryData}
                                fullWidth
                            >
                                Apply Filters
                            </Button>
                        </Grid>
                    </Grid>
                </CardContent>
            </Card>

            {/* Tabs */}
            <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
                <Tabs value={tabValue} onChange={handleTabChange} aria-label="appointment tabs">
                    <Tab label="Summary" />
                    <Tab label="Today's Appointments" />
                    <Tab label="Upcoming Appointments" />
                </Tabs>
            </Box>

            {/* Summary Tab */}
            <TabPanel value={tabValue} index={0}>
                {summaryData.loading ? (
                    <Box display="flex" justifyContent="center" alignItems="center" minHeight="200px">
                        <CircularProgress />
                    </Box>
                ) : summaryData.error ? (
                    <Alert severity="error" sx={{ mb: 2 }}>
                        Error loading summary data: {summaryData.error}
                    </Alert>
                ) : (
                    <Grid container spacing={3}>
                        {summaryData.data?.map((summary, index) => (
                            <Grid item xs={12} md={6} lg={4} key={index}>
                                <Card>
                                    <CardContent>
                                        <Typography variant="h6" gutterBottom>
                                            Provider {summary.provider_id} - {formatDate(summary.date)}
                                        </Typography>
                                        <Grid container spacing={2}>
                                            <Grid item xs={6}>
                                                <Box display="flex" alignItems="center" mb={1}>
                                                    <CalendarToday fontSize="small" sx={{ mr: 1 }} />
                                                    <Typography variant="body2">
                                                        Total: {summary.total_appointments}
                                                    </Typography>
                                                </Box>
                                                <Box display="flex" alignItems="center" mb={1}>
                                                    <TrendingUp fontSize="small" sx={{ mr: 1 }} />
                                                    <Typography variant="body2">
                                                        Completed: {summary.completed_appointments}
                                                    </Typography>
                                                </Box>
                                                <Box display="flex" alignItems="center" mb={1}>
                                                    <People fontSize="small" sx={{ mr: 1 }} />
                                                    <Typography variant="body2">
                                                        Unique Patients: {summary.unique_patients}
                                                    </Typography>
                                                </Box>
                                            </Grid>
                                            <Grid item xs={6}>
                                                <Typography variant="body2" color="text.secondary">
                                                    Completion Rate: {summary.completion_rate.toFixed(1)}%
                                                </Typography>
                                                <Typography variant="body2" color="text.secondary">
                                                    No Show Rate: {summary.no_show_rate.toFixed(1)}%
                                                </Typography>
                                                <Typography variant="body2" color="text.secondary">
                                                    Cancellation Rate: {summary.cancellation_rate.toFixed(1)}%
                                                </Typography>
                                            </Grid>
                                        </Grid>
                                        <Box display="flex" alignItems="center" mt={2}>
                                            <AttachMoney fontSize="small" sx={{ mr: 1 }} />
                                            <Typography variant="body2">
                                                Scheduled: {formatCurrency(summary.scheduled_production)}
                                            </Typography>
                                        </Box>
                                        <Typography variant="body2" color="text.secondary">
                                            Completed: {formatCurrency(summary.completed_production)}
                                        </Typography>
                                    </CardContent>
                                </Card>
                            </Grid>
                        ))}
                    </Grid>
                )}
            </TabPanel>

            {/* Today's Appointments Tab */}
            <TabPanel value={tabValue} index={1}>
                {todayAppointments.loading ? (
                    <Box display="flex" justifyContent="center" alignItems="center" minHeight="200px">
                        <CircularProgress />
                    </Box>
                ) : todayAppointments.error ? (
                    <Alert severity="error" sx={{ mb: 2 }}>
                        Error loading today's appointments: {todayAppointments.error}
                    </Alert>
                ) : (
                    <TableContainer component={Paper}>
                        <Table>
                            <TableHead>
                                <TableRow>
                                    <TableCell>Time</TableCell>
                                    <TableCell>Patient ID</TableCell>
                                    <TableCell>Provider</TableCell>
                                    <TableCell>Type</TableCell>
                                    <TableCell>Status</TableCell>
                                    <TableCell>Production</TableCell>
                                    <TableCell>Duration</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {todayAppointments.data?.map((appointment) => (
                                    <TableRow key={appointment.appointment_id}>
                                        <TableCell>{appointment.appointment_time}</TableCell>
                                        <TableCell>{appointment.patient_id}</TableCell>
                                        <TableCell>Provider {appointment.provider_id}</TableCell>
                                        <TableCell>{appointment.appointment_type}</TableCell>
                                        <TableCell>{getStatusChip(appointment)}</TableCell>
                                        <TableCell>{formatCurrency(appointment.scheduled_production_amount)}</TableCell>
                                        <TableCell>{appointment.appointment_length_minutes} min</TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </TableContainer>
                )}
            </TabPanel>

            {/* Upcoming Appointments Tab */}
            <TabPanel value={tabValue} index={2}>
                {upcomingAppointments.loading ? (
                    <Box display="flex" justifyContent="center" alignItems="center" minHeight="200px">
                        <CircularProgress />
                    </Box>
                ) : upcomingAppointments.error ? (
                    <Alert severity="error" sx={{ mb: 2 }}>
                        Error loading upcoming appointments: {upcomingAppointments.error}
                    </Alert>
                ) : (
                    <TableContainer component={Paper}>
                        <Table>
                            <TableHead>
                                <TableRow>
                                    <TableCell>Date</TableCell>
                                    <TableCell>Time</TableCell>
                                    <TableCell>Patient ID</TableCell>
                                    <TableCell>Provider</TableCell>
                                    <TableCell>Type</TableCell>
                                    <TableCell>Status</TableCell>
                                    <TableCell>Production</TableCell>
                                    <TableCell>Duration</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {upcomingAppointments.data?.map((appointment) => (
                                    <TableRow key={appointment.appointment_id}>
                                        <TableCell>{formatDate(appointment.appointment_date)}</TableCell>
                                        <TableCell>{appointment.appointment_time}</TableCell>
                                        <TableCell>{appointment.patient_id}</TableCell>
                                        <TableCell>Provider {appointment.provider_id}</TableCell>
                                        <TableCell>{appointment.appointment_type}</TableCell>
                                        <TableCell>{getStatusChip(appointment)}</TableCell>
                                        <TableCell>{formatCurrency(appointment.scheduled_production_amount)}</TableCell>
                                        <TableCell>{appointment.appointment_length_minutes} min</TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </TableContainer>
                )}
            </TabPanel>
        </Box>
    );
};

export default Appointments;
