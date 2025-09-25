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
    TextField
} from '@mui/material';
import { revenueApi } from '../services/api';
import { RevenueKPISummary, RevenueOpportunity, RevenueRecoveryPlan } from '../types/api';

const Revenue: React.FC = () => {
    const [kpiSummary, setKpiSummary] = useState<RevenueKPISummary | null>(null);
    const [opportunities, setOpportunities] = useState<RevenueOpportunity[]>([]);
    const [recoveryPlan, setRecoveryPlan] = useState<RevenueRecoveryPlan[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [selectedProvider, setSelectedProvider] = useState<number | ''>('');
    const [selectedOpportunityType, setSelectedOpportunityType] = useState<string>('');
    const [minPriorityScore, setMinPriorityScore] = useState<number>(50);

    useEffect(() => {
        loadRevenueData();
    }, [selectedProvider, selectedOpportunityType, minPriorityScore]);

    const loadRevenueData = async () => {
        setLoading(true);
        setError(null);

        try {
            const [kpiResponse, opportunitiesResponse, recoveryResponse] = await Promise.all([
                revenueApi.getKPISummary(),
                revenueApi.getOpportunities(0, 20, {
                    provider_id: selectedProvider || undefined,
                    opportunity_type: selectedOpportunityType || undefined,
                    min_priority_score: minPriorityScore
                }),
                revenueApi.getRecoveryPlan(minPriorityScore)
            ]);

            if (kpiResponse.error) {
                setError(kpiResponse.error);
            } else {
                setKpiSummary(kpiResponse.data || null);
            }

            if (opportunitiesResponse.error) {
                setError(opportunitiesResponse.error);
            } else {
                setOpportunities(opportunitiesResponse.data || []);
            }

            if (recoveryResponse.error) {
                setError(recoveryResponse.error);
            } else {
                setRecoveryPlan(recoveryResponse.data || []);
            }
        } catch (err) {
            setError('Failed to load revenue data');
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

    const getRecoveryPotentialColor = (potential: string) => {
        switch (potential) {
            case 'High': return 'error';
            case 'Medium': return 'warning';
            case 'Low': return 'info';
            default: return 'default';
        }
    };

    const getPriorityScoreColor = (score: number) => {
        if (score >= 80) return 'error';
        if (score >= 60) return 'warning';
        if (score >= 40) return 'info';
        return 'default';
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
                Revenue Analytics
            </Typography>
            <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                Financial performance and revenue optimization insights
            </Typography>

            {error && (
                <Alert severity="error" sx={{ mt: 2 }}>
                    {error}
                </Alert>
            )}

            {/* KPI Summary Cards */}
            {kpiSummary && (
                <Grid container spacing={3} sx={{ mt: 2 }}>
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    Total Revenue Lost
                                </Typography>
                                <Typography variant="h5" component="div">
                                    {formatCurrency(kpiSummary.total_revenue_lost)}
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    Recovery Potential
                                </Typography>
                                <Typography variant="h5" component="div">
                                    {formatCurrency(kpiSummary.total_recovery_potential)}
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    Total Opportunities
                                </Typography>
                                <Typography variant="h5" component="div">
                                    {kpiSummary.total_opportunities}
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Card>
                            <CardContent>
                                <Typography color="textSecondary" gutterBottom>
                                    Affected Patients
                                </Typography>
                                <Typography variant="h5" component="div">
                                    {kpiSummary.affected_patients}
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                </Grid>
            )}

            {/* Filters */}
            <Card sx={{ mt: 3 }}>
                <CardContent>
                    <Typography variant="h6" gutterBottom>
                        Filters
                    </Typography>
                    <Grid container spacing={2}>
                        <Grid item xs={12} sm={4}>
                            <FormControl fullWidth>
                                <InputLabel>Opportunity Type</InputLabel>
                                <Select
                                    value={selectedOpportunityType}
                                    label="Opportunity Type"
                                    onChange={(e) => setSelectedOpportunityType(e.target.value)}
                                >
                                    <MenuItem value="">All Types</MenuItem>
                                    <MenuItem value="Missed Appointment">Missed Appointment</MenuItem>
                                    <MenuItem value="Claim Rejection">Claim Rejection</MenuItem>
                                    <MenuItem value="Treatment Plan Delay">Treatment Plan Delay</MenuItem>
                                    <MenuItem value="Write Off">Write Off</MenuItem>
                                </Select>
                            </FormControl>
                        </Grid>
                        <Grid item xs={12} sm={4}>
                            <TextField
                                fullWidth
                                label="Minimum Priority Score"
                                type="number"
                                value={minPriorityScore}
                                onChange={(e) => setMinPriorityScore(Number(e.target.value))}
                                inputProps={{ min: 0, max: 100 }}
                            />
                        </Grid>
                    </Grid>
                </CardContent>
            </Card>

            {/* Revenue Opportunities Table */}
            <Card sx={{ mt: 3 }}>
                <CardContent>
                    <Typography variant="h6" gutterBottom>
                        Revenue Opportunities
                    </Typography>
                    <TableContainer component={Paper}>
                        <Table>
                            <TableHead>
                                <TableRow>
                                    <TableCell>Date</TableCell>
                                    <TableCell>Type</TableCell>
                                    <TableCell>Provider</TableCell>
                                    <TableCell>Lost Revenue</TableCell>
                                    <TableCell>Recovery Potential</TableCell>
                                    <TableCell>Priority Score</TableCell>
                                    <TableCell>Recovery Timeline</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {opportunities.map((opportunity) => (
                                    <TableRow key={opportunity.opportunity_id}>
                                        <TableCell>{opportunity.appointment_date}</TableCell>
                                        <TableCell>
                                            <Chip
                                                label={opportunity.opportunity_type}
                                                size="small"
                                                variant="outlined"
                                            />
                                        </TableCell>
                                        <TableCell>
                                            {opportunity.provider_first_name} {opportunity.provider_last_name}
                                        </TableCell>
                                        <TableCell>
                                            {opportunity.lost_revenue ? formatCurrency(opportunity.lost_revenue) : 'N/A'}
                                        </TableCell>
                                        <TableCell>
                                            <Chip
                                                label={opportunity.recovery_potential}
                                                color={getRecoveryPotentialColor(opportunity.recovery_potential)}
                                                size="small"
                                            />
                                        </TableCell>
                                        <TableCell>
                                            <Chip
                                                label={opportunity.recovery_priority_score}
                                                color={getPriorityScoreColor(opportunity.recovery_priority_score)}
                                                size="small"
                                            />
                                        </TableCell>
                                        <TableCell>{opportunity.recovery_timeline}</TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </TableContainer>
                </CardContent>
            </Card>

            {/* Recovery Plan */}
            <Card sx={{ mt: 3 }}>
                <CardContent>
                    <Typography variant="h6" gutterBottom>
                        Recovery Plan
                    </Typography>
                    <TableContainer component={Paper}>
                        <Table>
                            <TableHead>
                                <TableRow>
                                    <TableCell>Provider</TableCell>
                                    <TableCell>Patient ID</TableCell>
                                    <TableCell>Type</TableCell>
                                    <TableCell>Lost Revenue</TableCell>
                                    <TableCell>Priority</TableCell>
                                    <TableCell>Recommended Actions</TableCell>
                                    <TableCell>Timeline</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {recoveryPlan.map((plan) => (
                                    <TableRow key={plan.opportunity_id}>
                                        <TableCell>{plan.provider_name || 'N/A'}</TableCell>
                                        <TableCell>{plan.patient_id}</TableCell>
                                        <TableCell>
                                            <Chip
                                                label={plan.opportunity_type}
                                                size="small"
                                                variant="outlined"
                                            />
                                        </TableCell>
                                        <TableCell>
                                            {plan.lost_revenue ? formatCurrency(plan.lost_revenue) : 'N/A'}
                                        </TableCell>
                                        <TableCell>
                                            <Chip
                                                label={plan.priority_score}
                                                color={getPriorityScoreColor(plan.priority_score)}
                                                size="small"
                                            />
                                        </TableCell>
                                        <TableCell>
                                            <Box>
                                                {plan.recommended_actions.map((action, index) => (
                                                    <Chip
                                                        key={index}
                                                        label={action}
                                                        size="small"
                                                        variant="outlined"
                                                        sx={{ mr: 0.5, mb: 0.5 }}
                                                    />
                                                ))}
                                            </Box>
                                        </TableCell>
                                        <TableCell>{plan.recovery_timeline}</TableCell>
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

export default Revenue;
