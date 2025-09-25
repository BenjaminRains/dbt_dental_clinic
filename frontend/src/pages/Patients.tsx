import React, { useState, useEffect } from 'react';
import {
    Box,
    Typography,
    Card,
    CardContent,
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
    Button,
    Pagination
} from '@mui/material';
import { Patient } from '../types/api';
import { apiService } from '../services/api';

const Patients: React.FC = () => {
    const [patients, setPatients] = useState<Patient[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [currentPage, setCurrentPage] = useState(1);
    const [totalPages, setTotalPages] = useState(1);
    const patientsPerPage = 20;

    const fetchPatients = async (page: number = 1) => {
        setLoading(true);
        setError(null);

        try {
            const skip = (page - 1) * patientsPerPage;
            const response = await apiService.patient.getPatients(skip, patientsPerPage);

            if (response.error) {
                setError(response.error);
            } else if (response.data) {
                setPatients(response.data);
                // Estimate total pages (you might want to get this from API response)
                setTotalPages(Math.max(1, Math.ceil(response.data.length / patientsPerPage)));
            }
        } catch (err) {
            setError('Failed to fetch patients');
            console.error('Error fetching patients:', err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchPatients(currentPage);
    }, [currentPage]);

    const handlePageChange = (_event: React.ChangeEvent<unknown>, value: number) => {
        setCurrentPage(value);
    };

    const formatDate = (dateString?: string) => {
        if (!dateString) return 'N/A';
        return new Date(dateString).toLocaleDateString();
    };

    const getStatusColor = (status?: string) => {
        switch (status) {
            case 'Patient': return 'success';
            case 'Inactive': return 'warning';
            case 'Deceased': return 'error';
            case 'Deleted': return 'error';
            default: return 'default';
        }
    };

    const formatCurrency = (amount?: number) => {
        if (amount === undefined || amount === null) return 'N/A';
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD'
        }).format(amount);
    };

    return (
        <Box>
            <Typography variant="h4" component="h1" gutterBottom>
                Patient Management
            </Typography>
            <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                View and manage patient information from the dental clinic database
            </Typography>

            {loading && (
                <Box display="flex" justifyContent="center" mt={3}>
                    <CircularProgress />
                </Box>
            )}

            {error && (
                <Alert severity="error" sx={{ mt: 2 }}>
                    {error}
                    <Button onClick={() => fetchPatients(currentPage)} sx={{ ml: 2 }}>
                        Retry
                    </Button>
                </Alert>
            )}

            {!loading && !error && (
                <Card sx={{ mt: 3 }}>
                    <CardContent>
                        <Typography variant="h6" gutterBottom>
                            Patient List ({patients.length} patients)
                        </Typography>

                        <TableContainer component={Paper} sx={{ mt: 2 }}>
                            <Table>
                                <TableHead>
                                    <TableRow>
                                        <TableCell>Patient ID</TableCell>
                                        <TableCell>Name</TableCell>
                                        <TableCell>Status</TableCell>
                                        <TableCell>Age</TableCell>
                                        <TableCell>Gender</TableCell>
                                        <TableCell>First Visit</TableCell>
                                        <TableCell>Balance</TableCell>
                                        <TableCell>Insurance</TableCell>
                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {patients.map((patient) => (
                                        <TableRow key={patient.patient_id} hover>
                                            <TableCell>{patient.patient_id}</TableCell>
                                            <TableCell>
                                                {patient.preferred_name || `Patient ${patient.patient_id}`}
                                            </TableCell>
                                            <TableCell>
                                                <Chip
                                                    label={patient.patient_status || 'Unknown'}
                                                    color={getStatusColor(patient.patient_status) as any}
                                                    size="small"
                                                />
                                            </TableCell>
                                            <TableCell>
                                                {patient.age ? `${patient.age} (${patient.age_category})` : 'N/A'}
                                            </TableCell>
                                            <TableCell>
                                                {patient.gender === 'M' ? 'Male' :
                                                    patient.gender === 'F' ? 'Female' :
                                                        patient.gender || 'Unknown'}
                                            </TableCell>
                                            <TableCell>{formatDate(patient.first_visit_date)}</TableCell>
                                            <TableCell>
                                                {patient.total_balance ? formatCurrency(patient.total_balance) : 'N/A'}
                                            </TableCell>
                                            <TableCell>
                                                <Chip
                                                    label={patient.has_insurance_flag ? 'Yes' : 'No'}
                                                    color={patient.has_insurance_flag ? 'success' : 'default'}
                                                    size="small"
                                                />
                                            </TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </TableContainer>

                        {totalPages > 1 && (
                            <Box display="flex" justifyContent="center" mt={3}>
                                <Pagination
                                    count={totalPages}
                                    page={currentPage}
                                    onChange={handlePageChange}
                                    color="primary"
                                />
                            </Box>
                        )}
                    </CardContent>
                </Card>
            )}
        </Box>
    );
};

export default Patients;
