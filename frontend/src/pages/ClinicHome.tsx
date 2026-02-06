import React from 'react';
import { Box, Typography, Paper } from '@mui/material';
import LocalHospitalIcon from '@mui/icons-material/LocalHospital';

/**
 * Clinic site home page (clinic.dbtdentalclinic.com).
 * Placeholder until the clinic-specific UI is developed.
 * Not used on the demo/portfolio site (Portfolio_v2 at dbtdentalclinic.com).
 */
const ClinicHome: React.FC = () => {
    return (
        <Box sx={{ p: 3 }}>
            <Paper sx={{ p: 4, maxWidth: 600, mx: 'auto', textAlign: 'center' }} elevation={1}>
                <LocalHospitalIcon sx={{ fontSize: 48, color: 'primary.main', mb: 2 }} />
                <Typography variant="h5" component="h1" gutterBottom>
                    Clinic Analytics
                </Typography>
                <Typography variant="body1" color="text.secondary">
                    This is the clinic site (clinic.dbtdentalclinic.com). Content and workflows will be developed here.
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
                    Use the sidebar to open Dashboard, Revenue, AR Aging, and other analytics.
                </Typography>
            </Paper>
        </Box>
    );
};

export default ClinicHome;
