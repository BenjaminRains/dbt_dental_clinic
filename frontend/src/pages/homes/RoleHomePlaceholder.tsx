import React from 'react';
import { Box, Typography, Button, Grid, Paper } from '@mui/material';
import { useNavigate } from 'react-router-dom';
import { ROLE_LABELS, UserRole } from '../../context/roleTypes';

interface RoleHomePlaceholderProps {
    role: UserRole;
    summary: string;
    quickLinks: { label: string; path: string }[];
}

const RoleHomePlaceholder: React.FC<RoleHomePlaceholderProps> = ({
    role,
    summary,
    quickLinks,
}) => {
    const navigate = useNavigate();

    return (
        <Box>
            <Typography variant="h4" component="h1" gutterBottom>
                {ROLE_LABELS[role]} Home
            </Typography>
            <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                {summary}
            </Typography>
            <Grid container spacing={2} sx={{ mt: 2 }}>
                {quickLinks.map((link) => (
                    <Grid item xs={12} sm={6} md={4} key={link.path}>
                        <Paper variant="outlined" sx={{ p: 2 }}>
                            <Typography variant="subtitle1" gutterBottom>
                                {link.label}
                            </Typography>
                            <Button variant="outlined" onClick={() => navigate(link.path)}>
                                Open
                            </Button>
                        </Paper>
                    </Grid>
                ))}
            </Grid>
        </Box>
    );
};

export default RoleHomePlaceholder;
