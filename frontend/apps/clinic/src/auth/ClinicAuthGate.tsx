import React from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { Box, CircularProgress } from '@mui/material';
import { useAuth } from './AuthContext';
import { canAccessPath } from './roleAccess';

interface ClinicAuthGateProps {
    children: React.ReactNode;
}

/** Require login and enforce per-role route allowlists. */
const ClinicAuthGate: React.FC<ClinicAuthGateProps> = ({ children }) => {
    const { isAuthenticated, authLoading, homePath, role } = useAuth();
    const location = useLocation();
    const path = location.pathname;

    if (authLoading) {
        return (
            <Box
                sx={{
                    display: 'flex',
                    justifyContent: 'center',
                    alignItems: 'center',
                    minHeight: '100vh',
                }}
            >
                <CircularProgress />
            </Box>
        );
    }

    if (!isAuthenticated) {
        return <Navigate to="/login" replace state={{ from: path }} />;
    }

    if (path === '/' || path === '/login') {
        return <Navigate to={homePath} replace />;
    }

    if (!canAccessPath(role, path)) {
        return <Navigate to={homePath} replace />;
    }

    return <>{children}</>;
};

export default ClinicAuthGate;
