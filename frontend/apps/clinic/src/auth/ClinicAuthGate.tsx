import React from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { Box, CircularProgress } from '@mui/material';
import { useAuth } from './AuthContext';
import { isRoleHomePath } from '../utils/roleNavigation';

interface ClinicAuthGateProps {
    children: React.ReactNode;
}

/** Clinic app requires portal login before showing app content. */
const ClinicAuthGate: React.FC<ClinicAuthGateProps> = ({ children }) => {
    const { isAuthenticated, authLoading, homePath } = useAuth();
    const location = useLocation();

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
        return <Navigate to="/login" replace state={{ from: location.pathname }} />;
    }

    if (location.pathname === '/' || location.pathname === '/login') {
        return <Navigate to={homePath} replace />;
    }

    if (isRoleHomePath(location.pathname) && location.pathname !== homePath) {
        return <Navigate to={homePath} replace />;
    }

    return <>{children}</>;
};

export default ClinicAuthGate;
