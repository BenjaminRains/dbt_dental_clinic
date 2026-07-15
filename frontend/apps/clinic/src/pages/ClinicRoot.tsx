import React from 'react';
import { Navigate } from 'react-router-dom';
import { useAuth } from '../auth/AuthContext';

/** Clinic site root: send authenticated users to their role home. */
const ClinicRoot: React.FC = () => {
    const { homePath } = useAuth();
    return <Navigate to={homePath} replace />;
};

export default ClinicRoot;
