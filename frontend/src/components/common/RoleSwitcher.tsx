import React from 'react';
import { Typography } from '@mui/material';
import { isClinicSite } from '../../config/clinicSite';
import { ROLE_LABELS } from '../../context/roleTypes';
import { useAuth } from '../../context/AuthContext';

const RoleSwitcher: React.FC = () => {
    const { displayName, role } = useAuth();

    if (!isClinicSite()) {
        return null;
    }

    return (
        <Typography variant="body2" sx={{ ml: 2, textAlign: 'right', lineHeight: 1.3 }}>
            {displayName}
            <br />
            <Typography component="span" variant="caption" sx={{ opacity: 0.85 }}>
                {ROLE_LABELS[role]}
            </Typography>
        </Typography>
    );
};

export default RoleSwitcher;
