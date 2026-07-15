import React from 'react';
import { Typography } from '@mui/material';
import { ROLE_LABELS } from '../auth/roleTypes';
import { useAuth } from '../auth/AuthContext';

const RoleSwitcher: React.FC = () => {
    const { displayName, role } = useAuth();

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
