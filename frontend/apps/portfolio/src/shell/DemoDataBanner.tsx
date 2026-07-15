import React, { useEffect, useState } from 'react';
import { Alert, IconButton, Typography } from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';

const STORAGE_KEY = 'syntheticDataBannerDismissed';

/** Portfolio-only notice that demo dashboards use synthetic data. */
const DemoDataBanner: React.FC = () => {
    const [dismissed, setDismissed] = useState(false);

    useEffect(() => {
        if (localStorage.getItem(STORAGE_KEY) === 'true') {
            setDismissed(true);
        }
    }, []);

    if (dismissed) {
        return null;
    }

    const handleClose = () => {
        setDismissed(true);
        localStorage.setItem(STORAGE_KEY, 'true');
    };

    return (
        <Alert
            severity="info"
            onClose={handleClose}
            sx={{ mb: 3 }}
            action={
                <IconButton
                    aria-label="close"
                    color="inherit"
                    size="small"
                    onClick={handleClose}
                >
                    <CloseIcon fontSize="inherit" />
                </IconButton>
            }
        >
            <Typography variant="body2">
                <strong>Demo Site Notice:</strong> This demonstration dashboard uses
                synthetic data for portfolio purposes. The production platform I built
                serves real dental practices with live patient data across multiple clinics
                and 40,000+ patients, demonstrating production-grade data engineering
                capabilities.
            </Typography>
        </Alert>
    );
};

export default DemoDataBanner;
