import React from 'react';
import {
    Card,
    CardContent,
    Typography,
    Chip,
    Box,
    Button,
} from '@mui/material';
import { Link as RouterLink } from 'react-router-dom';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';

export type InsightSeverity = 'info' | 'warning' | 'error' | 'success';

export interface InsightCardProps {
    metric: string;
    narrative: string;
    severity?: InsightSeverity;
    recommendedAction?: string;
    drillDownLink?: string;
    drillDownLabel?: string;
}

const severityColors: Record<InsightSeverity, 'info' | 'warning' | 'error' | 'success'> = {
    info: 'info',
    warning: 'warning',
    error: 'error',
    success: 'success',
};

const InsightCard: React.FC<InsightCardProps> = ({
    metric,
    narrative,
    severity = 'info',
    recommendedAction,
    drillDownLink,
    drillDownLabel = 'Investigate',
}) => {
    return (
        <Card sx={{ height: '100%' }} variant="outlined">
            <CardContent>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 1 }}>
                    <Typography variant="subtitle2" color="text.secondary">
                        {metric}
                    </Typography>
                    <Chip label="Needs attention" size="small" color={severityColors[severity]} />
                </Box>
                <Typography variant="body1" sx={{ mb: recommendedAction ? 1.5 : 0 }}>
                    {narrative}
                </Typography>
                {recommendedAction && (
                    <Typography variant="body2" color="text.secondary" sx={{ mb: drillDownLink ? 1.5 : 0 }}>
                        <strong>Recommended:</strong> {recommendedAction}
                    </Typography>
                )}
                {drillDownLink && (
                    <Button
                        component={RouterLink}
                        to={drillDownLink}
                        size="small"
                        endIcon={<ArrowForwardIcon />}
                    >
                        {drillDownLabel}
                    </Button>
                )}
            </CardContent>
        </Card>
    );
};

export default InsightCard;
