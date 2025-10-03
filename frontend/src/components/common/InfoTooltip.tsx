import React, { useState } from 'react';
import {
    Tooltip,
    IconButton,
    Box,
    Typography,
    Divider,
    Chip,
    Link,
} from '@mui/material';
import {
    InfoOutlined,
    DataObject,
    Schedule,
    Business,
    Code,
} from '@mui/icons-material';
import { MetricLineageInfo } from '../../types/api';

interface InfoTooltipProps {
    metricName: string;
    lineageInfo?: MetricLineageInfo;
    loading?: boolean;
    error?: string;
}

const InfoTooltip: React.FC<InfoTooltipProps> = ({
    metricName,
    lineageInfo,
    loading = false,
    error
}) => {
    const [open, setOpen] = useState(false);

    const handleTooltipOpen = () => {
        setOpen(true);
    };

    const handleTooltipClose = () => {
        setOpen(false);
    };

    const renderTooltipContent = () => {
        if (loading) {
            return (
                <Box sx={{ p: 1, minWidth: 200 }}>
                    <Typography variant="body2">Loading lineage information...</Typography>
                </Box>
            );
        }

        if (error) {
            return (
                <Box sx={{ p: 1, minWidth: 200 }}>
                    <Typography variant="body2" color="error">
                        Error loading lineage: {error}
                    </Typography>
                </Box>
            );
        }

        if (!lineageInfo) {
            return (
                <Box sx={{ p: 1, minWidth: 200 }}>
                    <Typography variant="body2">
                        No lineage information available for this metric.
                    </Typography>
                </Box>
            );
        }

        return (
            <Box sx={{ p: 2, minWidth: 300, maxWidth: 400 }}>
                {/* Header */}
                <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                    <DataObject sx={{ mr: 1, fontSize: 16, color: 'primary.main' }} />
                    <Typography variant="subtitle2" fontWeight="bold">
                        Data Lineage
                    </Typography>
                </Box>

                {/* Source Information */}
                <Box sx={{ mb: 2 }}>
                    <Typography variant="body2" color="text.secondary" gutterBottom>
                        Source Model:
                    </Typography>
                    <Chip
                        label={`${lineageInfo.source_schema}.${lineageInfo.source_model}`}
                        size="small"
                        color="primary"
                        variant="outlined"
                        sx={{ mb: 1 }}
                    />
                </Box>

                {/* Business Definition */}
                <Box sx={{ mb: 2 }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                        <Business sx={{ mr: 1, fontSize: 14, color: 'success.main' }} />
                        <Typography variant="body2" fontWeight="medium">
                            Business Definition
                        </Typography>
                    </Box>
                    <Typography variant="body2" sx={{ pl: 3 }}>
                        {lineageInfo.business_definition}
                    </Typography>
                </Box>

                <Divider sx={{ my: 1 }} />

                {/* Calculation Details */}
                <Box sx={{ mb: 2 }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                        <Code sx={{ mr: 1, fontSize: 14, color: 'info.main' }} />
                        <Typography variant="body2" fontWeight="medium">
                            Calculation
                        </Typography>
                    </Box>
                    <Typography variant="body2" sx={{ pl: 3 }}>
                        {lineageInfo.calculation_description}
                    </Typography>
                </Box>

                {/* Data Dependencies */}
                {lineageInfo.dependencies && lineageInfo.dependencies.length > 0 && (
                    <Box sx={{ mb: 2 }}>
                        <Typography variant="body2" color="text.secondary" gutterBottom>
                            Dependencies:
                        </Typography>
                        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, pl: 3 }}>
                            {lineageInfo.dependencies.map((dep, index) => (
                                <Chip
                                    key={index}
                                    label={dep}
                                    size="small"
                                    variant="outlined"
                                    color="secondary"
                                />
                            ))}
                        </Box>
                    </Box>
                )}

                <Divider sx={{ my: 1 }} />

                {/* Data Freshness */}
                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <Box sx={{ display: 'flex', alignItems: 'center' }}>
                        <Schedule sx={{ mr: 1, fontSize: 14, color: 'warning.main' }} />
                        <Typography variant="body2" color="text.secondary">
                            Refresh: {lineageInfo.data_freshness}
                        </Typography>
                    </Box>
                    <Typography variant="caption" color="text.secondary">
                        {lineageInfo.last_updated &&
                            new Date(lineageInfo.last_updated).toLocaleDateString()
                        }
                    </Typography>
                </Box>

                {/* DBT Documentation Link */}
                <Box sx={{ mt: 2, pt: 1, borderTop: '1px solid', borderColor: 'divider' }}>
                    <Link
                        href="#"
                        variant="caption"
                        onClick={(e) => {
                            e.preventDefault();
                            // In a real implementation, this would open DBT docs
                            console.log('Open DBT documentation for:', lineageInfo.source_model);
                        }}
                        sx={{ textDecoration: 'none' }}
                    >
                        View DBT Documentation →
                    </Link>
                </Box>
            </Box>
        );
    };

    return (
        <Tooltip
            title={renderTooltipContent()}
            open={open}
            onOpen={handleTooltipOpen}
            onClose={handleTooltipClose}
            placement="top"
            arrow
            PopperProps={{
                sx: {
                    '& .MuiTooltip-tooltip': {
                        backgroundColor: 'background.paper',
                        color: 'text.primary',
                        border: '1px solid',
                        borderColor: 'divider',
                        boxShadow: 2,
                        p: 0,
                    },
                },
            }}
        >
            <IconButton
                size="small"
                sx={{
                    ml: 0.5,
                    p: 0.25,
                    color: 'text.secondary',
                    '&:hover': {
                        color: 'primary.main',
                        backgroundColor: 'action.hover',
                    },
                }}
                aria-label={`View data lineage for ${metricName}`}
            >
                <InfoOutlined fontSize="small" />
            </IconButton>
        </Tooltip>
    );
};

export default InfoTooltip;
