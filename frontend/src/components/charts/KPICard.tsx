import React from 'react';
import {
    Card,
    CardContent,
    Typography,
    Box,
    Chip,
    useTheme,
} from '@mui/material';
import {
    TrendingUp,
    TrendingDown,
    TrendingFlat,
} from '@mui/icons-material';

interface KPICardProps {
    title: string;
    value: string | number;
    subtitle?: string;
    trend?: {
        value: number;
        label: string;
        isPositive?: boolean;
    };
    color?: 'primary' | 'secondary' | 'success' | 'error' | 'warning' | 'info';
    icon?: React.ReactNode;
    format?: 'currency' | 'percentage' | 'number';
}

const KPICard: React.FC<KPICardProps> = ({
    title,
    value,
    subtitle,
    trend,
    color = 'primary',
    icon,
    format = 'number',
}) => {
    const theme = useTheme();

    const formatValue = (val: string | number): string => {
        if (typeof val === 'string') return val;

        switch (format) {
            case 'currency':
                return new Intl.NumberFormat('en-US', {
                    style: 'currency',
                    currency: 'USD',
                }).format(val);
            case 'percentage':
                return `${val.toFixed(1)}%`;
            default:
                return val.toLocaleString();
        }
    };

    const getTrendIcon = () => {
        if (!trend) return null;
        if (trend.value > 0) return <TrendingUp />;
        if (trend.value < 0) return <TrendingDown />;
        return <TrendingFlat />;
    };

    const getTrendColor = (): 'success' | 'error' | 'default' => {
        if (!trend) return 'default';
        if (trend.isPositive !== undefined) {
            return trend.isPositive ? 'success' : 'error';
        }
        if (trend.value > 0) return 'success';
        if (trend.value < 0) return 'error';
        return 'default';
    };

    return (
        <Card
            sx={{
                height: '100%',
                display: 'flex',
                flexDirection: 'column',
                transition: 'transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out',
                '&:hover': {
                    transform: 'translateY(-4px)',
                    boxShadow: theme.shadows[8],
                },
            }}
        >
            <CardContent sx={{ flexGrow: 1 }}>
                <Box display="flex" alignItems="center" justifyContent="space-between" mb={2}>
                    <Typography
                        variant="h6"
                        color="text.secondary"
                        sx={{ fontSize: '0.875rem', fontWeight: 500 }}
                    >
                        {title}
                    </Typography>
                    {icon && (
                        <Box
                            sx={{
                                p: 1,
                                borderRadius: 1,
                                backgroundColor: `${color}.light`,
                                color: `${color}.main`,
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                            }}
                        >
                            {icon}
                        </Box>
                    )}
                </Box>

                <Typography
                    variant="h4"
                    component="div"
                    sx={{
                        fontWeight: 'bold',
                        color: `${color}.main`,
                        mb: 1,
                    }}
                >
                    {formatValue(value)}
                </Typography>

                {subtitle && (
                    <Typography
                        variant="body2"
                        color="text.secondary"
                        sx={{ mb: 1 }}
                    >
                        {subtitle}
                    </Typography>
                )}

                {trend && (
                    <Box display="flex" alignItems="center" gap={1}>
                        <Chip
                            icon={getTrendIcon() || undefined}
                            label={`${trend.value > 0 ? '+' : ''}${trend.value.toFixed(1)}% ${trend.label}`}
                            size="small"
                            color={getTrendColor()}
                            variant="outlined"
                            sx={{
                                fontSize: '0.75rem',
                                height: 24,
                            }}
                        />
                    </Box>
                )}
            </CardContent>
        </Card>
    );
};

export default KPICard;