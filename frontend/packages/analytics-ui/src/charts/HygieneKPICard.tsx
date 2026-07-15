import React from 'react';
import {
    Card,
    CardContent,
    Typography,
    Box,
    LinearProgress,
    useTheme,
} from '@mui/material';

interface HygieneKPICardProps {
    title: string;
    value: number;
    isPercentage?: boolean;
    color?: string;
}

const HygieneKPICard: React.FC<HygieneKPICardProps> = ({
    title,
    value,
    isPercentage = false,
    color = '#26a69a', // Default teal color
}) => {
    const theme = useTheme();

    const formatValue = (): string => {
        if (isPercentage) {
            return `${value.toFixed(1)}%`;
        }
        return value.toLocaleString();
    };

    // Clamp percentage between 0 and 100 for progress bar
    const progressValue = isPercentage ? Math.min(Math.max(value, 0), 100) : 0;

    return (
        <Card
            sx={{
                height: '100%',
                display: 'flex',
                flexDirection: 'column',
                borderRadius: 2,
                boxShadow: 2,
                transition: 'transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out',
                '&:hover': {
                    transform: 'translateY(-4px)',
                    boxShadow: theme.shadows[8],
                },
            }}
        >
            <CardContent sx={{ flexGrow: 1, p: 3 }}>
                <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{
                        fontSize: '0.875rem',
                        fontWeight: 500,
                        color: '#757575',
                        mb: 2,
                    }}
                >
                    {title}
                </Typography>

                <Typography
                    variant="h3"
                    component="div"
                    sx={{
                        fontWeight: 'bold',
                        color: '#1a237e',
                        fontSize: '3rem',
                        mb: 2,
                        lineHeight: 1.2,
                    }}
                >
                    {formatValue()}
                </Typography>

                {isPercentage && (
                    <Box sx={{ mt: 2 }}>
                        <LinearProgress
                            variant="determinate"
                            value={progressValue}
                            sx={{
                                height: 8,
                                borderRadius: 4,
                                backgroundColor: '#e0e0e0',
                                '& .MuiLinearProgress-bar': {
                                    borderRadius: 4,
                                    backgroundColor: color,
                                },
                            }}
                        />
                    </Box>
                )}
            </CardContent>
        </Card>
    );
};

export default HygieneKPICard;

