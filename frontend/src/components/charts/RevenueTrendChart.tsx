import React from 'react';
import {
    LineChart,
    Line,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    ResponsiveContainer,
    Legend,
} from 'recharts';
import { Box, Typography, Paper } from '@mui/material';
import { RevenueTrend } from '../../types/api';

interface RevenueTrendChartProps {
    data: RevenueTrend[];
    title?: string;
    height?: number;
}

const RevenueTrendChart: React.FC<RevenueTrendChartProps> = ({
    data,
    title = "Revenue Trend Analysis",
    height = 400,
}) => {
    // Format data for better display
    const formattedData = data.map(item => ({
        ...item,
        date: new Date(item.date).toLocaleDateString('en-US', {
            month: 'short',
            day: 'numeric'
        }),
        revenue_lost_formatted: `$${(item.revenue_lost / 1000).toFixed(1)}k`,
        recovery_potential_formatted: `$${(item.recovery_potential / 1000).toFixed(1)}k`,
    }));

    const CustomTooltip = ({ active, payload, label }: any) => {
        if (active && payload && payload.length) {
            return (
                <Paper sx={{ p: 2, boxShadow: 3 }}>
                    <Typography variant="subtitle2" gutterBottom>
                        {label}
                    </Typography>
                    {payload.map((entry: any, index: number) => (
                        <Typography
                            key={index}
                            variant="body2"
                            sx={{ color: entry.color }}
                        >
                            {entry.name}: {entry.value.toLocaleString()}
                        </Typography>
                    ))}
                </Paper>
            );
        }
        return null;
    };

    return (
        <Box>
            <Typography variant="h6" gutterBottom>
                {title}
            </Typography>
            <ResponsiveContainer width="100%" height={height}>
                <LineChart
                    data={formattedData}
                    margin={{
                        top: 20,
                        right: 30,
                        left: 20,
                        bottom: 20,
                    }}
                >
                    <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                    <XAxis
                        dataKey="date"
                        stroke="#666"
                        fontSize={12}
                    />
                    <YAxis
                        stroke="#666"
                        fontSize={12}
                        tickFormatter={(value) => `$${(value / 1000).toFixed(0)}k`}
                    />
                    <Tooltip content={<CustomTooltip />} />
                    <Legend />
                    <Line
                        type="monotone"
                        dataKey="revenue_lost"
                        stroke="#f44336"
                        strokeWidth={3}
                        dot={{ fill: '#f44336', strokeWidth: 2, r: 4 }}
                        activeDot={{ r: 6, stroke: '#f44336', strokeWidth: 2 }}
                        name="Revenue Lost"
                    />
                    <Line
                        type="monotone"
                        dataKey="recovery_potential"
                        stroke="#4caf50"
                        strokeWidth={3}
                        dot={{ fill: '#4caf50', strokeWidth: 2, r: 4 }}
                        activeDot={{ r: 6, stroke: '#4caf50', strokeWidth: 2 }}
                        name="Recovery Potential"
                    />
                </LineChart>
            </ResponsiveContainer>
        </Box>
    );
};

export default RevenueTrendChart;
