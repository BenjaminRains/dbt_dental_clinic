import React from 'react';
import {
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableRow,
    Paper,
} from '@mui/material';

interface Metric {
    label: string;
    value: number | string;
    isPercentage?: boolean;
}

interface MetricsTableProps {
    metrics: Metric[];
}

const MetricsTable: React.FC<MetricsTableProps> = ({ metrics }) => {
    const formatValue = (value: number | string, isPercentage?: boolean): string => {
        if (typeof value === 'string') return value;
        if (isPercentage) {
            return `${value.toFixed(1)}%`;
        }
        return value.toLocaleString();
    };

    return (
        <TableContainer
            component={Paper}
            sx={{
                borderRadius: 2,
                boxShadow: 2,
            }}
        >
            <Table>
                <TableBody>
                    {metrics.map((metric, index) => (
                        <React.Fragment key={metric.label}>
                            <TableRow
                                sx={{
                                    '&:last-child td, &:last-child th': { border: 0 },
                                }}
                            >
                                <TableCell
                                    sx={{
                                        borderBottom: index < metrics.length - 1 ? '1px solid #e0e0e0' : 'none',
                                        color: '#757575',
                                        fontSize: '0.875rem',
                                    }}
                                >
                                    {metric.label}
                                </TableCell>
                                <TableCell
                                    align="right"
                                    sx={{
                                        borderBottom: index < metrics.length - 1 ? '1px solid #e0e0e0' : 'none',
                                        color: '#1a237e',
                                        fontSize: '1rem',
                                        fontWeight: 500,
                                    }}
                                >
                                    {formatValue(metric.value, metric.isPercentage)}
                                </TableCell>
                            </TableRow>
                        </React.Fragment>
                    ))}
                </TableBody>
            </Table>
        </TableContainer>
    );
};

export default MetricsTable;

