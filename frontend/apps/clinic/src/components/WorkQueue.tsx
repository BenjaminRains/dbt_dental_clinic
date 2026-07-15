import React from 'react';
import {
    Card,
    CardContent,
    Typography,
    Chip,
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableHead,
    TableRow,
    Paper,
    Box,
    Button,
    CircularProgress,
    Alert,
} from '@mui/material';
import { Link as RouterLink } from 'react-router-dom';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';

export interface WorkQueueColumn<T> {
    key: string;
    label: string;
    align?: 'left' | 'right' | 'center';
    render: (row: T) => React.ReactNode;
}

export interface WorkQueueProps<T> {
    title: string;
    rows: T[];
    columns: WorkQueueColumn<T>[];
    getRowKey: (row: T) => string | number;
    loading?: boolean;
    error?: string | null;
    severity?: 'info' | 'warning' | 'error' | 'success';
    emptyMessage?: string;
    viewAllLink?: string;
    viewAllLabel?: string;
}

function WorkQueue<T>({
    title,
    rows,
    columns,
    getRowKey,
    loading = false,
    error = null,
    severity,
    emptyMessage = 'No items in queue',
    viewAllLink,
    viewAllLabel = 'View all',
}: WorkQueueProps<T>) {
    return (
        <Card variant="outlined" sx={{ height: '100%' }}>
            <CardContent>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Typography variant="h6" component="h2">
                            {title}
                        </Typography>
                        {!loading && (
                            <Chip label={rows.length} size="small" color={severity ?? 'default'} />
                        )}
                    </Box>
                    {viewAllLink && (
                        <Button
                            component={RouterLink}
                            to={viewAllLink}
                            size="small"
                            endIcon={<ArrowForwardIcon />}
                        >
                            {viewAllLabel}
                        </Button>
                    )}
                </Box>

                {error && (
                    <Alert severity="error" sx={{ mb: 2 }}>
                        {error}
                    </Alert>
                )}

                {loading ? (
                    <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
                        <CircularProgress size={32} />
                    </Box>
                ) : (
                    <TableContainer component={Paper} variant="outlined">
                        <Table size="small">
                            <TableHead>
                                <TableRow>
                                    {columns.map((col) => (
                                        <TableCell key={col.key} align={col.align ?? 'left'}>
                                            {col.label}
                                        </TableCell>
                                    ))}
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {rows.length === 0 ? (
                                    <TableRow>
                                        <TableCell colSpan={columns.length} align="center">
                                            <Typography variant="body2" color="text.secondary">
                                                {emptyMessage}
                                            </Typography>
                                        </TableCell>
                                    </TableRow>
                                ) : (
                                    rows.map((row) => (
                                        <TableRow key={getRowKey(row)} hover>
                                            {columns.map((col) => (
                                                <TableCell key={col.key} align={col.align ?? 'left'}>
                                                    {col.render(row)}
                                                </TableCell>
                                            ))}
                                        </TableRow>
                                    ))
                                )}
                            </TableBody>
                        </Table>
                    </TableContainer>
                )}
            </CardContent>
        </Card>
    );
}

export default WorkQueue;
