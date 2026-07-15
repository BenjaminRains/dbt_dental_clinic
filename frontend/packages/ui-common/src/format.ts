import { ChipProps } from '@mui/material';

export function formatCurrency(amount: number): string {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        maximumFractionDigits: 0,
    }).format(amount);
}

export function formatCurrencyPrecise(amount: number): string {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
    }).format(amount);
}

export function formatPercent(value: number, decimals = 1): string {
    return `${value.toFixed(decimals)}%`;
}

export function getPriorityScoreColor(score: number): ChipProps['color'] {
    if (score >= 80) return 'error';
    if (score >= 60) return 'warning';
    if (score >= 40) return 'info';
    return 'default';
}

export function getRiskCategoryColor(category: string): ChipProps['color'] {
    switch (category.toLowerCase()) {
        case 'high risk':
            return 'error';
        case 'medium risk':
            return 'warning';
        case 'moderate risk':
            return 'info';
        case 'low risk':
            return 'success';
        default:
            return 'default';
    }
}

export function getRecoveryPotentialColor(potential: string): ChipProps['color'] {
    switch (potential) {
        case 'High':
            return 'error';
        case 'Medium':
            return 'warning';
        case 'Low':
            return 'info';
        default:
            return 'default';
    }
}
