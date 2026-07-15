import React from 'react';
import RoleHomePlaceholder from './RoleHomePlaceholder';

const AdminHome: React.FC = () => (
    <RoleHomePlaceholder
        role="admin"
        summary="Full clinic portal access — all role homes and reports."
        quickLinks={[
            { label: 'Dashboard', path: '/dashboard' },
            { label: 'Practice Manager home', path: '/home/practice-manager' },
            { label: 'Owner home', path: '/home/owner' },
            { label: 'Front Desk home', path: '/home/front-desk' },
            { label: 'Insurance home', path: '/home/insurance' },
            { label: 'AR aging', path: '/ar-aging' },
            { label: 'Revenue', path: '/revenue' },
            { label: 'Appointments', path: '/appointments' },
        ]}
    />
);

export default AdminHome;
