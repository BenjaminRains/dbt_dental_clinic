import React from 'react';
import RoleHomePlaceholder from './RoleHomePlaceholder';

const OwnerHome: React.FC = () => (
    <RoleHomePlaceholder
        role="owner"
        summary="Practice-wide performance, collections, and schedule at a glance."
        quickLinks={[
            { label: 'Revenue', path: '/revenue' },
            { label: 'AR aging', path: '/ar-aging' },
            { label: 'Appointments', path: '/appointments' },
            { label: 'Dashboard', path: '/dashboard' },
        ]}
    />
);

export default OwnerHome;
