import React from 'react';
import RoleHomePlaceholder from './RoleHomePlaceholder';

const InsuranceHome: React.FC = () => (
    <RoleHomePlaceholder
        role="insurance"
        summary="Claims, AR aging, and insurance follow-up."
        quickLinks={[
            { label: 'AR aging', path: '/ar-aging' },
            { label: 'Revenue', path: '/revenue' },
            { label: 'Treatment acceptance', path: '/treatment-acceptance' },
        ]}
    />
);

export default InsuranceHome;
