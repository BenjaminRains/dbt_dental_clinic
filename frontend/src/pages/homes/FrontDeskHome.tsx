import React from 'react';
import RoleHomePlaceholder from './RoleHomePlaceholder';

const FrontDeskHome: React.FC = () => (
    <RoleHomePlaceholder
        role="front-desk"
        summary="Today's schedule, check-in, and patient flow."
        quickLinks={[
            { label: "Today's appointments", path: '/appointments' },
            { label: 'Patients', path: '/patients' },
            { label: 'Hygiene retention', path: '/hygiene-retention' },
        ]}
    />
);

export default FrontDeskHome;
