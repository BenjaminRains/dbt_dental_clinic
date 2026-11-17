import React from 'react';
import ValidatedKpiPanel from './ValidatedKpiPanel';

const Dashboard: React.FC = () => (
    <ValidatedKpiPanel
        title="Executive Dashboard"
        subtitle="Daily net collections and daily production by procedure, using the definitions signed off against OpenDental Daily Payments and Production by Procedure (Central US dates)."
    />
);

export default Dashboard;
