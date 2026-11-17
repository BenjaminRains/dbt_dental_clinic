import React from 'react';
import ValidatedKpiPanel from '@mdc/analytics-ui/pages/ValidatedKpiPanel';

const PracticeManagerHome: React.FC = () => (
    <ValidatedKpiPanel
        title="Practice Manager Home"
        subtitle="Validated KPIs — reconciled against OpenDental Daily Payments and Production by Procedure (Central US dates)"
    />
);

export default PracticeManagerHome;
