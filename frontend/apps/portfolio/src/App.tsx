import { Routes, Route, Navigate } from 'react-router-dom';
import { Suspense, lazy, type ReactNode } from 'react';
import { Box, CircularProgress } from '@mui/material';
import DemoLayout from './shell/DemoLayout';
import Portfolio from './pages/Portfolio';
import AgentProfile from './pages/AgentProfile';

const Dashboard = lazy(() => import('@mdc/analytics-ui/pages/Dashboard'));
const Revenue = lazy(() => import('@mdc/analytics-ui/pages/Revenue'));
const Providers = lazy(() => import('@mdc/analytics-ui/pages/Providers'));
const Patients = lazy(() => import('@mdc/analytics-ui/pages/Patients'));
const Appointments = lazy(() => import('@mdc/analytics-ui/pages/Appointments'));
const AR = lazy(() => import('@mdc/analytics-ui/pages/AR'));
const TreatmentAcceptance = lazy(() => import('@mdc/analytics-ui/pages/TreatmentAcceptance'));
const HygieneRetention = lazy(() => import('@mdc/analytics-ui/pages/HygieneRetention'));
const ReferralSources = lazy(() => import('@mdc/analytics-ui/pages/ReferralSources'));
const KPIDefinitions = lazy(() => import('@mdc/analytics-ui/pages/KPIDefinitions'));
const EnvironmentManager = lazy(() => import('./pages/EnvironmentManager'));
const SchemaDiscovery = lazy(() => import('./pages/SchemaDiscovery'));

const PageLoader = () => (
    <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '400px' }}>
        <CircularProgress />
    </Box>
);

function withSuspense(element: ReactNode) {
    return <Suspense fallback={<PageLoader />}>{element}</Suspense>;
}

function pageShell(element: ReactNode) {
    return (
        <Box sx={{ display: 'flex', minHeight: '100vh' }}>
            <DemoLayout>{element}</DemoLayout>
        </Box>
    );
}

function App() {
    return (
        <Routes>
            <Route path="/" element={<Portfolio />} />
            <Route path="/login" element={<Navigate to="/" replace />} />
            <Route path="/home/*" element={<Navigate to="/" replace />} />

            <Route
                path="/agent-profile"
                element={
                    <Box sx={{ minHeight: '100vh' }}>
                        <AgentProfile />
                    </Box>
                }
            />

            <Route path="/dashboard" element={pageShell(withSuspense(<Dashboard />))} />
            <Route path="/revenue" element={pageShell(withSuspense(<Revenue />))} />
            <Route path="/providers" element={pageShell(withSuspense(<Providers />))} />
            <Route path="/patients" element={pageShell(withSuspense(<Patients />))} />
            <Route path="/appointments" element={pageShell(withSuspense(<Appointments />))} />
            <Route path="/ar-aging" element={pageShell(withSuspense(<AR />))} />
            <Route path="/treatment-acceptance" element={pageShell(withSuspense(<TreatmentAcceptance />))} />
            <Route path="/hygiene-retention" element={pageShell(withSuspense(<HygieneRetention />))} />
            <Route path="/referral-sources" element={pageShell(withSuspense(<ReferralSources />))} />
            <Route path="/kpi-definitions" element={pageShell(withSuspense(<KPIDefinitions />))} />
            <Route path="/environment-manager" element={pageShell(withSuspense(<EnvironmentManager />))} />
            <Route path="/schema-discovery" element={pageShell(withSuspense(<SchemaDiscovery />))} />

            <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
    );
}

export default App;
