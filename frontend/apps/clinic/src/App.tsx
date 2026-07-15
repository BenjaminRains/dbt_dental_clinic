import { Routes, Route, Navigate } from 'react-router-dom';
import { Suspense, lazy, type ReactNode } from 'react';
import { Box, CircularProgress } from '@mui/material';
import ClinicLayout from './shell/ClinicLayout';
import ClinicRoot from './pages/ClinicRoot';
import Login from './pages/Login';
import ClinicAuthGate from './auth/ClinicAuthGate';

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
const PracticeManagerHome = lazy(() => import('./pages/homes/PracticeManagerHome'));
const OwnerHome = lazy(() => import('./pages/homes/OwnerHome'));
const FrontDeskHome = lazy(() => import('./pages/homes/FrontDeskHome'));
const InsuranceHome = lazy(() => import('./pages/homes/InsuranceHome'));

const PageLoader = () => (
    <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '400px' }}>
        <CircularProgress />
    </Box>
);

function withSuspense(element: ReactNode) {
    return <Suspense fallback={<PageLoader />}>{element}</Suspense>;
}

function clinicPage(element: ReactNode) {
    return (
        <Box sx={{ display: 'flex', minHeight: '100vh' }}>
            <ClinicLayout>
                <ClinicAuthGate>{element}</ClinicAuthGate>
            </ClinicLayout>
        </Box>
    );
}

function App() {
    return (
        <Routes>
            <Route path="/login" element={<Login />} />
            <Route path="/" element={clinicPage(withSuspense(<ClinicRoot />))} />
            <Route path="/home/practice-manager" element={clinicPage(withSuspense(<PracticeManagerHome />))} />
            <Route path="/home/owner" element={clinicPage(withSuspense(<OwnerHome />))} />
            <Route path="/home/front-desk" element={clinicPage(withSuspense(<FrontDeskHome />))} />
            <Route path="/home/insurance" element={clinicPage(withSuspense(<InsuranceHome />))} />

            <Route path="/dashboard" element={clinicPage(withSuspense(<Dashboard />))} />
            <Route path="/revenue" element={clinicPage(withSuspense(<Revenue />))} />
            <Route path="/providers" element={clinicPage(withSuspense(<Providers />))} />
            <Route path="/patients" element={clinicPage(withSuspense(<Patients />))} />
            <Route path="/appointments" element={clinicPage(withSuspense(<Appointments />))} />
            <Route path="/ar-aging" element={clinicPage(withSuspense(<AR />))} />
            <Route path="/treatment-acceptance" element={clinicPage(withSuspense(<TreatmentAcceptance />))} />
            <Route path="/hygiene-retention" element={clinicPage(withSuspense(<HygieneRetention />))} />
            <Route path="/referral-sources" element={clinicPage(withSuspense(<ReferralSources />))} />
            <Route path="/kpi-definitions" element={clinicPage(withSuspense(<KPIDefinitions />))} />

            <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
    );
}

export default App;
