import { Routes, Route } from 'react-router-dom';
import { Suspense, lazy } from 'react';
import { Box, CircularProgress } from '@mui/material';
import Layout from './components/layout/Layout';
import Portfolio from './pages/Portfolio_v4';
import ClinicRoot from './pages/ClinicRoot';
import Login from './pages/Login';
import AgentProfile from './pages/AgentProfile';
import ClinicAuthGate from './components/auth/ClinicAuthGate';
import { isClinicSite } from './config/clinicSite';

const Dashboard = lazy(() => import('./pages/Dashboard'));
const Revenue = lazy(() => import('./pages/Revenue'));
const Providers = lazy(() => import('./pages/Providers'));
const Patients = lazy(() => import('./pages/Patients'));
const Appointments = lazy(() => import('./pages/Appointments'));
const AR = lazy(() => import('./pages/AR'));
const TreatmentAcceptance = lazy(() => import('./pages/TreatmentAcceptance'));
const HygieneRetention = lazy(() => import('./pages/HygieneRetention'));
const ReferralSources = lazy(() => import('./pages/ReferralSources'));
const KPIDefinitions = lazy(() => import('./pages/KPIDefinitions'));
const EnvironmentManager = lazy(() => import('./pages/EnvironmentManager'));
const SchemaDiscovery = lazy(() => import('./pages/SchemaDiscovery'));
const PracticeManagerHome = lazy(() => import('./pages/homes/PracticeManagerHome'));
const OwnerHome = lazy(() => import('./pages/homes/OwnerHome'));
const FrontDeskHome = lazy(() => import('./pages/homes/FrontDeskHome'));
const InsuranceHome = lazy(() => import('./pages/homes/InsuranceHome'));

const PageLoader = () => (
    <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '400px' }}>
        <CircularProgress />
    </Box>
);

function clinicPage(element: React.ReactNode) {
    return (
        <Box sx={{ display: 'flex', minHeight: '100vh' }}>
            <Layout>
                <ClinicAuthGate>{element}</ClinicAuthGate>
            </Layout>
        </Box>
    );
}

function demoPageShell(element: React.ReactNode) {
    return (
        <Box sx={{ display: 'flex', minHeight: '100vh' }}>
            <Layout>{element}</Layout>
        </Box>
    );
}

function pageShell(element: React.ReactNode) {
    if (!isClinicSite()) {
        return demoPageShell(element);
    }
    return clinicPage(element);
}

function App() {
    const clinicSite = isClinicSite();

    return (
        <Routes>
            <Route
                path="/login"
                element={clinicSite ? <Login /> : <Portfolio />}
            />

            <Route
                path="/"
                element={
                    clinicSite ? (
                        clinicPage(
                            <Suspense fallback={<PageLoader />}>
                                <ClinicRoot />
                            </Suspense>
                        )
                    ) : (
                        <Portfolio />
                    )
                }
            />

            <Route
                path="/home/practice-manager"
                element={pageShell(
                    <Suspense fallback={<PageLoader />}>
                        <PracticeManagerHome />
                    </Suspense>
                )}
            />
            <Route
                path="/home/owner"
                element={pageShell(
                    <Suspense fallback={<PageLoader />}>
                        <OwnerHome />
                    </Suspense>
                )}
            />
            <Route
                path="/home/front-desk"
                element={pageShell(
                    <Suspense fallback={<PageLoader />}>
                        <FrontDeskHome />
                    </Suspense>
                )}
            />
            <Route
                path="/home/insurance"
                element={pageShell(
                    <Suspense fallback={<PageLoader />}>
                        <InsuranceHome />
                    </Suspense>
                )}
            />

            <Route
                path="/agent-profile"
                element={
                    <Box sx={{ minHeight: '100vh' }}>
                        <AgentProfile />
                    </Box>
                }
            />

            <Route path="/dashboard" element={pageShell(
                <Suspense fallback={<PageLoader />}>
                    <Dashboard />
                </Suspense>
            )} />
            <Route path="/revenue" element={pageShell(
                <Suspense fallback={<PageLoader />}>
                    <Revenue />
                </Suspense>
            )} />
            <Route path="/providers" element={pageShell(
                <Suspense fallback={<PageLoader />}>
                    <Providers />
                </Suspense>
            )} />
            <Route path="/patients" element={pageShell(
                <Suspense fallback={<PageLoader />}>
                    <Patients />
                </Suspense>
            )} />
            <Route path="/appointments" element={pageShell(
                <Suspense fallback={<PageLoader />}>
                    <Appointments />
                </Suspense>
            )} />
            <Route path="/ar-aging" element={pageShell(
                <Suspense fallback={<PageLoader />}>
                    <AR />
                </Suspense>
            )} />
            <Route path="/treatment-acceptance" element={pageShell(
                <Suspense fallback={<PageLoader />}>
                    <TreatmentAcceptance />
                </Suspense>
            )} />
            <Route path="/hygiene-retention" element={pageShell(
                <Suspense fallback={<PageLoader />}>
                    <HygieneRetention />
                </Suspense>
            )} />
            <Route path="/referral-sources" element={pageShell(
                <Suspense fallback={<PageLoader />}>
                    <ReferralSources />
                </Suspense>
            )} />
            <Route path="/kpi-definitions" element={pageShell(
                <Suspense fallback={<PageLoader />}>
                    <KPIDefinitions />
                </Suspense>
            )} />
            <Route
                path="/environment-manager"
                element={pageShell(
                    <Suspense fallback={<PageLoader />}>
                        <EnvironmentManager />
                    </Suspense>
                )}
            />
            <Route
                path="/schema-discovery"
                element={pageShell(
                    <Suspense fallback={<PageLoader />}>
                        <SchemaDiscovery />
                    </Suspense>
                )}
            />
        </Routes>
    );
}

export default App;
