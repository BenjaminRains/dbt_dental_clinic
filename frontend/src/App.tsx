import { Routes, Route } from 'react-router-dom';
import { Suspense, lazy } from 'react';
import { Box, CircularProgress } from '@mui/material';
import Layout from './components/layout/Layout';
import Portfolio from './pages/Portfolio_v2';

// Lazy load dashboard pages for code splitting
const Dashboard = lazy(() => import('./pages/Dashboard'));
const Revenue = lazy(() => import('./pages/Revenue'));
const Providers = lazy(() => import('./pages/Providers'));
const Patients = lazy(() => import('./pages/Patients'));
const Appointments = lazy(() => import('./pages/Appointments'));
const AR = lazy(() => import('./pages/AR'));
const TreatmentAcceptance = lazy(() => import('./pages/TreatmentAcceptance'));
const HygieneRetention = lazy(() => import('./pages/HygieneRetention'));
const EnvironmentManager = lazy(() => import('./pages/EnvironmentManager'));
const SchemaDiscovery = lazy(() => import('./pages/SchemaDiscovery'));

// Loading fallback component
const PageLoader = () => (
    <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '400px' }}>
        <CircularProgress />
    </Box>
);

function App() {
    return (
        <Routes>
            {/* Portfolio page without Layout (standalone landing page) */}
            <Route path="/" element={<Portfolio />} />

            {/* All other pages with Layout (sidebar navigation) - lazy loaded for code splitting */}
            <Route path="/dashboard" element={
                <Box sx={{ display: 'flex', minHeight: '100vh' }}>
                    <Layout>
                        <Suspense fallback={<PageLoader />}>
                            <Dashboard />
                        </Suspense>
                    </Layout>
                </Box>
            } />
            <Route path="/revenue" element={
                <Box sx={{ display: 'flex', minHeight: '100vh' }}>
                    <Layout>
                        <Suspense fallback={<PageLoader />}>
                            <Revenue />
                        </Suspense>
                    </Layout>
                </Box>
            } />
            <Route path="/providers" element={
                <Box sx={{ display: 'flex', minHeight: '100vh' }}>
                    <Layout>
                        <Suspense fallback={<PageLoader />}>
                            <Providers />
                        </Suspense>
                    </Layout>
                </Box>
            } />
            <Route path="/patients" element={
                <Box sx={{ display: 'flex', minHeight: '100vh' }}>
                    <Layout>
                        <Suspense fallback={<PageLoader />}>
                            <Patients />
                        </Suspense>
                    </Layout>
                </Box>
            } />
            <Route path="/appointments" element={
                <Box sx={{ display: 'flex', minHeight: '100vh' }}>
                    <Layout>
                        <Suspense fallback={<PageLoader />}>
                            <Appointments />
                        </Suspense>
                    </Layout>
                </Box>
            } />
            <Route path="/ar-aging" element={
                <Box sx={{ display: 'flex', minHeight: '100vh' }}>
                    <Layout>
                        <Suspense fallback={<PageLoader />}>
                            <AR />
                        </Suspense>
                    </Layout>
                </Box>
            } />
            <Route path="/treatment-acceptance" element={
                <Box sx={{ display: 'flex', minHeight: '100vh' }}>
                    <Layout>
                        <Suspense fallback={<PageLoader />}>
                            <TreatmentAcceptance />
                        </Suspense>
                    </Layout>
                </Box>
            } />
            <Route path="/hygiene-retention" element={
                <Box sx={{ display: 'flex', minHeight: '100vh' }}>
                    <Layout>
                        <Suspense fallback={<PageLoader />}>
                            <HygieneRetention />
                        </Suspense>
                    </Layout>
                </Box>
            } />
            <Route
                path="/environment-manager"
                element={
                    <Suspense fallback={<PageLoader />}>
                        <EnvironmentManager />
                    </Suspense>
                }
            />
            <Route
                path="/schema-discovery"
                element={
                    <Suspense fallback={<PageLoader />}>
                        <SchemaDiscovery />
                    </Suspense>
                }
            />
        </Routes>
    );
}

export default App;
