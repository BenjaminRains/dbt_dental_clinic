import { Routes, Route } from 'react-router-dom';
import { Box } from '@mui/material';
import Layout from './components/layout/Layout';
import Dashboard from './pages/Dashboard';
import Revenue from './pages/Revenue';
import Providers from './pages/Providers';
import Patients from './pages/Patients';
import Appointments from './pages/Appointments';
import AR from './pages/AR';

function App() {
    return (
        <Box sx={{ display: 'flex', minHeight: '100vh' }}>
            <Layout>
                <Routes>
                    <Route path="/" element={<Dashboard />} />
                    <Route path="/revenue" element={<Revenue />} />
                    <Route path="/providers" element={<Providers />} />
                    <Route path="/patients" element={<Patients />} />
                    <Route path="/appointments" element={<Appointments />} />
                    <Route path="/ar-aging" element={<AR />} />
                </Routes>
            </Layout>
        </Box>
    );
}

export default App;
