import React, { useEffect, useState } from 'react';
import { Navigate, useLocation, useNavigate } from 'react-router-dom';
import {
    Alert,
    Box,
    Button,
    CircularProgress,
    Container,
    Paper,
    TextField,
    Typography,
} from '@mui/material';
import LockOutlinedIcon from '@mui/icons-material/LockOutlined';
import axios from 'axios';
import { authApi } from '@mdc/analytics-api/clinic';
import { useAuth } from '../auth/AuthContext';
import { resolvePostLoginPath } from '../utils/roleNavigation';

const PORTAL_MISCONFIGURED_MESSAGE =
    'Clinic portal login is not configured on the local API (users and/or session secret missing). Restart API with mdc api run --env local.';

function loginErrorMessage(error: unknown): string {
    if (axios.isAxiosError(error)) {
        const status = error.response?.status;
        const detail = error.response?.data?.detail;
        if (status === 404) {
            return 'Login service not found. Verify clinic portal auth routes are deployed.';
        }
        if (status === 503) {
            return PORTAL_MISCONFIGURED_MESSAGE;
        }
        if (
            status === 500 &&
            typeof detail === 'string' &&
            detail.includes('CLINIC_PORTAL_SESSION_SECRET')
        ) {
            return PORTAL_MISCONFIGURED_MESSAGE;
        }
        if (typeof detail === 'string') {
            return detail;
        }
    }
    return 'Sign-in failed. Check your username and password.';
}

const Login: React.FC = () => {
    const { isAuthenticated, login, homePath, authLoading } = useAuth();
    const navigate = useNavigate();
    const location = useLocation();

    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [error, setError] = useState<string | null>(null);
    const [submitting, setSubmitting] = useState(false);
    const [portalReady, setPortalReady] = useState(true);

    useEffect(() => {
        let cancelled = false;
        authApi
            .configured()
            .then((cfg) => {
                if (!cancelled) {
                    setPortalReady(cfg.portal_login_enabled);
                    if (!cfg.portal_login_enabled) {
                        setError(PORTAL_MISCONFIGURED_MESSAGE);
                    }
                }
            })
            .catch(() => {
                // Keep form usable; submission path will surface real API errors.
                if (!cancelled) {
                    setPortalReady(true);
                }
            });
        return () => {
            cancelled = true;
        };
    }, []);

    if (authLoading) {
        return (
            <Box
                sx={{
                    display: 'flex',
                    justifyContent: 'center',
                    alignItems: 'center',
                    minHeight: '100vh',
                }}
            >
                <CircularProgress />
            </Box>
        );
    }

    if (isAuthenticated) {
        return <Navigate to={homePath} replace />;
    }

    const handleSubmit = async (event: React.FormEvent) => {
        event.preventDefault();
        setError(null);
        if (!portalReady) {
            setError(PORTAL_MISCONFIGURED_MESSAGE);
            return;
        }
        setSubmitting(true);
        try {
            const session = await login(username.trim(), password);
            const fromPath = (location.state as { from?: string } | null)?.from;
            navigate(resolvePostLoginPath(session.role, fromPath), { replace: true });
        } catch (err) {
            setError(loginErrorMessage(err));
        } finally {
            setSubmitting(false);
        }
    };

    return (
        <Box
            sx={{
                minHeight: '100vh',
                display: 'flex',
                alignItems: 'center',
                bgcolor: 'background.default',
            }}
        >
            <Container maxWidth="xs">
                <Paper elevation={2} sx={{ p: 4 }}>
                    <Box sx={{ textAlign: 'center', mb: 3 }}>
                        <LockOutlinedIcon color="primary" sx={{ fontSize: 40, mb: 1 }} />
                        <Typography variant="h5" component="h1" gutterBottom>
                            MDC &amp; GLIC Analytics
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                            Sign in with your clinic account
                        </Typography>
                    </Box>

                    {error && (
                        <Alert severity="error" sx={{ mb: 2 }}>
                            {error}
                        </Alert>
                    )}

                    <Box component="form" onSubmit={handleSubmit}>
                        <TextField
                            label="Username"
                            fullWidth
                            required
                            autoComplete="username"
                            autoFocus
                            margin="normal"
                            value={username}
                            onChange={(e) => setUsername(e.target.value)}
                        />
                        <TextField
                            label="Password"
                            type="password"
                            fullWidth
                            required
                            autoComplete="current-password"
                            margin="normal"
                            value={password}
                            onChange={(e) => setPassword(e.target.value)}
                        />
                        <Button
                            type="submit"
                            variant="contained"
                            fullWidth
                            size="large"
                            disabled={submitting || !portalReady}
                            sx={{ mt: 3 }}
                        >
                            {submitting ? 'Signing in…' : 'Sign in'}
                        </Button>
                    </Box>
                </Paper>
            </Container>
        </Box>
    );
};

export default Login;
