import React, { useState, useEffect } from 'react';
import {
    AppBar,
    Alert,
    Box,
    CssBaseline,
    Drawer,
    IconButton,
    List,
    ListItem,
    ListItemButton,
    ListItemIcon,
    ListItemText,
    Toolbar,
    Typography,
    useTheme,
    useMediaQuery,
} from '@mui/material';
import {
    Menu as MenuIcon,
    Home as HomeIcon,
    Dashboard as DashboardIcon,
    AttachMoney as RevenueIcon,
    People as ProvidersIcon,
    Person as PatientsIcon,
    CalendarToday as AppointmentsIcon,
    AccountBalance as ARIcon,
    CheckCircle as TreatmentAcceptanceIcon,
    Spa as HygieneIcon,
    Close as CloseIcon,
    HelpOutline as HelpIcon,
} from '@mui/icons-material';
import { useNavigate, useLocation } from 'react-router-dom';

const drawerWidth = 240;

interface LayoutProps {
    children: React.ReactNode;
}

const menuItems = [
    { text: 'Portfolio', icon: <HomeIcon />, path: '/' },
    { text: 'Dashboard', icon: <DashboardIcon />, path: '/dashboard' },
    { text: 'KPI Definitions', icon: <HelpIcon />, path: '/kpi-definitions' },
    { text: 'Revenue', icon: <RevenueIcon />, path: '/revenue' },
    { text: 'AR Aging', icon: <ARIcon />, path: '/ar-aging' },
    { text: 'Treatment Acceptance', icon: <TreatmentAcceptanceIcon />, path: '/treatment-acceptance' },
    { text: 'Hygiene Retention', icon: <HygieneIcon />, path: '/hygiene-retention' },
    { text: 'Providers', icon: <ProvidersIcon />, path: '/providers' },
    { text: 'Patients', icon: <PatientsIcon />, path: '/patients' },
    { text: 'Appointments', icon: <AppointmentsIcon />, path: '/appointments' },
];

// Helper function to determine if we're in demo mode
const isDemoMode = (): boolean => {
    // Check environment variable first (explicit control)
    const envIsDemo = import.meta.env.VITE_IS_DEMO === 'true';

    // Fallback: check hostname for portfolio site
    const hostname = typeof window !== 'undefined' ? window.location.hostname : '';
    const isPortfolioSite = hostname === 'dbtdentalclinic.com' || hostname.includes('dbtdentalclinic');

    return envIsDemo || isPortfolioSite;
};

const Layout: React.FC<LayoutProps> = ({ children }) => {
    const [mobileOpen, setMobileOpen] = useState(false);
    const [bannerDismissed, setBannerDismissed] = useState(false);
    const [isDemo, setIsDemo] = useState(false);
    const navigate = useNavigate();
    const location = useLocation();
    const theme = useTheme();
    const isMobile = useMediaQuery(theme.breakpoints.down('md'));

    // Check if we're in demo mode and localStorage on mount
    useEffect(() => {
        setIsDemo(isDemoMode());
        const dismissed = localStorage.getItem('syntheticDataBannerDismissed');
        if (dismissed === 'true') {
            setBannerDismissed(true);
        }
    }, []);

    const handleDrawerToggle = () => {
        setMobileOpen(!mobileOpen);
    };

    const handleNavigation = (path: string) => {
        navigate(path);
        if (isMobile) {
            setMobileOpen(false);
        }
    };

    const handleBannerClose = () => {
        setBannerDismissed(true);
        localStorage.setItem('syntheticDataBannerDismissed', 'true');
    };

    const drawer = (
        <div>
            <Toolbar>
                <Typography variant="h6" noWrap component="div" sx={{ fontWeight: 'bold' }}>
                    🦷 Dental Analytics
                </Typography>
            </Toolbar>
            <List>
                {menuItems.map((item) => (
                    <ListItem key={item.text} disablePadding>
                        <ListItemButton
                            selected={location.pathname === item.path}
                            onClick={() => handleNavigation(item.path)}
                            sx={{
                                '&.Mui-selected': {
                                    backgroundColor: theme.palette.primary.main,
                                    color: 'white',
                                    '&:hover': {
                                        backgroundColor: theme.palette.primary.dark,
                                    },
                                    '& .MuiListItemIcon-root': {
                                        color: 'white',
                                    },
                                },
                            }}
                        >
                            <ListItemIcon>{item.icon}</ListItemIcon>
                            <ListItemText primary={item.text} />
                        </ListItemButton>
                    </ListItem>
                ))}
            </List>
        </div>
    );

    return (
        <Box sx={{ display: 'flex' }}>
            <CssBaseline />
            <AppBar
                position="fixed"
                sx={{
                    width: { md: `calc(100% - ${drawerWidth}px)` },
                    ml: { md: `${drawerWidth}px` },
                }}
            >
                <Toolbar>
                    <IconButton
                        color="inherit"
                        aria-label="open drawer"
                        edge="start"
                        onClick={handleDrawerToggle}
                        sx={{ mr: 2, display: { md: 'none' } }}
                    >
                        <MenuIcon />
                    </IconButton>
                    <Typography
                        variant="h6"
                        noWrap
                        component="div"
                        onClick={() => handleNavigation('/')}
                        sx={{
                            cursor: 'pointer',
                            '&:hover': {
                                opacity: 0.8,
                            },
                        }}
                    >
                        Dental Practice Analytics Dashboard
                    </Typography>
                </Toolbar>
            </AppBar>
            <Box
                component="nav"
                sx={{ width: { md: drawerWidth }, flexShrink: { md: 0 } }}
                aria-label="mailbox folders"
            >
                <Drawer
                    variant="temporary"
                    open={mobileOpen}
                    onClose={handleDrawerToggle}
                    ModalProps={{
                        keepMounted: true,
                    }}
                    sx={{
                        display: { xs: 'block', md: 'none' },
                        '& .MuiDrawer-paper': { boxSizing: 'border-box', width: drawerWidth },
                    }}
                >
                    {drawer}
                </Drawer>
                <Drawer
                    variant="permanent"
                    sx={{
                        display: { xs: 'none', md: 'block' },
                        '& .MuiDrawer-paper': { boxSizing: 'border-box', width: drawerWidth },
                    }}
                    open
                >
                    {drawer}
                </Drawer>
            </Box>
            <Box
                component="main"
                sx={{
                    flexGrow: 1,
                    p: 3,
                    width: { md: `calc(100% - ${drawerWidth}px)` },
                }}
            >
                <Toolbar />
                {isDemo && !bannerDismissed && (
                    <Alert
                        severity="info"
                        onClose={handleBannerClose}
                        sx={{ mb: 3 }}
                        action={
                            <IconButton
                                aria-label="close"
                                color="inherit"
                                size="small"
                                onClick={handleBannerClose}
                            >
                                <CloseIcon fontSize="inherit" />
                            </IconButton>
                        }
                    >
                        <Typography variant="body2">
                            <strong>Demo Site Notice:</strong> This demonstration dashboard uses synthetic data for portfolio purposes.
                            The production platform I built serves real dental practices with live patient data across multiple clinics
                            and 40,000+ patients, demonstrating production-grade data engineering capabilities.
                        </Typography>
                    </Alert>
                )}
                {children}
            </Box>
        </Box>
    );
};

export default Layout;
