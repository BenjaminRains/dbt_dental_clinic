import React, { useState } from 'react';
import {
    AppBar,
    Box,
    Button,
    CssBaseline,
    Drawer,
    IconButton,
    List,
    ListItem,
    ListItemButton,
    ListItemIcon,
    ListItemText,
    ListSubheader,
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
    ShareOutlined as ReferralIcon,
    HelpOutline as HelpIcon,
} from '@mui/icons-material';
import { useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../auth/AuthContext';
import { filterNavPaths } from '../auth/roleAccess';
import RoleSwitcher from '../components/RoleSwitcher';

const drawerWidth = 240;

interface ClinicLayoutProps {
    children: React.ReactNode;
}

const clinicReportItems = [
    { text: 'Dashboard', icon: <DashboardIcon />, path: '/dashboard' },
    { text: 'Revenue', icon: <RevenueIcon />, path: '/revenue' },
    { text: 'AR Aging', icon: <ARIcon />, path: '/ar-aging' },
    { text: 'Treatment Acceptance', icon: <TreatmentAcceptanceIcon />, path: '/treatment-acceptance' },
    { text: 'Hygiene Retention', icon: <HygieneIcon />, path: '/hygiene-retention' },
    { text: 'Referral sources', icon: <ReferralIcon />, path: '/referral-sources' },
    { text: 'Providers', icon: <ProvidersIcon />, path: '/providers' },
    { text: 'Patients', icon: <PatientsIcon />, path: '/patients' },
    { text: 'Appointments', icon: <AppointmentsIcon />, path: '/appointments' },
    { text: 'KPI Definitions', icon: <HelpIcon />, path: '/kpi-definitions' },
];

const ClinicLayout: React.FC<ClinicLayoutProps> = ({ children }) => {
    const [mobileOpen, setMobileOpen] = useState(false);
    const navigate = useNavigate();
    const location = useLocation();
    const theme = useTheme();
    const isMobile = useMediaQuery(theme.breakpoints.down('md'));
    const { homePath, logout, role } = useAuth();
    const visibleReports = filterNavPaths(role, clinicReportItems);

    const handleDrawerToggle = () => setMobileOpen((open) => !open);

    const handleNavigation = (path: string) => {
        navigate(path);
        if (isMobile) {
            setMobileOpen(false);
        }
    };

    const renderNavItem = (item: { text: string; icon: React.ReactNode; path: string }) => (
        <ListItem key={item.text} disablePadding>
            <ListItemButton
                selected={location.pathname === item.path}
                onClick={() => handleNavigation(item.path)}
                sx={{
                    '&.Mui-selected': {
                        backgroundColor: theme.palette.primary.main,
                        color: 'white',
                        '&:hover': { backgroundColor: theme.palette.primary.dark },
                        '& .MuiListItemIcon-root': { color: 'white' },
                    },
                }}
            >
                <ListItemIcon>{item.icon}</ListItemIcon>
                <ListItemText primary={item.text} />
            </ListItemButton>
        </ListItem>
    );

    const drawer = (
        <div>
            <Toolbar>
                <Typography
                    variant="subtitle1"
                    component="div"
                    sx={{ fontWeight: 'bold', lineHeight: 1.3, whiteSpace: 'normal', wordBreak: 'break-word' }}
                >
                    MDC & GLIC Analytics
                </Typography>
            </Toolbar>
            <List>
                {renderNavItem({ text: 'Home', icon: <HomeIcon />, path: homePath })}
                {visibleReports.length > 0 && (
                    <ListSubheader component="div" sx={{ lineHeight: 2.5 }}>
                        Reports
                    </ListSubheader>
                )}
                {visibleReports.map(renderNavItem)}
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
                        onClick={() => handleNavigation(homePath)}
                        sx={{ cursor: 'pointer', flexGrow: 1, '&:hover': { opacity: 0.8 } }}
                    >
                        MDC & GLIC Analytics
                    </Typography>
                    <RoleSwitcher />
                    <Button
                        color="inherit"
                        size="small"
                        onClick={() => {
                            logout();
                            navigate('/login');
                        }}
                        sx={{ ml: 1 }}
                    >
                        Sign out
                    </Button>
                </Toolbar>
            </AppBar>
            <Box
                component="nav"
                sx={{ width: { md: drawerWidth }, flexShrink: { md: 0 } }}
                aria-label="clinic navigation"
            >
                <Drawer
                    variant="temporary"
                    open={mobileOpen}
                    onClose={handleDrawerToggle}
                    ModalProps={{ keepMounted: true }}
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
                {children}
            </Box>
        </Box>
    );
};

export default ClinicLayout;
