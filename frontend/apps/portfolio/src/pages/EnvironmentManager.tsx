import React from 'react';
import { useNavigate } from 'react-router-dom';
import {
    Box,
    Container,
    Typography,
    Button,
    Grid,
    Card,
    CardContent,
    Chip,
    Link,
    Paper,
    Divider,
} from '@mui/material';
import {
    ArrowBack,
    GitHub,
    Security,
    Speed,
    AutoAwesome,
} from '@mui/icons-material';

const EnvironmentManager: React.FC = () => {
    const navigate = useNavigate();

    return (
        <Box sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
            {/* Header */}
            <Box
                sx={{
                    background: 'linear-gradient(135deg, #1e3c72 0%, #2a5298 100%)',
                    color: 'white',
                    py: { xs: 6, md: 8 },
                    px: 2,
                }}
            >
                <Container maxWidth="lg">
                    <Button
                        startIcon={<ArrowBack />}
                        onClick={() => navigate('/')}
                        sx={{ mb: 3, color: 'white', borderColor: 'white' }}
                        variant="outlined"
                    >
                        Back to Portfolio
                    </Button>
                    <Typography
                        variant="h2"
                        component="h1"
                        gutterBottom
                        sx={{
                            fontWeight: 700,
                            fontSize: { xs: '2rem', md: '3rem' },
                            mb: 2,
                        }}
                    >
                        🚀 Data Engineering Environment Manager
                    </Typography>
                    <Typography variant="h6" sx={{ opacity: 0.95, maxWidth: '800px' }}>
                        Custom PowerShell automation tool for streamlined development workflows
                    </Typography>
                </Container>
            </Box>

            <Container maxWidth="lg" sx={{ py: 6 }}>
                {/* Overview */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Overview
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ fontSize: '1.1rem', lineHeight: 1.8, textAlign: 'left' }}>
                        The <strong>Data Engineering Environment Manager</strong> is a custom PowerShell automation tool
                        designed to simplify development workflows in complex data engineering projects. It provides a
                        unified, intuitive interface for managing multiple independent environments (dbt, ETL pipelines,
                        API servers, audio processing, and frontend) within a single monorepo structure.
                    </Typography>

                    <Box
                        component="img"
                        src="/env_manager.png"
                        alt="Environment Manager Terminal Output"
                        sx={{
                            width: '100%',
                            maxWidth: '900px',
                            borderRadius: 2,
                            boxShadow: 3,
                            my: 4,
                            display: 'block',
                            mx: 'auto',
                        }}
                    />
                    <Typography variant="body2" color="text.secondary" sx={{ textAlign: 'center', fontStyle: 'italic' }}>
                        The Environment Manager automatically detects project components and provides a clear command reference
                    </Typography>
                </Box>

                <Divider sx={{ my: 6 }} />

                {/* Key Features */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 4 }}>
                        Key Features
                    </Typography>
                    <Grid container spacing={3}>
                        <Grid item xs={12} md={6}>
                            <Card elevation={2} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                        <Security sx={{ fontSize: 40, color: 'primary.main', mr: 2 }} />
                                        <Typography variant="h6" fontWeight="bold">
                                            🎯 Environment Isolation
                                        </Typography>
                                    </Box>
                                    <Typography variant="body2" color="text.secondary">
                                        Each component runs in its own isolated virtual environment with proper dependency
                                        management. The manager prevents accidental cross-contamination by ensuring only one
                                        environment is active at a time.
                                    </Typography>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Card elevation={2} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                        <Security sx={{ fontSize: 40, color: 'primary.main', mr: 2 }} />
                                        <Typography variant="h6" fontWeight="bold">
                                            🔐 Secure Configuration
                                        </Typography>
                                    </Box>
                                    <Typography variant="body2" color="text.secondary">
                                        Interactive prompts for environment selection (production/test) prevent accidental
                                        deployment to production. Environment variables are loaded from dedicated `.env` files
                                        specific to each component.
                                    </Typography>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Card elevation={2} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                        <Speed sx={{ fontSize: 40, color: 'primary.main', mr: 2 }} />
                                        <Typography variant="h6" fontWeight="bold">
                                            🚀 Simplified Workflows
                                        </Typography>
                                    </Box>
                                    <Typography variant="body2" color="text.secondary">
                                        Simple aliases replace complex multi-step commands. One command to initialize,
                                        configure, and run. Reduces setup time from 30+ minutes to 2 minutes.
                                    </Typography>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Card elevation={2} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                        <AutoAwesome sx={{ fontSize: 40, color: 'primary.main', mr: 2 }} />
                                        <Typography variant="h6" fontWeight="bold">
                                            📊 Project Awareness
                                        </Typography>
                                    </Box>
                                    <Typography variant="body2" color="text.secondary">
                                        Auto-detects available project components and provides contextual commands and
                                        suggestions. No need to remember project structure or setup procedures.
                                    </Typography>
                                </CardContent>
                            </Card>
                        </Grid>
                    </Grid>
                </Box>

                <Divider sx={{ my: 6 }} />

                {/* Before & After */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 4 }}>
                        Before & After
                    </Typography>
                    <Grid container spacing={3}>
                        <Grid item xs={12} md={6}>
                            <Paper elevation={2} sx={{ p: 3, bgcolor: '#ffe6e6', borderLeft: '4px solid #e74c3c' }}>
                                <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', color: '#c0392b' }}>
                                    ❌ Without the Manager
                                </Typography>
                                <Box
                                    component="pre"
                                    sx={{
                                        bgcolor: '#1e1e1e',
                                        color: '#d4d4d4',
                                        p: 2,
                                        borderRadius: 1,
                                        overflowX: 'auto',
                                        fontFamily: 'monospace',
                                        fontSize: '0.875rem',
                                        mb: 2,
                                    }}
                                >
                                    {`# Manual setup process
cd etl_pipeline
pipenv shell
# Load .env file manually
cd ..
python -m etl_pipeline.cli.main run`}
                                </Box>
                                <Typography variant="body2">
                                    Multiple steps, easy to make mistakes, requires knowledge of project structure
                                </Typography>
                            </Paper>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Paper elevation={2} sx={{ p: 3, bgcolor: '#e6ffe6', borderLeft: '4px solid #27ae60' }}>
                                <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', color: '#229954' }}>
                                    ✅ With the Manager
                                </Typography>
                                <Box
                                    component="pre"
                                    sx={{
                                        bgcolor: '#1e1e1e',
                                        color: '#d4d4d4',
                                        p: 2,
                                        borderRadius: 1,
                                        overflowX: 'auto',
                                        fontFamily: 'monospace',
                                        fontSize: '0.875rem',
                                        mb: 2,
                                    }}
                                >
                                    {`# Simple, intuitive commands
etl-init          # Selects environment, sets up venv, loads config
etl run --full    # Ready to go!`}
                                </Box>
                                <Typography variant="body2">
                                    One command, no mistakes, self-documenting
                                </Typography>
                            </Paper>
                        </Grid>
                    </Grid>
                </Box>

                <Divider sx={{ my: 6 }} />

                {/* Technical Implementation */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 4 }}>
                        How It Works
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ mb: 4, fontSize: '1.1rem' }}>
                        The Environment Manager operates on intelligent project detection and strict environment isolation.
                        Here's how it works at a high level:
                    </Typography>

                    <Grid container spacing={3}>
                        <Grid item xs={12} md={4}>
                            <Card elevation={2} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', mb: 2 }}>
                                        1. Auto-Detection
                                    </Typography>
                                    <Typography variant="body2" component="ul" sx={{ pl: 2, mb: 2 }}>
                                        <li>Scans project directory for configuration files (dbt_project.yml, Pipfile, etc.)</li>
                                        <li>Identifies available components automatically</li>
                                        <li>Provides contextual suggestions and commands</li>
                                        <li>No manual configuration required</li>
                                    </Typography>
                                </CardContent>
                            </Card>
                        </Grid>

                        <Grid item xs={12} md={4}>
                            <Card elevation={2} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', mb: 2 }}>
                                        2. Environment Isolation
                                    </Typography>
                                    <Typography variant="body2" component="ul" sx={{ pl: 2, mb: 2 }}>
                                        <li>Only one environment can be active at a time</li>
                                        <li>Blocks initialization if another environment is active</li>
                                        <li>Creates and manages virtual environments automatically</li>
                                        <li>Loads environment-specific configuration files</li>
                                    </Typography>
                                </CardContent>
                            </Card>
                        </Grid>

                        <Grid item xs={12} md={4}>
                            <Card elevation={2} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', mb: 2 }}>
                                        3. Command Routing
                                    </Typography>
                                    <Typography variant="body2" component="ul" sx={{ pl: 2, mb: 2 }}>
                                        <li>Validates environment is active before executing</li>
                                        <li>Routes commands to correct virtual environment</li>
                                        <li>Applies proper working directory and environment variables</li>
                                        <li>Passes through all command arguments seamlessly</li>
                                    </Typography>
                                </CardContent>
                            </Card>
                        </Grid>
                    </Grid>
                </Box>

                <Divider sx={{ my: 6 }} />

                {/* Benefits */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 4 }}>
                        Benefits
                    </Typography>
                    <Grid container spacing={2}>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body1" paragraph>
                                ✅ <strong>Reduces Onboarding Time:</strong> New team members can start contributing in minutes, not hours
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body1" paragraph>
                                ✅ <strong>Eliminates Configuration Errors:</strong> Interactive prompts and automatic detection prevent common mistakes
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body1" paragraph>
                                ✅ <strong>Standardizes Workflows:</strong> Consistent interface across all project components
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body1" paragraph>
                                ✅ <strong>Improves Developer Productivity:</strong> No more context-switching overhead or remembering complex commands
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body1" paragraph>
                                ✅ <strong>Enhances Safety:</strong> Explicit environment selection prevents accidental production operations
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body1" paragraph>
                                ✅ <strong>Portable Solution:</strong> Self-contained PowerShell script, no external dependencies
                            </Typography>
                        </Grid>
                    </Grid>
                </Box>

                <Divider sx={{ my: 6 }} />

                {/* Technical Highlights */}
                <Paper elevation={2} sx={{ p: 4, bgcolor: 'grey.50', mb: 6 }}>
                    <Typography variant="h5" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Architecture Highlights
                    </Typography>
                    <Grid container spacing={2}>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body2" paragraph>
                                <strong>Mutual Exclusion:</strong> Only one environment can be active at a time, preventing dependency conflicts
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body2" paragraph>
                                <strong>Fail-Fast Design:</strong> Clear error messages when prerequisites aren't met
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body2" paragraph>
                                <strong>Non-Intrusive:</strong> Doesn't modify system settings or require admin privileges
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body2" paragraph>
                                <strong>Project-Scoped:</strong> Loaded per-project, avoiding global configuration pollution
                            </Typography>
                        </Grid>
                    </Grid>
                </Paper>

                {/* Skills & Links */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', mb: 2 }}>
                        Skills Demonstrated
                    </Typography>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mb: 4 }}>
                        <Chip label="PowerShell Scripting" />
                        <Chip label="Automation" />
                        <Chip label="Developer Experience Design" />
                        <Chip label="System Integration" />
                        <Chip label="Configuration Management" />
                        <Chip label="Tooling Development" />
                        <Chip label="Error Handling" />
                        <Chip label="User Interface Design" />
                    </Box>

                    <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
                        <Button
                            variant="contained"
                            size="large"
                            component={Link}
                            href="https://github.com/BenjaminRains/dbt_dental_clinic/blob/main/scripts/environment_manager.ps1"
                            target="_blank"
                            rel="noopener noreferrer"
                            startIcon={<GitHub />}
                        >
                            View Code on GitHub
                        </Button>
                        <Button
                            variant="outlined"
                            size="large"
                            onClick={() => navigate('/')}
                            startIcon={<ArrowBack />}
                        >
                            Back to Portfolio
                        </Button>
                    </Box>
                </Box>
            </Container>
        </Box>
    );
};

export default EnvironmentManager;

