import React from 'react';
import { Link as RouterLink } from 'react-router-dom';
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
    Divider,
    Paper,
} from '@mui/material';
import {
    GitHub,
    LinkedIn,
    Description,
    Analytics,
    Build,
    Storage,
    Dashboard,
    Mic,
    Settings,
    SmartToy,
    PictureAsPdf,
    Code,
    CleaningServices,
} from '@mui/icons-material';

const Portfolio: React.FC = () => {
    return (
        <Box sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
            {/* Hero Section */}
            <Box
                sx={{
                    bgcolor: 'primary.main',
                    color: 'white',
                    py: { xs: 6, md: 8 },
                    px: 2,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                }}
            >
                <Container maxWidth="md" sx={{ textAlign: 'center' }}>
                    <Typography
                        variant="h2"
                        component="h1"
                        gutterBottom
                        sx={{
                            fontWeight: 700,
                            fontSize: { xs: '2.5rem', md: '3.5rem' },
                            mb: 2,
                            letterSpacing: '-0.02em',
                        }}
                    >
                        Benjamin Rains
                    </Typography>
                    <Typography
                        variant="h5"
                        component="h2"
                        gutterBottom
                        sx={{
                            mb: 4,
                            opacity: 0.95,
                            fontSize: { xs: '1.25rem', md: '1.5rem' },
                            fontWeight: 400,
                        }}
                    >
                        Data Engineer · Healthcare & Modern ELT
                    </Typography>
                    <Typography
                        variant="body1"
                        sx={{
                            mb: 5,
                            maxWidth: '700px',
                            mx: 'auto',
                            fontSize: { xs: '1rem', md: '1.125rem' },
                            lineHeight: 1.75,
                            opacity: 0.95,
                        }}
                    >
                        Building end-to-end analytics platforms for healthcare operations.
                        Specializing in modern ELT pipelines, dbt transformations, and
                        production-grade data infrastructure.
                    </Typography>
                    <Box
                        sx={{
                            display: 'flex',
                            gap: 2,
                            flexWrap: 'wrap',
                            justifyContent: 'center',
                            alignItems: 'center',
                        }}
                    >
                        <Button
                            variant="contained"
                            color="secondary"
                            size="large"
                            component={RouterLink}
                            to="/demo"
                            sx={{
                                minWidth: '200px',
                                py: 1.5,
                                px: 3,
                                fontSize: '1rem',
                                fontWeight: 600,
                                textTransform: 'none',
                            }}
                        >
                            <Dashboard sx={{ mr: 1.5, fontSize: '1.25rem' }} />
                            View Analytics Demo
                        </Button>
                        <Button
                            variant="outlined"
                            color="inherit"
                            size="large"
                            component={Link}
                            href="https://github.com/BenjaminRains/dbt_dental_clinic"
                            target="_blank"
                            rel="noopener noreferrer"
                            sx={{
                                minWidth: '200px',
                                py: 1.5,
                                px: 3,
                                fontSize: '1rem',
                                fontWeight: 600,
                                textTransform: 'none',
                                borderColor: 'white',
                                borderWidth: 2,
                                color: 'white',
                                '&:hover': {
                                    borderColor: 'white',
                                    borderWidth: 2,
                                    bgcolor: 'rgba(255, 255, 255, 0.1)',
                                },
                            }}
                        >
                            <GitHub sx={{ mr: 1.5, fontSize: '1.25rem' }} />
                            GitHub
                        </Button>
                        <Button
                            variant="outlined"
                            color="inherit"
                            size="large"
                            component={Link}
                            href="/assets/resume.pdf"
                            target="_blank"
                            rel="noopener noreferrer"
                            sx={{
                                minWidth: '200px',
                                py: 1.5,
                                px: 3,
                                fontSize: '1rem',
                                fontWeight: 600,
                                textTransform: 'none',
                                borderColor: 'white',
                                borderWidth: 2,
                                color: 'white',
                                '&:hover': {
                                    borderColor: 'white',
                                    borderWidth: 2,
                                    bgcolor: 'rgba(255, 255, 255, 0.1)',
                                },
                            }}
                        >
                            <Description sx={{ mr: 1.5, fontSize: '1.25rem' }} />
                            Resume
                        </Button>
                    </Box>
                </Container>
            </Box>

            {/* Project Metrics & Quick Links Section */}
            <Box sx={{ mb: 8, px: { xs: 3, sm: 4, md: 6, lg: 8 } }}>
                {/* Project Metrics - Subtle Inline Display */}
                <Box
                    sx={{
                        mb: 5,
                        display: 'flex',
                        flexWrap: 'wrap',
                        gap: 3,
                        justifyContent: 'flex-start',
                        alignItems: 'center',
                    }}
                >
                    <Box>
                        <Typography variant="h6" component="span" fontWeight="bold" color="primary">
                            157
                        </Typography>
                        <Typography variant="body2" component="span" color="text.secondary" sx={{ ml: 0.5 }}>
                            dbt Models
                        </Typography>
                    </Box>
                    <Box>
                        <Typography variant="h6" component="span" fontWeight="bold" color="primary">
                            432+
                        </Typography>
                        <Typography variant="body2" component="span" color="text.secondary" sx={{ ml: 0.5 }}>
                            Tables
                        </Typography>
                    </Box>
                    <Box>
                        <Typography variant="h6" component="span" fontWeight="bold" color="primary">
                            100%
                        </Typography>
                        <Typography variant="body2" component="span" color="text.secondary" sx={{ ml: 0.5 }}>
                            Test Coverage
                        </Typography>
                    </Box>
                    <Box>
                        <Typography variant="h6" component="span" fontWeight="bold" color="primary">
                            5 Stages
                        </Typography>
                        <Typography variant="body2" component="span" color="text.secondary" sx={{ ml: 0.5 }}>
                            Pipeline
                        </Typography>
                    </Box>
                </Box>

                {/* Quick Links */}
                <Typography variant="h5" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'left' }}>
                    Project Components
                </Typography>
                <Grid container spacing={2} sx={{ mb: 6 }}>
                    <Grid item xs={12} sm={6} md={4}>
                        <Card
                            elevation={2}
                            sx={{
                                height: '100%',
                                '&:hover': { elevation: 4 },
                                cursor: 'pointer',
                            }}
                            component={RouterLink}
                            to="/demo"
                        >
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                                    <Dashboard sx={{ fontSize: 28, color: 'primary.main', mr: 1.5 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        Analytics Dashboard
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary">
                                    Interactive React dashboard with real-time KPIs and analytics
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={4}>
                        <Card
                            elevation={2}
                            sx={{
                                height: '100%',
                                '&:hover': { elevation: 4 },
                                cursor: 'pointer',
                            }}
                            component={Link}
                            href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/dbt_dental_models"
                            target="_blank"
                            rel="noopener noreferrer"
                        >
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                                    <Analytics sx={{ fontSize: 28, color: 'primary.main', mr: 1.5 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        dbt Project
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary">
                                    157 staging, intermediate, and mart models
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={4}>
                        <Card
                            elevation={2}
                            sx={{
                                height: '100%',
                                '&:hover': { elevation: 4 },
                                cursor: 'pointer',
                            }}
                            component={Link}
                            href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/etl_pipeline"
                            target="_blank"
                            rel="noopener noreferrer"
                        >
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                                    <Storage sx={{ fontSize: 28, color: 'primary.main', mr: 1.5 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        ETL Pipeline
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary">
                                    Metadata-driven replication and incremental loading
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={4}>
                        <Card
                            elevation={2}
                            sx={{
                                height: '100%',
                                '&:hover': { elevation: 4 },
                                cursor: 'pointer',
                            }}
                            component={Link}
                            href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/api"
                            target="_blank"
                            rel="noopener noreferrer"
                        >
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                                    <Code sx={{ fontSize: 28, color: 'primary.main', mr: 1.5 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        FastAPI Backend
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary">
                                    RESTful API with comprehensive endpoints
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={4}>
                        <Card
                            elevation={2}
                            sx={{
                                height: '100%',
                                '&:hover': { elevation: 4 },
                                cursor: 'pointer',
                            }}
                            component={Link}
                            href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/consult_audio_pipe"
                            target="_blank"
                            rel="noopener noreferrer"
                        >
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                                    <Mic sx={{ fontSize: 28, color: 'primary.main', mr: 1.5 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        Consult Audio Pipeline
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary">
                                    ML-powered transcription and analysis
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={4}>
                        <Card
                            elevation={2}
                            sx={{
                                height: '100%',
                                '&:hover': { elevation: 4 },
                                cursor: 'pointer',
                            }}
                            component={Link}
                            href="#documentation"
                        >
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                                    <Description sx={{ fontSize: 28, color: 'primary.main', mr: 1.5 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        Documentation
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary">
                                    Architecture, setup guides, and API docs
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                </Grid>

                {/* Visual Examples Section */}
                <Typography variant="h5" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'left' }}>
                    Visual Examples
                </Typography>
                <Grid container spacing={3} sx={{ mb: 6 }}>
                    <Grid item xs={12} md={6}>
                        <Card elevation={3}>
                            <CardContent>
                                <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', mb: 2 }}>
                                    Dashboard Preview
                                </Typography>
                                <Box
                                    sx={{
                                        bgcolor: 'grey.100',
                                        borderRadius: 2,
                                        p: 4,
                                        minHeight: '300px',
                                        display: 'flex',
                                        alignItems: 'center',
                                        justifyContent: 'center',
                                    }}
                                >
                                    <Typography variant="body2" color="text.secondary">
                                        [Dashboard Screenshot Placeholder]
                                    </Typography>
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} md={6}>
                        <Card elevation={3}>
                            <CardContent>
                                <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', mb: 2 }}>
                                    dbt Lineage Graph
                                </Typography>
                                <Box
                                    sx={{
                                        bgcolor: 'grey.100',
                                        borderRadius: 2,
                                        p: 4,
                                        minHeight: '300px',
                                        display: 'flex',
                                        alignItems: 'center',
                                        justifyContent: 'center',
                                    }}
                                >
                                    <Typography variant="body2" color="text.secondary">
                                        [dbt Lineage Graph Placeholder]
                                    </Typography>
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12}>
                        <Card elevation={3}>
                            <CardContent>
                                <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', mb: 2 }}>
                                    System Architecture Diagram
                                </Typography>
                                <Box
                                    sx={{
                                        bgcolor: 'grey.100',
                                        borderRadius: 2,
                                        p: 4,
                                        minHeight: '400px',
                                        display: 'flex',
                                        alignItems: 'center',
                                        justifyContent: 'center',
                                    }}
                                >
                                    <Typography variant="body2" color="text.secondary">
                                        [Architecture Diagram Placeholder]
                                    </Typography>
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>
                </Grid>

                {/* Documentation Links */}
                <Box id="documentation">
                    <Typography variant="h5" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'left' }}>
                        Documentation & Resources
                    </Typography>
                    <Grid container spacing={2}>
                        <Grid item xs={12} sm={6} md={4}>
                            <Card elevation={1} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Typography variant="body2" fontWeight="bold" gutterBottom>
                                        Architecture Documentation
                                    </Typography>
                                    <Typography variant="body2" color="text.secondary" paragraph>
                                        System design and data flow
                                    </Typography>
                                    <Link href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/docs" target="_blank" rel="noopener noreferrer">
                                        View Docs →
                                    </Link>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} sm={6} md={4}>
                            <Card elevation={1} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Typography variant="body2" fontWeight="bold" gutterBottom>
                                        dbt Documentation
                                    </Typography>
                                    <Typography variant="body2" color="text.secondary" paragraph>
                                        Model lineage and data catalog
                                    </Typography>
                                    <Link href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/dbt_docs" target="_blank" rel="noopener noreferrer">
                                        View dbt Docs →
                                    </Link>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} sm={6} md={4}>
                            <Card elevation={1} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Typography variant="body2" fontWeight="bold" gutterBottom>
                                        API Documentation
                                    </Typography>
                                    <Typography variant="body2" color="text.secondary" paragraph>
                                        RESTful API endpoints reference
                                    </Typography>
                                    <Link href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/api/README.md" target="_blank" rel="noopener noreferrer">
                                        View API Docs →
                                    </Link>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} sm={6} md={4}>
                            <Card elevation={1} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Typography variant="body2" fontWeight="bold" gutterBottom>
                                        Setup Guide
                                    </Typography>
                                    <Typography variant="body2" color="text.secondary" paragraph>
                                        Local development setup instructions
                                    </Typography>
                                    <Link href="https://github.com/BenjaminRains/dbt_dental_clinic/blob/main/ENVIRONMENT_SETUP.md" target="_blank" rel="noopener noreferrer">
                                        View Guide →
                                    </Link>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} sm={6} md={4}>
                            <Card elevation={1} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Typography variant="body2" fontWeight="bold" gutterBottom>
                                        Deployment Guide
                                    </Typography>
                                    <Typography variant="body2" color="text.secondary" paragraph>
                                        AWS deployment and configuration
                                    </Typography>
                                    <Link href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/docs" target="_blank" rel="noopener noreferrer">
                                        View Guide →
                                    </Link>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} sm={6} md={4}>
                            <Card elevation={1} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Typography variant="body2" fontWeight="bold" gutterBottom>
                                        Data Quality Reports
                                    </Typography>
                                    <Typography variant="body2" color="text.secondary" paragraph>
                                        Test results and validation metrics
                                    </Typography>
                                    <Link href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/docs/data_quality" target="_blank" rel="noopener noreferrer">
                                        View Reports →
                                    </Link>
                                </CardContent>
                            </Card>
                        </Grid>
                    </Grid>
                </Box>
            </Box>

            <Divider sx={{ my: 8 }} />

            {/* Primary Project: dbt_dental_clinic */}
            <Box sx={{ mb: 8, px: { xs: 3, sm: 4, md: 6, lg: 8 } }}>
                <Typography
                    variant="h3"
                    component="h2"
                    gutterBottom
                    sx={{ fontWeight: 'bold', mb: 1, textAlign: 'left' }}
                >
                    dbt Dental Clinic Analytics Platform
                </Typography>
                <Typography variant="body1" color="text.secondary" paragraph sx={{ mb: 4, textAlign: 'left' }}>
                    Enterprise-grade analytics engineering for a real dental practice.
                    This platform demonstrates production-ready data infrastructure,
                    from source replication to interactive dashboards.
                </Typography>

                <Grid container spacing={3} sx={{ mb: 4 }}>
                    <Grid item xs={12} md={6}>
                        <Card elevation={3}>
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                    <Storage sx={{ fontSize: 40, color: 'primary.main', mr: 2 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        Data Infrastructure
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary" paragraph>
                                    <strong>450+ OpenDental source tables</strong> replicated into
                                    PostgreSQL using metadata-driven ELT pipeline. Automated
                                    schema discovery, incremental loading, and comprehensive
                                    test coverage.
                                </Typography>
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                                    <Chip label="PostgreSQL" size="small" />
                                    <Chip label="SQLAlchemy" size="small" />
                                    <Chip label="Python" size="small" />
                                    <Chip label="Metadata-Driven" size="small" />
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} md={6}>
                        <Card elevation={3}>
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                    <Analytics sx={{ fontSize: 40, color: 'primary.main', mr: 2 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        dbt Transformations
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary" paragraph>
                                    <strong>~150 staging, intermediate, and mart models</strong> with
                                    full dbt test coverage. Implements dimensional modeling,
                                    incremental strategies, and comprehensive data quality checks.
                                </Typography>
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                                    <Chip label="dbt" size="small" />
                                    <Chip label="Dimensional Modeling" size="small" />
                                    <Chip label="Data Quality" size="small" />
                                    <Chip label="Incremental Models" size="small" />
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} md={6}>
                        <Card elevation={3}>
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                    <Dashboard sx={{ fontSize: 40, color: 'primary.main', mr: 2 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        Analytics Frontend
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary" paragraph>
                                    <strong>FastAPI + React</strong> analytics dashboard with
                                    real-time KPIs, revenue analytics, provider performance,
                                    and patient insights. Deployed on AWS with CloudFront CDN.
                                </Typography>
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                                    <Chip label="FastAPI" size="small" />
                                    <Chip label="React + TypeScript" size="small" />
                                    <Chip label="Material-UI" size="small" />
                                    <Chip label="AWS" size="small" />
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} md={6}>
                        <Card elevation={3}>
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                    <Build sx={{ fontSize: 40, color: 'primary.main', mr: 2 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        Production Deployment
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary" paragraph>
                                    Full CI/CD pipeline, automated testing, Docker containerization,
                                    and cloud infrastructure. Built for a <strong>real clinic</strong>{' '}
                                    with ongoing operations and data updates.
                                </Typography>
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                                    <Chip label="Docker" size="small" />
                                    <Chip label="CI/CD" size="small" />
                                    <Chip label="AWS S3/CloudFront" size="small" />
                                    <Chip label="Production-Ready" size="small" />
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>
                </Grid>

                <Button
                    variant="contained"
                    size="large"
                    component={Link}
                    href="/demo"
                    sx={{ mr: 2 }}
                >
                    Explore Analytics Demo
                </Button>
                <Button
                    variant="outlined"
                    size="large"
                    component={Link}
                    href="https://github.com/BenjaminRains/dbt_dental_clinic"
                    target="_blank"
                    rel="noopener noreferrer"
                >
                    <GitHub sx={{ mr: 1 }} />
                    View on GitHub
                </Button>
            </Box>

            <Divider sx={{ my: 8 }} />

            {/* Consult Audio Whisper Pipeline */}
            <Box sx={{ mb: 8, px: { xs: 3, sm: 4, md: 6, lg: 8 } }}>
                <Typography
                    variant="h3"
                    component="h2"
                    gutterBottom
                    sx={{ fontWeight: 'bold', mb: 1, textAlign: 'left' }}
                >
                    Consult Audio Whisper Pipeline
                </Typography>
                <Typography variant="body1" color="text.secondary" paragraph sx={{ mb: 4, fontSize: '1.1rem', textAlign: 'left' }}>
                    A complete automated pipeline for processing dental consultation audio recordings,
                    from transcription to AI-powered analysis with multiple output formats. Provides
                    end-to-end automation from raw audio files to structured documentation and
                    professional reports.
                </Typography>

                {/* Five-Stage Pipeline */}
                <Typography variant="h5" gutterBottom sx={{ fontWeight: 'bold', mb: 3, mt: 4, textAlign: 'left' }}>
                    Five-Stage Processing Pipeline
                </Typography>
                <Grid container spacing={3} sx={{ mb: 4 }}>
                    <Grid item xs={12} sm={6} md={4}>
                        <Card elevation={2} sx={{ height: '100%' }}>
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                    <Mic sx={{ fontSize: 32, color: 'primary.main', mr: 1.5 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        1. Audio Transcription
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary" paragraph>
                                    <strong>OpenAI Whisper</strong> converts audio files to text with
                                    high accuracy. Processes multiple formats (.m4a, .wav, .mp3, .ogg)
                                    and generates timestamped outputs in .txt, .tsv, .srt, and .vtt formats.
                                </Typography>
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                                    <Chip label="Whisper AI" size="small" />
                                    <Chip label="PyTorch" size="small" />
                                    <Chip label="Multi-format" size="small" />
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={4}>
                        <Card elevation={2} sx={{ height: '100%' }}>
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                    <CleaningServices sx={{ fontSize: 32, color: 'primary.main', mr: 1.5 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        2. Transcript Cleaning
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary" paragraph>
                                    Applies <strong>dental corrections</strong> using pattern matching
                                    and JSON rules. Handles terminology, abbreviations, and common
                                    transcription errors while preserving timestamps and structure.
                                </Typography>
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                                    <Chip label="Pattern Matching" size="small" />
                                    <Chip label="JSON Rules" size="small" />
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={4}>
                        <Card elevation={2} sx={{ height: '100%' }}>
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                    <SmartToy sx={{ fontSize: 32, color: 'primary.main', mr: 1.5 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        3. LLM Analysis
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary" paragraph>
                                    <strong>Anthropic Claude AI</strong> generates structured summaries
                                    and detailed skill analysis. Provides consultation overviews, treatment
                                    discussions, and communication quality assessments.
                                </Typography>
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                                    <Chip label="Claude 3.5" size="small" />
                                    <Chip label="API Integration" size="small" />
                                    <Chip label="Rate Limiting" size="small" />
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={6}>
                        <Card elevation={2} sx={{ height: '100%' }}>
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                    <Code sx={{ fontSize: 32, color: 'primary.main', mr: 1.5 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        4. HTML Conversion
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary" paragraph>
                                    Converts markdown analysis files to <strong>web-ready HTML</strong> with
                                    responsive design, professional styling, and custom CSS. Optimized for
                                    viewing on multiple devices.
                                </Typography>
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                                    <Chip label="Markdown2" size="small" />
                                    <Chip label="Responsive Design" size="small" />
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} sm={6} md={6}>
                        <Card elevation={2} sx={{ height: '100%' }}>
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                    <PictureAsPdf sx={{ fontSize: 32, color: 'primary.main', mr: 1.5 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        5. PDF Conversion
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary" paragraph>
                                    Generates <strong>printable PDF documents</strong> using ReportLab with
                                    professional formatting, proper page breaks, and print-optimized
                                    typography for clinical documentation.
                                </Typography>
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                                    <Chip label="ReportLab" size="small" />
                                    <Chip label="Print-Optimized" size="small" />
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>
                </Grid>

                {/* Key Features */}
                <Paper elevation={2} sx={{ p: 4, mb: 4, bgcolor: 'grey.50' }}>
                    <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', mb: 2 }}>
                        Key Technical Features
                    </Typography>
                    <Grid container spacing={2}>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body2" paragraph>
                                <strong>Smart File Processing:</strong> Incremental processing, file
                                validation, error recovery, and comprehensive status tracking.
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body2" paragraph>
                                <strong>Memory Management:</strong> Streaming audio processing, batch
                                operations, and efficient model loading for large files.
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body2" paragraph>
                                <strong>API Integration:</strong> Rate limiting, exponential backoff retries,
                                response caching, and token usage tracking.
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body2" paragraph>
                                <strong>Quality Assurance:</strong> Transcript validation, correction
                                verification, output validation, and comprehensive audit logging.
                            </Typography>
                        </Grid>
                    </Grid>
                </Paper>

                {/* Engineering Highlights */}
                <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', mb: 2 }}>
                    Technology Stack
                </Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mb: 4 }}>
                    <Chip label="Python 3.8+" />
                    <Chip label="OpenAI Whisper" />
                    <Chip label="Anthropic Claude AI" />
                    <Chip label="PyTorch" />
                    <Chip label="Async Processing" />
                    <Chip label="Markdown2" />
                    <Chip label="ReportLab" />
                    <Chip label="Jinja2" />
                    <Chip label="Pytest" />
                    <Chip label="File Processing" />
                    <Chip label="Workflow Automation" />
                </Box>

                <Button
                    variant="outlined"
                    size="large"
                    component={Link}
                    href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/consult_audio_pipe"
                    target="_blank"
                    rel="noopener noreferrer"
                >
                    <GitHub sx={{ mr: 1 }} />
                    View on GitHub
                </Button>
            </Box>

            <Divider sx={{ my: 8 }} />

            {/* Supporting Projects */}
            <Box sx={{ mb: 8, px: { xs: 3, sm: 4, md: 6, lg: 8 } }}>
                <Typography
                    variant="h3"
                    component="h2"
                    gutterBottom
                    sx={{ fontWeight: 'bold', mb: 4, textAlign: 'left' }}
                >
                    Supporting Engineering Tools
                </Typography>
                <Grid container spacing={3}>
                    <Grid item xs={12} md={6}>
                        <Card
                            elevation={2}
                            sx={{
                                height: '100%',
                                cursor: 'pointer',
                                transition: 'transform 0.2s, box-shadow 0.2s',
                                '&:hover': {
                                    transform: 'translateY(-4px)',
                                    boxShadow: 6,
                                }
                            }}
                            component={RouterLink}
                            to="/environment-manager"
                        >
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                    <Settings sx={{ fontSize: 32, color: 'primary.main', mr: 1 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        Data Engineering Environment Manager
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary" paragraph>
                                    Custom PowerShell automation tool that simplifies development workflows.
                                    Unified interface for managing multiple environments (dbt, ETL, API, frontend)
                                    with auto-detection, environment isolation, and interactive configuration.
                                </Typography>
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mb: 2 }}>
                                    <Chip label="PowerShell" size="small" />
                                    <Chip label="Automation" size="small" />
                                    <Chip label="DevOps" size="small" />
                                    <Chip label="Tooling" size="small" />
                                </Box>
                                <Typography variant="caption" color="primary" sx={{ fontWeight: 600 }}>
                                    Learn more →
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} md={6}>
                        <Card elevation={2}>
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                    <Settings sx={{ fontSize: 32, color: 'primary.main', mr: 1 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        Schema Discovery & Replication
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary" paragraph>
                                    SQLAlchemy-based introspection tooling for automated schema
                                    discovery, metadata generation, and incremental load logic
                                    with comprehensive test coverage.
                                </Typography>
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                                    <Chip label="SQLAlchemy" size="small" />
                                    <Chip label="Schema Introspection" size="small" />
                                    <Chip label="Incremental Loading" size="small" />
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>
                </Grid>
            </Box>

            <Divider sx={{ my: 8 }} />

            {/* Skills & Tech Stack */}
            <Box sx={{ mb: 8, px: { xs: 3, sm: 4, md: 6, lg: 8 } }}>
                <Typography
                    variant="h3"
                    component="h2"
                    gutterBottom
                    sx={{ fontWeight: 'bold', mb: 4, textAlign: 'left' }}
                >
                    Skills & Tech Stack
                </Typography>
                <Grid container spacing={4}>
                    <Grid item xs={12} md={4}>
                        <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold' }}>
                            Data Engineering
                        </Typography>
                        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                            <Chip label="dbt" />
                            <Chip label="SQL" />
                            <Chip label="PostgreSQL" />
                            <Chip label="MySQL" />
                            <Chip label="ELT/ETL" />
                            <Chip label="Dimensional Modeling" />
                        </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                        <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold' }}>
                            Core Technologies
                        </Typography>
                        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                            <Chip label="Python" />
                            <Chip label="TypeScript" />
                            <Chip label="SQLAlchemy" />
                            <Chip label="FastAPI" />
                            <Chip label="React" />
                        </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                        <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold' }}>
                            Analytics & Frontend
                        </Typography>
                        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                            <Chip label="Material-UI" />
                            <Chip label="Recharts" />
                            <Chip label="Zustand" />
                            <Chip label="Data Visualization" />
                        </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                        <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold' }}>
                            Tools & Cloud
                        </Typography>
                        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                            <Chip label="AWS" />
                            <Chip label="Docker" />
                            <Chip label="Git" />
                            <Chip label="CI/CD" />
                            <Chip label="Airflow" />
                        </Box>
                    </Grid>
                </Grid>
            </Box>

            <Divider sx={{ my: 8 }} />

            {/* Experience Section */}
            <Box sx={{ mb: 8, px: { xs: 3, sm: 4, md: 6, lg: 8 } }}>
                <Typography
                    variant="h3"
                    component="h2"
                    gutterBottom
                    sx={{ fontWeight: 'bold', mb: 4, textAlign: 'left' }}
                >
                    Experience
                </Typography>
                <Paper elevation={2} sx={{ p: 4 }}>
                    <Typography variant="h5" gutterBottom sx={{ fontWeight: 'bold' }}>
                        Data Engineer — Merrillville Dental Center
                    </Typography>
                    <Typography variant="body2" color="text.secondary" paragraph>
                        Built and maintained end-to-end analytics infrastructure for a real dental
                        practice, from source data replication to interactive dashboards.
                    </Typography>
                    <Box component="ul" sx={{ pl: 3, mt: 2 }}>
                        <li>
                            <Typography variant="body2">
                                Designed and implemented metadata-driven ELT pipeline replicating
                                450+ source tables from OpenDental to PostgreSQL
                            </Typography>
                        </li>
                        <li>
                            <Typography variant="body2">
                                Developed comprehensive dbt project with ~150 models covering staging,
                                intermediate transformations, and dimensional marts
                            </Typography>
                        </li>
                        <li>
                            <Typography variant="body2">
                                Built FastAPI backend and React frontend for real-time analytics
                                dashboards used by clinic management
                            </Typography>
                        </li>
                        <li>
                            <Typography variant="body2">
                                Created automated ML pipeline for transcribing and analyzing
                                consultation recordings to improve treatment acceptance
                            </Typography>
                        </li>
                        <li>
                            <Typography variant="body2">
                                Deployed production infrastructure on AWS with CloudFront CDN,
                                automated CI/CD, and comprehensive testing
                            </Typography>
                        </li>
                    </Box>
                </Paper>
            </Box>

            <Divider sx={{ my: 8 }} />

            {/* Contact Section */}
            <Box sx={{ mb: 4, px: { xs: 3, sm: 4, md: 6, lg: 8 } }}>
                <Typography
                    variant="h3"
                    component="h2"
                    gutterBottom
                    sx={{ fontWeight: 'bold', mb: 2, textAlign: 'left' }}
                >
                    Get In Touch
                </Typography>
                <Typography variant="body1" color="text.secondary" paragraph sx={{ mb: 4, textAlign: 'left' }}>
                    Interested in collaborating or learning more about my work? Let's connect.
                </Typography>
                <Box sx={{ display: 'flex', justifyContent: 'flex-start', gap: 3, flexWrap: 'wrap' }}>
                    <Button
                        variant="contained"
                        size="large"
                        component={Link}
                        href="https://github.com/BenjaminRains"
                        target="_blank"
                        rel="noopener noreferrer"
                        startIcon={<GitHub />}
                    >
                        GitHub
                    </Button>
                    <Button
                        variant="contained"
                        size="large"
                        component={Link}
                        href="https://www.linkedin.com/in/yourprofile"
                        target="_blank"
                        rel="noopener noreferrer"
                        startIcon={<LinkedIn />}
                    >
                        LinkedIn
                    </Button>
                    <Button
                        variant="contained"
                        size="large"
                        component={Link}
                        href="/assets/resume.pdf"
                        target="_blank"
                        rel="noopener noreferrer"
                        startIcon={<Description />}
                    >
                        Resume
                    </Button>
                </Box>
            </Box>

            {/* Footer */}
            <Box
                sx={{
                    bgcolor: 'grey.900',
                    color: 'grey.300',
                    py: 4,
                    textAlign: 'center',
                }}
            >
                <Container maxWidth="lg">
                    <Typography variant="body2">
                        © {new Date().getFullYear()} Benjamin Rains. Built with React, TypeScript,
                        and Material-UI.
                    </Typography>
                </Container>
            </Box>
        </Box>
    );
};

export default Portfolio;

