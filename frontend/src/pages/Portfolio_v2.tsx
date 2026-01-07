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
import MermaidDiagram from '../components/common/MermaidDiagram';
import {
    GitHub,
    LinkedIn,
    Description,
    Email,
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
                        variant="h5"
                        component="h1"
                        gutterBottom
                        sx={{
                            fontWeight: 700,
                            fontSize: { xs: '1.5rem', md: '1.75rem' },
                            mb: 0.5,
                            letterSpacing: '0.05em',
                            opacity: 1,
                        }}
                    >
                        Benjamin Rains
                    </Typography>
                    <Typography
                        variant="h5"
                        component="h2"
                        gutterBottom
                        sx={{
                            mb: 2,
                            opacity: 1,
                            fontSize: { xs: '1.25rem', md: '1.5rem' },
                            fontWeight: 600,
                        }}
                    >
                        Data Engineer – Healthcare Analytics & Modern ELT
                    </Typography>
                    <Typography
                        variant="body2"
                        component="div"
                        sx={{
                            mb: 4,
                            opacity: 1,
                            fontSize: { xs: '0.875rem', md: '1rem' },
                            fontWeight: 500,
                        }}
                    >
                        Las Vegas, NV • rains.bp@gmail.com •{' '}
                        <Link
                            href="https://www.linkedin.com/in/benjamin-rains-154139270/"
                            target="_blank"
                            rel="noopener noreferrer"
                            sx={{
                                color: 'inherit',
                                textDecoration: 'underline',
                                fontWeight: 600,
                                '&:hover': {
                                    opacity: 0.9,
                                },
                            }}
                        >
                            LinkedIn
                        </Link>
                    </Typography>
                    <Typography
                        variant="body1"
                        sx={{
                            mb: 5,
                            maxWidth: '700px',
                            mx: 'auto',
                            fontSize: { xs: '1rem', md: '1.125rem' },
                            lineHeight: 1.75,
                            opacity: 1,
                            fontWeight: 500,
                        }}
                    >
                        I build end-to-end analytics platforms for healthcare operations,
                        from metadata-driven ELT pipelines and dbt warehouses to APIs,
                        dashboards, and ML-powered workflows. This portfolio demonstrates
                        a production platform for a real multi-clinic dental system,
                        transforming 450+ OpenDental source tables through a 150+ model
                        dbt project into actionable clinical, operational, and financial insights.
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
                            to="/dashboard"
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
                {/* Project Metrics */}
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
                            Source Tables
                        </Typography>
                    </Box>
                    <Box>
                        <Typography variant="h6" component="span" fontWeight="bold" color="primary">
                            95%
                        </Typography>
                        <Typography variant="body2" component="span" color="text.secondary" sx={{ ml: 0.5 }}>
                            dbt Test Coverage
                        </Typography>
                    </Box>
                    <Box>
                        <Typography variant="h6" component="span" fontWeight="bold" color="primary" sx={{ ml: 0.5 }}>
                            5 Stages
                        </Typography>
                        <Typography variant="body2" component="span" color="text.secondary" sx={{ ml: 0.5 }}>
                            End-to-End Pipeline
                        </Typography>
                    </Box>
                </Box>

                {/* Quick Links */}
                <Typography variant="h5" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'left' }}>
                    Project Components
                </Typography>
                <Box sx={{ mb: 6 }}>
                    <Grid container spacing={3}>
                        <Grid item xs={12} md={4}>
                            <Box sx={{ display: 'flex', alignItems: 'center' }}>
                                <Dashboard sx={{ fontSize: 28, color: 'primary.main', mr: 1.5 }} />
                                <Typography
                                    variant="h6"
                                    fontWeight="bold"
                                    component={RouterLink}
                                    to="/dashboard"
                                    sx={{
                                        textAlign: 'left',
                                        color: 'inherit',
                                        textDecoration: 'none',
                                        '&:hover': {
                                            textDecoration: 'underline',
                                            color: 'primary.main',
                                        },
                                    }}
                                >
                                    Analytics Dashboard
                                </Typography>
                            </Box>
                        </Grid>
                        <Grid item xs={12} md={8}>
                            <Typography variant="body2" color="text.secondary" sx={{ textAlign: 'left' }}>
                                React + TypeScript dashboard backed by FastAPI, delivering real-time KPIs,
                                production metrics, and provider performance analytics.
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={4}>
                            <Box sx={{ display: 'flex', alignItems: 'center' }}>
                                <Analytics sx={{ fontSize: 28, color: 'primary.main', mr: 1.5 }} />
                                <Typography
                                    variant="h6"
                                    fontWeight="bold"
                                    component={Link}
                                    href="https://dbtdentalclinic.com/dbt-docs/"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    sx={{
                                        textAlign: 'left',
                                        color: 'inherit',
                                        textDecoration: 'none',
                                        '&:hover': {
                                            textDecoration: 'underline',
                                            color: 'primary.main',
                                        },
                                    }}
                                >
                                    dbt Models
                                </Typography>
                            </Box>
                        </Grid>
                        <Grid item xs={12} md={8}>
                            <Typography variant="body2" color="text.secondary" sx={{ textAlign: 'left' }}>
                                150+ dbt models across staging, intermediate, and mart layers with
                                dimensional design, incremental strategies, and comprehensive tests.
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={4}>
                            <Box sx={{ display: 'flex', alignItems: 'center' }}>
                                <Storage sx={{ fontSize: 28, color: 'primary.main', mr: 1.5 }} />
                                <Typography
                                    variant="h6"
                                    fontWeight="bold"
                                    component={Link}
                                    href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/etl_pipeline"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    sx={{
                                        textAlign: 'left',
                                        color: 'inherit',
                                        textDecoration: 'none',
                                        '&:hover': {
                                            textDecoration: 'underline',
                                            color: 'primary.main',
                                        },
                                    }}
                                >
                                    ELT Pipeline
                                </Typography>
                            </Box>
                        </Grid>
                        <Grid item xs={12} md={8}>
                            <Typography variant="body2" color="text.secondary" sx={{ textAlign: 'left' }}>
                                Metadata-driven Python pipeline using SQLAlchemy introspection to replicate
                                450+ OpenDental tables into PostgreSQL with incremental loading and validation.
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={4}>
                            <Box sx={{ display: 'flex', alignItems: 'center' }}>
                                <Code sx={{ fontSize: 28, color: 'primary.main', mr: 1.5 }} />
                                <Typography
                                    variant="h6"
                                    fontWeight="bold"
                                    component={Link}
                                    href="https://api.dbtdentalclinic.com/docs#"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    sx={{
                                        textAlign: 'left',
                                        color: 'inherit',
                                        textDecoration: 'none',
                                        '&:hover': {
                                            textDecoration: 'underline',
                                            color: 'primary.main',
                                        },
                                    }}
                                >
                                    FastAPI Backend
                                </Typography>
                            </Box>
                        </Grid>
                        <Grid item xs={12} md={8}>
                            <Typography variant="body2" color="text.secondary" sx={{ textAlign: 'left' }}>
                                Production-oriented REST API exposing KPI and reporting endpoints directly
                                from dbt marts with pagination, caching, and modular routing.
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={4}>
                            <Box sx={{ display: 'flex', alignItems: 'center' }}>
                                <Mic sx={{ fontSize: 28, color: 'primary.main', mr: 1.5 }} />
                                <Typography
                                    variant="h6"
                                    fontWeight="bold"
                                    component={Link}
                                    href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/consult_audio_pipe"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    sx={{
                                        textAlign: 'left',
                                        color: 'inherit',
                                        textDecoration: 'none',
                                        '&:hover': {
                                            textDecoration: 'underline',
                                            color: 'primary.main',
                                        },
                                    }}
                                >
                                    Consult Audio Pipeline
                                </Typography>
                            </Box>
                        </Grid>
                        <Grid item xs={12} md={8}>
                            <Typography variant="body2" color="text.secondary" sx={{ textAlign: 'left' }}>
                                Whisper + LLM-based pipeline that converts dental consult audio into structured
                                summaries, skills analysis, and documentation-ready outputs.
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={4}>
                            <Box sx={{ display: 'flex', alignItems: 'center' }}>
                                <Description sx={{ fontSize: 28, color: 'primary.main', mr: 1.5 }} />
                                <Typography
                                    variant="h6"
                                    fontWeight="bold"
                                    component={Link}
                                    href="#documentation"
                                    sx={{
                                        textAlign: 'left',
                                        color: 'inherit',
                                        textDecoration: 'none',
                                        '&:hover': {
                                            textDecoration: 'underline',
                                            color: 'primary.main',
                                        },
                                    }}
                                >
                                    Documentation
                                </Typography>
                            </Box>
                        </Grid>
                        <Grid item xs={12} md={8}>
                            <Typography variant="body2" color="text.secondary" sx={{ textAlign: 'left' }}>
                                Architecture, setup, dbt docs, and API references for the entire platform.
                            </Typography>
                        </Grid>
                    </Grid>
                </Box>

                {/* Visual Examples Section */}
                <Typography variant="h5" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'left' }}>
                    Visual Examples
                </Typography>
                <Grid container spacing={3} sx={{ mb: 6 }}>
                    <Grid item xs={12}>
                        <Box
                            sx={{
                                borderRadius: 2,
                                overflow: 'hidden',
                                bgcolor: 'grey.100',
                                position: 'relative',
                                width: '100%',
                                boxShadow: 3,
                                '&:hover': {
                                    boxShadow: 6,
                                    transform: 'translateY(-2px)',
                                    transition: 'all 0.3s ease',
                                },
                            }}
                        >
                            <Typography
                                variant="h6"
                                sx={{
                                    fontWeight: 'bold',
                                    p: 2,
                                    pb: 1,
                                    bgcolor: 'background.paper',
                                    borderBottom: '1px solid',
                                    borderColor: 'divider',
                                }}
                            >
                                AR Aging Dashboard
                            </Typography>
                            <Box
                                sx={{
                                    width: '100%',
                                    maxHeight: '400px',
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'center',
                                    p: 2,
                                    overflow: 'hidden',
                                }}
                            >
                                <img
                                    src="/ar_aging.png"
                                    alt="AR Aging Dashboard"
                                    onError={(e) => {
                                        const target = e.target as HTMLImageElement;
                                        target.style.display = 'none';
                                    }}
                                    style={{
                                        width: '100%',
                                        height: 'auto',
                                        maxHeight: '400px',
                                        objectFit: 'contain',
                                        display: 'block',
                                    }}
                                />
                            </Box>
                        </Box>
                    </Grid>
                    <Grid item xs={12}>
                        <Box
                            sx={{
                                borderRadius: 2,
                                overflow: 'hidden',
                                bgcolor: 'grey.100',
                                position: 'relative',
                                width: '100%',
                                boxShadow: 3,
                                '&:hover': {
                                    boxShadow: 6,
                                    transform: 'translateY(-2px)',
                                    transition: 'all 0.3s ease',
                                },
                            }}
                        >
                            <Typography
                                variant="h6"
                                sx={{
                                    fontWeight: 'bold',
                                    p: 2,
                                    pb: 1,
                                    bgcolor: 'background.paper',
                                    borderBottom: '1px solid',
                                    borderColor: 'divider',
                                }}
                            >
                                Revenue Trend Dashboard
                            </Typography>
                            <Box
                                sx={{
                                    width: '100%',
                                    maxHeight: '400px',
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'center',
                                    p: 2,
                                    overflow: 'hidden',
                                }}
                            >
                                <img
                                    src="/rev_trend.png"
                                    alt="Revenue Trend Dashboard"
                                    onError={(e) => {
                                        const target = e.target as HTMLImageElement;
                                        target.style.display = 'none';
                                    }}
                                    style={{
                                        width: '100%',
                                        height: 'auto',
                                        maxHeight: '400px',
                                        objectFit: 'contain',
                                        display: 'block',
                                    }}
                                />
                            </Box>
                        </Box>
                    </Grid>
                    <Grid item xs={12}>
                        <Box
                            sx={{
                                borderRadius: 2,
                                overflow: 'hidden',
                                bgcolor: 'grey.100',
                                position: 'relative',
                                width: '100%',
                                boxShadow: 3,
                                '&:hover': {
                                    boxShadow: 6,
                                    transform: 'translateY(-2px)',
                                    transition: 'all 0.3s ease',
                                },
                            }}
                        >
                            <Typography
                                variant="h6"
                                sx={{
                                    fontWeight: 'bold',
                                    p: 2,
                                    pb: 1,
                                    bgcolor: 'background.paper',
                                    borderBottom: '1px solid',
                                    borderColor: 'divider',
                                }}
                            >
                                Revenue Opportunity Dashboard
                            </Typography>
                            <Box
                                sx={{
                                    width: '100%',
                                    maxHeight: '400px',
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'center',
                                    p: 2,
                                    overflow: 'hidden',
                                }}
                            >
                                <img
                                    src="/revenue_opp.png"
                                    alt="Revenue Opportunity Dashboard"
                                    onError={(e) => {
                                        const target = e.target as HTMLImageElement;
                                        target.style.display = 'none';
                                    }}
                                    style={{
                                        width: '100%',
                                        height: 'auto',
                                        maxHeight: '400px',
                                        objectFit: 'contain',
                                        display: 'block',
                                    }}
                                />
                            </Box>
                        </Box>
                    </Grid>
                    <Grid item xs={12}>
                        <Box
                            sx={{
                                borderRadius: 2,
                                overflow: 'hidden',
                                bgcolor: 'grey.100',
                                position: 'relative',
                                width: '100%',
                                boxShadow: 3,
                                '&:hover': {
                                    boxShadow: 6,
                                    transform: 'translateY(-2px)',
                                    transition: 'all 0.3s ease',
                                },
                            }}
                        >
                            <Typography
                                variant="h6"
                                sx={{
                                    fontWeight: 'bold',
                                    p: 2,
                                    pb: 1,
                                    bgcolor: 'background.paper',
                                    borderBottom: '1px solid',
                                    borderColor: 'divider',
                                }}
                            >
                                dbt Lineage Graph
                                dim_patient DAG shown below
                            </Typography>
                            <Box
                                sx={{
                                    width: '100%',
                                    maxHeight: '400px',
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'center',
                                    p: 2,
                                    overflow: 'hidden',
                                }}
                            >
                                <img
                                    src="/dim_patient_DAG.png"
                                    alt="dbt Lineage Graph - dim_patient DAG"
                                    onError={(e) => {
                                        const target = e.target as HTMLImageElement;
                                        target.style.display = 'none';
                                    }}
                                    style={{
                                        width: '100%',
                                        height: 'auto',
                                        maxHeight: '400px',
                                        objectFit: 'contain',
                                        display: 'block',
                                    }}
                                />
                            </Box>
                            <Box
                                sx={{
                                    bgcolor: 'background.paper',
                                    p: 2,
                                    display: 'flex',
                                    flexDirection: 'column',
                                    alignItems: 'center',
                                    gap: 2,
                                }}
                            >
                                <Typography
                                    variant="body2"
                                    color="text.secondary"
                                    sx={{ textAlign: 'center' }}
                                >
                                    Interactive dbt lineage showing how 150+ models connect raw OpenDental
                                    sources to business-ready marts and KPIs.
                                </Typography>
                                <Button
                                    variant="contained"
                                    color="primary"
                                    component={Link}
                                    href="https://dbtdentalclinic.com/dbt-docs/"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    sx={{
                                        textTransform: 'none',
                                    }}
                                >
                                    View dbt Lineage Graph
                                </Button>
                            </Box>
                        </Box>
                    </Grid>
                    <Grid item xs={12}>
                        <Card elevation={3}>
                            <CardContent>
                                <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', mb: 2 }}>
                                    System Architecture Diagram
                                </Typography>
                                <Box
                                    sx={{
                                        bgcolor: 'grey.50',
                                        borderRadius: 2,
                                        p: 4,
                                        display: 'flex',
                                        justifyContent: 'center',
                                        overflowX: 'auto',
                                        minHeight: '600px',
                                    }}
                                >
                                    <MermaidDiagram
                                        id="system-architecture"
                                        chart={`flowchart TD
    A["OpenDental MariaDB<br/>450+ source tables"] --> B["Python ETL / Schema Discovery<br/>• SQLAlchemy-powered introspection<br/>• Metadata-driven extraction<br/>• Incremental loading logic<br/>• Change tracking + validation"]
    B --> C["PostgreSQL Warehouse<br/>RAW → replicated source tables<br/>STAGING → typed & cleaned inputs<br/>INTERMEDIATE → business logic & joins<br/>MARTS → KPI & dimensional models"]
    C --> D["dbt Core<br/>150+ Models<br/>Tests, Docs, Lineage Graph"]
    D --> E["FastAPI Backend<br/>• REST API for KPIs & queries<br/>• Caching, pagination, auth<br/>• Direct access to marts schema"]
    E --> F["React Analytics Dashboard<br/>• Real-time KPI cards<br/>• Production & collections charts<br/>• Provider performance analytics<br/>• State mgmt via Zustand"]
    F --> G["AWS Deployment Prod<br/>CloudFront CDN → React frontend<br/>S3 → static assets<br/>EC2/Docker → FastAPI backend<br/>RDS PostgreSQL → warehouse<br/>CI/CD → automated tests & deploy"]
    G --> H["Consult Audio Pipeline Parallel<br/>Whisper → Transcript → Cleaning<br/>→ LLM Summary → Structured TSV/JSON<br/>optional warehouse load<br/>Enriches analytics with consultation insights"]
    
    style A fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    style B fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    style C fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    style D fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style E fill:#fff9c4,stroke:#f9a825,stroke-width:2px
    style F fill:#e1f5fe,stroke:#0277bd,stroke-width:2px
    style G fill:#fce4ec,stroke:#c2185b,stroke-width:2px
    style H fill:#f1f8e9,stroke:#558b2f,stroke-width:2px`}
                                    />
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
                                        System design, data flow diagrams, and platform overview.
                                    </Typography>
                                    <Link
                                        href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/docs"
                                        target="_blank"
                                        rel="noopener noreferrer"
                                    >
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
                                        Auto-generated dbt docs for models, tests, and lineage.
                                    </Typography>
                                    <Link
                                        href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/dbt_docs"
                                        target="_blank"
                                        rel="noopener noreferrer"
                                    >
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
                                        FastAPI endpoint reference and usage examples.
                                    </Typography>
                                    <Link
                                        href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/api/README.md"
                                        target="_blank"
                                        rel="noopener noreferrer"
                                    >
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
                                        Local development and environment setup instructions.
                                    </Typography>
                                    <Link
                                        href="https://github.com/BenjaminRains/dbt_dental_clinic/blob/main/ENVIRONMENT_SETUP.md"
                                        target="_blank"
                                        rel="noopener noreferrer"
                                    >
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
                                        AWS deployment, configuration, and CI/CD details.
                                    </Typography>
                                    <Link
                                        href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/docs"
                                        target="_blank"
                                        rel="noopener noreferrer"
                                    >
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
                                        dbt test coverage, failure summaries, and validation metrics.
                                    </Typography>
                                    <Link
                                        href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/docs/data_quality"
                                        target="_blank"
                                        rel="noopener noreferrer"
                                    >
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
                    variant="h4"
                    component="h2"
                    gutterBottom
                    sx={{ fontWeight: 'bold', mb: 1, textAlign: 'left' }}
                >
                    dbt Dental Clinic Analytics Platform
                </Typography>
                <Typography variant="body1" color="text.secondary" paragraph sx={{ mb: 4, textAlign: 'left' }}>
                    A production-grade analytics platform for a real dental organization. It combines
                    automated ELT, a tested dbt warehouse, and a React/FastAPI analytics frontend to
                    deliver reliable KPIs for clinical, operational, and financial decision-making.
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
                                    <strong>450+ OpenDental source tables</strong> replicated into PostgreSQL
                                    using a metadata-driven ELT pipeline. Automated schema discovery,
                                    incremental loads, and validation provide a stable foundation for analytics.
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
                                    <strong>150+ staging, intermediate, and mart models</strong> implementing
                                    dimensional modeling, SCD patterns, incremental strategies, and full dbt
                                    test coverage for trustworthy reporting.
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
                                    <strong>FastAPI + React</strong> analytics application surfacing daily and
                                    real-time KPIs for production, collections, scheduling, claims, and
                                    provider performance. Built with Material-UI, Zustand, and Recharts.
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
                                    Dockerized services, CI/CD, and AWS infrastructure (CloudFront, S3, EC2,
                                    RDS) for reliable daily updates and frictionless deployments to the clinic’s
                                    production environment.
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

                <Button variant="contained" size="large" component={RouterLink} to="/dashboard" sx={{ mr: 2 }}>
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
                    variant="h4"
                    component="h2"
                    gutterBottom
                    sx={{ fontWeight: 'bold', mb: 1, textAlign: 'left' }}
                >
                    Consult Audio Whisper Pipeline
                </Typography>
                <Typography
                    variant="body1"
                    color="text.secondary"
                    paragraph
                    sx={{ mb: 4, fontSize: '1.1rem', textAlign: 'left' }}
                >
                    An automated pipeline that turns dental consultation recordings into structured,
                    clinician-friendly documentation and analytics. It orchestrates transcription,
                    cleaning, LLM analysis, and multi-format outputs used for treatment acceptance
                    tracking and provider coaching.
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
                                    <strong>OpenAI Whisper</strong> converts consult audio into timestamped text
                                    across common formats (.m4a, .wav, .mp3, .ogg), producing .txt, .tsv, .srt,
                                    and .vtt outputs.
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
                                    Dental-specific rules and pattern matching correct terminology,
                                    abbreviations, and common transcription errors while preserving timestamps
                                    and structure.
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
                                    <strong>Anthropic Claude</strong> generates structured summaries, treatment
                                    discussions, and communication quality assessments that can feed into QA
                                    workflows and analytics.
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
                                    Converts markdown outputs into responsive HTML reports with professional
                                    styling, ready for browser-based review on any device.
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
                                    Generates print-optimized PDF documents using ReportLab, with clinical
                                    formatting, page breaks, and typography suitable for records or review.
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
                                <strong>Smart File Processing:</strong> Incremental processing, file validation,
                                error recovery, and explicit status tracking across long-running jobs.
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body2" paragraph>
                                <strong>Memory Management:</strong> Streaming audio handling, batch operations,
                                and efficient Whisper loading for large consult libraries.
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body2" paragraph>
                                <strong>API Integration:</strong> Robust handling of LLM APIs with rate limiting,
                                exponential backoff retries, response caching, and token tracking.
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Typography variant="body2" paragraph>
                                <strong>Quality Assurance:</strong> Transcript validation, correction checks,
                                output verification, and detailed audit logs for every run.
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
                    <Chip label="Anthropic Claude" />
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
                    variant="h4"
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
                                },
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
                                    PowerShell-based automation that manages dbt, ETL, API, and frontend
                                    environments from a single interface. Handles environment isolation,
                                    auto-detection, and interactive configuration to speed up daily workflows.
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
                        <Card
                            elevation={2}
                            sx={{
                                height: '100%',
                                cursor: 'pointer',
                                transition: 'transform 0.2s, box-shadow 0.2s',
                                '&:hover': {
                                    transform: 'translateY(-4px)',
                                    boxShadow: 6,
                                },
                            }}
                            component={RouterLink}
                            to="/schema-discovery"
                        >
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                    <Settings sx={{ fontSize: 32, color: 'primary.main', mr: 1 }} />
                                    <Typography variant="h6" fontWeight="bold">
                                        Schema Discovery & Replication
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary" paragraph>
                                    <strong>analyze_opendental_schema.py</strong> performs automated schema
                                    introspection using SQLAlchemy to discover 450+ tables, generate{' '}
                                    <Box component="span" sx={{ fontFamily: 'monospace', bgcolor: 'grey.200', px: 0.5, borderRadius: 0.5 }}>
                                        tables.yml
                                    </Box>{' '}
                                    configuration, and monitor slowly changing dimensions (SCDs). Powers
                                    incremental loading strategies and detects schema drift automatically.
                                </Typography>
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mb: 2 }}>
                                    <Chip label="SQLAlchemy" size="small" />
                                    <Chip label="Schema Introspection" size="small" />
                                    <Chip label="SCD Detection" size="small" />
                                    <Chip label="Incremental Loading" size="small" />
                                </Box>
                                <Typography variant="caption" color="primary" sx={{ fontWeight: 600 }}>
                                    Learn more →
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                </Grid>
            </Box>

            <Divider sx={{ my: 8 }} />

            {/* Skills & Tech Stack */}
            <Box sx={{ mb: 8, px: { xs: 3, sm: 4, md: 6, lg: 8 } }}>
                <Typography
                    variant="h4"
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
                    Interested in working together or learning more about the platform? Reach out below.
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
                        href="https://www.linkedin.com/in/benjamin-rains-154139270/"
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
                        href="mailto:rains.bp@gmail.com"
                        startIcon={<Email />}
                    >
                        Email
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
                        © {new Date().getFullYear()} Benjamin Rains. Built with React, TypeScript, and Material-UI.
                    </Typography>
                </Container>
            </Box>
        </Box>
    );
};

export default Portfolio;
