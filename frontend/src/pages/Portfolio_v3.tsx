/**
 * Portfolio v3 – Production system presentation per frontend/docs/PORTFOLIO_OVERHAUL_PLAN.md
 * Order: Hero → Production → Architecture → Production Evidence → dbt Lineage →
 * Synthetic Dashboards → GitHub/Code → Problem→System→Outcome → Docs → Consult Audio →
 * Supporting Tools → Skills → Contact → Footer
 */
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
    Toolbar,
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
    Schedule,
    Assessment,
    Replay,
    CheckCircle,
    Cloud,
    Dns,
} from '@mui/icons-material';

const ARCHITECTURE_CHART = `flowchart TD
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
    style H fill:#f1f8e9,stroke:#558b2f,stroke-width:2px`;

const NAV_LINKS = [
    { label: 'Production', href: '#production' },
    { label: 'Architecture', href: '#architecture' },
    { label: 'Evidence', href: '#evidence' },
    { label: 'Lineage', href: '#lineage' },
    { label: 'Dashboards', href: '#dashboards' },
    { label: 'Code', href: '#code' },
    { label: 'Documentation', href: '#documentation' },
    { label: 'Contact', href: '#contact' },
];

const PortfolioV3: React.FC = () => {
    return (
        <Box sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
            {/* Top ribbon — grey box; same width container as hero */}
            <Box
                sx={{
                    position: 'sticky',
                    top: 0,
                    zIndex: 1100,
                    px: { xs: 2, sm: 3 },
                    pt: 2,
                    pb: 0,
                }}
            >
                <Box
                    sx={{
                        bgcolor: '#616161',
                        color: '#fff',
                        borderRadius: 2,
                        boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
                        border: '1px solid rgba(0,0,0,0.08)',
                    }}
                >
                    <Toolbar disableGutters sx={{ px: { xs: 2, sm: 3 }, py: 1.5 }}>
                        <Typography
                            component={RouterLink}
                            to="/"
                            variant="h6"
                            fontWeight="bold"
                            sx={{
                                color: '#fff',
                                textDecoration: 'none',
                                letterSpacing: '0.02em',
                                mr: 4,
                            }}
                        >
                            Benjamin Rains
                        </Typography>
                        <Box sx={{ flex: 1, display: 'flex', justifyContent: 'flex-end', gap: { xs: 1, sm: 2 }, flexWrap: 'wrap' }}>
                            {NAV_LINKS.map(({ label, href }) => (
                                <Link
                                    key={href}
                                    href={href}
                                    sx={{
                                        color: 'rgba(255,255,255,0.95)',
                                        textDecoration: 'none',
                                        fontSize: '0.8125rem',
                                        fontWeight: 600,
                                        letterSpacing: '0.04em',
                                        textTransform: 'uppercase',
                                        '&:hover': { color: '#fff' },
                                    }}
                                >
                                    {label}
                                </Link>
                            ))}
                        </Box>
                    </Toolbar>
                </Box>
            </Box>

            {/* 1. Hero (§8) — light blue box; same width and shape as ribbon */}
            <Box sx={{ px: { xs: 2, sm: 3 }, mt: 2 }}>
                <Box
                    sx={{
                        bgcolor: '#90caf9',
                        color: '#0d47a1',
                        borderRadius: 2,
                        boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
                        border: '1px solid rgba(13,71,161,0.2)',
                        py: { xs: 6, md: 8 },
                        px: { xs: 2, sm: 3 },
                    }}
                >
                    <Box sx={{ display: 'flex', flexDirection: { xs: 'column', md: 'row' }, alignItems: { md: 'flex-start' }, justifyContent: 'space-between', gap: 3 }}>
                        <Box sx={{ flex: 1, minWidth: 0, textAlign: 'left' }}>
                            <Typography variant="h5" component="h1" gutterBottom sx={{ fontWeight: 700, fontSize: { xs: '1.5rem', md: '1.75rem' }, mb: 0.5, letterSpacing: '0.05em', color: '#01579b' }}>
                                Benjamin Rains
                            </Typography>
                            <Typography variant="h5" component="h2" gutterBottom sx={{ mb: 2, fontSize: { xs: '1.25rem', md: '1.5rem' }, fontWeight: 600, color: '#01579b' }}>
                                Analytics Engineer – Healthcare & Modern ELT
                            </Typography>
                            <Typography variant="h6" component="p" sx={{ mb: 1, fontWeight: 600, color: '#01579b' }}>
                                Production Analytics Platform for a Multi-Clinic Dental Organization
                            </Typography>
                            <Typography variant="body1" sx={{ mb: 4, maxWidth: '640px', lineHeight: 1.75, opacity: 0.95, color: '#0d47a1' }}>
                                Automated ELT, a tested dbt warehouse, and API-driven dashboards delivering trusted clinical, operational, and financial KPIs.
                            </Typography>
                            <Typography variant="body2" sx={{ mb: 2, opacity: 0.95, color: '#0d47a1' }}>
                                Remote US • rains.bp@gmail.com •{' '}
                                <Link href="https://www.linkedin.com/in/benjamin-rains-154139270/" target="_blank" rel="noopener noreferrer" sx={{ color: '#01579b', textDecoration: 'underline', fontWeight: 600 }}>
                                    LinkedIn
                                </Link>
                            </Typography>
                            <Typography variant="body2" sx={{ mb: 4, opacity: 0.95, color: '#0d47a1' }}>
                                For agents and crawlers:{' '}
                                <Link component={RouterLink} to="/agent-profile" sx={{ color: '#01579b', textDecoration: 'underline', fontWeight: 600 }}>
                                    machine-readable profile
                                </Link>
                            </Typography>
                        </Box>
                        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5, flexShrink: 0, width: { xs: '100%', md: 'auto' }, minWidth: { md: 220 } }}>
                            <Button variant="contained" size="large" component={RouterLink} to="/dashboard" sx={{ minWidth: '200px', py: 1.5, fontWeight: 600, textTransform: 'none', bgcolor: '#1565c0', color: 'white', '&:hover': { bgcolor: '#0d47a1' } }}>
                                <Dashboard sx={{ mr: 1.5, fontSize: '1.25rem' }} />
                                View Analytics Demo
                            </Button>
                            <Button variant="outlined" size="large" component={Link} href="https://github.com/BenjaminRains/dbt_dental_clinic" target="_blank" rel="noopener noreferrer" sx={{ minWidth: '200px', py: 1.5, fontWeight: 600, textTransform: 'none', borderColor: '#1565c0', color: '#0d47a1', '&:hover': { borderColor: '#0d47a1', bgcolor: 'rgba(13,71,161,0.08)' } }}>
                                <GitHub sx={{ mr: 1.5, fontSize: '1.25rem' }} />
                                GitHub
                            </Button>
                            <Button variant="outlined" size="large" component={Link} href="/assets/resume.pdf" target="_blank" rel="noopener noreferrer" sx={{ minWidth: '200px', py: 1.5, fontWeight: 600, textTransform: 'none', borderColor: '#1565c0', color: '#0d47a1', '&:hover': { borderColor: '#0d47a1', bgcolor: 'rgba(13,71,161,0.08)' } }}>
                                <Description sx={{ mr: 1.5, fontSize: '1.25rem' }} />
                                Resume
                            </Button>
                        </Box>
                    </Box>
                </Box>
            </Box>

            {/* 2. Production System (§6) */}
            <Box id="production" sx={{ px: { xs: 2, sm: 3 }, py: 4, scrollMarginTop: 56 }}>
                <Paper elevation={0} sx={{ p: 3, pb: 4, bgcolor: 'grey.50', borderLeft: 4, borderColor: '#5e7086', borderRadius: 1, textAlign: 'left' }}>
                    <Typography variant="h6" fontWeight="bold" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Build sx={{ color: '#5e7086', fontSize: 28 }} />
                        Production System   
                    </Typography>
                    <Paper variant="outlined" sx={{ p: 2, mb: 3, bgcolor: 'background.paper', borderColor: '#8b9cad' }}>
                        <Typography variant="body2" fontWeight="medium" color="text.primary">
                            Pipelines are idempotent and support reprocessing, backfills, and partial re-runs without full reloads.
                        </Typography>
                    </Paper>
                    <Grid container spacing={2} sx={{ justifyContent: 'flex-start' }}>
                        {[
                            { title: 'Automated scheduling', desc: 'Daily ELT runs on a schedule using Airflow; EC2 and API can be scheduled (e.g. 7 AM-9 PM) for cost control.', icon: <Schedule /> },
                            { title: 'Monitoring and logging', desc: 'Pipeline and API logging; metrics and error tracking so runs and failures are observable.', icon: <Assessment /> },
                            { title: 'Schema drift handling', desc: 'Metadata-driven schema discovery and type mapping; replication and dbt adapt to source changes.', icon: <Storage /> },
                            { title: 'Backfill support', desc: 'Incremental and full-table strategies with tracking; safe re-runs and historical backfills.', icon: <Replay /> },
                            { title: 'Tests gate deployments', desc: 'dbt tests and CI run before deploy; failing tests block bad data from reaching marts.', icon: <CheckCircle /> },
                            { title: 'Version-controlled models', desc: 'All dbt models, ELT config, and API code in Git; changes are reviewed and traceable.', icon: <Code /> },
                            { title: 'CI/CD pipelines', desc: 'Build, test, and deploy automated; pushes to main trigger validation and deployment.', icon: <Build /> },
                            { title: 'Containerized services', desc: 'API and services run in Docker containers; consistent environments from local to production.', icon: <Dns /> },
                            { title: 'Cloud deployment', desc: 'AWS (S3, CloudFront, EC2, RDS) for frontend, API, and warehouse; production runs in the cloud.', icon: <Cloud /> },
                        ].map(({ title, desc, icon }) => (
                            <Grid item xs={12} sm={6} md={4} key={title}>
                                <Card variant="outlined" sx={{ height: '100%', bgcolor: 'background.paper', textAlign: 'left' }}>
                                    <CardContent sx={{ '&:last-child': { pb: 2 }, textAlign: 'left' }}>
                                        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-start', gap: 1, mb: 0.5 }}>
                                            {React.cloneElement(icon, { sx: { color: '#5e7086', fontSize: 20 } })}
                                            <Typography variant="subtitle2" fontWeight="bold">{title}</Typography>
                                        </Box>
                                        <Typography variant="body2" color="text.secondary" sx={{ textAlign: 'left' }}>{desc}</Typography>
                                    </CardContent>
                                </Card>
                            </Grid>
                        ))}
                    </Grid>
                </Paper>
            </Box>

            {/* 3. Platform Architecture (§1) + 4. Data Flow Example (§4) */}
            <Box id="architecture" sx={{ px: { xs: 2, sm: 3, md: 6, lg: 8 }, pb: 6, scrollMarginTop: 56 }}>
                <Typography variant="h4" fontWeight="bold" gutterBottom sx={{ textAlign: 'left', mb: 1 }}>
                    Platform Architecture
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 3, textAlign: 'left' }}>
                    End-to-end architecture showing how raw OpenDental data flows through automated ELT, a tested dbt warehouse, and API-driven services to power analytics applications.
                </Typography>
                <Card elevation={2} sx={{ textAlign: 'left' }}>
                    <CardContent sx={{ textAlign: 'left' }}>
                        <Box sx={{ bgcolor: 'grey.50', borderRadius: 2, p: 4, overflowX: 'auto', minHeight: 520 }}>
                            <MermaidDiagram id="portfolio-v3-architecture" chart={ARCHITECTURE_CHART} />
                        </Box>
                        <Typography variant="subtitle2" fontWeight="bold" sx={{ mt: 3, mb: 1, textAlign: 'left' }}>Concrete behaviors</Typography>
                        <Box component="ul" sx={{ m: 0, pl: 2, '& li': { typography: 'body2', color: 'text.secondary', mb: 0.5 } }}>
                            <li>Metadata-driven MySQL → PostgreSQL replication</li>
                            <li>Incremental + backfill-safe ELT</li>
                            <li>dbt staging → intermediate → marts with tests</li>
                            <li>FastAPI analytics service</li>
                            <li>React + TypeScript dashboards</li>
                            <li>CI/CD + Docker + AWS deployment</li>
                        </Box>
                        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mt: 2, mb: 2, justifyContent: 'flex-start' }}>
                            {['Incremental Loads', 'Schema Drift Handling', 'Idempotent Pipelines', 'Data Quality Tests', 'Versioned Models', 'API Contracts', 'Containerized Services', 'Environment Separation (dev / staging / prod)'].map((label) => (
                                <Chip key={label} label={label} size="small" variant="outlined" />
                            ))}
                        </Box>
                        <Paper variant="outlined" sx={{ p: 2, bgcolor: 'grey.50', textAlign: 'left' }}>
                            <Typography variant="subtitle2" fontWeight="bold" gutterBottom>Example: From source to dashboard</Typography>
                            <Typography variant="body2" color="text.secondary" sx={{ fontFamily: 'monospace', fontSize: '0.875rem' }}>
                                OpenDental Procedures → stg_procedure → int_patient_procedure → mart_revenue → FastAPI /revenue-opportunities → Revenue Opportunity Dashboard
                            </Typography>
                        </Paper>
                    </CardContent>
                </Card>
            </Box>

            {/* 5. Production Evidence (§2) */}
            <Box id="evidence" sx={{ px: { xs: 2, sm: 3, md: 6, lg: 8 }, pb: 6, scrollMarginTop: 56 }}>
                <Typography variant="h5" fontWeight="bold" gutterBottom sx={{ textAlign: 'left', mb: 1 }}>
                    Production Evidence (Redacted)
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 3, textAlign: 'left' }}>
                    Operational proof that this is an operated system, not a UI demo.
                </Typography>
                <Grid container spacing={2} sx={{ mb: 3, justifyContent: 'flex-start' }}>
                    <Grid item xs={12} md={6}>
                        <Card variant="outlined" sx={{ height: '100%', textAlign: 'left' }}>
                            <CardContent sx={{ textAlign: 'left' }}>
                                <Typography variant="subtitle2" fontWeight="bold" gutterBottom>Pipeline & warehouse</Typography>
                                <Box component="ul" sx={{ m: 0, pl: 2, '& li': { typography: 'body2', color: 'text.secondary', mb: 0.5 } }}>
                                    <li>Daily scheduled ELT pipelines processing 450+ OpenDental tables</li>
                                    <li>150+ dbt models with full test coverage</li>
                                    <li>Three-phase ETL: MySQL → replication → PostgreSQL raw; full / incremental / incremental_chunked strategies</li>
                                    <li>Schema conversion, type mapping, connection pooling, retry, backfill-safe incremental loads</li>
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} md={6}>
                        <Card variant="outlined" sx={{ height: '100%', textAlign: 'left' }}>
                            <CardContent sx={{ textAlign: 'left' }}>
                                <Typography variant="subtitle2" fontWeight="bold" gutterBottom>Deployment & operations</Typography>
                                <Box component="ul" sx={{ m: 0, pl: 2, '& li': { typography: 'body2', color: 'text.secondary', mb: 0.5 } }}>
                                    <li>CI/CD pipelines gating deployments</li>
                                    <li>Dockerized services</li>
                                    <li>FastAPI serving versioned analytics endpoints</li>
                                    <li>Deployed on AWS (S3, EC2, RDS, CloudFront)</li>
                                    <li>Cost optimization: EC2 scheduling (Lambda + EventBridge), region consolidation—73% cost reduction</li>
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>
                </Grid>
                <Typography variant="subtitle2" fontWeight="bold" gutterBottom sx={{ textAlign: 'left' }}>Artifacts</Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2, justifyContent: 'flex-start' }}>
                    <Link href="https://dbtdentalclinic.com/dbt-docs/" target="_blank" rel="noopener noreferrer" sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5 }}><Description sx={{ fontSize: 18 }} /> dbt docs lineage</Link>
                    <Link href="https://api.dbtdentalclinic.com/docs" target="_blank" rel="noopener noreferrer" sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5 }}><Code sx={{ fontSize: 18 }} /> OpenAPI documentation</Link>
                    <Link href="https://github.com/BenjaminRains/dbt_dental_clinic" target="_blank" rel="noopener noreferrer" sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5 }}><GitHub sx={{ fontSize: 18 }} /> Repository</Link>
                </Box>
            </Box>

            {/* 6. dbt Lineage */}
            <Box id="lineage" sx={{ px: { xs: 2, sm: 3, md: 6, lg: 8 }, pb: 6, scrollMarginTop: 56 }}>
                <Card elevation={2}>
                    <Typography variant="h6" fontWeight="bold" sx={{ p: 2, pb: 1, bgcolor: 'background.paper', borderBottom: 1, borderColor: 'divider' }}>
                        dbt Lineage Graph (dim_patient DAG)
                    </Typography>
                    <Box sx={{ width: '100%', maxHeight: 400, display: 'flex', alignItems: 'center', justifyContent: 'center', p: 2, overflow: 'hidden', bgcolor: 'grey.100' }}>
                        <img src="/dim_patient_DAG.png" alt="dbt Lineage - dim_patient DAG" onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }} style={{ width: '100%', height: 'auto', maxHeight: 400, objectFit: 'contain' }} />
                    </Box>
                    <Box sx={{ p: 2, bgcolor: 'background.paper', display: 'flex', flexDirection: 'column', alignItems: 'flex-start', gap: 2 }}>
                        <Typography variant="body2" color="text.secondary" sx={{ textAlign: 'left' }}>
                            Interactive dbt lineage showing how 150+ models connect raw OpenDental sources to business-ready marts and KPIs.
                        </Typography>
                        <Button variant="contained" component={Link} href="https://dbtdentalclinic.com/dbt-docs/" target="_blank" rel="noopener noreferrer" sx={{ textTransform: 'none', bgcolor: '#5e7086', '&:hover': { bgcolor: '#6b7d94' } }}>View dbt Lineage Graph</Button>
                    </Box>
                </Card>
            </Box>

            {/* 7. Synthetic Dashboards (§3) */}
            <Box id="dashboards" sx={{ px: { xs: 2, sm: 3, md: 6, lg: 8 }, pb: 6, scrollMarginTop: 56 }}>
                <Typography variant="h5" fontWeight="bold" gutterBottom sx={{ textAlign: 'left', mb: 1 }}>
                    Dashboard Examples (Synthetic Data)
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 3, textAlign: 'left' }}>
                    Synthetic data. Layout and metrics match production system.
                </Typography>
                <Grid container spacing={3}>
                    {[
                        { title: 'Revenue & Collections', img: '/rev_trend.png', alt: 'Revenue Trend Dashboard' },
                        { title: 'AR Aging', img: '/ar_aging.png', alt: 'AR Aging Dashboard' },
                        { title: 'Revenue Opportunity (Provider Performance)', img: '/revenue_opp.png', alt: 'Revenue Opportunity Dashboard' },
                    ].map(({ title, img, alt }) => (
                        <Grid item xs={12} key={title}>
                            <Box sx={{ borderRadius: 2, overflow: 'hidden', bgcolor: 'grey.100', boxShadow: 3 }}>
                                <Typography variant="h6" fontWeight="bold" sx={{ p: 2, pb: 1, bgcolor: 'background.paper', borderBottom: 1, borderColor: 'divider' }}>{title}</Typography>
                                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', px: 2, py: 0.5, bgcolor: 'grey.50' }}>Synthetic data. Layout and metrics match production system.</Typography>
                                <Box sx={{ width: '100%', maxHeight: 400, display: 'flex', alignItems: 'center', justifyContent: 'center', p: 2, overflow: 'hidden' }}>
                                    <img src={img} alt={alt} onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }} style={{ width: '100%', height: 'auto', maxHeight: 400, objectFit: 'contain' }} />
                                </Box>
                            </Box>
                        </Grid>
                    ))}
                </Grid>
            </Box>

            {/* 8. GitHub / Code Access (§7) */}
            <Box id="code" sx={{ px: { xs: 2, sm: 3, md: 6, lg: 8 }, pb: 6, scrollMarginTop: 56 }}>
                <Typography variant="h5" fontWeight="bold" gutterBottom sx={{ textAlign: 'left', mb: 2 }}>
                    Code & Repositories
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 3, textAlign: 'left' }}>
                    This repository powers the production dental analytics platform demonstrated in this portfolio.
                </Typography>
                <Grid container spacing={2} sx={{ justifyContent: 'flex-start' }}>
                    {[
                        { name: 'ETL Replicator', href: 'https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/etl_pipeline', icon: <Storage />, desc: 'Metadata-driven ELT, incremental loads, 450+ tables.' },
                        { name: 'dbt Project', href: 'https://dbtdentalclinic.com/dbt-docs/', icon: <Analytics />, desc: '150+ models, staging → intermediate → marts, tests, lineage.' },
                        { name: 'FastAPI Service', href: 'https://api.dbtdentalclinic.com/docs', icon: <Code />, desc: 'REST API for KPIs, Pydantic models, versioned endpoints.' },
                        { name: 'Frontend Application', href: 'https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/frontend', icon: <Dashboard />, desc: 'React + TypeScript dashboards, Material-UI, Zustand.' },
                    ].map(({ name, href, icon, desc }) => (
                        <Grid item xs={12} sm={6} key={name}>
                            <Card variant="outlined" sx={{ height: '100%', textAlign: 'left' }}>
                                <CardContent sx={{ textAlign: 'left' }}>
                                    <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-start', gap: 1, mb: 1 }}>
                                        {React.cloneElement(icon, { sx: { color: '#5e7086', fontSize: 28 } })}
                                        <Typography variant="h6" fontWeight="bold" component={Link} href={href} target="_blank" rel="noopener noreferrer" sx={{ color: 'inherit', textDecoration: 'none', '&:hover': { textDecoration: 'underline', color: '#5e7086' } }}>{name}</Typography>
                                    </Box>
                                    <Typography variant="body2" color="text.secondary" sx={{ textAlign: 'left' }}>{desc}</Typography>
                                </CardContent>
                            </Card>
                        </Grid>
                    ))}
                </Grid>
            </Box>

            <Divider sx={{ my: 6 }} />

            {/* 9. Project cards: Problem → System → Outcome (§5) */}
            <Box sx={{ px: { xs: 2, sm: 3, md: 6, lg: 8 }, pb: 6 }}>
                <Typography variant="h4" component="h2" fontWeight="bold" gutterBottom sx={{ textAlign: 'left', mb: 2 }}>
                    dbt Dental Clinic Analytics Platform
                </Typography>
                <Paper variant="outlined" sx={{ p: 3, mb: 4, bgcolor: 'grey.50', textAlign: 'left' }}>
                    <Typography variant="subtitle2" fontWeight="bold" gutterBottom>Problem</Typography>
                    <Typography variant="body2" color="text.secondary" paragraph sx={{ m: 0 }}>
                        Clinic had no centralized analytics, manual reporting, and unreliable KPIs.
                    </Typography>
                    <Typography variant="subtitle2" fontWeight="bold" gutterBottom sx={{ mt: 2 }}>System built</Typography>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mb: 2 }}>
                        {['Metadata-driven ELT', 'Incremental replication', 'dbt warehouse', 'API-backed analytics layer', 'React frontend'].map((label) => (
                            <Chip key={label} label={label} size="small" />
                        ))}
                    </Box>
                    <Typography variant="subtitle2" fontWeight="bold" gutterBottom>Outcome</Typography>
                    <Box component="ul" sx={{ m: 0, pl: 2, '& li': { typography: 'body2', color: 'text.secondary' } }}>
                        <li>Automated daily KPI delivery</li>
                        <li>Identified revenue leakage</li>
                        <li>Enabled leadership prioritization of collections</li>
                        <li>Reduced manual reporting effort</li>
                    </Box>
                </Paper>
                <Button variant="contained" size="large" component={RouterLink} to="/dashboard" sx={{ mr: 2 }}>Explore Analytics Demo</Button>
                <Button variant="outlined" size="large" component={Link} href="https://github.com/BenjaminRains/dbt_dental_clinic" target="_blank" rel="noopener noreferrer"><GitHub sx={{ mr: 1 }} /> View on GitHub</Button>
            </Box>

            {/* 10. Documentation & Resources */}
            <Box id="documentation" sx={{ px: { xs: 2, sm: 3, md: 6, lg: 8 }, pb: 6, scrollMarginTop: 56 }}>
                <Typography variant="h5" fontWeight="bold" gutterBottom sx={{ textAlign: 'left', mb: 3 }}>
                    Documentation & Resources
                </Typography>
                <Grid container spacing={2} sx={{ justifyContent: 'flex-start' }}>
                    {[
                        { title: 'Architecture Documentation', desc: 'System design, data flow diagrams, platform overview.', href: 'https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/docs' },
                        { title: 'dbt Documentation', desc: 'Auto-generated dbt docs for models, tests, lineage.', href: 'https://dbtdentalclinic.com/dbt-docs/' },
                        { title: 'API Documentation', desc: 'FastAPI endpoint reference and usage.', href: 'https://api.dbtdentalclinic.com/docs' },
                        { title: 'Setup Guide', desc: 'Local development and environment setup.', href: 'https://github.com/BenjaminRains/dbt_dental_clinic/blob/main/ENVIRONMENT_SETUP.md' },
                        { title: 'Deployment Guide', desc: 'AWS deployment, configuration, CI/CD.', href: 'https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/docs' },
                        { title: 'Data Quality Reports', desc: 'dbt test coverage, validation metrics.', href: 'https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/docs/data_quality' },
                    ].map(({ title, desc, href }) => (
                        <Grid item xs={12} sm={6} md={4} key={title}>
                            <Card elevation={1} sx={{ height: '100%', textAlign: 'left' }}>
                                <CardContent sx={{ textAlign: 'left' }}>
                                    <Typography variant="body2" fontWeight="bold" gutterBottom>{title}</Typography>
                                    <Typography variant="body2" color="text.secondary" paragraph sx={{ textAlign: 'left' }}>{desc}</Typography>
                                    <Link href={href} target="_blank" rel="noopener noreferrer">View →</Link>
                                </CardContent>
                            </Card>
                        </Grid>
                    ))}
                </Grid>
            </Box>

            <Divider sx={{ my: 6 }} />

            {/* 11. Consult Audio Pipeline */}
            <Box sx={{ px: { xs: 2, sm: 3, md: 6, lg: 8 }, pb: 6 }}>
                <Typography variant="h4" component="h2" fontWeight="bold" gutterBottom sx={{ textAlign: 'left', mb: 1 }}>
                    Consult Audio Whisper Pipeline
                </Typography>
                <Typography variant="body1" color="text.secondary" paragraph sx={{ mb: 4, textAlign: 'left' }}>
                    An automated pipeline that turns dental consultation recordings into structured, clinician-friendly documentation and analytics. Whisper transcription, cleaning, LLM analysis, and multi-format outputs for treatment acceptance tracking and provider coaching.
                </Typography>
                <Typography variant="h6" fontWeight="bold" gutterBottom sx={{ textAlign: 'left', mb: 2 }}>Five-stage pipeline</Typography>
                <Grid container spacing={2} sx={{ mb: 4, justifyContent: 'flex-start' }}>
                    {[
                        { step: '1. Audio Transcription', icon: <Mic />, text: 'OpenAI Whisper: timestamped text, .txt/.tsv/.srt/.vtt.', chips: ['Whisper AI', 'PyTorch'] },
                        { step: '2. Transcript Cleaning', icon: <CleaningServices />, text: 'Dental-specific rules, terminology correction.', chips: ['Pattern Matching', 'JSON Rules'] },
                        { step: '3. LLM Analysis', icon: <SmartToy />, text: 'Anthropic Claude: summaries, treatment discussions, QA.', chips: ['Claude 3.5', 'API Integration'] },
                        { step: '4. HTML Conversion', icon: <Code />, text: 'Markdown → responsive HTML reports.', chips: ['Markdown2'] },
                        { step: '5. PDF Conversion', icon: <PictureAsPdf />, text: 'ReportLab: print-optimized PDF for records.', chips: ['ReportLab'] },
                    ].map(({ step, icon, text, chips }) => (
                        <Grid item xs={12} sm={6} md={4} key={step}>
                            <Card elevation={2} sx={{ height: '100%', textAlign: 'left' }}>
                                <CardContent sx={{ textAlign: 'left' }}>
                                    <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-start', mb: 1 }}>{React.cloneElement(icon, { sx: { color: '#5e7086', mr: 1.5 } })}<Typography variant="h6" fontWeight="bold">{step}</Typography></Box>
                                    <Typography variant="body2" color="text.secondary" paragraph sx={{ textAlign: 'left' }}>{text}</Typography>
                                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, justifyContent: 'flex-start' }}>{chips.map((c) => <Chip key={c} label={c} size="small" />)}</Box>
                                </CardContent>
                            </Card>
                        </Grid>
                    ))}
                </Grid>
                <Button variant="outlined" size="large" component={Link} href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/consult_audio_pipe" target="_blank" rel="noopener noreferrer"><GitHub sx={{ mr: 1 }} /> View on GitHub</Button>
            </Box>

            <Divider sx={{ my: 6 }} />

            {/* 12. Supporting Tools */}
            <Box sx={{ px: { xs: 2, sm: 3, md: 6, lg: 8 }, pb: 6 }}>
                <Typography variant="h4" component="h2" fontWeight="bold" gutterBottom sx={{ textAlign: 'left', mb: 3 }}>
                    Supporting Engineering Tools
                </Typography>
                <Grid container spacing={3} sx={{ justifyContent: 'flex-start' }}>
                    <Grid item xs={12} md={6}>
                        <Card elevation={2} sx={{ height: '100%', cursor: 'pointer', textAlign: 'left', '&:hover': { transform: 'translateY(-4px)', boxShadow: 6 } }} component={RouterLink} to="/environment-manager">
                            <CardContent sx={{ textAlign: 'left' }}>
                                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-start', mb: 2 }}><Settings sx={{ color: '#5e7086', mr: 1 }} /><Typography variant="h6" fontWeight="bold">Data Engineering Environment Manager</Typography></Box>
                                <Typography variant="body2" color="text.secondary" paragraph sx={{ textAlign: 'left' }}>PowerShell automation for dbt, ETL, API, and frontend environments. Environment isolation, auto-detection, interactive configuration.</Typography>
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, justifyContent: 'flex-start' }}>{['PowerShell', 'Automation', 'DevOps'].map((c) => <Chip key={c} label={c} size="small" />)}</Box>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} md={6}>
                        <Card elevation={2} sx={{ height: '100%', cursor: 'pointer', textAlign: 'left', '&:hover': { transform: 'translateY(-4px)', boxShadow: 6 } }} component={RouterLink} to="/schema-discovery">
                            <CardContent sx={{ textAlign: 'left' }}>
                                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-start', mb: 2 }}><Settings sx={{ color: '#5e7086', mr: 1 }} /><Typography variant="h6" fontWeight="bold">Schema Discovery & Replication</Typography></Box>
                                <Typography variant="body2" color="text.secondary" paragraph sx={{ textAlign: 'left' }}>analyze_opendental_schema.py: SQLAlchemy introspection, 450+ tables, tables.yml config, SCD monitoring, schema drift detection.</Typography>
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, justifyContent: 'flex-start' }}>{['SQLAlchemy', 'Schema Introspection', 'Incremental Loading'].map((c) => <Chip key={c} label={c} size="small" />)}</Box>
                            </CardContent>
                        </Card>
                    </Grid>
                </Grid>
            </Box>

            <Divider sx={{ my: 6 }} />

            {/* 13. Skills & Tech Stack */}
            <Box sx={{ px: { xs: 2, sm: 3, md: 6, lg: 8 }, pb: 6 }}>
                <Typography variant="h4" component="h2" fontWeight="bold" gutterBottom sx={{ textAlign: 'left', mb: 3 }}>
                    Skills & Tech Stack
                </Typography>
                <Grid container spacing={4} sx={{ justifyContent: 'flex-start' }}>
                    {[
                        { title: 'Data Engineering', chips: ['dbt', 'SQL', 'PostgreSQL', 'MySQL', 'ELT/ETL', 'Dimensional Modeling'] },
                        { title: 'Core Technologies', chips: ['Python', 'TypeScript', 'SQLAlchemy', 'FastAPI', 'React'] },
                        { title: 'Analytics & Frontend', chips: ['Material-UI', 'Recharts', 'Zustand', 'Data Visualization'] },
                        { title: 'Tools & Cloud', chips: ['AWS', 'Docker', 'Git', 'CI/CD', 'Airflow'] },
                    ].map(({ title, chips }) => (
                        <Grid item xs={12} md={4} key={title}>
                            <Typography variant="h6" fontWeight="bold" gutterBottom sx={{ textAlign: 'left' }}>{title}</Typography>
                            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, justifyContent: 'flex-start' }}>{chips.map((c) => <Chip key={c} label={c} />)}</Box>
                        </Grid>
                    ))}
                </Grid>
            </Box>

            <Divider sx={{ my: 6 }} />

            {/* 14. Contact */}
            <Box id="contact" sx={{ px: { xs: 2, sm: 3, md: 6, lg: 8 }, pb: 6, scrollMarginTop: 56 }}>
                <Typography variant="h4" component="h2" fontWeight="bold" gutterBottom sx={{ textAlign: 'left', mb: 2 }}>Get In Touch</Typography>
                <Typography variant="body1" color="text.secondary" paragraph sx={{ mb: 4, textAlign: 'left' }}>Interested in working together or learning more about the platform? Reach out below.</Typography>
                <Box sx={{ display: 'flex', gap: 3, flexWrap: 'wrap', justifyContent: 'flex-start' }}>
                    <Button variant="contained" size="large" component={Link} href="https://github.com/BenjaminRains" target="_blank" rel="noopener noreferrer" startIcon={<GitHub />}>GitHub</Button>
                    <Button variant="contained" size="large" component={Link} href="https://www.linkedin.com/in/benjamin-rains-154139270/" target="_blank" rel="noopener noreferrer" startIcon={<LinkedIn />}>LinkedIn</Button>
                    <Button variant="contained" size="large" component={Link} href="mailto:rains.bp@gmail.com" startIcon={<Email />}>Email</Button>
                    <Button variant="contained" size="large" component={Link} href="/assets/resume.pdf" target="_blank" rel="noopener noreferrer" startIcon={<Description />}>Resume</Button>
                </Box>
            </Box>

            {/* 15. Footer */}
            <Box sx={{ bgcolor: 'grey.900', color: 'grey.300', py: 4, textAlign: 'center' }}>
                <Container maxWidth="lg">
                    <Typography variant="body2" sx={{ mb: 1 }}>
                        © {new Date().getFullYear()} Benjamin Rains. Built with React, TypeScript, and Material-UI.
                    </Typography>
                    <Typography variant="body2">
                        Agent-readable profile:{' '}
                        <Link href="/agent-profile" sx={{ color: 'primary.light' }}>
                            /agent-profile
                        </Link>
                    </Typography>
                </Container>
            </Box>
        </Box>
    );
};

export default PortfolioV3;
