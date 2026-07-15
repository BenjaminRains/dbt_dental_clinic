/**
 * Portfolio v4 – Hiring-focused case study layout.
 * Hero → Snapshot → Narrative → Architecture → Outcomes → Production Evidence →
 * Lineage → Artifacts → Dashboards → Additional Projects → Contact
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
    Paper,
    Toolbar,
} from '@mui/material';
import { MermaidDiagram } from '@mdc/ui-common';
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
    Code,
    Schedule,
    Assessment,
    Replay,
    CheckCircle,
    Cloud,
    Dns,
    OpenInNew,
    ArrowForward,
} from '@mui/icons-material';

const GITHUB = 'https://github.com/BenjaminRains/dbt_dental_clinic';
const GITHUB_MAIN = `${GITHUB}/blob/main`;

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
    { label: 'Platform', href: '#platform' },
    { label: 'Architecture', href: '#architecture' },
    { label: 'Outcomes', href: '#outcomes' },
    { label: 'Evidence', href: '#evidence' },
    { label: 'Artifacts', href: '#artifacts' },
    { label: 'Dashboards', href: '#dashboards' },
    { label: 'Contact', href: '#contact' },
];

const SNAPSHOT_METRICS = [
    { value: '450+', label: 'Source Tables' },
    { value: '150+', label: 'dbt Models' },
    { value: 'AWS', label: 'Production Deployment' },
    { value: 'Daily', label: 'Automated Pipelines' },
    { value: 'Demo + Clinic', label: 'Environment Separation' },
    { value: 'Full Stack', label: 'ELT → API → React' },
];

type CapabilityLink = {
    title: string;
    desc: string;
    icon: React.ReactElement;
    href: string;
    linkLabel: string;
    external?: boolean;
};

const PRODUCTION_CAPABILITIES: CapabilityLink[] = [
    {
        title: 'Automated scheduling',
        desc: 'Daily ELT via Airflow DAGs; EC2 and API scheduling for cost control.',
        icon: <Schedule />,
        href: `${GITHUB}/tree/main/airflow`,
        linkLabel: 'Airflow DAGs README',
    },
    {
        title: 'Monitoring and logging',
        desc: 'Pipeline and API logging with CloudWatch metrics and request tracing.',
        icon: <Assessment />,
        href: `${GITHUB_MAIN}/api/README.md`,
        linkLabel: 'API operations docs',
    },
    {
        title: 'Schema drift handling',
        desc: 'Metadata-driven schema discovery and tables.yml generation when sources change.',
        icon: <Storage />,
        href: `${GITHUB_MAIN}/airflow/dags/schema_analysis_dag.py`,
        linkLabel: 'Schema analysis DAG',
    },
    {
        title: 'Backfill support',
        desc: 'Incremental, chunked, and full-refresh strategies with safe re-runs.',
        icon: <Replay />,
        href: `${GITHUB}/tree/main/etl_pipeline`,
        linkLabel: 'ETL pipeline README',
    },
    {
        title: 'Tests gate deployments',
        desc: 'dbt tests and CI validation run before data reaches marts.',
        icon: <CheckCircle />,
        href: 'https://dbtdentalclinic.com/dbt-docs/',
        linkLabel: 'dbt docs & tests',
    },
    {
        title: 'Version-controlled models',
        desc: 'dbt models, ELT config, and API code in Git with reviewable history.',
        icon: <Code />,
        href: `${GITHUB}/tree/main/dbt_dental_models`,
        linkLabel: 'dbt project',
    },
    {
        title: 'CI/CD pipelines',
        desc: 'Automated validation on push; build, test, and deploy workflows.',
        icon: <Build />,
        href: `${GITHUB_MAIN}/.github/workflows/mdc_cli.yml`,
        linkLabel: 'GitHub Actions workflow',
    },
    {
        title: 'Containerized services',
        desc: 'Docker Compose for local parity; containers from dev through production.',
        icon: <Dns />,
        href: `${GITHUB_MAIN}/docker-compose.yml`,
        linkLabel: 'docker-compose.yml',
    },
    {
        title: 'Cloud deployment',
        desc: 'AWS (S3, CloudFront, EC2, RDS) for frontend, API, and warehouse.',
        icon: <Cloud />,
        href: `${GITHUB}/tree/main/tools/mdc_cli`,
        linkLabel: 'Deploy CLI & runbooks',
    },
];

const ARTIFACT_LINKS = [
    { title: 'GitHub Repository', desc: 'Full monorepo: ETL, dbt, API, frontend.', href: GITHUB, icon: <GitHub /> },
    { title: 'dbt Documentation', desc: 'Models, tests, and interactive lineage.', href: 'https://dbtdentalclinic.com/dbt-docs/', icon: <Analytics /> },
    { title: 'API Documentation', desc: 'FastAPI OpenAPI reference.', href: 'https://api.dbtdentalclinic.com/docs', icon: <Code /> },
    { title: 'Architecture Docs', desc: 'System design and environment setup.', href: `${GITHUB}/tree/main/docs`, icon: <Description /> },
    { title: 'ETL Pipeline', desc: 'Replication, incremental loads, 450+ tables.', href: `${GITHUB}/tree/main/etl_pipeline`, icon: <Storage /> },
    { title: 'Frontend App', desc: 'React dashboards and portfolio site.', href: `${GITHUB}/tree/main/frontend`, icon: <Dashboard /> },
];

const sectionSx = { px: { xs: 2, sm: 3, md: 6, lg: 8 }, pb: 6, scrollMarginTop: 72 };

function CapabilityCard({ title, desc, icon, href, linkLabel, external = true }: CapabilityLink) {
    const cardProps = external
        ? { component: 'a' as const, href, target: '_blank', rel: 'noopener noreferrer' }
        : { component: RouterLink, to: href };

    return (
        <Card
            {...cardProps}
            variant="outlined"
            sx={{
                height: '100%',
                textDecoration: 'none',
                color: 'inherit',
                display: 'block',
                transition: 'box-shadow 0.2s, transform 0.2s',
                '&:hover': {
                    boxShadow: 4,
                    transform: 'translateY(-2px)',
                    borderColor: '#5e7086',
                },
            }}
        >
            <CardContent sx={{ '&:last-child': { pb: 2 } }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                    {React.cloneElement(icon, { sx: { color: '#5e7086', fontSize: 20 } })}
                    <Typography variant="subtitle2" fontWeight="bold">{title}</Typography>
                </Box>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 1.5 }}>
                    {desc}
                </Typography>
                <Typography
                    variant="caption"
                    sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5, color: '#5e7086', fontWeight: 600 }}
                >
                    {linkLabel}
                    {external ? <OpenInNew sx={{ fontSize: 14 }} /> : <ArrowForward sx={{ fontSize: 14 }} />}
                </Typography>
            </CardContent>
        </Card>
    );
}

const PortfolioV4: React.FC = () => {
    return (
        <Box sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
            <Box sx={{ position: 'sticky', top: 0, zIndex: 1100, px: { xs: 2, sm: 3 }, pt: 2, pb: 0 }}>
                <Box sx={{ bgcolor: '#616161', color: '#fff', borderRadius: 2, boxShadow: '0 2px 8px rgba(0,0,0,0.15)' }}>
                    <Toolbar disableGutters sx={{ px: { xs: 2, sm: 3 }, py: 1.5 }}>
                        <Typography
                            component={RouterLink}
                            to="/"
                            variant="h6"
                            fontWeight="bold"
                            sx={{ color: '#fff', textDecoration: 'none', mr: 4 }}
                        >
                            Benjamin Rains
                        </Typography>
                        <Box sx={{ flex: 1, display: 'flex', justifyContent: 'flex-end', gap: { xs: 1, sm: 2 }, flexWrap: 'wrap' }}>
                            {NAV_LINKS.map(({ label, href }) => (
                                <Link key={href} href={href} sx={{ color: 'rgba(255,255,255,0.95)', textDecoration: 'none', fontSize: '0.8125rem', fontWeight: 600, letterSpacing: '0.04em', textTransform: 'uppercase', '&:hover': { color: '#fff' } }}>
                                    {label}
                                </Link>
                            ))}
                        </Box>
                    </Toolbar>
                </Box>
            </Box>

            {/* Hero */}
            <Box sx={{ px: { xs: 2, sm: 3 }, mt: 2 }}>
                <Box sx={{ bgcolor: '#90caf9', color: '#0d47a1', borderRadius: 2, boxShadow: '0 2px 8px rgba(0,0,0,0.15)', py: { xs: 5, md: 7 }, px: { xs: 2, sm: 3 } }}>
                    <Box sx={{ display: 'flex', flexDirection: { xs: 'column', md: 'row' }, alignItems: { md: 'flex-start' }, justifyContent: 'space-between', gap: 3 }}>
                        <Box sx={{ flex: 1, minWidth: 0 }}>
                            <Typography variant="h5" component="h1" fontWeight={700} sx={{ mb: 0.5, color: '#01579b' }}>
                                Benjamin Rains
                            </Typography>
                            <Typography variant="h5" component="h2" fontWeight={600} sx={{ mb: 2, color: '#01579b' }}>
                                Data Engineer &amp; Analytics Engineer
                            </Typography>
                            <Typography variant="body1" sx={{ mb: 3, maxWidth: 720, lineHeight: 1.75, color: '#0d47a1' }}>
                                Built a production healthcare analytics platform processing 450+ operational tables using Python, dbt, PostgreSQL, FastAPI, React, and AWS.
                            </Typography>
                            <Typography variant="body2" sx={{ color: '#0d47a1', opacity: 0.9 }}>
                                Remote US ·{' '}
                                <Link component={RouterLink} to="/agent-profile" sx={{ color: '#01579b', fontWeight: 600 }}>
                                    Agent-readable profile
                                </Link>
                            </Typography>
                        </Box>
                        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5, minWidth: { md: 220 } }}>
                            <Button variant="contained" size="large" component={RouterLink} to="/dashboard" sx={{ minWidth: 200, py: 1.5, textTransform: 'none', fontWeight: 600, bgcolor: '#1565c0', '&:hover': { bgcolor: '#0d47a1' } }}>
                                <Dashboard sx={{ mr: 1.5 }} /> View Platform Demo
                            </Button>
                            <Button variant="outlined" size="large" component={Link} href={GITHUB} target="_blank" rel="noopener noreferrer" sx={{ minWidth: 200, py: 1.5, textTransform: 'none', fontWeight: 600, borderColor: '#1565c0', color: '#0d47a1' }}>
                                <GitHub sx={{ mr: 1.5 }} /> GitHub
                            </Button>
                            <Button variant="outlined" size="large" component={Link} href="/assets/resume.pdf" target="_blank" rel="noopener noreferrer" sx={{ minWidth: 200, py: 1.5, textTransform: 'none', fontWeight: 600, borderColor: '#1565c0', color: '#0d47a1' }}>
                                <Description sx={{ mr: 1.5 }} /> Resume
                            </Button>
                        </Box>
                    </Box>
                </Box>
            </Box>

            {/* Engineering Snapshot */}
            <Box id="platform" sx={{ ...sectionSx, pt: 4 }}>
                <Grid container spacing={2}>
                    {SNAPSHOT_METRICS.map(({ value, label }) => (
                        <Grid item xs={6} sm={4} md={2} key={label}>
                            <Paper variant="outlined" sx={{ p: 2, textAlign: 'center', height: '100%' }}>
                                <Typography variant="h6" fontWeight="bold" color="#5e7086">{value}</Typography>
                                <Typography variant="caption" color="text.secondary">{label}</Typography>
                            </Paper>
                        </Grid>
                    ))}
                </Grid>
                <Typography variant="body1" color="text.secondary" sx={{ mt: 4, maxWidth: 800, lineHeight: 1.75 }}>
                    I designed and implemented an end-to-end healthcare analytics platform for a multi-clinic dental organization. The platform replicates operational data, transforms it with dbt, exposes analytics through APIs, and delivers business intelligence through a React application—with strict separation between public demo and clinic environments.
                </Typography>
            </Box>

            {/* Architecture */}
            <Box id="architecture" sx={sectionSx}>
                <Typography variant="h4" fontWeight="bold" gutterBottom>Platform Architecture</Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 3, maxWidth: 720 }}>
                    End-to-end flow from OpenDental through ELT, a tested dbt warehouse, and API-driven services to analytics applications.
                </Typography>
                <Card elevation={2}>
                    <CardContent>
                        <Box sx={{ bgcolor: 'grey.50', borderRadius: 2, p: { xs: 2, md: 4 }, overflowX: 'auto', minHeight: 480 }}>
                            <MermaidDiagram id="portfolio-v4-architecture" chart={ARCHITECTURE_CHART} />
                        </Box>
                        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mt: 2 }}>
                            {['Incremental Loads', 'Schema Drift Handling', 'Idempotent Pipelines', 'Data Quality Tests', 'API Contracts', 'Environment Separation'].map((label) => (
                                <Chip key={label} label={label} size="small" variant="outlined" />
                            ))}
                        </Box>
                        <Paper variant="outlined" sx={{ p: 2, mt: 2, bgcolor: 'grey.50' }}>
                            <Typography variant="subtitle2" fontWeight="bold" gutterBottom>Example data path</Typography>
                            <Typography variant="body2" color="text.secondary" sx={{ fontFamily: 'monospace', fontSize: '0.875rem' }}>
                                OpenDental Procedures → stg_procedure → int_patient_procedure → mart_revenue → FastAPI /revenue-opportunities → Revenue Dashboard
                            </Typography>
                        </Paper>
                    </CardContent>
                </Card>
            </Box>

            {/* Business Outcomes */}
            <Box id="outcomes" sx={sectionSx}>
                <Typography variant="h4" fontWeight="bold" gutterBottom>Business Outcomes</Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 3, maxWidth: 720 }}>
                    Technology in service of operational impact—not dashboards for their own sake.
                </Typography>
                <Grid container spacing={2}>
                    {[
                        { title: 'Automated daily KPI delivery', desc: 'Scheduled pipelines replace manual report pulls for leadership.' },
                        { title: 'Collections prioritization', desc: 'AR aging and opportunity metrics surfaced for front-office action.' },
                        { title: 'Revenue leakage visibility', desc: 'Treatment and production gaps identified at provider and clinic level.' },
                        { title: 'Reduced manual reporting', desc: 'Self-serve dashboards backed by tested warehouse models.' },
                        { title: 'Vendor metric recreation', desc: 'Rebuilt key OpenDental reports in SQL with auditable lineage.' },
                        { title: '73% AWS cost reduction', desc: 'EC2 scheduling and region consolidation without sacrificing uptime.' },
                    ].map(({ title, desc }) => (
                        <Grid item xs={12} sm={6} md={4} key={title}>
                            <Paper variant="outlined" sx={{ p: 2, height: '100%' }}>
                                <Typography variant="subtitle2" fontWeight="bold" gutterBottom>{title}</Typography>
                                <Typography variant="body2" color="text.secondary">{desc}</Typography>
                            </Paper>
                        </Grid>
                    ))}
                </Grid>
            </Box>

            {/* Production Evidence */}
            <Box id="evidence" sx={sectionSx}>
                <Typography variant="h4" fontWeight="bold" gutterBottom>Production Evidence</Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 1, maxWidth: 720 }}>
                    Operational proof that this is a deployed system—not a UI mockup. Each capability links to source or live artifacts.
                </Typography>
                <Paper variant="outlined" sx={{ p: 2, mb: 3, bgcolor: 'grey.50' }}>
                    <Typography variant="body2" color="text.primary">
                        Pipelines are idempotent and support reprocessing, backfills, and partial re-runs without full reloads.
                    </Typography>
                </Paper>
                <Grid container spacing={2} sx={{ mb: 4 }}>
                    <Grid item xs={12} md={6}>
                        <Paper variant="outlined" sx={{ p: 2, height: '100%' }}>
                            <Typography variant="subtitle2" fontWeight="bold" gutterBottom>Pipeline &amp; warehouse</Typography>
                            <Box component="ul" sx={{ m: 0, pl: 2, '& li': { typography: 'body2', color: 'text.secondary', mb: 0.5 } }}>
                                <li>Daily scheduled ELT processing 450+ OpenDental tables</li>
                                <li>150+ dbt models with comprehensive test coverage</li>
                                <li>MySQL replication → PostgreSQL raw with incremental strategies</li>
                                <li>Schema conversion, retry logic, and backfill-safe loads</li>
                            </Box>
                        </Paper>
                    </Grid>
                    <Grid item xs={12} md={6}>
                        <Paper variant="outlined" sx={{ p: 2, height: '100%' }}>
                            <Typography variant="subtitle2" fontWeight="bold" gutterBottom>Deployment &amp; operations</Typography>
                            <Box component="ul" sx={{ m: 0, pl: 2, '& li': { typography: 'body2', color: 'text.secondary', mb: 0.5 } }}>
                                <li>CI/CD gating deployments before production</li>
                                <li>Dockerized API and local dev parity</li>
                                <li>FastAPI serving versioned analytics endpoints</li>
                                <li>AWS: S3, CloudFront, EC2, RDS with demo/clinic separation</li>
                            </Box>
                        </Paper>
                    </Grid>
                </Grid>
                <Typography variant="h6" fontWeight="bold" gutterBottom>Production capabilities</Typography>
                <Grid container spacing={2}>
                    {PRODUCTION_CAPABILITIES.map((cap) => (
                        <Grid item xs={12} sm={6} md={4} key={cap.title}>
                            <CapabilityCard {...cap} />
                        </Grid>
                    ))}
                </Grid>
            </Box>

            {/* dbt Lineage */}
            <Box id="lineage" sx={sectionSx}>
                <Card elevation={2}>
                    <Typography variant="h6" fontWeight="bold" sx={{ p: 2, pb: 1, borderBottom: 1, borderColor: 'divider' }}>
                        dbt Lineage Graph (dim_patient DAG)
                    </Typography>
                    <Box sx={{ maxHeight: 400, p: 2, bgcolor: 'grey.100', display: 'flex', justifyContent: 'center' }}>
                        <img src="/dim_patient_DAG.png" alt="dbt lineage for dim_patient" onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }} style={{ width: '100%', maxHeight: 400, objectFit: 'contain' }} />
                    </Box>
                    <Box sx={{ p: 2, display: 'flex', flexDirection: { xs: 'column', sm: 'row' }, alignItems: { sm: 'center' }, justifyContent: 'space-between', gap: 2 }}>
                        <Typography variant="body2" color="text.secondary">
                            150+ models connecting raw OpenDental sources to business-ready marts and KPIs.
                        </Typography>
                        <Button variant="contained" component={Link} href="https://dbtdentalclinic.com/dbt-docs/" target="_blank" rel="noopener noreferrer" sx={{ textTransform: 'none', bgcolor: '#5e7086', flexShrink: 0, '&:hover': { bgcolor: '#6b7d94' } }}>
                            View interactive lineage
                        </Button>
                    </Box>
                </Card>
            </Box>

            {/* Engineering Artifacts */}
            <Box id="artifacts" sx={sectionSx}>
                <Typography variant="h4" fontWeight="bold" gutterBottom>Engineering Artifacts</Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
                    Validate the claims above in a few minutes—source code, generated docs, and live API reference.
                </Typography>
                <Grid container spacing={2}>
                    {ARTIFACT_LINKS.map(({ title, desc, href, icon }) => (
                        <Grid item xs={12} sm={6} md={4} key={title}>
                            <Card
                                component="a"
                                href={href}
                                target="_blank"
                                rel="noopener noreferrer"
                                variant="outlined"
                                sx={{
                                    height: '100%',
                                    textDecoration: 'none',
                                    color: 'inherit',
                                    display: 'block',
                                    '&:hover': { boxShadow: 3, borderColor: '#5e7086' },
                                }}
                            >
                                <CardContent>
                                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                                        {React.cloneElement(icon, { sx: { color: '#5e7086' } })}
                                        <Typography variant="subtitle1" fontWeight="bold">{title}</Typography>
                                    </Box>
                                    <Typography variant="body2" color="text.secondary">{desc}</Typography>
                                </CardContent>
                            </Card>
                        </Grid>
                    ))}
                </Grid>
            </Box>

            {/* Dashboard Examples */}
            <Box id="dashboards" sx={sectionSx}>
                <Typography variant="h4" fontWeight="bold" gutterBottom>Dashboard Examples</Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
                    Synthetic data—layout and metrics match production. The platform engineering is the primary story; dashboards are the output layer.
                </Typography>
                <Grid container spacing={3}>
                    {[
                        { title: 'Revenue & Collections', img: '/rev_trend.png' },
                        { title: 'AR Aging', img: '/ar_aging.png' },
                        { title: 'Revenue Opportunity', img: '/revenue_opp.png' },
                    ].map(({ title, img }) => (
                        <Grid item xs={12} md={4} key={title}>
                            <Paper variant="outlined" sx={{ overflow: 'hidden', height: '100%' }}>
                                <Typography variant="subtitle2" fontWeight="bold" sx={{ p: 1.5, borderBottom: 1, borderColor: 'divider' }}>{title}</Typography>
                                <Box sx={{ p: 1, bgcolor: 'grey.100' }}>
                                    <img src={img} alt={title} onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }} style={{ width: '100%', height: 'auto', display: 'block' }} />
                                </Box>
                            </Paper>
                        </Grid>
                    ))}
                </Grid>
                <Button variant="outlined" component={RouterLink} to="/dashboard" sx={{ mt: 3, textTransform: 'none' }}>
                    Explore live demo dashboards
                </Button>
            </Box>

            {/* Additional Projects (compressed) */}
            <Box sx={sectionSx}>
                <Typography variant="h5" fontWeight="bold" gutterBottom>Additional Projects</Typography>
                <Grid container spacing={2}>
                    <Grid item xs={12} md={4}>
                        <Card
                            component="a"
                            href={`${GITHUB}/tree/main/consult_audio_pipe`}
                            target="_blank"
                            rel="noopener noreferrer"
                            variant="outlined"
                            sx={{ height: '100%', textDecoration: 'none', color: 'inherit', display: 'block', '&:hover': { boxShadow: 3 } }}
                        >
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                                    <Mic sx={{ color: '#5e7086' }} />
                                    <Typography variant="subtitle2" fontWeight="bold">Consult Audio Pipeline</Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary">
                                    Whisper transcription → dental-specific cleaning → LLM summaries for treatment acceptance analytics.
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} md={4}>
                        <Card component={RouterLink} to="/environment-manager" variant="outlined" sx={{ height: '100%', textDecoration: 'none', color: 'inherit', display: 'block', '&:hover': { boxShadow: 3 } }}>
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                                    <Settings sx={{ color: '#5e7086' }} />
                                    <Typography variant="subtitle2" fontWeight="bold">Environment Manager</Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary">
                                    PowerShell automation for isolated dbt, ETL, API, and frontend environments.
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                    <Grid item xs={12} md={4}>
                        <Card component={RouterLink} to="/schema-discovery" variant="outlined" sx={{ height: '100%', textDecoration: 'none', color: 'inherit', display: 'block', '&:hover': { boxShadow: 3 } }}>
                            <CardContent>
                                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                                    <Storage sx={{ color: '#5e7086' }} />
                                    <Typography variant="subtitle2" fontWeight="bold">Schema Discovery</Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary">
                                    SQLAlchemy introspection across 450+ tables—tables.yml generation and drift detection.
                                </Typography>
                            </CardContent>
                        </Card>
                    </Grid>
                </Grid>
            </Box>

            {/* Contact */}
            <Box id="contact" sx={{ ...sectionSx, pb: 4 }}>
                <Typography variant="h5" fontWeight="bold" gutterBottom>Contact</Typography>
                <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
                    <Button variant="text" component={Link} href="mailto:rains.bp@gmail.com" startIcon={<Email />} sx={{ textTransform: 'none' }}>rains.bp@gmail.com</Button>
                    <Button variant="text" component={Link} href="https://www.linkedin.com/in/benjamin-rains-154139270/" target="_blank" rel="noopener noreferrer" startIcon={<LinkedIn />} sx={{ textTransform: 'none' }}>LinkedIn</Button>
                    <Button variant="text" component={Link} href={GITHUB} target="_blank" rel="noopener noreferrer" startIcon={<GitHub />} sx={{ textTransform: 'none' }}>GitHub</Button>
                </Box>
            </Box>

            <Box sx={{ bgcolor: 'grey.900', color: 'grey.300', py: 3, textAlign: 'center' }}>
                <Container maxWidth="lg">
                    <Typography variant="body2">
                        © {new Date().getFullYear()} Benjamin Rains ·{' '}
                        <Link component={RouterLink} to="/agent-profile" sx={{ color: 'primary.light' }}>/agent-profile</Link>
                    </Typography>
                </Container>
            </Box>
        </Box>
    );
};

export default PortfolioV4;
