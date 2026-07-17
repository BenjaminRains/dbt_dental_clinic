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
    Verified,
    Gavel,
    FactCheck,
    Lock,
} from '@mui/icons-material';
import { MermaidDiagram } from '@mdc/ui-common';

const GITHUB = 'https://github.com/BenjaminRains/dbt_dental_clinic';
const GITHUB_MAIN = `${GITHUB}/blob/main`;
const KPI_VALIDATION_ROOT = `${GITHUB}/tree/main/dbt_dental_models/validation/kpi`;

const METHODOLOGY_CHART = `flowchart TB
  L0["Layer 0 — MySQL vs raw replica"]
  L1["Layer 1 — OD golden vs staging"]
  L2["Layer 2 — Staging vs mart"]
  L3["Layer 3 — Mart vs OD golden"]
  L4["Layer 4 — API smoke tests"]
  Reg["Registry within_tolerance"]
  Allow["validatedKpis allowlist"]
  UI["Practice Manager Home"]

  L0 --> L1 --> L2 --> L3
  L3 --> L4
  L3 --> Reg --> Allow --> UI
`;

/** Portfolio mirror of clinic validatedKpis metadata — do not import from clinic app. */
const SIGNED_OFF_KPIS = [
    {
        name: 'Daily net collections',
        model: 'mart_daily_payments',
        field: 'net_collections_amount',
        odReport: 'Daily Payments',
        odMenuPath: 'Reports → Standard → Daily → Payments',
        signedOff: '2026-06-26',
        reportHref: `${GITHUB_MAIN}/dbt_dental_models/validation/kpi/daily-payments/VALIDATION_REPORT.md`,
    },
    {
        name: 'Daily production by procedure',
        model: 'mart_daily_production_by_procedure',
        field: 'sum(total_fees)',
        odReport: 'Production by Procedure',
        odMenuPath: 'Reports → Standard → Daily → Production by Procedure',
        signedOff: '2026-07-16',
        reportHref: `${GITHUB_MAIN}/dbt_dental_models/validation/kpi/daily-production-by-procedure/VALIDATION_REPORT.md`,
    },
] as const;

const ARTIFACT_LINKS = [
    {
        title: 'KPI validation README',
        desc: 'Methodology, layers, tolerance, and workflow.',
        href: `${GITHUB_MAIN}/dbt_dental_models/validation/kpi/README.md`,
    },
    {
        title: 'KPI validation registry',
        desc: 'Status board mapping KPIs to OpenDental reports.',
        href: `${GITHUB_MAIN}/dbt_dental_models/validation/kpi/KPI_VALIDATION_REGISTRY.md`,
    },
    {
        title: 'Daily payments sign-off',
        desc: 'VALIDATION_REPORT for net collections.',
        href: `${GITHUB_MAIN}/dbt_dental_models/validation/kpi/daily-payments/VALIDATION_REPORT.md`,
    },
    {
        title: 'Production by procedure sign-off',
        desc: 'VALIDATION_REPORT for daily production.',
        href: `${GITHUB_MAIN}/dbt_dental_models/validation/kpi/daily-production-by-procedure/VALIDATION_REPORT.md`,
    },
];

const KpiValidation: React.FC = () => {
    const navigate = useNavigate();

    return (
        <Box sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
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
                        KPI Validation vs OpenDental
                    </Typography>
                    <Typography variant="h6" sx={{ opacity: 0.95, maxWidth: '800px' }}>
                        Warehouse KPIs are not shown to staff until they match OpenDental&apos;s own
                        reports within tolerance.
                    </Typography>
                </Container>
            </Box>

            <Container maxWidth="lg" sx={{ py: 6 }}>
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Overview
                    </Typography>
                    <Typography
                        variant="body1"
                        paragraph
                        sx={{ fontSize: '1.1rem', lineHeight: 1.8, textAlign: 'left' }}
                    >
                        Clinic role homes follow <strong>Design Principle #0</strong>: a metric may
                        appear only after its registry status is <code>within_tolerance</code>. Until
                        then it stays on legacy report routes. Validation proves{' '}
                        <strong>logic parity</strong> with OpenDental standard reports—same
                        definitions, filters, exclusions, and grain—not a soft &quot;looks right&quot;
                        label.
                    </Typography>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mt: 2 }}>
                        {[
                            'Golden OD exports',
                            'Layered compare SQL',
                            'Registry gate',
                            'UI allowlist',
                            'API smoke tests',
                        ].map((label) => (
                            <Chip key={label} label={label} size="small" variant="outlined" />
                        ))}
                    </Box>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Methodology
                    </Typography>
                    <Typography variant="body1" color="text.secondary" sx={{ mb: 3, maxWidth: 720 }}>
                        Each signed-off KPI runs through layered compares. Matching totals within
                        tolerance is evidence the business rules are correct—not an excuse to widen
                        the threshold.
                    </Typography>
                    <Card elevation={2}>
                        <CardContent>
                            <Box
                                sx={{
                                    bgcolor: 'grey.50',
                                    borderRadius: 2,
                                    p: { xs: 2, md: 4 },
                                    overflowX: 'auto',
                                    minHeight: { xs: 420, md: 520 },
                                }}
                            >
                                <MermaidDiagram id="kpi-validation-methodology" chart={METHODOLOGY_CHART} />
                            </Box>
                            <Grid container spacing={2} sx={{ mt: 2 }}>
                                {[
                                    {
                                        layer: '0',
                                        title: 'Replica fidelity',
                                        desc: 'MySQL source vs PostgreSQL raw — catch ETL drift before KPI compares.',
                                    },
                                    {
                                        layer: '1',
                                        title: 'Staging reconstruction',
                                        desc: 'OD golden CSV vs staging — ETL has the same rows OD used.',
                                    },
                                    {
                                        layer: '2',
                                        title: 'Mart aggregation',
                                        desc: 'Staging vs mart — dbt rollups are faithful to grain and filters.',
                                    },
                                    {
                                        layer: '3',
                                        title: 'End-to-end sign-off',
                                        desc: 'Mart vs OD golden + FIELD_MAP — KPI logic matches the vendor report.',
                                    },
                                    {
                                        layer: '4',
                                        title: 'API pass-through',
                                        desc: 'API-equivalent SQL + Python smoke tests — clinic UI does no extra math.',
                                    },
                                ].map(({ layer, title, desc }) => (
                                    <Grid item xs={12} sm={6} md={4} key={layer}>
                                        <Paper variant="outlined" sx={{ p: 2, height: '100%' }}>
                                            <Typography variant="caption" color="text.secondary">
                                                Layer {layer}
                                            </Typography>
                                            <Typography variant="subtitle2" fontWeight="bold" gutterBottom>
                                                {title}
                                            </Typography>
                                            <Typography variant="body2" color="text.secondary">
                                                {desc}
                                            </Typography>
                                        </Paper>
                                    </Grid>
                                ))}
                            </Grid>
                        </CardContent>
                    </Card>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Tolerance
                    </Typography>
                    <Paper variant="outlined" sx={{ p: 3, bgcolor: 'grey.50' }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2 }}>
                            <Gavel sx={{ color: '#5e7086' }} />
                            <Typography variant="subtitle1" fontWeight="bold">
                                PASS if either threshold holds
                            </Typography>
                        </Box>
                        <Typography variant="body1" paragraph sx={{ mb: 1 }}>
                            Typically <strong>±$10</strong> or <strong>±0.5%</strong>. A compare run
                            passes when absolute dollars <em>or</em> percent delta is within limit.
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                            The goal is shared business rules with OpenDental—not widening tolerance to
                            force a match. Known rule or timing differences are documented as{' '}
                            <code>known_deltas</code> in the registry.
                        </Typography>
                    </Paper>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Signed-off KPIs
                    </Typography>
                    <Typography variant="body2" color="text.secondary" sx={{ mb: 3, maxWidth: 720 }}>
                        Registry status <code>within_tolerance</code> — eligible for clinic role-home
                        display.
                    </Typography>
                    <Grid container spacing={3}>
                        {SIGNED_OFF_KPIS.map((kpi) => (
                            <Grid item xs={12} md={6} key={kpi.name}>
                                <Card variant="outlined" sx={{ height: '100%' }}>
                                    <CardContent>
                                        <Box
                                            sx={{
                                                display: 'flex',
                                                alignItems: 'flex-start',
                                                justifyContent: 'space-between',
                                                gap: 1,
                                                mb: 2,
                                            }}
                                        >
                                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                                                <Verified sx={{ color: 'success.main' }} />
                                                <Typography variant="h6" fontWeight="bold">
                                                    {kpi.name}
                                                </Typography>
                                            </Box>
                                            <Chip
                                                label="within_tolerance"
                                                size="small"
                                                color="success"
                                                variant="outlined"
                                            />
                                        </Box>
                                        <Typography variant="body2" color="text.secondary" paragraph>
                                            Model: <code>{kpi.model}</code>
                                            <br />
                                            Field: <code>{kpi.field}</code>
                                        </Typography>
                                        <Typography variant="body2" paragraph>
                                            <strong>OD report:</strong> {kpi.odReport}
                                        </Typography>
                                        <Typography variant="body2" color="text.secondary" paragraph>
                                            {kpi.odMenuPath}
                                        </Typography>
                                        <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 2 }}>
                                            Signed off {kpi.signedOff}
                                        </Typography>
                                        <Button
                                            size="small"
                                            component={Link}
                                            href={kpi.reportHref}
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            startIcon={<GitHub />}
                                            sx={{ textTransform: 'none' }}
                                        >
                                            Validation report
                                        </Button>
                                    </CardContent>
                                </Card>
                            </Grid>
                        ))}
                    </Grid>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        In progress
                    </Typography>
                    <Paper variant="outlined" sx={{ p: 3 }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                            <FactCheck sx={{ color: '#5e7086' }} />
                            <Typography variant="subtitle1" fontWeight="bold">
                                AR total (Aging of A/R)
                            </Typography>
                            <Chip label="delta_documented" size="small" variant="outlined" />
                        </Box>
                        <Typography variant="body2" color="text.secondary">
                            Mart vs warehouse checks can pass while live OpenDental BalTotal still
                            diverges (e.g. patient lag). Documented deltas stay out of the role-home
                            allowlist until status is <code>within_tolerance</code>.
                        </Typography>
                    </Paper>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Product gate
                    </Typography>
                    <Grid container spacing={3}>
                        <Grid item xs={12} md={6}>
                            <Paper variant="outlined" sx={{ p: 3, height: '100%' }}>
                                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                                    <Lock sx={{ color: '#5e7086' }} />
                                    <Typography variant="subtitle1" fontWeight="bold">
                                        Clinic allowlist
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary" paragraph>
                                    The clinic app keeps a hard allowlist of signed-off KPIs (
                                    <code>validatedKpis.ts</code>). Practice Manager Home only renders
                                    metrics on that list—unvalidated totals stay on legacy dashboard
                                    routes.
                                </Typography>
                                <Typography variant="body2" color="text.secondary">
                                    UI chips such as &quot;Validated vs OD Daily Payments&quot; make the
                                    provenance visible to staff without exposing compare SQL or golden
                                    exports.
                                </Typography>
                            </Paper>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Paper variant="outlined" sx={{ p: 3, height: '100%', bgcolor: 'grey.50' }}>
                                <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
                                    Why this matters
                                </Typography>
                                <Typography variant="body2" component="ul" sx={{ pl: 2, m: 0, color: 'text.secondary' }}>
                                    <li>Prevents leadership acting on unverified warehouse numbers</li>
                                    <li>Turns vendor report recreation into an auditable promotion path</li>
                                    <li>Separates &quot;built a dashboard&quot; from &quot;proved parity with OD&quot;</li>
                                </Typography>
                            </Paper>
                        </Grid>
                    </Grid>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Artifacts
                    </Typography>
                    <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
                        Public methodology and sign-off docs (no clinic dollar amounts or PHI).
                    </Typography>
                    <Grid container spacing={2}>
                        {ARTIFACT_LINKS.map(({ title, desc, href }) => (
                            <Grid item xs={12} sm={6} key={title}>
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
                                            <GitHub sx={{ color: '#5e7086', fontSize: 20 }} />
                                            <Typography variant="subtitle2" fontWeight="bold">
                                                {title}
                                            </Typography>
                                        </Box>
                                        <Typography variant="body2" color="text.secondary">
                                            {desc}
                                        </Typography>
                                    </CardContent>
                                </Card>
                            </Grid>
                        ))}
                    </Grid>
                    <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', mt: 4 }}>
                        <Button
                            variant="contained"
                            size="large"
                            component={Link}
                            href={KPI_VALIDATION_ROOT}
                            target="_blank"
                            rel="noopener noreferrer"
                            startIcon={<GitHub />}
                        >
                            Browse validation/kpi on GitHub
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

export default KpiValidation;
