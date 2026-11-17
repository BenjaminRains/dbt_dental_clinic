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
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableRow,
} from '@mui/material';
import {
    ArrowBack,
    GitHub,
    AccountTree,
    Lock,
    Speed,
} from '@mui/icons-material';
import { MermaidDiagram } from '@mdc/ui-common';

const GITHUB = 'https://github.com/BenjaminRains/dbt_dental_clinic';
const GITHUB_MAIN = `${GITHUB}/blob/main`;

const ARCHITECTURE_CHART = `%%{init: {"themeVariables": {"fontSize": "20px"}, "flowchart": {"htmlLabels": false, "padding": 28, "nodeSpacing": 28, "rankSpacing": 56}}}%%
flowchart TB
  PG["Demo Postgres · opendental_demo.raw"]
  EXP["Export · PUT and COPY INTO"]
  RAW["Snowflake RAW · payment, paysplit, procedurelog, fee"]
  STG["dbt staging · tag:snowflake"]
  DAILY["Daily collections · mart_daily_payments"]
  DETAIL["Payment detail · fact_payment"]
  PG --> EXP --> RAW --> STG
  STG --> DAILY
  STG --> DETAIL
`;

const PROOF_ROWS = [
    { date: '2025-11-13', net: '45,473.55', count: '372' },
    { date: '2025-12-13', net: '83,152.80', count: '401' },
    { date: '2026-01-12', net: '39,676.25', count: '334' },
] as const;

const ARTIFACT_LINKS = [
    {
        title: 'Integration plan',
        desc: 'Locked decisions, phases, and the portability rule.',
        href: `${GITHUB_MAIN}/docs/snowflake/SNOWFLAKE_INTEGRATION_PLAN.md`,
    },
    {
        title: 'Export script',
        desc: 'Demo Postgres to Snowflake COPY. Refuses clinic database names.',
        href: `${GITHUB_MAIN}/scripts/snowflake/export_demo_to_snowflake.py`,
    },
    {
        title: 'Parity script',
        desc: 'Compares net collections and payment count for every date.',
        href: `${GITHUB_MAIN}/scripts/snowflake/compare_mart_daily_payments.py`,
    },
    {
        title: 'Daily payments mart',
        desc: 'One row per payment date on both engines.',
        href: `${GITHUB_MAIN}/dbt_dental_models/models/marts/mart_daily_payments.sql`,
    },
    {
        title: 'Payment detail fact',
        desc: 'One row per payment split on the same Snowflake target.',
        href: `${GITHUB_MAIN}/dbt_dental_models/models/marts/fact_payment.sql`,
    },
];

const SnowflakeWarehouse: React.FC = () => {
    const navigate = useNavigate();

    return (
        <Box sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
            <Box
                sx={{
                    background: 'linear-gradient(135deg, #0d47a1 0%, #29b6f6 100%)',
                    color: 'white',
                    py: { xs: 6, md: 8 },
                    px: 2,
                }}
            >
                <Container maxWidth="lg" sx={{ textAlign: 'left' }}>
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
                            textAlign: 'center',
                        }}
                    >
                        Snowflake payments warehouse
                    </Typography>
                    <Typography variant="h6" sx={{ opacity: 0.95, maxWidth: '800px', textAlign: 'left' }}>
                        One dbt project builds daily collections and payment-line detail on
                        Postgres and on a small Snowflake warehouse. Synthetic demo data only.
                        Daily collections match on every date.
                    </Typography>
                </Container>
            </Box>

            <Container maxWidth="lg" sx={{ py: 6, textAlign: 'left' }}>
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'center' }}>
                        What it produces
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ fontSize: '1.1rem', lineHeight: 1.8 }}>
                        Daily collections is one row per date: net collections and payment count,
                        from patient payments and insurance checks (<code>mart_daily_payments</code>).
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ fontSize: '1.1rem', lineHeight: 1.8 }}>
                        Payment detail is one row per payment split, with the procedure and fee it
                        applied to (<code>fact_payment</code> via <code>int_payment_split</code>).
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ fontSize: '1.1rem', lineHeight: 1.8 }}>
                        Clinic Postgres stays the system of record. Snowflake is a second target of
                        the same project.
                    </Typography>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mt: 2 }}>
                        {[
                            'Synthetic demo only',
                            'One dbt project',
                            'Daily totals and payment lines',
                            'Key-pair auth',
                            'Auto-suspend warehouse',
                        ].map((label) => (
                            <Chip key={label} label={label} size="small" variant="outlined" />
                        ))}
                    </Box>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'center' }}>
                        Architecture
                    </Typography>
                    <Typography variant="body1" color="text.secondary" sx={{ mb: 3, maxWidth: 720 }}>
                        Demo Postgres is copied into Snowflake, then the same staging and mart
                        models run there.
                    </Typography>
                    <Card elevation={2}>
                        <CardContent>
                            <Box
                                sx={{
                                    bgcolor: 'grey.50',
                                    borderRadius: 2,
                                    px: { xs: 1, md: 3 },
                                    py: { xs: 3, md: 4 },
                                    display: 'flex',
                                    justifyContent: 'center',
                                    '& iframe': {
                                        width: 'min(100%, 640px) !important',
                                        height: '640px !important',
                                        border: 0,
                                    },
                                }}
                            >
                                <MermaidDiagram id="snowflake-architecture" chart={ARCHITECTURE_CHART} />
                            </Box>
                            <Grid container spacing={2} sx={{ mt: 2 }}>
                                {[
                                    {
                                        title: 'Landing',
                                        desc: 'Patient payments, insurance checks, payment splits (21,279), procedures (23,692), and fees land in Snowflake RAW.',
                                    },
                                    {
                                        title: 'Portable SQL',
                                        desc: 'Money is numeric(18, 2). Postgres-only syntax stays behind a target check, so both engines run the same models.',
                                    },
                                    {
                                        title: 'Two outputs',
                                        desc: 'Daily totals for 61 dates, matched to Postgres, and payment-line detail on the same warehouse.',
                                    },
                                ].map(({ title, desc }) => (
                                    <Grid item xs={12} md={4} key={title}>
                                        <Paper variant="outlined" sx={{ p: 2, height: '100%' }}>
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
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'center' }}>
                        Parity proof
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ maxWidth: 720 }}>
                        On 30 Sep 2026, Snowflake daily collections matched the local demo database
                        and the EC2 demo database on all 61 dates. Net collections and payment count
                        agreed on every date. The table is the first, middle, and last of those
                        dates.
                    </Typography>
                    <Table size="small" sx={{ maxWidth: 480, mb: 2 }}>
                        <TableHead>
                            <TableRow>
                                <TableCell>Payment date</TableCell>
                                <TableCell align="right">Net collections</TableCell>
                                <TableCell align="right">Payments</TableCell>
                            </TableRow>
                        </TableHead>
                        <TableBody>
                            {PROOF_ROWS.map((row) => (
                                <TableRow key={row.date}>
                                    <TableCell>{row.date}</TableCell>
                                    <TableCell align="right">{row.net}</TableCell>
                                    <TableCell align="right">{row.count}</TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                    <Typography variant="body2" color="text.secondary">
                        Payment-line detail uses the same payment-split row count as the export.
                    </Typography>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'center' }}>
                        Cost and access
                    </Typography>
                    <Grid container spacing={3}>
                        <Grid item xs={12} md={4}>
                            <Paper variant="outlined" sx={{ p: 3, height: '100%' }}>
                                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                                    <Speed sx={{ color: '#5e7086' }} />
                                    <Typography variant="subtitle1" fontWeight="bold">
                                        XS warehouse
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary">
                                    <code>WH_DEMO_XS</code> suspends after 60 seconds idle. The
                                    portfolio slice stays near the small-warehouse budget.
                                </Typography>
                            </Paper>
                        </Grid>
                        <Grid item xs={12} md={4}>
                            <Paper variant="outlined" sx={{ p: 3, height: '100%' }}>
                                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                                    <Lock sx={{ color: '#5e7086' }} />
                                    <Typography variant="subtitle1" fontWeight="bold">
                                        Key-pair auth
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary">
                                    dbt connects as role <code>TRANSFORMER</code> with an RSA key.
                                    The export refuses clinic and analytics database names.
                                </Typography>
                            </Paper>
                        </Grid>
                        <Grid item xs={12} md={4}>
                            <Paper variant="outlined" sx={{ p: 3, height: '100%' }}>
                                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                                    <AccountTree sx={{ color: '#5e7086' }} />
                                    <Typography variant="subtitle1" fontWeight="bold">
                                        One project
                                    </Typography>
                                </Box>
                                <Typography variant="body2" color="text.secondary">
                                    Postgres-only tests stay off this target. Both marts live in one
                                    Snowflake database. Another subject area is another set of tagged
                                    models in this project.
                                </Typography>
                            </Paper>
                        </Grid>
                    </Grid>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 4 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'center' }}>
                        Source
                    </Typography>
                    <Grid container spacing={2}>
                        {ARTIFACT_LINKS.map(({ title, desc, href }) => (
                            <Grid item xs={12} sm={6} key={title}>
                                <Card
                                    component={Link}
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
                                            <GitHub sx={{ color: '#5e7086' }} />
                                            <Typography variant="subtitle1" fontWeight="bold">
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
                </Box>
            </Container>
        </Box>
    );
};

export default SnowflakeWarehouse;
