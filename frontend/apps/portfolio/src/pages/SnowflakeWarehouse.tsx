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
    AcUnit,
    AccountTree,
    Lock,
    Speed,
} from '@mui/icons-material';
import { MermaidDiagram } from '@mdc/ui-common';

const GITHUB = 'https://github.com/BenjaminRains/dbt_dental_clinic';
const GITHUB_SF = `${GITHUB}/blob/feature/snowflake-wave1-reload`;

const ARCHITECTURE_CHART = `flowchart LR
  PG["Demo Postgres<br/>opendental_demo.raw"]
  EXP["Export<br/>PUT + COPY INTO"]
  RAW["Snowflake RAW<br/>payment · claimpayment"]
  STG["dbt staging<br/>numeric amounts"]
  MART["mart_daily_payments<br/>one row per date"]

  PG --> EXP --> RAW --> STG --> MART
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
        href: `${GITHUB_SF}/docs/snowflake/SNOWFLAKE_INTEGRATION_PLAN.md`,
    },
    {
        title: 'Export script',
        desc: 'Demo Postgres to Snowflake COPY. Refuses clinic database names.',
        href: `${GITHUB_SF}/scripts/snowflake/export_demo_to_snowflake.py`,
    },
    {
        title: 'Parity script',
        desc: 'Compares net collections and payment count for every date.',
        href: `${GITHUB_SF}/scripts/snowflake/compare_mart_daily_payments.py`,
    },
    {
        title: 'Daily payments mart',
        desc: 'Same model on Postgres and Snowflake. One row per payment date.',
        href: `${GITHUB_SF}/dbt_dental_models/models/marts/mart_daily_payments.sql`,
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
                        Snowflake payments warehouse
                    </Typography>
                    <Typography variant="h6" sx={{ opacity: 0.95, maxWidth: '800px' }}>
                        The same dbt project builds daily net collections on Postgres and on a
                        small Snowflake warehouse. The load is synthetic demo data only.
                    </Typography>
                </Container>
            </Box>

            <Container maxWidth="lg" sx={{ py: 6 }}>
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Domain
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ fontSize: '1.1rem', lineHeight: 1.8 }}>
                        Wave 1 is payments and collections. Patient payment headers and insurance
                        check totals roll up to <code>mart_daily_payments</code>: one row per
                        calendar date, with <code>net_collections_amount</code> and{' '}
                        <code>payment_count</code>. Clinic Postgres stays the system of record.
                        Snowflake is a portfolio warehouse beside it, grown by tagging more models
                        rather than forking the project.
                    </Typography>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mt: 2 }}>
                        {[
                            'Synthetic demo only',
                            'One dbt project',
                            'tag:snowflake',
                            'RSA key-pair',
                            'XS auto-suspend',
                        ].map((label) => (
                            <Chip key={label} label={label} size="small" variant="outlined" />
                        ))}
                    </Box>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Architecture
                    </Typography>
                    <Typography variant="body1" color="text.secondary" sx={{ mb: 3, maxWidth: 720 }}>
                        Demo Postgres is exported into Snowflake, then the existing staging and
                        mart models run on that target.
                    </Typography>
                    <Card elevation={2}>
                        <CardContent>
                            <Box
                                sx={{
                                    bgcolor: 'grey.50',
                                    borderRadius: 2,
                                    p: { xs: 2, md: 4 },
                                    overflowX: 'auto',
                                    minHeight: { xs: 280, md: 360 },
                                }}
                            >
                                <MermaidDiagram id="snowflake-wave1-architecture" chart={ARCHITECTURE_CHART} />
                            </Box>
                            <Grid container spacing={2} sx={{ mt: 2 }}>
                                {[
                                    {
                                        title: 'Landing',
                                        desc: '21,279 payment rows and 1,817 claim-payment rows in OPENDENTAL_SF.RAW.',
                                    },
                                    {
                                        title: 'Same SQL',
                                        desc: 'Money columns are numeric(18, 2). Daily sums match across both engines.',
                                    },
                                    {
                                        title: 'Wave 1 models',
                                        desc: 'stg_opendental__payment, stg_opendental__claimpayment, and mart_daily_payments.',
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
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Parity proof
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ maxWidth: 720 }}>
                        <code>compare_mart_daily_payments.py</code> checks every payment date.
                        On 2026-09-30 both the local demo database and the EC2 demo database
                        matched Snowflake on all 61 dates. The dbt build for{' '}
                        <code>tag:snowflake</code> finished PASS=76, WARN=1, ERROR=0. The warning
                        is an empty bank branch on every synthetic claim payment.
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
                        Those three dates are the first, middle, and last rows. The other 58 dates
                        matched the same way.
                    </Typography>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
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
                                    Postgres-only tests stay off this target. The next slice is{' '}
                                    <code>fact_payment</code>, using the same database and tag.
                                </Typography>
                            </Paper>
                        </Grid>
                    </Grid>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 4 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
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
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 3 }}>
                        <AcUnit sx={{ color: '#29b6f6' }} />
                        <Typography variant="body2" color="text.secondary">
                            Links point at <code>feature/snowflake-wave1-reload</code> until that
                            branch is on main.
                        </Typography>
                    </Box>
                </Box>
            </Container>
        </Box>
    );
};

export default SnowflakeWarehouse;
