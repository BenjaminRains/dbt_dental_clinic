import React from 'react';
import { Link as RouterLink, useNavigate } from 'react-router-dom';
import {
    Box,
    Button,
    Card,
    CardContent,
    Chip,
    Container,
    Divider,
    Link,
    Paper,
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableHead,
    TableRow,
    Typography,
} from '@mui/material';
import { ArrowBack, GitHub } from '@mui/icons-material';
import { MermaidDiagram } from '@mdc/ui-common';

const GITHUB = 'https://github.com/BenjaminRains/dbt_dental_clinic';
const GITHUB_MAIN = `${GITHUB}/blob/main`;

function sourceLink(path: string, line?: number) {
    return line ? `${GITHUB_MAIN}/${path}#L${line}` : `${GITHUB_MAIN}/${path}`;
}

const LAYERS_CHART = `flowchart TB
  od["OpenDental Daily Payments"]
  snap["Section totals only"]
  mart["mart_daily_payments"]
  api["GET /kpi/daily-collections"]
  home["Practice Manager Home"]
  od --> snap --> mart --> api --> home`;

const DAY_CHART = `flowchart TB
  headers["Patient headers on PayDate"]
  checks["Claim checks on CheckDate"]
  net["net_collections_amount"]
  headers --> net
  checks --> net`;

const GOLDENS = [
    {
        date: '2026-06-24',
        day: 'Tuesday',
        patient: '$7,219.60',
        insurance: '$3,977.80',
        net: '$11,197.40',
        count: '39',
    },
    {
        date: '2025-10-07',
        day: 'Tuesday',
        patient: '$8,577.82',
        insurance: '$13,169.48',
        net: '$21,747.30',
        count: '59',
    },
    {
        date: '2025-11-08',
        day: 'Saturday',
        patient: '$3,791.65',
        insurance: '$0.00',
        net: '$3,791.65',
        count: '7',
    },
];

const OCTOBER_SECTIONS = [
    ['Claim checks', '$13,169.48', 'claimpayment on CheckDate, 23 rows'],
    ['Patient check', '$265.05', 'payment type 69, 1 header'],
    ['Cash', '$180.00', 'payment type 70, 2 headers'],
    ['Credit card', '$7,843.37', 'payment type 71, 32 headers'],
    ['Care Credit', '$289.40', 'payment type 391, 1 header'],
    ['Income transfer', '$0.00', 'OpenDental section nets to zero and stays out of the net'],
    ['Net collections', '$21,747.30', 'patient $8,577.82 + insurance $13,169.48'],
];

const JUNE_SECTIONS = [
    ['Check', '$1,051.30', 'payment type 69'],
    ['Cash', '$677.00', 'payment type 70'],
    ['Credit card', '$4,172.10', 'payment type 71'],
    ['Cherry', '$1,319.20', 'payment type 634'],
    ['Claim checks', '$3,977.80', 'claimpayment on CheckDate'],
    ['Net collections', '$11,197.40', 'patient $7,219.60 + insurance $3,977.80'],
];

const DEFECTS = [
    {
        id: 'DP-001',
        issue: 'Mart was $3,977.80 under the report on 2026-06-24.',
        cause: 'The mart read fact_payment only and had no claim-payment path.',
        fix: 'Insurance now comes from staging claim payments, dated on CheckDate.',
    },
    {
        id: 'DP-002',
        issue: 'Patient dollars were short on the same day.',
        cause: 'Pay type ids were wrong, and the paysplit grain dropped unallocated amounts.',
        fix: 'The mart sums non-zero payment headers from staging.',
    },
    {
        id: 'DP-003',
        issue: 'Staging was $156.30 under the report before the sync.',
        cause: 'Two payments were in OpenDental and not yet in the warehouse.',
        fix: 'Run the payment sync before a same-day compare. After that sync the day matched.',
    },
];

const LAYERS = [
    {
        layer: '1',
        name: 'OpenDental export',
        what: 'Daily Payments for one date. Providers all, unearned included. The CSV stays on the clinic site.',
    },
    {
        layer: '2',
        name: 'Section snapshot',
        what: 'A local parser keeps section totals and row counts. Names and check numbers are not written into the repo.',
    },
    {
        layer: '3',
        name: 'Warehouse compare',
        what: 'Staging reconstruction equals the mart. Status PASS means the mart net and the OpenDental net differ by at most $10, or by less than 0.5 percent. These three dates differ by $0.00.',
    },
    {
        layer: '4',
        name: 'API and home card',
        what: 'The service selects one mart row and does no further math. Smoke on 2026-06-26 returned the same three nets, 3/3 pass.',
    },
];

const ARTIFACTS = [
    {
        title: 'Validation report',
        desc: 'Sign-off on 2026-06-26. Three golden dates, four layers, defects closed.',
        href: sourceLink('dbt_dental_models/validation/kpi/daily-payments/VALIDATION_REPORT.md'),
    },
    {
        title: 'Field map',
        desc: 'How each mart column lines up with a Daily Payments section.',
        href: sourceLink('dbt_dental_models/validation/kpi/daily-payments/FIELD_MAP.md'),
    },
    {
        title: 'Registry row',
        desc: 'within_tolerance for net_collections_amount. Tolerance is $10 or 0.5 percent.',
        href: sourceLink('dbt_dental_models/validation/kpi/KPI_VALIDATION_REGISTRY.md'),
    },
    {
        title: 'Mart SQL',
        desc: 'Patient headers plus claim checks, rounded to cents, one row per date.',
        href: sourceLink('dbt_dental_models/models/marts/mart_daily_payments.sql', 117),
    },
    {
        title: 'API service',
        desc: 'One SELECT from the mart. The clinic card shows those columns.',
        href: sourceLink('api/services/kpi_service.py', 28),
    },
    {
        title: 'API smoke',
        desc: 'Checks the three golden dates against the live service response.',
        href: sourceLink('api/tests/kpi/verify_daily_collections.py'),
    },
];

function FlowDiagram({
    id,
    title,
    intro,
    chart,
}: {
    id: string;
    title: string;
    intro: string;
    chart: string;
}) {
    return (
        <Box sx={{ mb: 4 }}>
            <Typography variant="h5" gutterBottom sx={{ fontWeight: 'bold' }}>
                {title}
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2, maxWidth: 760 }}>
                {intro}
            </Typography>
            <Card elevation={2}>
                <CardContent>
                    <Box
                        sx={{
                            bgcolor: 'grey.50',
                            borderRadius: 2,
                            p: { xs: 1, md: 3 },
                            overflowX: 'auto',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            minHeight: 220,
                        }}
                    >
                        <MermaidDiagram id={id} chart={chart} />
                    </Box>
                </CardContent>
            </Card>
        </Box>
    );
}

const DailyNetCollections: React.FC = () => {
    const navigate = useNavigate();

    return (
        <Box sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
            <Box
                sx={{
                    background: 'linear-gradient(135deg, #004d40 0%, #00897b 100%)',
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
                            mb: 1,
                            textAlign: 'left',
                        }}
                    >
                        Daily net collections
                    </Typography>
                    <Typography
                        variant="h3"
                        component="p"
                        sx={{
                            fontWeight: 700,
                            fontSize: { xs: '2rem', md: '2.6rem' },
                            textAlign: 'left',
                            mb: 1,
                        }}
                    >
                        Exact on three clinic dates
                    </Typography>
                    <Typography variant="h6" sx={{ opacity: 0.95, textAlign: 'left', mb: 2 }}>
                        Signed off 26 June 2026 · OpenDental Daily Payments
                    </Typography>
                    <Typography variant="body1" sx={{ opacity: 0.95, maxWidth: 760 }}>
                        The check is the clinic warehouse against the clinic report. Each golden date
                        matched to the cent. The synthetic demo database is not part of this sign-off.
                    </Typography>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mt: 3 }}>
                        {['within_tolerance', 'Clinic data', '$0.00 difference', 'API 3/3'].map((label) => (
                            <Chip
                                key={label}
                                label={label}
                                size="small"
                                sx={{ bgcolor: 'rgba(255,255,255,0.16)', color: 'white' }}
                            />
                        ))}
                    </Box>
                </Container>
            </Box>

            <Container maxWidth="lg" sx={{ py: 6, textAlign: 'left' }}>
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'left' }}>
                        Why this metric
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ maxWidth: 760 }}>
                        Daily net collections is the practice-manager card for money taken in on a
                        calendar day. The reference is OpenDental, Reports, Standard, Daily, Payments.
                        The mart column is <code>net_collections_amount</code>. The registry status is{' '}
                        <code>within_tolerance</code> because every golden date below is PASS. The method for
                        the other signed-off metrics is on{' '}
                        <Link component={RouterLink} to="/kpi-validation">
                            KPI validation
                        </Link>
                        .
                    </Typography>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'left' }}>
                        Golden dates
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ maxWidth: 760 }}>
                        Status <strong>PASS</strong> is the compare result for one date. The OpenDental
                        Daily Payments net and <code>net_collections_amount</code> differ by at most $10,
                        or by less than 0.5 percent. Each row below is an exact PASS: the difference is
                        $0.00. Patient, insurance, net, and header count match in the report, the mart,
                        and the API smoke from 26 June 2026. The three dates are two weekdays with
                        insurance and one Saturday with patient payments only.
                    </Typography>
                    <TableContainer component={Paper} variant="outlined" sx={{ mb: 2 }}>
                        <Table size="small" sx={{ minWidth: 720 }}>
                            <TableHead>
                                <TableRow>
                                    <TableCell>Date</TableCell>
                                    <TableCell>Day</TableCell>
                                    <TableCell align="right">Patient</TableCell>
                                    <TableCell align="right">Insurance</TableCell>
                                    <TableCell align="right">Net</TableCell>
                                    <TableCell align="right">Headers</TableCell>
                                    <TableCell>Status</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {GOLDENS.map((row) => (
                                    <TableRow key={row.date}>
                                        <TableCell>{row.date}</TableCell>
                                        <TableCell>{row.day}</TableCell>
                                        <TableCell align="right">{row.patient}</TableCell>
                                        <TableCell align="right">{row.insurance}</TableCell>
                                        <TableCell align="right">{row.net}</TableCell>
                                        <TableCell align="right">{row.count}</TableCell>
                                        <TableCell>PASS · $0.00</TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </TableContainer>
                    <Typography variant="body2" color="text.secondary" sx={{ maxWidth: 760 }}>
                        The OpenDental export used for the compare contains names and check numbers.
                        That file stays on the clinic site. Published here are the section totals.
                    </Typography>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'left' }}>
                        What a day contains
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ maxWidth: 760, fontSize: '1.05rem', lineHeight: 1.8 }}>
                        A patient amount is one payment header: <code>PayDate</code> and <code>PayAmt</code>.
                        An insurance amount is one claim-payment header: <code>CheckDate</code> and{' '}
                        <code>CheckAmt</code>. The net is those two sums, rounded to cents. Zero-dollar
                        patient headers stay out of the count and do not change the dollars. Income
                        transfer is an OpenDental section that must total $0.00, so it is not part of
                        the net.
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ maxWidth: 760, fontSize: '1.05rem', lineHeight: 1.8 }}>
                        The report&apos;s detail rows are provider splits. The mart counts headers.
                        On 8 November 2025 the credit-card section is $3,791.65 either way: 11 report
                        rows and 7 payment headers. The dollars are the check. The row counts are not
                        supposed to match.
                    </Typography>
                    <FlowDiagram
                        id="daily-net-day"
                        title="The two inputs"
                        intro="Patient headers and claim checks. On 8 November 2025 the check side is $0.00, so the net is the patient sum alone."
                        chart={DAY_CHART}
                    />
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'left' }}>
                        Section match
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ maxWidth: 760 }}>
                        7 October 2025 is the mixed day. Leaving claim payments out would show
                        $8,577.82 instead of $21,747.30.
                    </Typography>
                    <TableContainer component={Paper} variant="outlined" sx={{ mb: 4 }}>
                        <Table size="small" sx={{ minWidth: 640 }}>
                            <TableHead>
                                <TableRow>
                                    <TableCell>Section</TableCell>
                                    <TableCell align="right">Amount</TableCell>
                                    <TableCell>Warehouse</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {OCTOBER_SECTIONS.map(([section, amount, warehouse]) => (
                                    <TableRow key={section}>
                                        <TableCell>{section}</TableCell>
                                        <TableCell align="right">{amount}</TableCell>
                                        <TableCell>{warehouse}</TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </TableContainer>
                    <Typography variant="body1" paragraph sx={{ maxWidth: 760 }}>
                        24 June 2026 is the day that exposed the missing insurance path. After the
                        mart change, each section matched.
                    </Typography>
                    <TableContainer component={Paper} variant="outlined">
                        <Table size="small" sx={{ minWidth: 640 }}>
                            <TableHead>
                                <TableRow>
                                    <TableCell>Section</TableCell>
                                    <TableCell align="right">Amount</TableCell>
                                    <TableCell>Warehouse</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {JUNE_SECTIONS.map(([section, amount, warehouse]) => (
                                    <TableRow key={section}>
                                        <TableCell>{section}</TableCell>
                                        <TableCell align="right">{amount}</TableCell>
                                        <TableCell>{warehouse}</TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </TableContainer>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'left' }}>
                        How the check runs
                    </Typography>
                    <FlowDiagram
                        id="daily-net-layers"
                        title="Clinic path"
                        intro="The report is the reference. The page shows totals. The clinic card reads the mart row the compare already checked."
                        chart={LAYERS_CHART}
                    />
                    <TableContainer component={Paper} variant="outlined">
                        <Table size="small" sx={{ minWidth: 640 }}>
                            <TableHead>
                                <TableRow>
                                    <TableCell>Layer</TableCell>
                                    <TableCell>Name</TableCell>
                                    <TableCell>What is checked</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {LAYERS.map((row) => (
                                    <TableRow key={row.layer}>
                                        <TableCell>{row.layer}</TableCell>
                                        <TableCell sx={{ whiteSpace: 'nowrap' }}>{row.name}</TableCell>
                                        <TableCell>{row.what}</TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </TableContainer>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'left' }}>
                        What the first compare found
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ maxWidth: 760 }}>
                        24 June 2026 did not match on the first pass. The three gaps below were
                        closed before the date was signed off. No defect was still open on 26 June 2026.
                    </Typography>
                    <TableContainer component={Paper} variant="outlined">
                        <Table size="small" sx={{ minWidth: 720 }}>
                            <TableHead>
                                <TableRow>
                                    <TableCell>Id</TableCell>
                                    <TableCell>What was wrong</TableCell>
                                    <TableCell>Cause</TableCell>
                                    <TableCell>Resolution</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {DEFECTS.map((row) => (
                                    <TableRow key={row.id}>
                                        <TableCell>{row.id}</TableCell>
                                        <TableCell>{row.issue}</TableCell>
                                        <TableCell>{row.cause}</TableCell>
                                        <TableCell>{row.fix}</TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </TableContainer>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'left' }}>
                        Boundary
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ maxWidth: 760 }}>
                        The sign-off is section totals and the net on three clinic dates. The field map
                        is the rule set the mart is written to follow. The compares sum the exported
                        Amount column. They do not read the OpenDental report query. Income transfer on
                        7 October 2025 is $0.00, so that date matches whether or not those rows are
                        filtered out. Row identity was not part of the check.
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ maxWidth: 760 }}>
                        This sign-off is Daily Payments on clinic data. Production and Income is a
                        different report: it sums paysplits on <code>DatePay</code> and drops unearned
                        types whose definition item value is <code>X</code>. In 2025 that exclusion
                        makes this mart $40,152.63 lower than that report&apos;s total income.
                        Insurance on this mart matches that report in 2025 and 2026. Copying the
                        synthetic database to Snowflake is a separate check and is not this sign-off.
                    </Typography>
                    <Link
                        href={sourceLink('dbt_dental_models/validation/kpi/production-and-income/FIELD_MAP.md', 73)}
                        target="_blank"
                        rel="noopener noreferrer"
                    >
                        production-and-income/FIELD_MAP.md
                    </Link>
                </Box>

                <Divider sx={{ my: 6 }} />

                <Box sx={{ mb: 4 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3, textAlign: 'left' }}>
                        Artifacts
                    </Typography>
                    <Box
                        sx={{
                            display: 'grid',
                            gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' },
                            gap: 2,
                        }}
                    >
                        {ARTIFACTS.map(({ title, desc, href }) => (
                            <Card
                                key={title}
                                component="a"
                                href={href}
                                target="_blank"
                                rel="noopener noreferrer"
                                variant="outlined"
                                sx={{
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
                        ))}
                    </Box>
                    <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', mt: 4 }}>
                        <Button variant="outlined" size="large" onClick={() => navigate('/kpi-validation')}>
                            KPI validation
                        </Button>
                    </Box>
                </Box>
            </Container>
        </Box>
    );
};

export default DailyNetCollections;
