import React from 'react';
import { Link as RouterLink, useNavigate } from 'react-router-dom';
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
    TableContainer,
    TableHead,
    TableRow,
} from '@mui/material';
import {
    ArrowBack,
    GitHub,
    CheckCircle,
    FactCheck,
    PlayArrow,
    CloudUpload,
    Lan,
    VpnKey,
    Extension,
    Science,
} from '@mui/icons-material';

const GITHUB = 'https://github.com/BenjaminRains/dbt_dental_clinic';
const GITHUB_MAIN = `${GITHUB}/blob/main`;
const MDC_ROOT = `${GITHUB}/tree/main/tools/mdc_cli`;

const ACCENT = '#5e7086';
const MONO = '"Cascadia Code", "Fira Code", Consolas, Menlo, monospace';

const TERM = {
    bg: '#0d1117',
    bar: '#161b22',
    border: '#30363d',
    text: '#c9d1d9',
    dim: '#8b949e',
    green: '#3fb950',
    cyan: '#56d4dd',
    yellow: '#d29922',
};

const FACTS = [
    { value: '12', label: 'Command groups' },
    { value: '11', label: 'Component and stage targets' },
    { value: '190+', label: 'Unit tests' },
    { value: 'Typer + Rich', label: 'Built on' },
];

const STAGES = ['local', 'test', 'demo', 'clinic', 'snowflake'] as const;

/** Mirrors API_STAGES / ETL_STAGES / DBT_STAGES in tools/mdc_cli/mdc_cli/paths.py. */
const STAGE_MATRIX: { component: string; loader: string; stages: readonly string[] }[] = [
    { component: 'API', loader: 'api/settings.py', stages: ['local', 'test', 'demo', 'clinic'] },
    { component: 'ETL', loader: 'etl_pipeline/config/settings_v2.py', stages: ['local', 'test', 'clinic'] },
    { component: 'dbt', loader: 'mdc_cli/dbt_env.py', stages: ['local', 'demo', 'clinic', 'snowflake'] },
];

/** `mdc status` rows: component, stage, config, profile. */
const STATUS_ROWS: [string, string, string, string][] = [
    ['api', 'local', 'api\\.env_api_local', '-'],
    ['api', 'demo', 'api\\.env_api_demo', '-'],
    ['api', 'clinic', 'api\\.env_api_clinic', '-'],
    ['api', 'test', 'api\\.env_api_test', '-'],
    ['etl', 'local', 'etl_pipeline\\.env_local', 'load'],
    ['etl', 'clinic', 'etl_pipeline\\.env_clinic', 'full'],
    ['etl', 'test', 'etl_pipeline\\.env_test', 'full'],
    ['dbt', 'local', 'dbt_dental_models\\.env_local', '-'],
    ['dbt', 'clinic', 'deployment_credentials.json', '-'],
    ['dbt', 'demo', 'deployment_credentials.json', '-'],
    ['dbt', 'snowflake', 'dbt_dental_models\\.env_snowflake', '-'],
];

const RUN_STEPS: { title: string; body: React.ReactNode }[] = [
    {
        title: 'Check the stage',
        body: (
            <>
                <code>--env</code> must be a stage the component supports.{' '}
                <code>mdc etl run --env demo</code> fails on the argument, before any file is read.
            </>
        ),
    },
    {
        title: 'Scrub the shell',
        body: (
            <>
                While settings load, stage-scoped variables already exported in the terminal (
                <code>POSTGRES_ANALYTICS_*</code>, <code>DBT_*</code>, <code>CLINIC_*</code>, and
                others) are removed, so a leftover export cannot override the file.
            </>
        ),
    },
    {
        title: 'Load typed settings',
        body: (
            <>
                The stage&apos;s config goes through a typed loader. API and ETL stages use the
                pydantic loaders in their own packages, so the CLI holds no second copy of the
                rules. A missing or malformed value stops the run here.
            </>
        ),
    },
    {
        title: 'Build the child environment',
        body: (
            <>
                The subprocess gets a short allowlist of OS variables plus the validated settings.
                The rest of the parent environment is not passed down.
            </>
        ),
    },
    {
        title: "Run in the component's venv",
        body: (
            <>
                The CLI finds the component&apos;s interpreter (<code>api/venv</code>, or the Pipenv
                environment for ETL and dbt), starts the process in that component&apos;s directory,
                and returns its exit code. For dbt it adds <code>--target</code> when none was
                given.
            </>
        ),
    },
];

const COMMAND_GROUPS: {
    title: string;
    icon: React.ReactElement;
    commands: string[];
    desc: string;
}[] = [
    {
        title: 'Validate',
        icon: <FactCheck />,
        commands: [
            'mdc status',
            'mdc api health --env <stage>',
            'mdc etl validate --env <stage>',
            'mdc dbt validate --env <stage>',
        ],
        desc: "Load and check a stage's settings without starting anything.",
    },
    {
        title: 'Run',
        icon: <PlayArrow />,
        commands: [
            'mdc api run --env <stage>',
            'mdc etl run --env <stage>',
            'mdc dbt run|test|docs --env <stage>',
        ],
        desc: 'Start a service or pipeline for one stage. Arguments after -- pass through to the underlying tool.',
    },
    {
        title: 'Deploy and publish',
        icon: <CloudUpload />,
        commands: [
            'mdc deploy frontend --target demo|clinic',
            'mdc deploy api --env clinic',
            'mdc deploy dbt-docs',
            'mdc publish analytics --env clinic',
        ],
        desc: 'Ship the frontends and dbt docs to S3 and CloudFront, push API config to EC2, and copy local marts to the clinic RDS.',
    },
    {
        title: 'Reach private infrastructure',
        icon: <Lan />,
        commands: ['mdc tunnel clinic-db', 'mdc ssm status', 'mdc ssm connect api'],
        desc: 'Port-forwards and shell sessions over AWS SSM. The clinic database sits in a private VPC.',
    },
    {
        title: 'Secrets',
        icon: <VpnKey />,
        commands: ['mdc secrets pull', 'mdc secrets pull clinic'],
        desc: 'Sync the live RDS secret from Secrets Manager into local env files, or rotate the demo database password after a confirmation.',
    },
    {
        title: 'Adjacent tools',
        icon: <Extension />,
        commands: [
            'mdc frontend dev --app portfolio|clinic',
            'mdc consult-audio pipeline run',
            'mdc snowflake status',
            'mdc airflow start',
        ],
        desc: 'The same entry point covers the Vite apps, the consult audio pipeline, the Snowflake demo warehouse, and native Airflow.',
    },
];

const GUARDRAILS: { title: string; body: React.ReactNode }[] = [
    {
        title: 'Wrong bundle, wrong bucket',
        body: (
            <>
                Before upload, the frontend deploy confirms the workspace package matches the
                target, rejects a demo build pointed at the clinic API (and the reverse), and reads
                the built <code>index.html</code> for the other app&apos;s product marker.
            </>
        ),
    },
    {
        title: 'Deep links on static hosting',
        body: (
            <>
                S3 has no rewrite rules, so each client route gets its own copy of{' '}
                <code>index.html</code>. The route lists are kept per target, so the portfolio
                bucket never receives clinic login paths. This page&apos;s route is in that list.
            </>
        ),
    },
    {
        title: 'Passwords stay out of the terminal',
        body: (
            <>
                Secret sync writes to the env file and prints only that it changed. Rotation shows
                its plan first and defaults to no. With no target and no terminal attached, it
                changes nothing.
            </>
        ),
    },
    {
        title: 'Publish checks the tunnel first',
        body: (
            <>
                <code>mdc publish analytics</code> confirms the SSM tunnel port is listening before
                it starts <code>pg_dump</code> and <code>pg_restore</code>. It supports{' '}
                <code>--dry-run</code> and can open its own tunnel for unattended Airflow runs.
            </>
        ),
    },
];

const SOURCE_LINKS = [
    {
        title: 'README',
        desc: 'Command reference, PowerShell aliases, and stage defaults.',
        href: `${GITHUB_MAIN}/tools/mdc_cli/README.md`,
    },
    {
        title: 'main.py',
        desc: 'Root Typer app and the twelve command groups.',
        href: `${GITHUB_MAIN}/tools/mdc_cli/mdc_cli/main.py`,
    },
    {
        title: 'run_helper.py',
        desc: 'Shell scrubbing, the OS allowlist, and isolated subprocess runs.',
        href: `${GITHUB_MAIN}/tools/mdc_cli/mdc_cli/run_helper.py`,
    },
    {
        title: 'deploy_frontend.py',
        desc: 'Build preflights, S3 sync, SPA fallbacks, and CloudFront invalidation.',
        href: `${GITHUB_MAIN}/tools/mdc_cli/mdc_cli/deploy_frontend.py`,
    },
    {
        title: 'CI workflow',
        desc: 'Pytest and smoke commands on GitHub Actions.',
        href: `${GITHUB_MAIN}/.github/workflows/mdc_cli.yml`,
    },
    {
        title: 'Environment files',
        desc: 'Which env file each component reads for each stage.',
        href: `${GITHUB_MAIN}/docs/deployment/ENVIRONMENT_FILES.md`,
    },
];

const sectionTitleSx = { fontWeight: 'bold', mb: 3 };

function TerminalWindow({ title, children }: { title: string; children: React.ReactNode }) {
    return (
        <Box
            sx={{
                bgcolor: TERM.bg,
                border: `1px solid ${TERM.border}`,
                borderRadius: 2,
                overflow: 'hidden',
                boxShadow: 3,
            }}
        >
            <Box
                sx={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 0.75,
                    px: 1.5,
                    py: 1,
                    bgcolor: TERM.bar,
                    borderBottom: `1px solid ${TERM.border}`,
                }}
            >
                {['#ff5f56', '#ffbd2e', '#27c93f'].map((color) => (
                    <Box key={color} sx={{ width: 10, height: 10, borderRadius: '50%', bgcolor: color }} />
                ))}
                <Typography sx={{ ml: 1, fontFamily: MONO, fontSize: '0.75rem', color: TERM.dim }}>
                    {title}
                </Typography>
            </Box>
            <Box
                component="pre"
                sx={{
                    m: 0,
                    p: 2,
                    overflowX: 'auto',
                    fontFamily: MONO,
                    fontSize: { xs: '0.75rem', sm: '0.8125rem' },
                    lineHeight: 1.7,
                    color: TERM.text,
                }}
            >
                {children}
            </Box>
        </Box>
    );
}

const Dim = ({ children }: { children: React.ReactNode }) => (
    <Box component="span" sx={{ color: TERM.dim }}>{children}</Box>
);
const Green = ({ children }: { children: React.ReactNode }) => (
    <Box component="span" sx={{ color: TERM.green }}>{children}</Box>
);
const Cyan = ({ children }: { children: React.ReactNode }) => (
    <Box component="span" sx={{ color: TERM.cyan }}>{children}</Box>
);

function Prompt({ command }: { command: string }) {
    return (
        <>
            <Green>$</Green> {command}
            {'\n'}
        </>
    );
}

const MdcCli: React.FC = () => {
    const navigate = useNavigate();

    return (
        <Box sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
            {/* Hero */}
            <Box
                sx={{
                    background: 'linear-gradient(135deg, #111827 0%, #1e3c72 100%)',
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
                        sx={{ fontWeight: 700, fontSize: { xs: '2rem', md: '3rem' }, mb: 2 }}
                    >
                        mdc CLI
                    </Typography>
                    <Typography variant="h6" sx={{ opacity: 0.95, maxWidth: '800px', mb: 3 }}>
                        One Python command line for the whole monorepo. Each command names its
                        stage, validates that stage&apos;s config, and runs the work in an isolated
                        child process.
                    </Typography>
                    <Box
                        sx={{
                            display: 'inline-block',
                            maxWidth: '100%',
                            overflowX: 'auto',
                            whiteSpace: 'nowrap',
                            bgcolor: 'rgba(0, 0, 0, 0.35)',
                            border: '1px solid rgba(255, 255, 255, 0.2)',
                            borderRadius: 1.5,
                            px: 2,
                            py: 1.25,
                            fontFamily: MONO,
                            fontSize: { xs: '0.8125rem', sm: '0.9375rem' },
                        }}
                    >
                        <Green>$</Green> mdc dbt run --env clinic --tunnel-db
                    </Box>
                </Container>
            </Box>

            {/* Global #app CSS centers text; long-form copy and terminal blocks read better left-aligned. */}
            <Container maxWidth="lg" sx={{ py: 6, textAlign: 'left' }}>
                {/* Overview */}
                <Box sx={{ mb: 6 }}>
                    <Grid container spacing={2} sx={{ mb: 4 }}>
                        {FACTS.map(({ value, label }) => (
                            <Grid item xs={6} md={3} key={label}>
                                <Paper variant="outlined" sx={{ p: 2, textAlign: 'center', height: '100%' }}>
                                    <Typography variant="h6" fontWeight="bold" color={ACCENT}>
                                        {value}
                                    </Typography>
                                    <Typography variant="caption" color="text.secondary">
                                        {label}
                                    </Typography>
                                </Paper>
                            </Grid>
                        ))}
                    </Grid>
                    <Typography variant="h4" gutterBottom sx={sectionTitleSx}>
                        Overview
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ fontSize: '1.1rem', lineHeight: 1.8 }}>
                        The monorepo holds an API, an ETL pipeline, a dbt project, two frontends,
                        and the AWS infrastructure behind them. Each Python component has its own
                        virtualenv and its own env files for up to four stages. <code>mdc</code> is
                        the single entry point for all of it: a Typer application, installed with{' '}
                        <code>pip install -e tools/mdc_cli</code>, that validates config, starts
                        services, runs pipelines, opens tunnels, and deploys.
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ fontSize: '1.1rem', lineHeight: 1.8 }}>
                        It replaced the PowerShell{' '}
                        <Link component={RouterLink} to="/environment-manager">
                            Environment Manager
                        </Link>
                        , which kept the active environment in the shell session.
                    </Typography>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mt: 2 }}>
                        {[
                            'Typer',
                            'Rich',
                            'pydantic-settings',
                            'Isolated child env',
                            'AWS SSM',
                            'Secrets Manager',
                            'GitHub Actions',
                        ].map((label) => (
                            <Chip key={label} label={label} size="small" variant="outlined" />
                        ))}
                    </Box>
                </Box>

                <Divider sx={{ my: 6 }} />

                {/* Stages and components */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={sectionTitleSx}>
                        Stages and components
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ maxWidth: 800 }}>
                        A stage is a named target: <code>local</code> for development,{' '}
                        <code>test</code> for the test databases, <code>demo</code> for the public
                        synthetic deployment, <code>clinic</code> for the live clinic, and{' '}
                        <code>snowflake</code> for the dbt-only portfolio warehouse. Each component
                        accepts only the stages it has config for.
                    </Typography>
                    <TableContainer component={Paper} variant="outlined" sx={{ mb: 4 }}>
                        <Table size="small">
                            <TableHead>
                                <TableRow>
                                    <TableCell>Component</TableCell>
                                    <TableCell>Settings loader</TableCell>
                                    {STAGES.map((stage) => (
                                        <TableCell key={stage} align="center" sx={{ fontFamily: MONO }}>
                                            {stage}
                                        </TableCell>
                                    ))}
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {STAGE_MATRIX.map(({ component, loader, stages }) => (
                                    <TableRow key={component}>
                                        <TableCell sx={{ fontWeight: 'bold' }}>{component}</TableCell>
                                        <TableCell sx={{ fontFamily: MONO, fontSize: '0.8125rem', color: 'text.secondary' }}>
                                            {loader}
                                        </TableCell>
                                        {STAGES.map((stage) => (
                                            <TableCell key={stage} align="center">
                                                {stages.includes(stage) ? (
                                                    <CheckCircle
                                                        aria-label={`${component} supports ${stage}`}
                                                        sx={{ fontSize: 18, color: 'success.main', verticalAlign: 'middle' }}
                                                    />
                                                ) : (
                                                    <Typography component="span" color="text.disabled">
                                                        –
                                                    </Typography>
                                                )}
                                            </TableCell>
                                        ))}
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </TableContainer>
                    <Typography variant="body1" paragraph sx={{ maxWidth: 800 }}>
                        <code>mdc status</code> loads all eleven pairs through their settings
                        loaders and prints one row for each. It then compares local clinic
                        credentials against Secrets Manager and probes warehouse freshness, warning
                        when ETL loads are more than 36 hours old or mart refreshes more than 48.
                    </Typography>
                    <TerminalWindow title="mdc status">
                        <Prompt command="mdc status" />
                        <Dim>
                            {`${'Component'.padEnd(11)}${'Stage'.padEnd(11)}${'Config'.padEnd(35)}${'File'.padEnd(6)}${'Profile'.padEnd(9)}Valid\n`}
                        </Dim>
                        {STATUS_ROWS.map(([component, stage, config, profile]) => (
                            <React.Fragment key={`${component}-${stage}`}>
                                <Cyan>{component.padEnd(11)}</Cyan>
                                {stage.padEnd(11)}
                                {config.padEnd(35)}
                                {'yes'.padEnd(6)}
                                {profile.padEnd(9)}
                                <Green>ok</Green>
                                {'\n'}
                            </React.Fragment>
                        ))}
                        {'\n'}
                        <Dim>Configuration authority: api/settings.py, etl_pipeline/.../settings_v2.py</Dim>
                    </TerminalWindow>
                    <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>
                        First table of the output on a configured dev machine. Venv python and Notes
                        columns are left out to fit the page.
                    </Typography>
                </Box>

                <Divider sx={{ my: 6 }} />

                {/* How a run works */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={sectionTitleSx}>
                        How a run works
                    </Typography>
                    <Typography variant="body1" color="text.secondary" sx={{ mb: 4, maxWidth: 720 }}>
                        <code>mdc dbt run --env clinic</code> does five things, in this order.
                    </Typography>
                    <Grid container spacing={4}>
                        <Grid item xs={12} md={7}>
                            <Box component="ol" sx={{ listStyle: 'none', m: 0, p: 0 }}>
                                {RUN_STEPS.map(({ title, body }, index) => {
                                    const isLast = index === RUN_STEPS.length - 1;
                                    return (
                                        <Box component="li" key={title} sx={{ display: 'flex', gap: 2 }}>
                                            <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                                                <Box
                                                    sx={{
                                                        width: 32,
                                                        height: 32,
                                                        flexShrink: 0,
                                                        borderRadius: '50%',
                                                        bgcolor: ACCENT,
                                                        color: 'white',
                                                        display: 'flex',
                                                        alignItems: 'center',
                                                        justifyContent: 'center',
                                                        fontWeight: 700,
                                                        fontSize: '0.875rem',
                                                    }}
                                                >
                                                    {index + 1}
                                                </Box>
                                                {!isLast && (
                                                    <Box sx={{ width: 2, flexGrow: 1, bgcolor: 'divider', my: 0.5 }} />
                                                )}
                                            </Box>
                                            <Box sx={{ pb: isLast ? 0 : 3, pt: 0.5 }}>
                                                <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
                                                    {title}
                                                </Typography>
                                                <Typography variant="body2" color="text.secondary">
                                                    {body}
                                                </Typography>
                                            </Box>
                                        </Box>
                                    );
                                })}
                            </Box>
                        </Grid>
                        <Grid item xs={12} md={5}>
                            <TerminalWindow title="what the child process sees">
                                <Dim>{'# OS allowlist\n'}</Dim>
                                {'PATH, TEMP, HOME, USERPROFILE, ...\n\n'}
                                <Dim>{'# validated stage settings\n'}</Dim>
                                <Cyan>POSTGRES_ANALYTICS_HOST</Cyan>
                                {'=...\n'}
                                <Cyan>POSTGRES_ANALYTICS_PORT</Cyan>
                                {'=...\n'}
                                <Cyan>POSTGRES_ANALYTICS_PASSWORD</Cyan>
                                {'=...\n\n'}
                                <Dim>{'# component venv\n'}</Dim>
                                <Cyan>VIRTUAL_ENV</Cyan>
                                {'=...\n\n'}
                                <Dim>{'# not passed down\n'}</Dim>
                                <Box component="span" sx={{ color: TERM.yellow }}>
                                    everything else in the shell
                                </Box>
                            </TerminalWindow>
                        </Grid>
                    </Grid>

                    <Grid container spacing={3} sx={{ mt: 2 }}>
                        <Grid item xs={12} md={6}>
                            <Paper variant="outlined" sx={{ p: 3, height: '100%' }}>
                                <Typography variant="overline" color="text.secondary">
                                    Before
                                </Typography>
                                <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
                                    State in the shell
                                </Typography>
                                <Box
                                    component="pre"
                                    sx={{
                                        bgcolor: TERM.bg,
                                        color: TERM.text,
                                        p: 2,
                                        borderRadius: 1,
                                        overflowX: 'auto',
                                        fontFamily: MONO,
                                        fontSize: '0.8125rem',
                                        my: 2,
                                    }}
                                >
                                    {'etl-init        # choose an environment\netl run'}
                                </Box>
                                <Typography variant="body2" color="text.secondary">
                                    An init command picked the environment, activated a venv, and
                                    loaded its config into the session. One environment could be
                                    active at a time, and every later command inherited it.
                                </Typography>
                            </Paper>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Paper variant="outlined" sx={{ p: 3, height: '100%', borderColor: ACCENT }}>
                                <Typography variant="overline" color="text.secondary">
                                    Now
                                </Typography>
                                <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
                                    State in the command
                                </Typography>
                                <Box
                                    component="pre"
                                    sx={{
                                        bgcolor: TERM.bg,
                                        color: TERM.text,
                                        p: 2,
                                        borderRadius: 1,
                                        overflowX: 'auto',
                                        fontFamily: MONO,
                                        fontSize: '0.8125rem',
                                        my: 2,
                                    }}
                                >
                                    {'mdc etl validate --env clinic\nmdc etl run --env clinic'}
                                </Box>
                                <Typography variant="body2" color="text.secondary">
                                    Each command names its stage. Config is validated, handed to
                                    one child process, and gone when that process exits. Nothing is
                                    left behind in the shell.
                                </Typography>
                            </Paper>
                        </Grid>
                    </Grid>
                </Box>

                <Divider sx={{ my: 6 }} />

                {/* Command surface */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={sectionTitleSx}>
                        Command surface
                    </Typography>
                    <Grid container spacing={3}>
                        {COMMAND_GROUPS.map(({ title, icon, commands, desc }) => (
                            <Grid item xs={12} md={6} key={title}>
                                <Card variant="outlined" sx={{ height: '100%' }}>
                                    <CardContent>
                                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5 }}>
                                            {React.cloneElement(icon, { sx: { color: ACCENT } })}
                                            <Typography variant="subtitle1" fontWeight="bold">
                                                {title}
                                            </Typography>
                                        </Box>
                                        <Box
                                            sx={{
                                                bgcolor: 'grey.50',
                                                border: 1,
                                                borderColor: 'divider',
                                                borderRadius: 1,
                                                p: 1.5,
                                                mb: 1.5,
                                            }}
                                        >
                                            {commands.map((command) => (
                                                <Typography
                                                    key={command}
                                                    sx={{
                                                        fontFamily: MONO,
                                                        fontSize: '0.8125rem',
                                                        lineHeight: 1.8,
                                                        overflowWrap: 'anywhere',
                                                        pl: '2ch',
                                                        textIndent: '-2ch',
                                                    }}
                                                >
                                                    {command}
                                                </Typography>
                                            ))}
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

                <Divider sx={{ my: 6 }} />

                {/* Guardrails */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={sectionTitleSx}>
                        Guardrails
                    </Typography>
                    <Typography variant="body1" color="text.secondary" sx={{ mb: 3, maxWidth: 720 }}>
                        Checks that run before anything is uploaded, restored, or rotated.
                    </Typography>
                    <Grid container spacing={3}>
                        {GUARDRAILS.map(({ title, body }) => (
                            <Grid item xs={12} md={6} key={title}>
                                <Paper
                                    variant="outlined"
                                    sx={{ p: 3, height: '100%', borderLeft: `4px solid ${ACCENT}` }}
                                >
                                    <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
                                        {title}
                                    </Typography>
                                    <Typography variant="body2" color="text.secondary">
                                        {body}
                                    </Typography>
                                </Paper>
                            </Grid>
                        ))}
                    </Grid>
                </Box>

                <Divider sx={{ my: 6 }} />

                {/* Tests and CI */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={sectionTitleSx}>
                        Tests and CI
                    </Typography>
                    <Paper variant="outlined" sx={{ p: 3, bgcolor: 'grey.50' }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2 }}>
                            <Science sx={{ color: ACCENT }} />
                            <Typography variant="subtitle1" fontWeight="bold">
                                Tested without the infrastructure
                            </Typography>
                        </Box>
                        <Typography variant="body1" paragraph>
                            More than 190 pytest tests cover stage validation, env composition,
                            child-process isolation, deploy preflights, SPA route lists, tunnels, and
                            secrets handling. AWS and subprocess calls are mocked.
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                            The GitHub Actions workflow runs on pull requests and pushes to main
                            that touch the CLI or the API and ETL settings loaders. It installs the
                            package on Ubuntu with no AWS credentials and no env files, runs pytest,
                            then smoke-tests <code>mdc --help</code>, <code>mdc version</code>, and{' '}
                            <code>mdc status --env local</code>. The status command must exit 0 there
                            even when its validation rows report <code>fail</code>.
                        </Typography>
                    </Paper>
                </Box>

                <Divider sx={{ my: 6 }} />

                {/* Source */}
                <Box sx={{ mb: 4 }}>
                    <Typography variant="h4" gutterBottom sx={sectionTitleSx}>
                        Source
                    </Typography>
                    <Grid container spacing={2}>
                        {SOURCE_LINKS.map(({ title, desc, href }) => (
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
                                        '&:hover': { boxShadow: 3, borderColor: ACCENT },
                                    }}
                                >
                                    <CardContent>
                                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                                            <GitHub sx={{ color: ACCENT, fontSize: 20 }} />
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
                            href={MDC_ROOT}
                            target="_blank"
                            rel="noopener noreferrer"
                            startIcon={<GitHub />}
                        >
                            Browse tools/mdc_cli on GitHub
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

export default MdcCli;
