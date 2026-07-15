import React from 'react';
import { Box, Container, Typography, Link, Divider } from '@mui/material';
import { Link as RouterLink } from 'react-router-dom';

const AgentProfile: React.FC = () => {
    return (
        <Box component="main" sx={{ minHeight: '100vh', bgcolor: 'background.default', py: 6 }}>
            <Container maxWidth="md">
                <Typography variant="h3" component="h1" gutterBottom>
                    Agent-Friendly Portfolio Profile (Benjamin Rains)
                </Typography>

                <Typography variant="body1" sx={{ mb: 2 }}>
                    <strong>Name</strong>: Benjamin Rains
                </Typography>
                <Typography variant="body1" sx={{ mb: 2 }}>
                    <strong>Primary Role</strong>: Analytics Engineer / Data Engineer (Healthcare)
                </Typography>
                <Typography variant="body1" sx={{ mb: 2 }}>
                    <strong>Location</strong>: Remote, United States
                </Typography>
                <Typography variant="body1" sx={{ mb: 4 }}>
                    <strong>Email</strong>: <Link href="mailto:rains.bp@gmail.com">rains.bp@gmail.com</Link>
                </Typography>

                <Divider sx={{ my: 4 }} />

                <Typography variant="h5" component="h2" gutterBottom>
                    1. Headline Summary
                </Typography>
                <Typography variant="body1" paragraph>
                    I design, build, and operate modern analytics platforms for healthcare organizations, with a focus on
                    OpenDental → PostgreSQL → dbt → FastAPI → React. My core project is a production analytics platform
                    for a multi-clinic dental organization, delivering automated, trusted KPIs across clinical, operational,
                    and financial domains.
                </Typography>

                <Divider sx={{ my: 4 }} />

                <Typography variant="h5" component="h2" gutterBottom>
                    2. Target Roles
                </Typography>
                <Typography variant="body1" component="ul" sx={{ pl: 3 }}>
                    <li>Analytics Engineer</li>
                    <li>Data Engineer</li>
                    <li>Analytics Platform / Data Platform Engineer</li>
                    <li>Healthcare Analytics Engineer (Dental, Medical)</li>
                </Typography>

                <Divider sx={{ my: 4 }} />

                <Typography variant="h5" component="h2" gutterBottom>
                    3. Core Technical Stack
                </Typography>
                <Typography variant="body1" component="ul" sx={{ pl: 3 }}>
                    <li><strong>Languages</strong>: Python, SQL, TypeScript</li>
                    <li><strong>Data &amp; Warehousing</strong>: PostgreSQL, MariaDB/MySQL (OpenDental), dimensional modeling</li>
                    <li><strong>Transformation</strong>: dbt Core (staging → intermediate → marts, tests, lineage)</li>
                    <li><strong>Backend</strong>: FastAPI (REST APIs, Pydantic models, versioned endpoints)</li>
                    <li><strong>Frontend</strong>: React, TypeScript, Material-UI, Recharts, Zustand</li>
                    <li><strong>Infrastructure &amp; Ops</strong>: Docker, AWS (S3, CloudFront, EC2, RDS, Lambda, EventBridge), CI/CD, Airflow</li>
                    <li><strong>ML / LLM</strong>: OpenAI Whisper, Anthropic Claude (LLM analysis pipeline)</li>
                </Typography>

                <Divider sx={{ my: 4 }} />

                <Typography variant="h5" component="h2" gutterBottom>
                    4. Primary System: dbt Dental Clinic Analytics Platform
                </Typography>
                <Typography variant="subtitle1" gutterBottom>
                    Problem
                </Typography>
                <Typography variant="body1" paragraph>
                    The dental organization had no centralized analytics, relied on manual Excel-style reporting, and suffered
                    from unreliable KPIs. Leadership could not easily see revenue leakage, collections performance, or provider
                    productivity.
                </Typography>
                <Typography variant="subtitle1" gutterBottom>
                    System Built
                </Typography>
                <Typography variant="body1" component="ul" sx={{ pl: 3 }}>
                    <li>Metadata-driven ELT from OpenDental (MariaDB) into PostgreSQL</li>
                    <li>Incremental replication strategies (full, incremental, incremental_chunked) with schema drift handling</li>
                    <li>
                        dbt warehouse with clean layers:
                        <ul>
                            <li>Raw (replicated OpenDental sources)</li>
                            <li>Staging (typed, cleaned tables)</li>
                            <li>Intermediate (business logic and joins, derived entities)</li>
                            <li>Marts (KPI and dimensional models ready for analytics)</li>
                        </ul>
                    </li>
                    <li>API-backed analytics layer using FastAPI (versioned endpoints, documented via OpenAPI)</li>
                    <li>React frontend with dashboards and KPI cards for financial, operational, and clinical analytics</li>
                </Typography>
                <Typography variant="subtitle1" gutterBottom>
                    Outcomes
                </Typography>
                <Typography variant="body1" component="ul" sx={{ pl: 3 }}>
                    <li>Automated daily KPI delivery instead of manual reporting</li>
                    <li>Identification of revenue leakage and collections opportunities</li>
                    <li>Improved leadership prioritization of collections and provider performance</li>
                    <li>Significant reduction in manual reporting effort</li>
                    <li>Production-grade platform with tests, monitoring, and cost controls</li>
                </Typography>

                <Divider sx={{ my: 4 }} />

                <Typography variant="h5" component="h2" gutterBottom>
                    5. Architecture (Text Version)
                </Typography>
                <Typography
                    component="pre"
                    variant="body2"
                    sx={{ whiteSpace: 'pre-wrap', fontFamily: 'monospace', bgcolor: 'grey.50', p: 2, borderRadius: 1 }}
                >{`End-to-end data flow:

1. Source: OpenDental MariaDB (450+ source tables)
2. ELT / Replication:
   - Python-based ELT and schema discovery
   - SQLAlchemy-powered introspection and metadata-driven extraction
   - Incremental and backfill-safe loading logic
   - Schema drift detection and type mapping
3. Warehouse (PostgreSQL):
   - Raw schema: replicated OpenDental tables
   - Staging schema: typed, cleaned tables
   - Intermediate schema: business logic, joins, derived entities
   - Marts schema: KPI and dimensional models ready for analytics
4. Transformation (dbt Core):
   - 150+ dbt models
   - Tests and documentation gating deployments
   - Lineage graph showing full chain from raw sources to marts
5. Analytics Service (FastAPI):
   - REST API exposing KPIs and analytic queries
   - Pydantic models, versioned endpoints, OpenAPI documentation
   - Access primarily over marts schema (analytics-ready tables)
6. Frontend (React + TypeScript):
   - Dashboards for revenue, AR aging, and provider performance
   - KPI cards, trends, and opportunity views
   - State management using Zustand
7. Deployment (AWS):
   - CloudFront + S3: static React frontend and assets
   - EC2 + Docker: FastAPI backend and services
   - RDS PostgreSQL: analytics warehouse
   - CI/CD: automated build, test, and deploy pipelines
   - Cost optimization via EC2 scheduling (Lambda + EventBridge), region consolidation

Parallel pipeline – Consult Audio Whisper Pipeline:
  - Input: dental consultation recordings
  - Stages:
    1. Audio Transcription: OpenAI Whisper → timestamped text (.txt, .tsv, .srt, .vtt)
    2. Transcript Cleaning: dental-specific rules and terminology normalization
    3. LLM Analysis: Anthropic Claude → summaries, treatment discussions, QA, provider coaching signals
    4. HTML Conversion: markdown results rendered into responsive HTML reports
    5. PDF Conversion: print-ready PDFs using ReportLab
  - Output: clinician-friendly documentation for treatment acceptance tracking and coaching; optional structured TSV/JSON for analytics and optional warehouse load.`}</Typography>

                <Divider sx={{ my: 4 }} />

                <Typography variant="h5" component="h2" gutterBottom>
                    6. Production Evidence
                </Typography>
                <Typography variant="subtitle1" gutterBottom>
                    Pipelines and Warehouse
                </Typography>
                <Typography variant="body1" component="ul" sx={{ pl: 3 }}>
                    <li>Daily scheduled ELT processing 450+ OpenDental tables</li>
                    <li>150+ dbt models with tests and lineage documentation</li>
                    <li>Three-phase ETL: MySQL → replication → PostgreSQL raw; full / incremental / incremental_chunked strategies</li>
                    <li>Schema conversion, type mapping, connection pooling, retries, and backfill-safe incremental loads</li>
                </Typography>
                <Typography variant="subtitle1" gutterBottom>
                    Deployment and Operations
                </Typography>
                <Typography variant="body1" component="ul" sx={{ pl: 3 }}>
                    <li>CI/CD pipelines gating deployments (tests must pass before production)</li>
                    <li>Dockerized services for consistent environments</li>
                    <li>FastAPI serving versioned analytics endpoints</li>
                    <li>Deployed on AWS: S3, EC2, RDS, CloudFront</li>
                    <li>Cost optimization via EC2 scheduling (Lambda + EventBridge), region consolidation, ~73% cost reduction</li>
                </Typography>

                <Divider sx={{ my: 4 }} />

                <Typography variant="h5" component="h2" gutterBottom>
                    7. Key Skills in Practice
                </Typography>
                <Typography variant="body1" component="ul" sx={{ pl: 3 }}>
                    <li>Data Engineering: dbt, SQL, PostgreSQL, MySQL, ELT/ETL, dimensional modeling</li>
                    <li>Core Technologies: Python, TypeScript, SQLAlchemy, FastAPI, React</li>
                    <li>Analytics &amp; Frontend: Material-UI, Recharts, Zustand, dashboard design and KPIs</li>
                    <li>Tools &amp; Cloud: AWS, Docker, Git, CI/CD, Airflow, environment management automation</li>
                    <li>ML / LLM Integration: Whisper transcription, Claude-based summarization and analysis, multi-format reporting</li>
                </Typography>

                <Divider sx={{ my: 4 }} />

                <Typography variant="h5" component="h2" gutterBottom>
                    8. Links and Artifacts
                </Typography>
                <Typography variant="body1" component="ul" sx={{ pl: 3 }}>
                    <li>
                        Portfolio:{' '}
                        <Link href="https://dbtdentalclinic.com" target="_blank" rel="noopener noreferrer">
                            https://dbtdentalclinic.com
                        </Link>
                    </li>
                    <li>
                        GitHub Repo (full system):{' '}
                        <Link href="https://github.com/BenjaminRains/dbt_dental_clinic" target="_blank" rel="noopener noreferrer">
                            https://github.com/BenjaminRains/dbt_dental_clinic
                        </Link>
                    </li>
                    <li>
                        dbt Documentation and Lineage:{' '}
                        <Link href="https://dbtdentalclinic.com/dbt-docs/" target="_blank" rel="noopener noreferrer">
                            https://dbtdentalclinic.com/dbt-docs/
                        </Link>
                    </li>
                    <li>
                        API Documentation (FastAPI):{' '}
                        <Link href="https://api.dbtdentalclinic.com/docs" target="_blank" rel="noopener noreferrer">
                            https://api.dbtdentalclinic.com/docs
                        </Link>
                    </li>
                    <li>
                        Consult Audio Pipeline Code:{' '}
                        <Link
                            href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/consult_audio_pipe"
                            target="_blank"
                            rel="noopener noreferrer"
                        >
                            https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/consult_audio_pipe
                        </Link>
                    </li>
                </Typography>

                <Divider sx={{ my: 4 }} />

                <Typography variant="h5" component="h2" gutterBottom>
                    9. One-Sentence Classifier Summary
                </Typography>
                <Typography variant="body1" paragraph>
                    Benjamin Rains is an Analytics Engineer / Data Engineer specializing in healthcare (dental) ELT and analytics
                    platforms, building tested dbt warehouses, API-backed metrics services, and production dashboards on AWS
                    (S3, CloudFront, EC2, RDS) using Python, SQL, dbt, FastAPI, and React.
                </Typography>

                <Divider sx={{ my: 4 }} />

                <Typography variant="body2">
                    Return to the portfolio:{' '}
                    <Link component={RouterLink} to="/">
                        View visual portfolio
                    </Link>
                </Typography>
            </Container>
        </Box>
    );
};

export default AgentProfile;

