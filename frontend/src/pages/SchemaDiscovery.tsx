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
    Link,
    Paper,
    Divider,
} from '@mui/material';
import {
    ArrowBack,
    GitHub,
    Storage,
    AutoAwesome,
    TrendingUp,
    Security,
    Code as CodeIcon,
} from '@mui/icons-material';

const SchemaDiscovery: React.FC = () => {
    const navigate = useNavigate();

    return (
        <Box sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
            {/* Header */}
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
                        🔍 Schema Discovery & Replication
                    </Typography>
                    <Typography variant="h6" sx={{ opacity: 0.95, maxWidth: '800px' }}>
                        Automated schema introspection and configuration generation for 450+ OpenDental tables
                    </Typography>
                </Container>
            </Box>

            <Container maxWidth="lg" sx={{ py: 6 }}>
                {/* Overview */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Overview
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ fontSize: '1.1rem', lineHeight: 1.8, textAlign: 'left' }}>
                        <strong>analyze_opendental_schema.py</strong> is the single source of truth for schema analysis
                        in the ETL pipeline. It uses SQLAlchemy introspection to discover table structures, generate
                        performance-optimized configurations, and monitor slowly changing dimensions (SCDs) across
                        450+ OpenDental source tables.
                    </Typography>

                    <Grid container spacing={3} sx={{ mt: 2 }}>
                        <Grid item xs={12} md={4}>
                            <Card elevation={2}>
                                <CardContent>
                                    <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                        <Storage sx={{ fontSize: 32, color: 'primary.main', mr: 1 }} />
                                        <Typography variant="h6" fontWeight="bold">
                                            450+ Tables
                                        </Typography>
                                    </Box>
                                    <Typography variant="body2" color="text.secondary">
                                        Automatically discovers and analyzes all OpenDental source tables
                                    </Typography>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} md={4}>
                            <Card elevation={2}>
                                <CardContent>
                                    <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                        <AutoAwesome sx={{ fontSize: 32, color: 'primary.main', mr: 1 }} />
                                        <Typography variant="h6" fontWeight="bold">
                                            SCD Detection
                                        </Typography>
                                    </Box>
                                    <Typography variant="body2" color="text.secondary">
                                        Monitors schema changes and tracks slowly changing dimensions over time
                                    </Typography>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} md={4}>
                            <Card elevation={2}>
                                <CardContent>
                                    <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                                        <TrendingUp sx={{ fontSize: 32, color: 'primary.main', mr: 1 }} />
                                        <Typography variant="h6" fontWeight="bold">
                                            Performance Optimized
                                        </Typography>
                                    </Box>
                                    <Typography variant="body2" color="text.secondary">
                                        Generates batch sizes, priorities, and extraction strategies automatically
                                    </Typography>
                                </CardContent>
                            </Card>
                        </Grid>
                    </Grid>
                </Box>

                <Divider sx={{ my: 6 }} />

                {/* How It Works */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        How It Works
                    </Typography>
                    <Grid container spacing={3}>
                        <Grid item xs={12} md={6}>
                            <Card elevation={2} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', mb: 2 }}>
                                        1. Schema Discovery
                                    </Typography>
                                    <Typography variant="body2" color="text.secondary" paragraph>
                                        Uses SQLAlchemy inspector to discover:
                                    </Typography>
                                    <Box component="ul" sx={{ pl: 2, mb: 2 }}>
                                        <li>Table structures (columns, types, constraints)</li>
                                        <li>Primary keys and foreign key relationships</li>
                                        <li>Indexes and performance characteristics</li>
                                        <li>Estimated row counts and table sizes</li>
                                    </Box>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Card elevation={2} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', mb: 2 }}>
                                        2. Strategy Determination
                                    </Typography>
                                    <Typography variant="body2" color="text.secondary" paragraph>
                                        Intelligently determines extraction strategies:
                                    </Typography>
                                    <Box component="ul" sx={{ pl: 2, mb: 2 }}>
                                        <li><strong>Full table</strong> for small, frequently-changing tables</li>
                                        <li><strong>Incremental</strong> for large tables with timestamp columns</li>
                                        <li><strong>Batch sizing</strong> based on table size and performance</li>
                                        <li><strong>Priority assignment</strong> for processing order</li>
                                    </Box>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Card elevation={2} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', mb: 2 }}>
                                        3. SCD Monitoring
                                    </Typography>
                                    <Typography variant="body2" color="text.secondary" paragraph>
                                        Tracks schema changes over time:
                                    </Typography>
                                    <Box component="ul" sx={{ pl: 2, mb: 2 }}>
                                        <li>Generates schema hash for change detection</li>
                                        <li>Compares with previous configuration</li>
                                        <li>Detects added/removed tables and columns</li>
                                        <li>Identifies breaking changes automatically</li>
                                    </Box>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Card elevation={2} sx={{ height: '100%' }}>
                                <CardContent>
                                    <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', mb: 2 }}>
                                        4. Configuration Generation
                                    </Typography>
                                    <Typography variant="body2" color="text.secondary" paragraph>
                                        Outputs <code>tables.yml</code> with:
                                    </Typography>
                                    <Box component="ul" sx={{ pl: 2, mb: 2 }}>
                                        <li>Extraction strategies per table</li>
                                        <li>Incremental column definitions</li>
                                        <li>Performance metadata and batch sizes</li>
                                        <li>Monitoring thresholds and alerts</li>
                                    </Box>
                                </CardContent>
                            </Card>
                        </Grid>
                    </Grid>
                </Box>

                <Divider sx={{ my: 6 }} />

                {/* tables.yml Example */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Configuration Output: tables.yml
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ fontSize: '1.1rem', lineHeight: 1.8, textAlign: 'left', mb: 3 }}>
                        The script generates <code>etl_pipeline/config/tables.yml</code>, which serves as the
                        configuration file for the ETL pipeline. Each table entry includes extraction strategy,
                        performance metadata, and incremental loading configuration.
                    </Typography>

                    <Paper
                        elevation={2}
                        sx={{
                            p: 3,
                            bgcolor: 'grey.900',
                            color: 'grey.100',
                            overflow: 'auto',
                            borderRadius: 2,
                        }}
                    >
                        <Typography variant="caption" color="grey.400" sx={{ mb: 1, display: 'block', textAlign: 'left' }}>
                            Example: adjustment table configuration
                        </Typography>
                        <Box
                            component="pre"
                            sx={{
                                fontFamily: 'monospace',
                                fontSize: '0.875rem',
                                lineHeight: 1.6,
                                m: 0,
                                whiteSpace: 'pre-wrap',
                                textAlign: 'left',
                            }}
                        >
                            {`adjustment:
  table_name: adjustment
  extraction_strategy: incremental
  estimated_rows: 260127
  estimated_size_mb: 65.83
  batch_size: 50000
  incremental_columns:
    - AdjNum
    - SecDateTEdit
  incremental_strategy: and_logic
  primary_incremental_column: AdjNum
  performance_category: medium
  processing_priority: medium
  time_gap_threshold_days: 30
  estimated_processing_time_minutes: 1.7
  memory_requirements_mb: 73
  monitoring:
    alert_on_failure: true
    alert_on_slow_extraction: true
    performance_threshold_records_per_second: 2000
    memory_alert_threshold_mb: 146
  schema_hash: 642816942962377761
  last_analyzed: '2025-10-07T10:28:34.450152'
  primary_key: AdjNum`}
                        </Box>
                    </Paper>

                    <Typography variant="body2" color="text.secondary" sx={{ mt: 2, textAlign: 'left' }}>
                        <strong>Key Fields:</strong>
                    </Typography>
                    <Box component="ul" sx={{ pl: 3, mt: 1, textAlign: 'left' }}>
                        <li>
                            <strong>extraction_strategy</strong>: Determines how data is extracted (full_table vs incremental)
                        </li>
                        <li>
                            <strong>incremental_columns</strong>: Columns used for change detection (e.g., timestamp columns)
                        </li>
                        <li>
                            <strong>schema_hash</strong>: Hash fingerprint for detecting schema changes (SCD monitoring)
                        </li>
                        <li>
                            <strong>performance_category</strong>: Size-based category (tiny, small, medium, large) for batch sizing
                        </li>
                        <li>
                            <strong>monitoring</strong>: Thresholds for alerting on failures or slow extractions
                        </li>
                    </Box>
                </Box>

                <Divider sx={{ my: 6 }} />

                {/* SCD Detection */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Slowly Changing Dimension (SCD) Detection
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ fontSize: '1.1rem', lineHeight: 1.8, textAlign: 'left' }}>
                        OpenDental's schema evolves over time as the software is updated. The analyzer automatically
                        detects and tracks these changes to prevent ETL failures and maintain data quality.
                    </Typography>

                    <Grid container spacing={3} sx={{ mt: 2 }}>
                        <Grid item xs={12} md={6}>
                            <Card elevation={2} sx={{ bgcolor: 'success.light', color: 'success.contrastText' }}>
                                <CardContent>
                                    <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold' }}>
                                        ✅ Schema Hash Comparison
                                    </Typography>
                                    <Typography variant="body2">
                                        Generates MD5 hash of schema structure (tables, columns, primary keys).
                                        Compares current hash with previous configuration to detect any changes.
                                    </Typography>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Card elevation={2} sx={{ bgcolor: 'info.light', color: 'info.contrastText' }}>
                                <CardContent>
                                    <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold' }}>
                                        📊 Change Detection
                                    </Typography>
                                    <Typography variant="body2">
                                        Automatically detects:
                                        <br />• Added/removed tables
                                        <br />• Added/removed columns
                                        <br />• Primary key changes
                                        <br />• Breaking changes requiring attention
                                    </Typography>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Card elevation={2} sx={{ bgcolor: 'warning.light', color: 'warning.contrastText' }}>
                                <CardContent>
                                    <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold' }}>
                                        ⚠️ Breaking Changes
                                    </Typography>
                                    <Typography variant="body2">
                                        Identifies critical changes that will cause ETL failures:
                                        <br />• Removed columns referenced in config
                                        <br />• Primary key changes affecting incremental logic
                                        <br />• Removed tables
                                    </Typography>
                                </CardContent>
                            </Card>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <Card elevation={2} sx={{ bgcolor: 'secondary.light', color: 'secondary.contrastText' }}>
                                <CardContent>
                                    <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold' }}>
                                        📝 Changelog Generation
                                    </Typography>
                                    <Typography variant="body2">
                                        Generates human-readable changelog files:
                                        <br />• Markdown changelog for review
                                        <br />• JSON analysis reports
                                        <br />• Configuration backups for rollback
                                    </Typography>
                                </CardContent>
                            </Card>
                        </Grid>
                    </Grid>

                    <Paper elevation={2} sx={{ p: 3, mt: 3, bgcolor: 'grey.50' }}>
                        <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold' }}>
                            Example: Schema Change Detection Logic
                        </Typography>
                        <Box
                            component="pre"
                            sx={{
                                fontFamily: 'monospace',
                                fontSize: '0.875rem',
                                lineHeight: 1.6,
                                bgcolor: 'grey.900',
                                color: 'grey.100',
                                p: 2,
                                borderRadius: 1,
                                overflow: 'auto',
                                textAlign: 'left',
                            }}
                        >
                            {`def compare_with_previous_schema(current_config, previous_config_path):
    """Compare current schema with previous to detect SCD changes."""
    changes = {
        'added_tables': [],
        'removed_tables': [],
        'added_columns': {},
        'removed_columns': {},
        'schema_hash_changed': False,
        'breaking_changes': []
    }
    
    # Compare schema hashes
    current_hash = current_config['metadata']['schema_hash']
    previous_hash = previous_config['metadata']['schema_hash']
    changes['schema_hash_changed'] = current_hash != previous_hash
    
    # Detect table changes
    current_tables = set(current_config['tables'].keys())
    previous_tables = set(previous_config['tables'].keys())
    changes['added_tables'] = list(current_tables - previous_tables)
    changes['removed_tables'] = list(previous_tables - current_tables)
    
    # Detect column changes in common tables
    for table_name in current_tables & previous_tables:
        current_cols = set(current_config['tables'][table_name]['incremental_columns'])
        previous_cols = set(previous_config['tables'][table_name]['incremental_columns'])
        
        added = current_cols - previous_cols
        removed = previous_cols - current_cols
        
        if removed:
            changes['breaking_changes'].append({
                'type': 'removed_columns',
                'table': table_name,
                'columns': list(removed),
                'severity': 'high'
            })
    
    return changes`}
                        </Box>
                    </Paper>
                </Box>

                <Divider sx={{ my: 6 }} />

                {/* Integration with ETL Pipeline */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Integration with ETL Pipeline
                    </Typography>
                    <Typography variant="body1" paragraph sx={{ fontSize: '1.1rem', lineHeight: 1.8, textAlign: 'left' }}>
                        The schema analyzer is the foundation of the ETL pipeline. It runs periodically (via Airflow
                        or manually) to keep the configuration up-to-date and detect schema changes.
                    </Typography>

                    <Grid container spacing={3} sx={{ mt: 2 }}>
                        <Grid item xs={12}>
                            <Card elevation={2}>
                                <CardContent>
                                    <Typography variant="h6" gutterBottom sx={{ fontWeight: 'bold', mb: 2, textAlign: 'left' }}>
                                        Pipeline Flow
                                    </Typography>
                                    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, textAlign: 'left' }}>
                                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                                            <Box
                                                sx={{
                                                    width: 40,
                                                    height: 40,
                                                    borderRadius: '50%',
                                                    bgcolor: 'primary.main',
                                                    color: 'white',
                                                    display: 'flex',
                                                    alignItems: 'center',
                                                    justifyContent: 'center',
                                                    fontWeight: 'bold',
                                                }}
                                            >
                                                1
                                            </Box>
                                            <Box>
                                                <Typography variant="subtitle1" fontWeight="bold">
                                                    Schema Analysis
                                                </Typography>
                                                <Typography variant="body2" color="text.secondary">
                                                    analyze_opendental_schema.py discovers all tables and generates tables.yml
                                                </Typography>
                                            </Box>
                                        </Box>
                                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                                            <Box
                                                sx={{
                                                    width: 40,
                                                    height: 40,
                                                    borderRadius: '50%',
                                                    bgcolor: 'primary.main',
                                                    color: 'white',
                                                    display: 'flex',
                                                    alignItems: 'center',
                                                    justifyContent: 'center',
                                                    fontWeight: 'bold',
                                                }}
                                            >
                                                2
                                            </Box>
                                            <Box>
                                                <Typography variant="subtitle1" fontWeight="bold">
                                                    Configuration Loading
                                                </Typography>
                                                <Typography variant="body2" color="text.secondary">
                                                    SimpleMySQLReplicator reads tables.yml to determine extraction strategies
                                                </Typography>
                                            </Box>
                                        </Box>
                                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                                            <Box
                                                sx={{
                                                    width: 40,
                                                    height: 40,
                                                    borderRadius: '50%',
                                                    bgcolor: 'primary.main',
                                                    color: 'white',
                                                    display: 'flex',
                                                    alignItems: 'center',
                                                    justifyContent: 'center',
                                                    fontWeight: 'bold',
                                                }}
                                            >
                                                3
                                            </Box>
                                            <Box>
                                                <Typography variant="subtitle1" fontWeight="bold">
                                                    Data Extraction
                                                </Typography>
                                                <Typography variant="body2" color="text.secondary">
                                                    Uses extraction_strategy and incremental_columns from config to extract data
                                                </Typography>
                                            </Box>
                                        </Box>
                                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                                            <Box
                                                sx={{
                                                    width: 40,
                                                    height: 40,
                                                    borderRadius: '50%',
                                                    bgcolor: 'primary.main',
                                                    color: 'white',
                                                    display: 'flex',
                                                    alignItems: 'center',
                                                    justifyContent: 'center',
                                                    fontWeight: 'bold',
                                                }}
                                            >
                                                4
                                            </Box>
                                            <Box>
                                                <Typography variant="subtitle1" fontWeight="bold">
                                                    Change Detection
                                                </Typography>
                                                <Typography variant="body2" color="text.secondary">
                                                    On next run, compares schema_hash to detect changes and alert on breaking changes
                                                </Typography>
                                            </Box>
                                        </Box>
                                    </Box>
                                </CardContent>
                            </Card>
                        </Grid>
                    </Grid>
                </Box>

                <Divider sx={{ my: 6 }} />

                {/* Key Features */}
                <Box sx={{ mb: 6 }}>
                    <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', mb: 3 }}>
                        Key Features
                    </Typography>
                    <Grid container spacing={2}>
                        <Grid item xs={12} sm={6} md={4}>
                            <Paper elevation={1} sx={{ p: 2, height: '100%' }}>
                                <CodeIcon sx={{ fontSize: 32, color: 'primary.main', mb: 1 }} />
                                <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
                                    SQLAlchemy Introspection
                                </Typography>
                                <Typography variant="body2" color="text.secondary">
                                    Uses SQLAlchemy inspector for reliable schema discovery across database versions
                                </Typography>
                            </Paper>
                        </Grid>
                        <Grid item xs={12} sm={6} md={4}>
                            <Paper elevation={1} sx={{ p: 2, height: '100%' }}>
                                <Security sx={{ fontSize: 32, color: 'primary.main', mb: 1 }} />
                                <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
                                    Automatic Backups
                                </Typography>
                                <Typography variant="body2" color="text.secondary">
                                    Backs up existing configuration before generating new one for rollback capability
                                </Typography>
                            </Paper>
                        </Grid>
                        <Grid item xs={12} sm={6} md={4}>
                            <Paper elevation={1} sx={{ p: 2, height: '100%' }}>
                                <TrendingUp sx={{ fontSize: 32, color: 'primary.main', mb: 1 }} />
                                <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
                                    Performance Optimization
                                </Typography>
                                <Typography variant="body2" color="text.secondary">
                                    Calculates optimal batch sizes, priorities, and memory requirements automatically
                                </Typography>
                            </Paper>
                        </Grid>
                        <Grid item xs={12} sm={6} md={4}>
                            <Paper elevation={1} sx={{ p: 2, height: '100%' }}>
                                <AutoAwesome sx={{ fontSize: 32, color: 'primary.main', mb: 1 }} />
                                <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
                                    Incremental Detection
                                </Typography>
                                <Typography variant="body2" color="text.secondary">
                                    Automatically identifies timestamp columns and primary keys for incremental loading
                                </Typography>
                            </Paper>
                        </Grid>
                        <Grid item xs={12} sm={6} md={4}>
                            <Paper elevation={1} sx={{ p: 2, height: '100%' }}>
                                <Storage sx={{ fontSize: 32, color: 'primary.main', mb: 1 }} />
                                <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
                                    Batch Processing
                                </Typography>
                                <Typography variant="body2" color="text.secondary">
                                    Processes tables in batches of 5 for efficiency and rate limiting
                                </Typography>
                            </Paper>
                        </Grid>
                        <Grid item xs={12} sm={6} md={4}>
                            <Paper elevation={1} sx={{ p: 2, height: '100%' }}>
                                <GitHub sx={{ fontSize: 32, color: 'primary.main', mb: 1 }} />
                                <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
                                    dbt Integration
                                </Typography>
                                <Typography variant="body2" color="text.secondary">
                                    Discovers dbt models and marks tables as modeled in configuration
                                </Typography>
                            </Paper>
                        </Grid>
                    </Grid>
                </Box>

                {/* Links */}
                <Box sx={{ mt: 6, textAlign: 'center' }}>
                    <Button
                        variant="contained"
                        size="large"
                        component={Link}
                        href="https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/etl_pipeline/scripts"
                        target="_blank"
                        rel="noopener noreferrer"
                        startIcon={<GitHub />}
                        sx={{ mr: 2 }}
                    >
                        View Source Code
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
            </Container>
        </Box>
    );
};

export default SchemaDiscovery;

