-- SQL script to populate DBT model metadata for lineage tracking
-- This script creates the tables and populates them with metadata from the DBT models

-- Create the dbt_model_metadata table if it doesn't exist
CREATE TABLE IF NOT EXISTS dbt_model_metadata (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(255) UNIQUE NOT NULL,
    model_type VARCHAR(50) NOT NULL,
    schema_name VARCHAR(100) NOT NULL,
    description TEXT,
    business_context TEXT,
    technical_specs TEXT,
    dependencies JSONB,
    downstream_models JSONB,
    data_quality_notes TEXT,
    refresh_frequency VARCHAR(50),
    grain_definition TEXT,
    source_tables JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Create the dbt_metric_lineage table if it doesn't exist
CREATE TABLE IF NOT EXISTS dbt_metric_lineage (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(255) NOT NULL,
    metric_display_name VARCHAR(255) NOT NULL,
    source_model VARCHAR(255) NOT NULL,
    source_column VARCHAR(255),
    calculation_logic TEXT,
    business_definition TEXT,
    data_freshness VARCHAR(100),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Insert mart model metadata
INSERT INTO dbt_model_metadata (
    model_name, model_type, schema_name, description, business_context, 
    technical_specs, dependencies, refresh_frequency, grain_definition, is_active
) VALUES 
-- Revenue Lost Mart
('mart_revenue_lost', 'mart', 'raw_marts', 
 'Comprehensive revenue loss analysis and recovery opportunity tracking',
 'Enables revenue optimization through identification of lost revenue opportunities and recovery potential analysis',
 'Grain: One row per appointment + opportunity combination. Refresh: Daily',
 '["fact_appointment", "fact_claim", "fact_payment", "dim_provider", "dim_patient"]',
 'Daily',
 'One row per appointment + opportunity combination',
 TRUE),

-- Provider Performance Mart  
('mart_provider_performance', 'mart', 'raw_marts',
 'Provider performance analytics and efficiency tracking',
 'Enables provider performance monitoring, benchmarking, and operational optimization',
 'Grain: One row per date + provider combination. Refresh: Daily',
 '["fact_appointment", "fact_claim", "fact_payment", "dim_provider", "dim_patient"]',
 'Daily',
 'One row per date + provider combination',
 TRUE),

-- Patient Retention Mart
('mart_patient_retention', 'mart', 'raw_marts',
 'Patient lifecycle and retention analysis',
 'Enables patient retention optimization and churn risk assessment',
 'Grain: One row per date + patient combination. Refresh: Daily',
 '["fact_appointment", "fact_payment", "fact_claim", "dim_patient", "dim_provider"]',
 'Daily',
 'One row per date + patient combination',
 TRUE),

-- New Patient Mart
('mart_new_patient', 'mart', 'raw_marts',
 'New patient acquisition and onboarding analytics',
 'Enables new patient acquisition analysis and onboarding optimization',
 'Grain: One row per date + patient combination. Refresh: Daily',
 '["dim_patient", "fact_appointment", "fact_payment"]',
 'Daily',
 'One row per date + patient combination',
 TRUE),

-- AR Summary Mart
('mart_ar_summary', 'mart', 'raw_marts',
 'Accounts receivable aging and collection analytics',
 'Enables AR management and collection optimization',
 'Grain: One row per date + patient combination. Refresh: Daily',
 '["fact_payment", "fact_claim", "dim_patient", "dim_insurance"]',
 'Daily',
 'One row per date + patient combination',
 TRUE)

ON CONFLICT (model_name) DO UPDATE SET
    description = EXCLUDED.description,
    business_context = EXCLUDED.business_context,
    technical_specs = EXCLUDED.technical_specs,
    dependencies = EXCLUDED.dependencies,
    refresh_frequency = EXCLUDED.refresh_frequency,
    grain_definition = EXCLUDED.grain_definition,
    updated_at = CURRENT_TIMESTAMP;

-- Insert metric lineage mapping
INSERT INTO dbt_metric_lineage (
    metric_name, metric_display_name, source_model, source_column, 
    calculation_logic, business_definition, data_freshness, is_active
) VALUES 
-- Revenue metrics
('revenue_lost', 'Revenue Lost', 'mart_revenue_lost', 'lost_revenue',
 'Sum of lost_revenue column from mart_revenue_lost',
 'Total revenue lost due to missed appointments, billing errors, and collection issues',
 'Daily', TRUE),

('recovery_potential', 'Recovery Potential', 'mart_revenue_lost', 'estimated_recoverable_amount',
 'Sum of estimated_recoverable_amount column from mart_revenue_lost',
 'Potential revenue that can be recovered through follow-up actions',
 'Daily', TRUE),

-- Provider metrics
('active_providers', 'Active Providers', 'mart_provider_performance', 'provider_id',
 'Count of distinct provider_id from mart_provider_performance',
 'Number of providers who have scheduled or completed appointments',
 'Daily', TRUE),

('total_production', 'Total Production', 'mart_provider_performance', 'total_production',
 'Sum of total_production column from mart_provider_performance',
 'Total production value generated by providers',
 'Daily', TRUE),

('total_collection', 'Total Collection', 'mart_provider_performance', 'total_collections',
 'Sum of total_collections column from mart_provider_performance',
 'Total amount collected from patients and insurance',
 'Daily', TRUE),

('collection_rate', 'Collection Rate', 'mart_provider_performance', 'collection_efficiency',
 'Average of collection_efficiency column from mart_provider_performance',
 'Percentage of production that has been collected',
 'Daily', TRUE),

('completion_rate', 'Completion Rate', 'mart_provider_performance', 'completion_rate',
 'Average of completion_rate column from mart_provider_performance',
 'Percentage of scheduled appointments that were completed',
 'Daily', TRUE),

('no_show_rate', 'No-Show Rate', 'mart_provider_performance', 'daily_no_show_rate',
 'Average of daily_no_show_rate column from mart_provider_performance',
 'Percentage of appointments where patients did not show up',
 'Daily', TRUE),

('total_appointments', 'Total Appointments', 'mart_provider_performance', 'total_completed_appointments',
 'Sum of total_completed_appointments column from mart_provider_performance',
 'Total number of scheduled appointments',
 'Daily', TRUE),

('total_unique_patients', 'Total Unique Patients', 'mart_provider_performance', 'total_unique_patients',
 'Sum of total_unique_patients column from mart_provider_performance',
 'Total number of unique patients with appointments',
 'Daily', TRUE)

ON CONFLICT (metric_name) DO UPDATE SET
    metric_display_name = EXCLUDED.metric_display_name,
    source_model = EXCLUDED.source_model,
    source_column = EXCLUDED.source_column,
    calculation_logic = EXCLUDED.calculation_logic,
    business_definition = EXCLUDED.business_definition,
    data_freshness = EXCLUDED.data_freshness,
    last_updated = CURRENT_TIMESTAMP;

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_dbt_model_metadata_model_name ON dbt_model_metadata(model_name);
CREATE INDEX IF NOT EXISTS idx_dbt_model_metadata_model_type ON dbt_model_metadata(model_type);
CREATE INDEX IF NOT EXISTS idx_dbt_model_metadata_schema_name ON dbt_model_metadata(schema_name);
CREATE INDEX IF NOT EXISTS idx_dbt_model_metadata_is_active ON dbt_model_metadata(is_active);

CREATE INDEX IF NOT EXISTS idx_dbt_metric_lineage_metric_name ON dbt_metric_lineage(metric_name);
CREATE INDEX IF NOT EXISTS idx_dbt_metric_lineage_source_model ON dbt_metric_lineage(source_model);
CREATE INDEX IF NOT EXISTS idx_dbt_metric_lineage_is_active ON dbt_metric_lineage(is_active);

-- Grant permissions (adjust as needed for your environment)
-- GRANT SELECT ON dbt_model_metadata TO analytics_user;
-- GRANT SELECT ON dbt_metric_lineage TO analytics_user;
