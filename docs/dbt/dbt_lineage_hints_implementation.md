# DBT Lineage Hints Implementation Guide

## Overview

This document describes the implementation of DBT lineage hints in the dental clinic analytics UI.
 The feature provides optional "i" tooltips that tell staff which mart model powers each metric,
  helping build trust and governance in the data.

## Architecture

### Backend Components

1. **Database Models** (`api/models/dbt_metadata.py`)
   - `DBTModelMetadata`: Stores DBT model information and lineage
   - `DBTMetricLineage`: Maps UI metrics to their DBT model sources

2. **API Endpoints** (`api/routers/dbt_metadata.py`)
   - `GET /dbt/metric-lineage/{metric_name}`: Get lineage for specific metric
   - `GET /dbt/metric-lineage`: Get all metric lineage information
   - `GET /dbt/model-metadata/{model_name}`: Get DBT model metadata
   - `GET /dbt/models`: Get all DBT models with filtering

3. **API Types** (`api/api_types.py`)
   - `MetricLineageInfo`: Lineage information for UI display
   - `DBTModelMetadata`: DBT model metadata structure

### Frontend Components

1. **InfoTooltip Component** (`frontend/src/components/common/InfoTooltip.tsx`)
   - Displays comprehensive lineage information in a tooltip
   - Shows source model, business definition, calculation logic, dependencies
   - Includes data freshness and last updated information

2. **Enhanced KPICard** (`frontend/src/components/charts/KPICard.tsx`)
   - Added `metricName` and `showLineageTooltip` props
   - Integrates InfoTooltip component for lineage display

3. **API Service** (`frontend/src/services/api.ts`)
   - `dbtMetadataApi`: Functions to fetch lineage information
   - Integrated into main `apiService` object

## Metric Lineage Mapping

The following metrics are mapped to their DBT model sources:

### Revenue Metrics
- **revenue_lost**: `mart_revenue_lost.lost_revenue`
- **recovery_potential**: `mart_revenue_lost.estimated_recoverable_amount`

### Provider Metrics  
- **active_providers**: `mart_provider_performance.provider_id` (count distinct)
- **total_production**: `mart_provider_performance.total_production`
- **total_collection**: `mart_provider_performance.total_collections`
- **collection_rate**: `mart_provider_performance.collection_efficiency`
- **completion_rate**: `mart_provider_performance.completion_rate`
- **no_show_rate**: `mart_provider_performance.daily_no_show_rate`
- **total_appointments**: `mart_provider_performance.total_completed_appointments`
- **total_unique_patients**: `mart_provider_performance.total_unique_patients`

## Implementation Steps

### 1. Database Setup

Run the SQL script to create tables and populate metadata:

```sql
-- Execute the script
\i scripts/populate_dbt_metadata.sql
```

### 2. Backend Configuration

The DBT metadata router is already included in the main FastAPI application:

```python
# In api/main.py
from routers import dbt_metadata
app.include_router(dbt_metadata.router)
```

### 3. Frontend Integration

#### Using KPICard with Lineage Tooltips

```tsx
import KPICard from '../components/charts/KPICard';

<KPICard
    title="Revenue Lost"
    value={data.revenue.total_revenue_lost}
    format="currency"
    color="error"
    icon={<AttachMoney />}
    metricName="revenue_lost"           // Required for lineage
    showLineageTooltip={true}           // Enable tooltip
/>
```

#### Using InfoTooltip Directly

```tsx
import InfoTooltip from '../components/common/InfoTooltip';

<InfoTooltip 
    metricName="revenue_lost"
    lineageInfo={lineageData}  // Optional: pre-loaded data
/>
```

### 4. API Usage

#### Fetch Lineage for Specific Metric

```typescript
import { apiService } from '../services/api';

const lineageResponse = await apiService.dbt.getMetricLineage('revenue_lost');
if (lineageResponse.data) {
    console.log('Source Model:', lineageResponse.data.source_model);
    console.log('Business Definition:', lineageResponse.data.business_definition);
}
```

#### Fetch All Metric Lineage

```typescript
const allLineageResponse = await apiService.dbt.getAllMetricLineage();
```

## Tooltip Content

The InfoTooltip displays:

1. **Source Information**
   - Schema and model name
   - Model type and refresh frequency

2. **Business Context**
   - Business definition of the metric
   - How the metric is used in decision-making

3. **Technical Details**
   - Calculation logic and methodology
   - Data dependencies and upstream models

4. **Data Quality**
   - Data freshness information
   - Last updated timestamp
   - Data quality notes

5. **Documentation Links**
   - Link to DBT documentation (placeholder)

## Benefits

### For Staff Trust & Governance
- **Transparency**: Clear visibility into data sources and calculations
- **Accountability**: Understanding of data lineage and dependencies
- **Quality Assurance**: Knowledge of data freshness and quality metrics

### For Data Teams
- **Documentation**: Self-documenting metrics with business context
- **Change Management**: Easy identification of impact when models change
- **Onboarding**: New team members can quickly understand data sources

### For Business Users
- **Confidence**: Understanding of metric definitions and calculations
- **Context**: Business definitions help interpret metric values
- **Governance**: Clear audit trail of data sources and transformations

## Maintenance

### Adding New Metrics

1. **Update Backend Mapping**
   ```python
   # In api/routers/dbt_metadata.py
   METRIC_LINEAGE_MAPPING['new_metric'] = {
       'source_model': 'mart_new_model',
       'source_schema': 'raw_marts',
       'calculation_description': 'Description of calculation',
       'data_freshness': 'Daily',
       'business_definition': 'Business meaning',
       'dependencies': ['fact_table1', 'dim_table1'],
       'last_updated': datetime.now()
   }
   ```

2. **Update Database**
   ```sql
   INSERT INTO dbt_metric_lineage (metric_name, metric_display_name, ...)
   VALUES ('new_metric', 'New Metric Display Name', ...);
   ```

3. **Use in Frontend**
   ```tsx
   <KPICard
       metricName="new_metric"
       showLineageTooltip={true}
       // ... other props
   />
   ```

### Updating Existing Metrics

1. **Modify Backend Mapping**
2. **Update Database Records**
3. **Refresh Frontend Components**

## Future Enhancements

1. **Interactive Lineage Graphs**: Visual representation of data flow
2. **Impact Analysis**: Show downstream effects of model changes
3. **Data Quality Scores**: Real-time quality metrics in tooltips
4. **Automated Documentation**: Sync with DBT docs generation
5. **User Preferences**: Customizable tooltip content and display options

## Troubleshooting

### Common Issues

1. **Tooltip Not Showing**
   - Verify `metricName` prop is provided
   - Check `showLineageTooltip={true}`
   - Ensure metric exists in `METRIC_LINEAGE_MAPPING`

2. **API Errors**
   - Check database connection
   - Verify DBT metadata tables exist
   - Ensure proper permissions on tables

3. **Missing Lineage Data**
   - Run `scripts/populate_dbt_metadata.sql`
   - Check metric name spelling in mapping
   - Verify database records are active (`is_active = true`)

### Debug Mode

Enable debug logging in the InfoTooltip component:

```tsx
<InfoTooltip 
    metricName="revenue_lost"
    debug={true}  // Shows loading/error states
/>
```

## Security Considerations

1. **Database Permissions**: Limit access to DBT metadata tables
2. **API Rate Limiting**: Prevent excessive lineage requests
3. **Data Sensitivity**: Ensure no sensitive information in tooltips
4. **Audit Logging**: Track lineage information access

## Performance Considerations

1. **Caching**: Consider caching lineage data on frontend
2. **Lazy Loading**: Load lineage data only when tooltip is opened
3. **Database Indexing**: Ensure proper indexes on metric_name and model_name
4. **API Optimization**: Use connection pooling for database queries
