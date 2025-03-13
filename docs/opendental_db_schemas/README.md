# OpenDental Schema Documentation

## Overview
This directory contains DDL (Data Definition Language) files from OpenDental's database schema. These files are valuable for understanding table structures, relationships, and indexing strategies.

## Directory Structure
```
opendental_schemas/
├── tables/                    # Table DDL files
│   ├── patient/              # Patient-related tables
│   │   ├── patient_ddl.sql
│   │   ├── patientlink_ddl.sql
│   │   └── patientnote_ddl.sql
│   ├── appointment/          # Appointment-related tables
│   │   ├── appointment_ddl.sql
│   │   └── appointmenttype_ddl.sql
│   ├── billing/              # Billing-related tables
│   │   ├── adjustment_ddl.sql
│   │   ├── payment_ddl.sql
│   │   └── payplan_ddl.sql
│   └── clinical/             # Clinical-related tables
│       ├── procedurelog_ddl.sql
│       └── periomeasure_ddl.sql
├── indexes/                  # Index documentation
│   ├── performance_indexes.md
│   └── business_indexes.md
└── relationships/           # Table relationships
    ├── patient_relationships.md
    └── billing_relationships.md
```

## Usage

### 1. Table Analysis
Use these DDL files to:
- Understand table structures
- Review indexing strategies
- Analyze foreign key relationships
- Plan query optimizations

### 2. Index Documentation
Key indexes for common queries:
```sql
-- Patient Search Optimization
KEY `indexPatNum` (`PatNum`),
KEY `indexProvNum` (`ProvNum`),
KEY `indexPNAmt` (`ProcNum`,`AdjAmt`)
```

### 3. Common Relationships
Important table relationships:
- patient → patientlink → family
- appointment → patient → provider
- procedurelog → patient → insurance

### 4. Performance Considerations
Important indexes for common operations:
1. Patient Lookup
   - `PatNum` (Primary Key)
   - `LName`, `FName` combination

2. Appointment Scheduling
   - `AptDateTime`
   - `ProvNum`, `AptDateTime` combination

3. Financial Tracking
   - `ProcNum`, `AdjAmt` combination
   - `SecDateTEdit`, `PatNum` combination

## Best Practices

1. **Index Usage**
   - Use covering indexes when possible
   - Consider index size vs. query performance
   - Monitor index utilization

2. **Query Optimization**
   - Leverage existing indexes
   - Use EXPLAIN for query analysis
   - Consider table partitioning

3. **Schema Updates**
   - Document index changes
   - Test performance impact
   - Monitor space usage

## Related Documentation
- [Database Connection Guide](../connections/README.md)
- [Query Optimization Guide](../performance/query_optimization.md)
- [Data Dictionary](../data_dictionary.md) 