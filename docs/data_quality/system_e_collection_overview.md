# System E: Collections - Overview

## Purpose

The collections system provides models for tracking, managing, and analyzing collection activities
 for overdue accounts receivable (AR). It builds upon the AR analysis models in System D to provide
  actionable insights and operational support for collection efforts.

## Models

### 1. int_collection_campaigns

**Purpose**: Track collection campaigns that target specific patient segments based on AR aging
 and balance criteria.

**Key Features**:
- Campaign definition with target criteria (balance range, aging)
- Performance tracking of collection efforts
- Assignment to responsible users
- Campaign status lifecycle management (planned, active, completed)

**Usage Examples**:
- Creating targeted campaigns for high-balance accounts over 90 days
- Tracking collection rate by campaign
- Assigning campaigns to specific collection staff

### 2. int_collection_tasks

**Purpose**: Track individual collection tasks associated with campaigns.

**Key Features**:
- Task definition with due dates and responsible users
- Links to campaigns and patients
- Track task outcomes and payment promises
- Follow-up scheduling and management

**Usage Examples**:
- Assigning call tasks to collection staff
- Tracking payment promises resulting from collection calls
- Managing follow-up activities for incomplete tasks

### 3. int_collection_communication

**Purpose**: Track all communications related to collection efforts.

**Key Features**:
- Communication details (type, direction, content)
- Links to campaigns, tasks, and patients
- Outcome tracking and payment promises
- Follow-up scheduling and tracking

**Usage Examples**:
- Tracking all collection-related communications
- Analyzing patient responses to collection efforts
- Documenting promised payments

### 4. int_collection_metrics

**Purpose**: Provide metrics on collection performance at campaign, user, and overall levels.

**Key Features**:
- Collection rates by aging bucket
- Collection efficiency metrics
- Payment fulfillment and punctuality rates
- Communication and task activity tracking

**Usage Examples**:
- Analyzing collection effectiveness by campaign
- Evaluating collector performance
- Tracking payment promise fulfillment rates
- Optimizing collection strategies

## Relationships and Dependencies

The collection system models are interconnected as follows:

1. **Campaign → Tasks**: Each campaign generates multiple collection tasks
2. **Tasks → Communications**: Tasks lead to communications with patients
3. **Communications → Metrics**: Communication outcomes feed into metrics calculation

External dependencies:
- **System D (AR Analysis)**: Collection campaigns target accounts based on AR aging and balance information
- **Communication logs**: Collection communications are derived from the communication log data
- **Task management**: Collection tasks integrate with the task management system

## Usage Guidelines

1. **Campaign Creation**:
   - Define clear criteria for account targeting
   - Set realistic goals for collection amounts
   - Assign appropriate priority and staff

2. **Task Management**:
   - Create specific, actionable tasks with clear deadlines
   - Track outcomes and payment promises
   - Schedule follow-ups for incomplete tasks

3. **Communication Tracking**:
   - Document all collection-related communications
   - Record promised payment amounts and dates
   - Update actual payment information when received

4. **Metrics Analysis**:
   - Regularly review collection performance metrics
   - Compare metrics across campaigns and collectors
   - Use insights to refine collection strategies

## Implementation Details

The collection system models use a combination of incremental and table materialization strategies:

- **Campaigns**: Table materialization for fast access to campaign definitions
- **Tasks**: Incremental materialization to efficiently process new tasks
- **Communications**: Incremental materialization to handle ongoing communications
- **Metrics**: Incremental materialization with snapshot dates for trend analysis

Data is sourced from:
- Task records in the OpenDental system
- Communication logs from the OpenDental system
- AR balance information from System D models

## Future Enhancements

Planned enhancements for the collection system include:

1. Integration with automated communication systems
2. Advanced payment prediction modeling
3. Collection strategy optimization based on patient segments
4. Enhanced reporting dashboards for management
5. Integration with payment processing systems for real-time tracking