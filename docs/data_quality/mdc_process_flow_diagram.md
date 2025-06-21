# Dental Practice Business Process Flow

This document describes the business process flow represented in the Mermaid diagram 
(`mdc_process_flow_diagram.mmd`). The diagram visualizes the complete patient journey through our 
dental practice, from initial appointment to payment collection and account resolution.

## Process Overview

The dental practice workflow is divided into seven interconnected systems:

1. **System A: Fee Processing & Verification**
2. **System B: Insurance Processing**
3. **System C: Payment Allocation & Reconciliation**
4. **System D: AR Analysis**
5. **System E: Collection Process**
6. **System F: Patient–Clinic Communications**
7. **System G: Scheduling & Referrals**

These systems work together to support the complete patient financial journey.

## Detailed System Descriptions

### System A: Fee Processing & Verification

This system handles the creation and verification of procedure fees.

**Key Processes:**
- Setting initial clinic fees
- Checking fee schedules for contracted rates
- Fee validation
- Fee decision-making when schedules don't exist

**Data Stores:**
- ProcedureLog
- Contracted Rates

**Business Rules:**
- Fees must be validated before being finalized
- Contracted rates take precedence over standard fees
- Invalid fees must be corrected before proceeding

### System B: Insurance Processing

This system manages the entire insurance claim lifecycle.

**Key Processes:**
- Creating claims from procedures
- Analyzing and submitting claim batches
- Generating ClaimProc records
- Estimating insurance payments
- Tracking claim status
- Handling claim rejections and errors

**Data Stores:**
- Claim Records
- ClaimProc Records

**External Entities:**
- Insurance Carrier/Plan

**Business Rules:**
- Claims must be batched efficiently
- Rejected claims need proper error handling
- Unresolvable claims must be closed and tagged

### System C: Payment Allocation & Reconciliation

This system processes and allocates payments from both patients and insurance.

**Key Processes:**
- Creating payment splits
- Classifying split types (Regular, Unearned Revenue, Treatment Plan Deposit)
- Managing transfers
- Analyzing payment patterns
- Validating payment rules
- Reconciling payments
- Verifying transaction dates for AR processing

**Data Stores:**
- Payment Records
- PaySplit Records

**Business Rules:**
- Payments can be split across multiple procedures
- Different split types follow different accounting rules
- Transaction dates determine inclusion in AR calculations
- Payments must reconcile at the end of each day

### System D: AR Analysis

This system analyzes accounts receivable aging.

**Key Processes:**
- Categorizing AR aging into buckets
- Monitoring AR metrics
- Generating alerts for collection

**Data Stores:**
- Current AR
- 30-60d AR
- 60-90d AR
- 90+d AR

**Business Rules:**
- AR is categorized by days outstanding
- AR trends trigger alerts for the collection system

### System E: Collection Process

This system manages the collection of outstanding balances.

**Key Processes:**
- Checking collection status
- Taking collection actions
- Sending notices (First, Second, Final)
- Setting up payment plans
- Escalating collection options
- Recording successful collections

**Business Rules:**
- Collection follows a progressive notice sequence
- Payment plans are an alternative to standard collection
- Final notices that receive no response require escalation

### System F: Patient–Clinic Communications

This system manages all patient communications.

**Key Processes:**
- Initiating communication via multiple channels
- Sending reminders and notifications
- Tracking responses

**Data Stores:**
- CommLog

**Business Rules:**
- Communications use various channels (Phone, SMS, Email, Portal)
- Communications are categorized by purpose
- Response tracking is required for all communications

### System G: Scheduling & Referrals

This system handles appointment scheduling and referral management.

**Key Processes:**
- Creating and updating appointments
- Sending no-show notices
- Creating and managing referrals (inbound/outbound)

**Data Stores:**
- Appointment Table
- Referral Records

**External Entities:**
- External Provider

**Business Rules:**
- Completed appointments link to procedures
- No-shows trigger patient communications
- Referrals may require insurance pre-authorization

## Cross-System Flows

The diagram shows several important cross-system flows:

1. **Patient Journey Initialization**
   - Patient → Scheduling → Appointment → Procedure

2. **Financial Journey**
   - Procedure → Fee Processing → Insurance Processing → Payment Allocation

3. **Collection Journey**
   - AR Analysis → Collection Process → Payment Allocation

4. **Communication Flows**
   - Communications connect to Scheduling, AR Analysis, and Collections

5. **Journey Completion**
   - Successful collections or resolutions lead to journey closure

## Critical Paths

The diagram highlights critical paths in red, including:
- Final Notice → Escalate Options
- Unresolvable Claims → Close & Tag

These represent high-risk points in the patient journey that require special attention.

## Implementation in Data Models

This process flow directly informs our data modeling approach:

1. Each system corresponds to specific intermediate models
2. Cross-system flows are represented by models that join data across systems
3. Critical paths inform our alerting and monitoring priorities
4. Data store entities map to specific tables in our OpenDental schema

The `mdc_process_flow_diagram.mmd` file provides the Mermaid syntax for visualizing this entire 
process.

## Relationship to DBT Models

Our DBT project structure mirrors this business process flow:

1. **Staging Models**: Raw data from OpenDental
2. **Intermediate Models**: System-specific data transformations
3. **Mart Models**: Business-focused analytics views

The intermediate models specifically align with the systems in this diagram, creating a direct 
mapping between our business processes and our data models.