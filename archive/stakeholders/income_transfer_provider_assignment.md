# Income Transfers and Provider Assignment in Open Dental

## Overview

This document outlines how income transfers, provider assignments, unearned types, and related financial concepts are handled in Open Dental and our DBT project. It serves as a reference for stakeholders to understand the financial data model and business rules.

## Key Concepts

### Payment Splits (Paysplits)

Payment splits are the fundamental building blocks of income allocation in Open Dental. They allow income to be allocated to:
- Clinics
- Production items
- Providers
- Unearned income types

Key attributes of paysplits:
- Entry Date (immutable creation date)
- Payment Date
- Amount
- Unearned Type
- Clinic
- Provider
- Patient
- Attached Procedure/Adjustment

### Unearned Income Types

Unearned income represents payments that haven't been allocated to specific procedures yet. Common types include:
- Prepayments
- Overpayments
- Copays
- Treatment plan prepayments

Unearned income can be:
- **Unallocated**: Not yet assigned to any procedure
- **Allocated**: Assigned to specific treatment planned procedures

### Provider Assignment

Providers are assigned to paysplits in several ways:
1. Inherited from attached procedures
2. Manually selected during payment entry
3. Defaulted based on patient's primary provider
4. Inherited from payment plans

Provider restrictions:
- Providers can be restricted to specific clinics
- Only providers available for the selected clinic can be chosen
- Provider income must be tracked for valid transfers

## Income Transfer Process

### Types of Transfers

1. **Manual Income Transfers**
   - Created as $0 payments
   - Must have offsetting negative and positive paysplits
   - Can transfer between:
     - Procedures
     - Adjustments
     - Payment plans
     - Unearned types
     - Providers/clinics

2. **Unearned Income Allocation**
   - Automatically allocates unearned income to procedures
   - Can be done through:
     - Allocate Unearned tool
     - Manual income transfers
     - Income Transfer Manager

3. **Treatment Plan Prepayments**
   - Special type of unearned income
   - Hidden from account until procedure completion
   - Automatically transfers when procedure is completed

### Transfer Rules

1. **Balance Validation**
   - Cannot transfer more than available balance
   - Cannot create negative unearned balances
   - Must maintain zero-sum across all splits

2. **Provider/Clinic Matching**
   - Income must exist where being subtracted from
   - Transfers must maintain valid provider/clinic combinations
   - Rigorous mode requires procedure/adjustment attachment

3. **Allocation Priority**
   - Matches provider of unearned income to production items first
   - Uses oldest unearned income if no provider match
   - Considers insurance estimates and write-offs

## DBT Project Implementation

### Data Model

The DBT project models these concepts through:

1. **Base Tables**
   - `paysplit`: Core payment split data
   - `procedure`: Procedure information
   - `provider`: Provider details
   - `clinic`: Clinic information
   - `unearned_type`: Unearned type definitions

2. **Intermediate Models**
   - Income transfer validation
   - Provider assignment tracking
   - Unearned balance calculations

3. **Mart Models**
   - Income allocation reports
   - Provider income tracking
   - Unearned income analysis

### Key Business Rules

1. **Income Transfer Validation**
   - Zero-sum validation across splits
   - Provider/clinic combination validation
   - Balance availability checks

2. **Unearned Income Tracking**
   - Balance calculations by type
   - Allocation history
   - Treatment plan prepayment tracking

3. **Provider Income Attribution**
   - Income assignment to providers
   - Clinic/provider combination validation
   - Income transfer impact tracking

## Reporting and Monitoring

### Standard Reports

1. **Unearned Income Reports**
   - Unearned Accounts Report
   - Unearned Allocation Report
   - Net Unearned Income Report
   - Line Item Unearned Income Report

2. **Income Transfer Reports**
   - Transfer history
   - Provider income impact
   - Unearned allocation tracking

### Monitoring Considerations

1. **Data Quality**
   - Negative unearned balances
   - Invalid provider assignments
   - Unallocated income tracking

2. **Business Process**
   - Treatment plan prepayment handling
   - Provider income attribution
   - Clinic income distribution

## Best Practices

1. **Income Transfer Process**
   - Always validate balances before transfers
   - Maintain proper provider/clinic assignments
   - Document transfer reasons

2. **Unearned Income Management**
   - Regular review of unallocated balances
   - Proper treatment plan prepayment handling
   - Timely allocation of unearned income

3. **Provider Assignment**
   - Consistent provider selection
   - Valid clinic/provider combinations
   - Accurate income attribution

## References

- [Open Dental Manual: Payment Splits](https://opendental.com/manual243/paysplit.html)
- [Open Dental Manual: Allocate Unearned Income](https://opendental.com/manual243/unearnedallocate.html)
- [Open Dental Manual: Unearned/Prepayment](https://opendental.com/manual243/unearnedprepayment.html)
- [Open Dental Manual: Unearned Allocation Report](https://opendental.com/manual243/reportunearnedallocation.html)
- [Open Dental Manual: Unearned Income Reports](https://opendental.com/manual243/reportunearnedincome.html)
- [Open Dental Manual: Income Transfer](https://opendental.com/manual243/incometransfer.html)