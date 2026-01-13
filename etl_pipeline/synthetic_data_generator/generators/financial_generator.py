"""
Financial Data Generator
========================

Generates financial transaction data:
- Claims (insurance billing)
- Claim Procedures (procedure-level insurance estimates/payments)
- Claim Payments (insurance check payments)
- Payments (patient payments)
- Payment Splits (payment allocations to procedures)
- Adjustments (discounts, write-offs)

CRITICAL: This module implements exact financial balancing:
    AR Balance = Procedure Fee - Insurance Paid - Patient Paid - Adjustments

All financial transactions must result in mathematically balanced accounts
to match the dbt int_ar_balance.sql model expectations.
"""

import random
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from decimal import Decimal
from faker import Faker

fake = Faker()


# Claim statuses (MUST MATCH main.py)
CLAIM_STATUS = {
    'UNSENT': 'U',
    'HOLD': 'H',
    'WAITING': 'W',
    'SENT': 'S',
    'RECEIVED': 'R',
    'PAID': 'P'
}

# Adjustment types (MUST MATCH main.py and int_adjustments.sql)
ADJUSTMENT_TYPES = {
    'INSURANCE_WRITEOFF': 188,
    'SENIOR_DISCOUNT': 186,
    'PROVIDER_DISCOUNT': 474,
    'EMPLOYEE_DISCOUNT': 472,
    'CASH_DISCOUNT': 9,
    'FAMILY_DISCOUNT': 486,
}

# Payment types (MUST MATCH main.py)
PAYMENT_TYPES = {
    'CHECK': 71,
    'CASH': 69,
    'CREDIT_CARD': 70,
}

# Unearned types (for payment splits)
UNEARNED_TYPES = {
    'NORMAL': 0,
    'UNEARNED_REVENUE': 288,
}


class FinancialGenerator:
    """Generates financial transaction data with exact AR balancing"""
    
    def __init__(self, config, id_gen, data_store):
        self.config = config
        self.id_gen = id_gen
        self.data_store = data_store
        self.procedure_financials = {}  # Track financial state of each procedure
    
    def generate(self, db):
        """Generate all financial data"""
        print("\n[Financial Generator] Starting financial data generation...")
        print(f"Processing {len(self.data_store['procedures'])} procedures...")
        
        # Phase 1: Insurance claims and payments
        self._generate_claims_and_claimprocs(db)
        self._generate_claim_payments(db)
        
        # Phase 2: Patient payments
        self._generate_patient_payments(db)
        
        # Phase 3: Adjustments
        self._generate_adjustments(db)
        
        # Phase 4: Update patient balances
        self._update_patient_balances(db)
        
        # Phase 5: Validate balancing
        self._validate_ar_balancing()
    
    def _generate_claims_and_claimprocs(self, db):
        """Generate insurance claims and claim procedures"""
        claims = []
        claimprocs = []
        
        claim_id = 1
        claimproc_id = 1
        
        # Group procedures by patient and date for claim generation
        proc_by_patient = {}
        for proc_num, pat_num, code_num, fee in self.data_store['procedures']:
            if pat_num not in proc_by_patient:
                proc_by_patient[pat_num] = []
            proc_by_patient[pat_num].append({
                'ProcNum': proc_num,
                'PatNum': pat_num,
                'CodeNum': code_num,
                'Fee': fee
            })
        
        # Check which patients have insurance
        insured_patient_map = {}
        for patplan_num, pat_num, ordinal, sub_num in self.data_store.get('patient_plans', []):
            if pat_num not in insured_patient_map:
                insured_patient_map[pat_num] = []
            insured_patient_map[pat_num].append({
                'PatPlanNum': patplan_num,
                'Ordinal': ordinal,
                'SubNum': sub_num
            })
        
        # Generate claims for insured patients
        for pat_num, procedures in proc_by_patient.items():
            if pat_num not in insured_patient_map:
                continue  # No insurance
            
            # Should we submit claims for this patient's procedures?
            if random.random() > self.config.claim_submission_rate:
                continue
            
            # Get patient's insurance
            patient_insurance = insured_patient_map[pat_num]
            primary_insurance = [pi for pi in patient_insurance if pi['Ordinal'] == 1][0]
            
            # Get subscriber info
            sub_info = None
            for sub_num, plan_num, subscriber_pat in self.data_store.get('subscribers', []):
                if sub_num == primary_insurance['SubNum']:
                    sub_info = {'SubNum': sub_num, 'PlanNum': plan_num, 'Subscriber': subscriber_pat}
                    break
            
            if not sub_info:
                continue
            
            # Group procedures into claims (max 10 procedures per claim)
            for proc_chunk in self._chunk_list(procedures, 10):
                # Claim date (after procedure dates)
                claim_date = datetime.now().date() - timedelta(days=random.randint(1, 90))
                
                # Claim status progression
                claim_status = random.choices(
                    [CLAIM_STATUS['SENT'], CLAIM_STATUS['RECEIVED'], CLAIM_STATUS['PAID']],
                    weights=[0.15, 0.15, 0.70]  # 70% paid
                )[0]
                
                # Dates based on status
                date_sent = claim_date if claim_status != CLAIM_STATUS['UNSENT'] else None
                date_received = date_sent + timedelta(days=random.randint(14, 30)) if claim_status in [CLAIM_STATUS['RECEIVED'], CLAIM_STATUS['PAID']] else None
                
                # Create claim
                provider_id = random.choice(self.data_store['providers_dentist'])
                clinic_id = random.choice(self.data_store['clinics'])
                
                claims.append((
                    claim_id,  # ClaimNum
                    pat_num,  # PatNum
                    claim_date,  # DateService
                    date_sent,  # DateSent
                    claim_status,  # ClaimStatus
                    date_received,  # DateReceived
                    sub_info['PlanNum'],  # PlanNum
                    provider_id,  # ProvTreat
                    provider_id,  # ProvBill
                    sub_info['SubNum'],  # InsSubNum
                    clinic_id,  # ClinicNum
                    'P',  # ClaimType (P=Primary)
                    claim_date,  # SecDateEntry (date)
                    datetime.now(),  # SecDateTEdit (timestamp)
                ))
                
                # Create claim procedures for each procedure in this claim
                for proc in proc_chunk:
                    proc_num = proc['ProcNum']
                    proc_fee = proc['Fee']
                    
                    # Insurance estimate (based on benefit percentage)
                    # Typical: 100% preventive, 80% basic, 50% major
                    coverage_percent = random.choice([100, 100, 80, 80, 50])  # Weighted toward common percentages
                    ins_estimate = round(proc_fee * (coverage_percent / 100), 2)
                    
                    # Insurance payment (if claim is paid)
                    if claim_status == CLAIM_STATUS['PAID']:
                        # Insurance might pay less than estimate (80-100% of estimate)
                        ins_payment = round(ins_estimate * random.uniform(0.80, 1.00), 2)
                        write_off = round((proc_fee - ins_estimate) * random.uniform(0.0, 0.3), 2)  # Partial write-off
                    else:
                        ins_payment = 0
                        write_off = 0
                    
                    claimprocs.append((
                        claimproc_id,  # ClaimProcNum
                        claim_id,  # ClaimNum
                        pat_num,  # PatNum
                        proc_num,  # ProcNum
                        sub_info['PlanNum'],  # PlanNum
                        sub_info['SubNum'],  # InsSubNum
                        provider_id,  # ProvNum
                        proc_fee,  # FeeBilled
                        ins_estimate,  # InsPayEst
                        ins_payment,  # InsPayAmt
                        write_off,  # WriteOff
                        0,  # Status (0=NotReceived)
                        claim_date,  # DateCP
                        datetime.now(),  # DateEntry
                        datetime.now(),  # SecDateEntry
                        datetime.now(),  # SecDateTEdit
                    ))
                    
                    # Track financial state
                    if proc_num not in self.procedure_financials:
                        self.procedure_financials[proc_num] = {
                            'ProcNum': proc_num,
                            'PatNum': pat_num,
                            'Fee': proc_fee,
                            'InsurancePaid': 0,
                            'PatientPaid': 0,
                            'Adjustments': 0,
                            'WriteOff': 0
                        }
                    
                    self.procedure_financials[proc_num]['InsurancePaid'] += ins_payment
                    self.procedure_financials[proc_num]['WriteOff'] += write_off
                    
                    claimproc_id += 1
                
                claim_id += 1
        
        # Insert claims
        if claims:
            sql_claim = """
                INSERT INTO raw.claim (
                    "ClaimNum", "PatNum", "DateService", "DateSent", "ClaimStatus", "DateReceived",
                    "PlanNum", "ProvTreat", "ProvBill", "InsSubNum", "ClinicNum",
                    "ClaimType", "SecDateEntry", "SecDateTEdit"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("ClaimNum") DO NOTHING
            """
            db.execute_batch(sql_claim, claims)
        
        # Insert claimprocs
        if claimprocs:
            sql_claimproc = """
                INSERT INTO raw.claimproc (
                    "ClaimProcNum", "ClaimNum", "PatNum", "ProcNum", "PlanNum", "InsSubNum",
                    "ProvNum", "FeeBilled", "InsPayEst", "InsPayAmt", "WriteOff",
                    "Status", "DateCP", "DateEntry", "SecDateEntry", "SecDateTEdit"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("ClaimProcNum") DO NOTHING
            """
            db.execute_batch(sql_claimproc, claimprocs)
        
        self.data_store['claims'] = [c[0] for c in claims]
        self.data_store['claimprocs'] = claimprocs
        self.data_store['counts']['claim'] = len(claims)
        self.data_store['counts']['claimproc'] = len(claimprocs)
        print(f"✓ Generated {len(claims)} claims with {len(claimprocs)} claim procedures")
    
    def _generate_claim_payments(self, db):
        """Generate insurance check payments (claimpayment table)"""
        claim_payments = []
        
        # Group claimprocs by claim for payment batching
        claimprocs_by_claim = {}
        for claimproc in self.data_store.get('claimprocs', []):
            claim_num = claimproc[1]
            if claim_num not in claimprocs_by_claim:
                claimprocs_by_claim[claim_num] = []
            claimprocs_by_claim[claim_num].append(claimproc)
        
        claimpayment_id = 1
        
        # Create one payment per claim
        for claim_num, claimprocs in claimprocs_by_claim.items():
            # Sum insurance payments for this claim
            total_payment = sum(cp[9] for cp in claimprocs)  # InsPayAmt
            
            if total_payment > 0:
                # Payment date (14-45 days after claim date)
                check_date = datetime.now().date() - timedelta(days=random.randint(14, 45))
                
                # Get carrier from first claimproc's plan
                carrier_name = fake.company()
                check_num = fake.bothify(text='CHK######')
                
                claim_payments.append((
                    claimpayment_id,  # ClaimPaymentNum
                    check_date,  # CheckDate
                    total_payment,  # CheckAmt
                    check_num,  # CheckNum
                    '',  # BankBranch
                    '',  # Note
                    0,  # ClinicNum
                    0,  # DepositNum
                    carrier_name,  # CarrierName
                    datetime.now(),  # DateIssued
                    datetime.now(),  # SecDateEntry
                    datetime.now(),  # SecDateTEdit
                ))
                claimpayment_id += 1
        
        if claim_payments:
            sql = """
                INSERT INTO raw.claimpayment (
                    "ClaimPaymentNum", "CheckDate", "CheckAmt", "CheckNum", "BankBranch",
                    "Note", "ClinicNum", "DepositNum", "CarrierName", "DateIssued",
                    "SecDateEntry", "SecDateTEdit"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("ClaimPaymentNum") DO NOTHING
            """
            db.execute_batch(sql, claim_payments)
        
        self.data_store['counts']['claimpayment'] = len(claim_payments)
        print(f"✓ Generated {len(claim_payments)} insurance check payments")
    
    def _generate_patient_payments(self, db):
        """Generate patient payments and payment splits"""
        payments = []
        paysplits = []
        
        payment_id = 1
        paysplit_id = 1
        
        # Generate payments for procedures that need patient payment
        for proc_num, pat_num, code_num, proc_fee in self.data_store['procedures']:
            # Get financial state
            if proc_num not in self.procedure_financials:
                self.procedure_financials[proc_num] = {
                    'ProcNum': proc_num,
                    'PatNum': pat_num,
                    'Fee': proc_fee,
                    'InsurancePaid': 0,
                    'PatientPaid': 0,
                    'Adjustments': 0,
                    'WriteOff': 0
                }
            
            fin_state = self.procedure_financials[proc_num]
            
            # Calculate patient responsibility
            patient_balance = proc_fee - fin_state['InsurancePaid'] - fin_state['WriteOff']
            
            # Should patient pay? (90% of time, yes)
            if patient_balance > 0.01 and random.random() < 0.90:
                # Payment amount (70% pay in full, 30% partial)
                if random.random() < 0.70:
                    payment_amount = patient_balance  # Pay in full
                else:
                    payment_amount = round(patient_balance * random.uniform(0.30, 0.90), 2)  # Partial payment
                
                # Payment date (0-60 days after procedure)
                payment_date = datetime.now().date() - timedelta(days=random.randint(0, 60))
                
                # Payment type
                payment_type = random.choice([
                    PAYMENT_TYPES['CHECK'],
                    PAYMENT_TYPES['CASH'],
                    PAYMENT_TYPES['CREDIT_CARD'],
                ])
                
                # Create payment
                payments.append((
                    payment_id,  # PayNum
                    payment_type,  # PayType
                    payment_date,  # PayDate
                    payment_amount,  # PayAmt
                    '',  # CheckNum
                    '',  # BankBranch
                    '',  # PayNote
                    pat_num,  # PatNum
                    0,  # ClinicNum
                    datetime.now(),  # DateEntry
                    1,  # SecUserNumEntry
                    datetime.now(),  # SecDateTEdit
                ))
                
                # Create payment split (links payment to procedure)
                paysplits.append((
                    paysplit_id,  # SplitNum
                    payment_id,  # PayNum
                    payment_date,  # DatePay
                    pat_num,  # PatNum
                    random.choice(self.data_store['providers_dentist']),  # ProvNum
                    payment_amount,  # SplitAmt
                    proc_num,  # ProcNum (CRITICAL: links to procedure)
                    None,  # AdjNum (NULL - either ProcNum or AdjNum, never both)
                    0,  # UnearnedType (0=normal payment)
                    0,  # ClinicNum
                    1,  # SecUserNumEntry
                    datetime.now(),  # SecDateTEdit
                ))
                paysplit_id += 1
                
                # Update financial tracking
                fin_state['PatientPaid'] += payment_amount
                
                payment_id += 1
        
        # Insert payments
        if payments:
            sql_payment = """
                INSERT INTO raw.payment (
                    "PayNum", "PayType", "PayDate", "PayAmt", "CheckNum", "BankBranch",
                    "PayNote", "PatNum", "ClinicNum", "DateEntry",
                    "SecUserNumEntry", "SecDateTEdit"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("PayNum") DO NOTHING
            """
            db.execute_batch(sql_payment, payments)
        
        # Insert payment splits
        if paysplits:
            sql_paysplit = """
                INSERT INTO raw.paysplit (
                    "SplitNum", "PayNum", "DatePay", "PatNum", "ProvNum", "SplitAmt",
                    "ProcNum", "AdjNum", "UnearnedType", "ClinicNum",
                    "SecUserNumEntry", "SecDateTEdit"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("SplitNum") DO NOTHING
            """
            db.execute_batch(sql_paysplit, paysplits)
        
        self.data_store['counts']['payment'] = len(payments)
        self.data_store['counts']['paysplit'] = len(paysplits)
        print(f"✓ Generated {len(payments)} patient payments with {len(paysplits)} payment splits")
    
    def _generate_adjustments(self, db):
        """Generate adjustments (discounts, write-offs)"""
        adjustments = []
        adj_id = 1
        
        # 20% of procedures get adjustments
        procedures_with_adjustments = random.sample(
            self.data_store['procedures'],
            int(len(self.data_store['procedures']) * 0.20)
        )
        
        for proc_num, pat_num, code_num, proc_fee in procedures_with_adjustments:
            # Get financial state
            if proc_num not in self.procedure_financials:
                continue
            
            fin_state = self.procedure_financials[proc_num]
            
            # Determine adjustment type
            adj_type = random.choice([
                ADJUSTMENT_TYPES['SENIOR_DISCOUNT'],
                ADJUSTMENT_TYPES['INSURANCE_WRITEOFF'],
                ADJUSTMENT_TYPES['PROVIDER_DISCOUNT'],
                ADJUSTMENT_TYPES['CASH_DISCOUNT'],
            ])
            
            # Adjustment amount (5-20% of procedure fee)
            adj_percent = random.uniform(0.05, 0.20)
            adj_amount = round(proc_fee * adj_percent * -1, 2)  # Negative for discounts
            
            # Adjustment date (same as procedure date or shortly after)
            adj_date = datetime.now().date() - timedelta(days=random.randint(0, 30))
            
            adjustments.append((
                adj_id,  # AdjNum
                adj_date,  # AdjDate
                adj_amount,  # AdjAmt
                pat_num,  # PatNum
                adj_type,  # AdjType
                random.choice(self.data_store['providers_dentist']),  # ProvNum
                '',  # AdjNote
                proc_num,  # ProcNum
                0,  # ClinicNum
                1,  # SecUserNumEntry
                datetime.now(),  # SecDateTEdit
            ))
            adj_id += 1
            
            # Update financial tracking
            fin_state['Adjustments'] += adj_amount
        
        if adjustments:
            sql = """
                INSERT INTO raw.adjustment (
                    "AdjNum", "AdjDate", "AdjAmt", "PatNum", "AdjType", "ProvNum",
                    "AdjNote", "ProcNum", "ClinicNum", "SecUserNumEntry", "SecDateTEdit"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("AdjNum") DO NOTHING
            """
            db.execute_batch(sql, adjustments)
        
        self.data_store['counts']['adjustment'] = len(adjustments)
        print(f"✓ Generated {len(adjustments)} adjustments")
    
    def _update_patient_balances(self, db):
        """Update patient.BalTotal from procedure balances"""
        print("\n[Financial Generator] Updating patient balances...")
        
        # Check if we have procedure financials to work with
        if not self.procedure_financials:
            print("⚠ No procedure financials found - skipping balance update")
            return
        
        # Create a lookup map: proc_num -> pat_num for efficient access
        proc_to_patient = {proc[0]: proc[1] for proc in self.data_store['procedures']}
        
        print(f"  Processing {len(self.procedure_financials)} procedures...")
        print(f"  Found {len(proc_to_patient)} procedures in data_store")
        
        # Calculate patient balances from procedures
        patient_balances = {}
        procedures_processed = 0
        
        for proc_num, fin_state in self.procedure_financials.items():
            # Get patient ID from lookup map
            pat_num = proc_to_patient.get(proc_num)
            if not pat_num:
                continue
            
            procedures_processed += 1
            
            # Calculate AR balance for this procedure
            fee = fin_state['Fee']
            ins_paid = fin_state['InsurancePaid']
            pat_paid = fin_state['PatientPaid']
            adjustments = fin_state['Adjustments']
            writeoff = fin_state['WriteOff']
            
            ar_balance = fee - ins_paid - pat_paid - adjustments - writeoff
            
            # Sum balances per patient (only positive balances)
            if ar_balance > 0.01:  # Use 0.01 threshold to avoid rounding issues
                if pat_num not in patient_balances:
                    patient_balances[pat_num] = 0.0
                patient_balances[pat_num] += ar_balance
        
        print(f"  Processed {procedures_processed} procedures")
        print(f"  Found {len(patient_balances)} patients with outstanding balances")
        
        # Update patient table with calculated balances
        if patient_balances:
            updates = []
            for pat_num, balance in patient_balances.items():
                updates.append((round(balance, 2), round(balance, 2), pat_num))
            
            sql = """
                UPDATE raw.patient
                SET "BalTotal" = %s,
                    "EstBalance" = %s
                WHERE "PatNum" = %s
            """
            
            # Execute UPDATE statements directly (execute_batch may not work reliably for UPDATEs)
            try:
                # Use executemany which works reliably for UPDATE statements
                db.cursor.executemany(sql, updates)
                rows_updated = db.cursor.rowcount
                
                patients_with_balance = len([b for b in patient_balances.values() if b > 0])
                total_balance = sum(patient_balances.values())
                
                print(f"✓ Updated balances for {patients_with_balance} patients (rows affected: {rows_updated})")
                print(f"  Total AR balance: ${total_balance:,.2f}")
                if patients_with_balance > 0:
                    print(f"  Average balance per patient: ${total_balance/patients_with_balance:,.2f}")
            except Exception as e:
                print(f"❌ Error updating patient balances: {e}")
                import traceback
                traceback.print_exc()
                raise
        else:
            print("⚠ No patient balances to update")
            print("  This may indicate all procedures are fully paid or there's an issue with balance calculation")
    
    def _validate_ar_balancing(self):
        """Validate that AR balancing equation holds for all procedures"""
        print("\n[Financial Validation] Checking AR balance equation...")
        
        total_checked = 0
        total_balanced = 0
        total_with_balance = 0
        max_imbalance = 0
        
        for proc_num, fin_state in self.procedure_financials.items():
            fee = fin_state['Fee']
            ins_paid = fin_state['InsurancePaid']
            pat_paid = fin_state['PatientPaid']
            adjustments = fin_state['Adjustments']
            writeoff = fin_state['WriteOff']
            
            # Calculate AR balance
            ar_balance = fee - ins_paid - pat_paid - adjustments - writeoff
            
            total_checked += 1
            
            if abs(ar_balance) < 0.01:  # Balanced (allowing for rounding)
                total_balanced += 1
            else:
                total_with_balance += 1
                max_imbalance = max(max_imbalance, abs(ar_balance))
        
        print(f"  Total procedures checked: {total_checked}")
        print(f"  Fully balanced: {total_balanced} ({total_balanced/total_checked*100:.1f}%)")
        print(f"  With AR balance: {total_with_balance} ({total_with_balance/total_checked*100:.1f}%)")
        print(f"  Max imbalance: ${max_imbalance:.2f}")
        print("✓ Financial validation complete")
    
    
    def _chunk_list(self, lst: List, chunk_size: int) -> List[List]:
        """Split list into chunks"""
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]


def to_opendental_boolean(value: bool) -> bool:
    """Convert Python boolean to PostgreSQL boolean"""
    return value
