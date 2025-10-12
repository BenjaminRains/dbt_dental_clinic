"""
Insurance Data Generator
========================

Generates insurance-related data:
- Carriers (insurance companies)
- Employers
- Insurance Plans
- Subscribers
- Patient Plans (patient-plan relationships)
- Benefits (coverage rules)

This module creates realistic insurance coverage scenarios matching
the configured insurance_coverage_rate (default 60%).
"""

import random
from datetime import datetime, timedelta
from typing import Dict, List
from faker import Faker
from decimal import Decimal

fake = Faker()


# Major insurance carriers
INSURANCE_CARRIERS = [
    'Delta Dental',
    'MetLife',
    'Cigna Dental',
    'Aetna Dental',
    'Guardian Dental',
    'Humana Dental',
    'United Concordia',
    'Ameritas',
    'Principal Dental',
    'Blue Cross Blue Shield Dental',
    'UnitedHealthcare Dental',
    'Anthem Dental',
    'VSP Dental',
    'Aflac Dental',
    'Assurant Dental',
]


class InsuranceGenerator:
    """Generates insurance data"""
    
    def __init__(self, config, id_gen, data_store):
        self.config = config
        self.id_gen = id_gen
        self.data_store = data_store
    
    def generate(self, db):
        """Generate all insurance data"""
        self._generate_carriers(db)
        self._generate_employers(db)
        self._generate_insurance_plans(db)
        self._generate_subscribers(db)
        self._generate_patient_plans(db)
        self._generate_benefits(db)
    
    def _generate_carriers(self, db):
        """Generate insurance carriers"""
        carriers = []
        
        for i, carrier_name in enumerate(INSURANCE_CARRIERS, start=1):
            carriers.append((
                i,  # CarrierNum
                carrier_name,  # CarrierName
                fake.street_address()[:100],  # Address
                fake.city(),  # City
                fake.state_abbr(),  # State
                fake.zipcode(),  # Zip
                fake.phone_number(),  # Phone
                '00000',  # ElectID (Electronic ID)
                0,  # NoSendElect (int2)
                to_opendental_boolean(True),  # IsCDA
                '02',  # CDAnetVersion
                to_opendental_boolean(False),  # IsHidden
                datetime.now(),  # SecDateTEdit
            ))
        
        sql = """
            INSERT INTO raw.carrier (
                "CarrierNum", "CarrierName", "Address", "City", "State", "Zip",
                "Phone", "ElectID", "NoSendElect", "IsCDA", "CDAnetVersion",
                "IsHidden", "SecDateTEdit"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("CarrierNum") DO NOTHING
        """
        
        db.execute_batch(sql, carriers)
        self.data_store['carriers'] = [c[0] for c in carriers]
        self.data_store['counts']['carrier'] = len(carriers)
        print(f"✓ Generated {len(carriers)} insurance carriers")
    
    def _generate_employers(self, db):
        """Generate employers for insurance subscribers"""
        employers = []
        
        for i in range(1, 51):  # 50 employers
            employers.append((
                i,  # EmployerNum
                fake.company(),  # EmpName
                fake.street_address()[:100],  # Address
                '',  # Address2
                fake.city(),  # City
                fake.state_abbr(),  # State
                fake.zipcode(),  # Zip
                fake.phone_number(),  # Phone
            ))
        
        sql = """
            INSERT INTO raw.employer (
                "EmployerNum", "EmpName", "Address", "Address2", "City", "State", "Zip", "Phone"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("EmployerNum") DO NOTHING
        """
        
        db.execute_batch(sql, employers)
        self.data_store['employers'] = [e[0] for e in employers]
        self.data_store['counts']['employer'] = len(employers)
        print(f"✓ Generated {len(employers)} employers")
    
    def _generate_insurance_plans(self, db):
        """Generate insurance plans"""
        plans = []
        
        # Create 2-3 plans per carrier
        plan_id = 1
        for carrier_id in self.data_store['carriers']:
            num_plans = random.randint(2, 3)
            
            for plan_idx in range(num_plans):
                # Select employer for group plan
                employer_id = random.choice(self.data_store['employers'])
                
                # Group name/number
                group_name = fake.company()
                group_num = fake.bothify(text='GRP-####??')
                
                # Plan type
                plan_type = random.choice([
                    '',  # Standard
                    'p',  # PPO
                    'c',  # Capitation
                    'f',  # Flat Copay
                ])
                
                # Fee schedule (link to our fee schedules)
                fee_sched = random.choice(self.data_store['fee_schedules'])
                
                plans.append((
                    plan_id,  # PlanNum
                    group_name,  # GroupName
                    group_num,  # GroupNum
                    'Insurance plan notes',  # PlanNote
                    fee_sched,  # FeeSched
                    plan_type,  # PlanType
                    to_opendental_boolean(False),  # UseAltCode
                    to_opendental_boolean(True),  # ClaimsUseUCR
                    employer_id,  # EmployerNum
                    carrier_id,  # CarrierNum
                    to_opendental_boolean(False),  # IsHidden
                    datetime.now(),  # SecDateTEdit
                ))
                plan_id += 1
        
        sql = """
            INSERT INTO raw.insplan (
                "PlanNum", "GroupName", "GroupNum", "PlanNote", "FeeSched",
                "PlanType", "UseAltCode", "ClaimsUseUCR", "EmployerNum", "CarrierNum",
                "IsHidden", "SecDateTEdit"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("PlanNum") DO NOTHING
        """
        
        db.execute_batch(sql, plans)
        self.data_store['insurance_plans'] = [p[0] for p in plans]
        self.data_store['counts']['insplan'] = len(plans)
        print(f"✓ Generated {len(plans)} insurance plans")
    
    def _generate_subscribers(self, db):
        """Generate insurance subscribers (patients who hold insurance)"""
        subscribers = []
        
        # Select patients who will have insurance (based on insurance_coverage_rate)
        num_insured = int(len(self.data_store['patients']) * self.config.insurance_coverage_rate)
        insured_patients = random.sample(self.data_store['patients'], num_insured)
        
        sub_id = 1
        for patient_id in insured_patients:
            # Select random insurance plan
            plan_id = random.choice(self.data_store['insurance_plans'])
            
            # Subscriber ID (insurance card number)
            subscriber_id = fake.bothify(text='SUB########')
            
            subscribers.append((
                sub_id,  # InsSubNum
                plan_id,  # PlanNum
                patient_id,  # Subscriber (patient who holds the insurance)
                datetime.now().date(),  # DateEffective
                None,  # DateTerm (not terminated)
                to_opendental_boolean(False),  # ReleaseInfo
                to_opendental_boolean(True),  # AssignBen
                subscriber_id,  # SubscriberID
                datetime.now(),  # SecDateTEdit
            ))
            sub_id += 1
        
        sql = """
            INSERT INTO raw.inssub (
                "InsSubNum", "PlanNum", "Subscriber", "DateEffective", "DateTerm",
                "ReleaseInfo", "AssignBen", "SubscriberID", "SecDateTEdit"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("InsSubNum") DO NOTHING
        """
        
        db.execute_batch(sql, subscribers)
        self.data_store['subscribers'] = [(s[0], s[1], s[2]) for s in subscribers]  # (SubNum, PlanNum, PatNum)
        self.data_store['insured_patients'] = insured_patients
        self.data_store['counts']['inssub'] = len(subscribers)
        print(f"✓ Generated {len(subscribers)} insurance subscribers")
    
    def _generate_patient_plans(self, db):
        """Generate patient plan relationships (links patients to insurance)"""
        patient_plans = []
        
        patplan_id = 1
        
        # Relationship codes (int2)
        RELATIONSHIP_SELF = 0
        RELATIONSHIP_SPOUSE = 1
        RELATIONSHIP_CHILD = 2
        RELATIONSHIP_OTHER = 3
        
        # Create patient plans for all subscribers
        for sub_num, plan_num, subscriber_patient_id in self.data_store['subscribers']:
            # Primary insurance for subscriber
            patient_plans.append((
                patplan_id,  # PatPlanNum
                subscriber_patient_id,  # PatNum
                1,  # Ordinal (1=primary, 2=secondary)
                sub_num,  # InsSubNum
                random.choice([RELATIONSHIP_SELF, RELATIONSHIP_SELF, RELATIONSHIP_SPOUSE]),  # Relationship (numeric)
                datetime.now(),  # SecDateTEdit
            ))
            patplan_id += 1
            
            # Some patients have secondary insurance (10%)
            if random.random() < 0.10:
                # Secondary insurance
                secondary_sub = random.choice([s for s in self.data_store['subscribers'] if s[0] != sub_num])
                patient_plans.append((
                    patplan_id,
                    subscriber_patient_id,
                    2,  # Secondary
                    secondary_sub[0],
                    random.choice([RELATIONSHIP_SELF, RELATIONSHIP_SPOUSE]),  # Numeric relationship
                    datetime.now(),
                ))
                patplan_id += 1
        
        sql = """
            INSERT INTO raw.patplan (
                "PatPlanNum", "PatNum", "Ordinal", "InsSubNum", "Relationship", "SecDateTEdit"
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT ("PatPlanNum") DO NOTHING
        """
        
        db.execute_batch(sql, patient_plans)
        self.data_store['patient_plans'] = [(pp[0], pp[1], pp[2], pp[3]) for pp in patient_plans]  # (PatPlanNum, PatNum, Ordinal, SubNum)
        self.data_store['counts']['patplan'] = len(patient_plans)
        print(f"✓ Generated {len(patient_plans)} patient plan relationships")
    
    def _generate_benefits(self, db):
        """Generate benefit coverage rules for insurance plans"""
        benefits = []
        benefit_id = 1
        
        # Define standard benefit percentages
        # Note: Coverage is defined by CoverageLevel, not individual codes
        benefit_templates = [
            # Diagnostic (coverage level 1)
            {'coverage_level': 1, 'percent': 100, 'cov_cat_num': 1},  # 100% diagnostic
            # Preventive (coverage level 2)
            {'coverage_level': 2, 'percent': 100, 'cov_cat_num': 2},  # 100% preventive
            # Basic Restorative (coverage level 3)
            {'coverage_level': 3, 'percent': 80, 'cov_cat_num': 3},   # 80% basic
            # Major Restorative (coverage level 4)
            {'coverage_level': 4, 'percent': 50, 'cov_cat_num': 4},   # 50% major
        ]
        
        # Create benefits for each plan
        for plan_id in self.data_store['insurance_plans']:
            for template in benefit_templates:
                # Vary percentages slightly per plan
                percent_variance = random.choice([-10, -5, 0, 0, 0, 5])
                final_percent = max(0, min(100, template['percent'] + percent_variance))
                
                benefits.append((
                    benefit_id,  # BenefitNum
                    plan_id,  # PlanNum
                    0,  # PatPlanNum (0 = applies to all patients on this plan)
                    template['cov_cat_num'],  # CovCatNum (coverage category)
                    1,  # BenefitType (1=percentage)
                    final_percent,  # Percent
                    0.0,  # MonetaryAmt (not used for percentage benefits)
                    1,  # TimePeriod (1=calendar year)
                    1,  # QuantityQualifier
                    0,  # Quantity
                    None,  # CodeNum (NULL for category-wide benefits)
                    template['coverage_level'],  # CoverageLevel (1=diag, 2=prev, 3=basic, 4=major)
                    datetime.now(),  # SecDateTEntry
                    datetime.now(),  # SecDateTEdit
                    0,  # TreatArea (0=None, int2)
                ))
                benefit_id += 1
        
        sql = """
            INSERT INTO raw.benefit (
                "BenefitNum", "PlanNum", "PatPlanNum", "CovCatNum", "BenefitType",
                "Percent", "MonetaryAmt", "TimePeriod", "QuantityQualifier", "Quantity",
                "CodeNum", "CoverageLevel", "SecDateTEntry", "SecDateTEdit", "TreatArea"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("BenefitNum") DO NOTHING
        """
        
        db.execute_batch(sql, benefits)
        self.data_store['counts']['benefit'] = len(benefits)
        print(f"✓ Generated {len(benefits)} benefit rules")


def to_opendental_boolean(value: bool) -> bool:
    """Convert Python boolean to PostgreSQL boolean"""
    return value
