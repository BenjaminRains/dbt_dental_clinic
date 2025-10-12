"""
Patient Data Generator
======================

Generates patient demographics and family data:
- Patients (5,000)
- Patient Notes (5,000 - 1:1 relationship)
- Patient Links (family relationships)
- Zipcodes (reference data)

This module creates realistic patient populations with age distributions,
family groupings, and geographic diversity.
"""

import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from faker import Faker

fake = Faker()


class PatientGenerator:
    """Generates patient demographic data"""
    
    def __init__(self, config, id_gen, data_store):
        self.config = config
        self.id_gen = id_gen
        self.data_store = data_store
    
    def generate(self, db):
        """Generate all patient data"""
        self._generate_zipcodes(db)
        self._generate_patients(db)
        self._generate_patient_notes(db)
        self._generate_patient_links(db)
    
    def _generate_zipcodes(self, db):
        """Generate zipcode reference data"""
        zipcodes = []
        
        # Generate 100 unique zipcodes
        used_zips = set()
        zip_id = 1
        while len(used_zips) < 100:
            zip_code = fake.zipcode()
            if zip_code not in used_zips:
                used_zips.add(zip_code)
                
                zipcodes.append((
                    zip_id,  # ZipCodeNum (primary key)
                    zip_code,  # ZipCodeDigits
                    fake.city(),  # City
                    fake.state_abbr(),  # State
                    to_opendental_boolean(False),  # IsFrequent
                ))
                zip_id += 1
        
        sql = """
            INSERT INTO raw.zipcode (
                "ZipCodeNum", "ZipCodeDigits", "City", "State", "IsFrequent"
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT ("ZipCodeNum") DO NOTHING
        """
        
        db.execute_batch(sql, zipcodes)
        self.data_store['zipcodes'] = list(used_zips)
        self.data_store['counts']['zipcode'] = len(zipcodes)
        print(f"✓ Generated {len(zipcodes)} zipcodes")
    
    def _generate_patients(self, db):
        """Generate patient demographics with family groupings"""
        patients = []
        families = []  # Track families for family links later
        
        num_families = self.config.num_patients // 2.5  # Average family size ~2.5
        num_families = int(num_families)
        
        patient_id = 1
        
        for family_idx in range(num_families):
            # Determine family size (1-5 members)
            family_size = random.choices(
                [1, 2, 3, 4, 5],
                weights=[0.20, 0.30, 0.25, 0.15, 0.10]  # Realistic distribution
            )[0]
            
            # Create guarantor (head of household)
            guarantor_id = patient_id
            clinic_id = random.choice(self.data_store['clinics'])
            primary_provider = random.choice(self.data_store['providers_dentist'])
            fee_sched = random.choice(self.data_store['fee_schedules'])
            billing_type = 1  # Default billing type
            zipcode = random.choice(self.data_store['zipcodes'])
            
            # Guarantor is adult (25-70 years old)
            guarantor_birthdate = fake.date_of_birth(minimum_age=25, maximum_age=70)
            
            family_members = []
            
            # Create family members
            for member_idx in range(family_size):
                is_guarantor = (member_idx == 0)
                
                if is_guarantor:
                    birthdate = guarantor_birthdate
                    fname = fake.first_name()
                    lname = fake.last_name()
                else:
                    # Spouse or dependent
                    if member_idx == 1 and family_size >= 2:
                        # Spouse (similar age to guarantor)
                        birthdate = guarantor_birthdate + timedelta(days=random.randint(-1825, 1825))  # ±5 years
                        fname = fake.first_name()
                    else:
                        # Child (0-25 years old)
                        birthdate = fake.date_of_birth(minimum_age=0, maximum_age=25)
                        fname = fake.first_name()
                    
                    lname = patients[guarantor_id - 1][2] if guarantor_id > 0 else fake.last_name()
                
                # Determine patient status
                status = random.choices(
                    [0, 1, 2],  # 0=Patient, 1=NonPatient, 2=Inactive
                    weights=[0.85, 0.05, 0.10]
                )[0]
                
                # Create patient record
                created_date = self.config.start_date + timedelta(
                    days=random.randint(0, (self.config.end_date - self.config.start_date).days)
                )
                
                patients.append((
                    patient_id,  # PatNum
                    lname,  # LName
                    fname,  # FName
                    '',  # MiddleI
                    'Ms.' if random.random() > 0.5 else 'Mr.',  # Preferred
                    status,  # PatStatus
                    random.choice([1, 2]),  # Gender (1=Male, 2=Female)
                    random.choice([0, 1, 2, 3]),  # Position (0=Single, 1=Married, 2=Child, 3=Widowed)
                    birthdate,  # Birthdate
                    fake.ssn(),  # SSN (fake)
                    fake.street_address()[:100],  # Address
                    fake.street_address()[:100] if random.random() < 0.1 else '',  # Address2
                    fake.city(),  # City
                    fake.state_abbr(),  # State
                    zipcode,  # Zip
                    fake.phone_number(),  # HmPhone
                    fake.phone_number() if random.random() < 0.7 else '',  # WkPhone
                    fake.phone_number() if random.random() < 0.8 else '',  # WirelessPhone
                    guarantor_id,  # Guarantor
                    fake.email() if random.random() < 0.9 else '',  # Email
                    primary_provider,  # PriProv
                    0,  # SecProv (optional secondary provider)
                    fee_sched,  # FeeSched
                    billing_type,  # BillingType
                    'Y' if random.random() < 0.6 else 'N',  # HasIns (varchar: 'Y'/'N')
                    created_date,  # DateFirstVisit
                    clinic_id,  # ClinicNum
                    2,  # PreferConfirmMethod (0=None, 1=Phone, 2=Email, 3=Text)
                    2,  # PreferContactMethod (0=None, 1=Phone, 2=Email, 3=Text)
                    2,  # PreferRecallMethod (0=None, 1=Phone, 2=Email, 3=Text)
                    0,  # AskToArriveEarly (minutes)
                    datetime.now(),  # DateTStamp
                    created_date,  # SecDateEntry
                ))
                
                family_members.append(patient_id)
                patient_id += 1
                
                # Stop if we've reached target patient count
                if patient_id > self.config.num_patients:
                    break
            
            families.append(family_members)
            
            if patient_id > self.config.num_patients:
                break
        
        sql = """
            INSERT INTO raw.patient (
                "PatNum", "LName", "FName", "MiddleI", "Preferred",
                "PatStatus", "Gender", "Position", "Birthdate", "SSN",
                "Address", "Address2", "City", "State", "Zip",
                "HmPhone", "WkPhone", "WirelessPhone", "Guarantor", "Email",
                "PriProv", "SecProv", "FeeSched", "BillingType", "HasIns",
                "DateFirstVisit", "ClinicNum",
                "PreferConfirmMethod", "PreferContactMethod", "PreferRecallMethod",
                "AskToArriveEarly", "DateTStamp", "SecDateEntry"
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT ("PatNum") DO NOTHING
        """
        
        db.execute_batch(sql, patients)
        self.data_store['patients'] = [p[0] for p in patients]
        self.data_store['families'] = families
        self.data_store['guarantors'] = [f[0] for f in families if f]  # First member of each family
        self.data_store['counts']['patient'] = len(patients)
        print(f"✓ Generated {len(patients)} patients in {len(families)} families")
    
    def _generate_patient_notes(self, db):
        """Generate patient notes (1:1 with patients)"""
        patient_notes = []
        
        sample_notes = [
            'Patient is anxious about dental work',
            'Prefers morning appointments',
            'Requires antibiotic pre-medication',
            'Has history of TMJ',
            'Sensitive to cold',
            'Prefers no fluoride treatments',
            'History of periodontal disease',
            'Diabetic - requires special care',
            'Latex allergy noted',
            'Good oral hygiene habits',
        ]
        
        for patient_id in self.data_store['patients']:
            # 70% of patients have notes
            if random.random() < 0.70:
                family_financial_note = fake.sentence() if random.random() < 0.3 else ''
                medical_note = random.choice(sample_notes) if random.random() < 0.5 else ''
                service_note = fake.sentence() if random.random() < 0.2 else ''
                
                patient_notes.append((
                    patient_id,  # PatNum
                    family_financial_note,  # FamFinancial
                    '',  # ApptPhone
                    medical_note,  # Medical
                    service_note,  # Service
                    '',  # MedicalComp
                    '',  # Treatment
                    '',  # ICEName
                    '',  # ICEPhone
                    None,  # OrthoMonthsTreatOverride (int4 - NULL for most patients)
                    datetime.now(),  # SecDateTEntry
                    datetime.now(),  # SecDateTEdit
                ))
        
        sql = """
            INSERT INTO raw.patientnote (
                "PatNum", "FamFinancial", "ApptPhone", "Medical", "Service",
                "MedicalComp", "Treatment", "ICEName", "ICEPhone",
                "OrthoMonthsTreatOverride", "SecDateTEntry", "SecDateTEdit"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("PatNum") DO NOTHING
        """
        
        db.execute_batch(sql, patient_notes)
        self.data_store['counts']['patientnote'] = len(patient_notes)
        print(f"✓ Generated {len(patient_notes)} patient notes")
    
    def _generate_patient_links(self, db):
        """Generate family relationship links"""
        patient_links = []
        link_id = 1
        
        # Create links between family members
        for family in self.data_store['families']:
            if len(family) < 2:
                continue  # No links needed for single-person families
            
            guarantor_id = family[0]
            
            # Link all family members to guarantor
            for member_id in family[1:]:
                # LinkType is boolean in schema (True/False)
                link_type = random.choice([True, False])
                
                patient_links.append((
                    link_id,  # PatientLinkNum
                    guarantor_id,  # PatNumFrom (guarantor)
                    member_id,  # PatNumTo (family member)
                    link_type,  # LinkType (boolean)
                    datetime.now(),  # DateTimeLink
                ))
                link_id += 1
        
        if patient_links:
            sql = """
                INSERT INTO raw.patientlink (
                    "PatientLinkNum", "PatNumFrom", "PatNumTo", "LinkType", "DateTimeLink"
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT ("PatientLinkNum") DO NOTHING
            """
            
            db.execute_batch(sql, patient_links)
        
        self.data_store['counts']['patientlink'] = len(patient_links)
        print(f"✓ Generated {len(patient_links)} family relationship links")


def to_opendental_boolean(value: bool) -> bool:
    """Convert Python boolean to PostgreSQL boolean"""
    return value
