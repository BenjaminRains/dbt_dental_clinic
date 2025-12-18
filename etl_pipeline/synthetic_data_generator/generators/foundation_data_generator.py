"""
Foundation Data Generator
========================

Generates core configuration tables:
- Clinics
- Providers
- Operatories
- Procedure Codes (CDT codes)
- Definitions (lookup values)
- Fee Schedules and Fees
- Users

This module creates the foundation that all other data depends on.
"""

import random
import logging
from datetime import datetime
from typing import Dict, List
from faker import Faker

fake = Faker()
logger = logging.getLogger(__name__)


# Procedure category IDs (must be numeric)
PROC_CATEGORY = {
    'Diagnostic': 1,
    'Preventive': 2,
    'Restorative': 3,
    'Endodontics': 4,
    'Periodontics': 5,
    'Prosthodontics': 6,
    'Oral Surgery': 7,
}

# CDT Procedure Codes (Common dental procedures)
CDT_CODES = [
    # Diagnostic (D0000-D0999)
    {'code': 'D0120', 'desc': 'Periodic oral evaluation', 'fee': 50, 'category': PROC_CATEGORY['Diagnostic']},
    {'code': 'D0140', 'desc': 'Limited oral evaluation', 'fee': 45, 'category': PROC_CATEGORY['Diagnostic']},
    {'code': 'D0150', 'desc': 'Comprehensive oral evaluation', 'fee': 75, 'category': PROC_CATEGORY['Diagnostic']},
    {'code': 'D0210', 'desc': 'Intraoral - complete series', 'fee': 125, 'category': PROC_CATEGORY['Diagnostic']},
    {'code': 'D0220', 'desc': 'Intraoral - periapical first film', 'fee': 35, 'category': PROC_CATEGORY['Diagnostic']},
    {'code': 'D0230', 'desc': 'Intraoral - periapical each additional', 'fee': 25, 'category': PROC_CATEGORY['Diagnostic']},
    {'code': 'D0270', 'desc': 'Bitewing - single film', 'fee': 30, 'category': PROC_CATEGORY['Diagnostic']},
    {'code': 'D0272', 'desc': 'Bitewings - two films', 'fee': 45, 'category': PROC_CATEGORY['Diagnostic']},
    {'code': 'D0274', 'desc': 'Bitewings - four films', 'fee': 65, 'category': PROC_CATEGORY['Diagnostic']},
    {'code': 'D0330', 'desc': 'Panoramic film', 'fee': 95, 'category': PROC_CATEGORY['Diagnostic']},
    
    # Preventive (D1000-D1999)
    {'code': 'D1110', 'desc': 'Prophylaxis - adult', 'fee': 85, 'category': PROC_CATEGORY['Preventive']},
    {'code': 'D1120', 'desc': 'Prophylaxis - child', 'fee': 70, 'category': PROC_CATEGORY['Preventive']},
    {'code': 'D1206', 'desc': 'Topical fluoride - child', 'fee': 35, 'category': PROC_CATEGORY['Preventive']},
    {'code': 'D1208', 'desc': 'Topical fluoride - adult', 'fee': 40, 'category': PROC_CATEGORY['Preventive']},
    {'code': 'D1351', 'desc': 'Sealant - per tooth', 'fee': 45, 'category': PROC_CATEGORY['Preventive']},
    
    # Restorative (D2000-D2999)
    {'code': 'D2140', 'desc': 'Amalgam - one surface', 'fee': 125, 'category': PROC_CATEGORY['Restorative']},
    {'code': 'D2150', 'desc': 'Amalgam - two surfaces', 'fee': 150, 'category': PROC_CATEGORY['Restorative']},
    {'code': 'D2160', 'desc': 'Amalgam - three surfaces', 'fee': 175, 'category': PROC_CATEGORY['Restorative']},
    {'code': 'D2330', 'desc': 'Resin - one surface anterior', 'fee': 135, 'category': PROC_CATEGORY['Restorative']},
    {'code': 'D2331', 'desc': 'Resin - two surfaces anterior', 'fee': 160, 'category': PROC_CATEGORY['Restorative']},
    {'code': 'D2332', 'desc': 'Resin - three surfaces anterior', 'fee': 185, 'category': PROC_CATEGORY['Restorative']},
    {'code': 'D2391', 'desc': 'Resin - one surface posterior', 'fee': 155, 'category': PROC_CATEGORY['Restorative']},
    {'code': 'D2392', 'desc': 'Resin - two surfaces posterior', 'fee': 185, 'category': PROC_CATEGORY['Restorative']},
    {'code': 'D2393', 'desc': 'Resin - three surfaces posterior', 'fee': 215, 'category': PROC_CATEGORY['Restorative']},
    {'code': 'D2740', 'desc': 'Crown - porcelain/ceramic', 'fee': 1200, 'category': PROC_CATEGORY['Restorative']},
    {'code': 'D2750', 'desc': 'Crown - porcelain fused to high noble metal', 'fee': 1250, 'category': PROC_CATEGORY['Restorative']},
    {'code': 'D2751', 'desc': 'Crown - porcelain fused to predominantly base metal', 'fee': 1100, 'category': PROC_CATEGORY['Restorative']},
    {'code': 'D2790', 'desc': 'Crown - full cast high noble metal', 'fee': 1150, 'category': PROC_CATEGORY['Restorative']},
    {'code': 'D2791', 'desc': 'Crown - full cast predominantly base metal', 'fee': 1000, 'category': PROC_CATEGORY['Restorative']},
    {'code': 'D2950', 'desc': 'Core buildup', 'fee': 250, 'category': PROC_CATEGORY['Restorative']},
    
    # Endodontics (D3000-D3999)
    {'code': 'D3310', 'desc': 'Root canal - anterior', 'fee': 650, 'category': PROC_CATEGORY['Endodontics']},
    {'code': 'D3320', 'desc': 'Root canal - bicuspid', 'fee': 850, 'category': PROC_CATEGORY['Endodontics']},
    {'code': 'D3330', 'desc': 'Root canal - molar', 'fee': 1050, 'category': PROC_CATEGORY['Endodontics']},
    
    # Periodontics (D4000-D4999)
    {'code': 'D4210', 'desc': 'Gingivectomy - per quadrant', 'fee': 550, 'category': PROC_CATEGORY['Periodontics']},
    {'code': 'D4341', 'desc': 'Periodontal scaling and root planing - per quadrant', 'fee': 250, 'category': PROC_CATEGORY['Periodontics']},
    {'code': 'D4910', 'desc': 'Periodontal maintenance', 'fee': 110, 'category': PROC_CATEGORY['Periodontics']},
    
    # Prosthodontics (D5000-D5999)
    {'code': 'D5110', 'desc': 'Complete denture - upper', 'fee': 1500, 'category': PROC_CATEGORY['Prosthodontics']},
    {'code': 'D5120', 'desc': 'Complete denture - lower', 'fee': 1500, 'category': PROC_CATEGORY['Prosthodontics']},
    {'code': 'D5213', 'desc': 'Partial denture - upper', 'fee': 1350, 'category': PROC_CATEGORY['Prosthodontics']},
    {'code': 'D5214', 'desc': 'Partial denture - lower', 'fee': 1350, 'category': PROC_CATEGORY['Prosthodontics']},
    
    # Oral Surgery (D7000-D7999)
    {'code': 'D7140', 'desc': 'Extraction - erupted tooth', 'fee': 150, 'category': PROC_CATEGORY['Oral Surgery']},
    {'code': 'D7210', 'desc': 'Extraction - impacted tooth - soft tissue', 'fee': 225, 'category': PROC_CATEGORY['Oral Surgery']},
    {'code': 'D7220', 'desc': 'Extraction - impacted tooth - partial bony', 'fee': 275, 'category': PROC_CATEGORY['Oral Surgery']},
    {'code': 'D7230', 'desc': 'Extraction - impacted tooth - complete bony', 'fee': 350, 'category': PROC_CATEGORY['Oral Surgery']},
    {'code': 'D7240', 'desc': 'Extraction - impacted tooth - unusual surgical', 'fee': 450, 'category': PROC_CATEGORY['Oral Surgery']},
]


class FoundationGenerator:
    """Generates foundation configuration data"""
    
    def __init__(self, config, id_gen, data_store):
        self.config = config
        self.id_gen = id_gen
        self.data_store = data_store
        self.data_store['counts'] = {}
    
    def generate(self, db):
        """Generate all foundation data"""
        self._generate_users(db)
        self._generate_clinics(db)
        self._generate_providers(db)
        self._generate_operatories(db)
        self._generate_procedure_codes(db)
        self._generate_definitions(db)
        self._generate_fee_schedules(db)
        self._generate_fees(db)
    
    def _generate_users(self, db):
        """Generate system users"""
        users = []
        
        # Admin user (always ID 1)
        users.append((
            1,  # UserNum
            'admin',  # UserName
            'Admin User',  # DomainUser
            to_opendental_boolean(False),  # IsHidden
            to_opendental_boolean(True),  # PasswordIsStrong
            None,  # ClinicNum
            None,  # ProvNum
            datetime.now(),  # DateTLastLogin
        ))
        
        # Additional users
        for i in range(2, 11):  # Users 2-10
            users.append((
                i,
                f'user{i}',
                fake.name(),
                to_opendental_boolean(False),
                to_opendental_boolean(True),
                None,
                None,
                datetime.now(),
            ))
        
        sql = """
            INSERT INTO raw.userod (
                "UserNum", "UserName", "DomainUser", "IsHidden", 
                "PasswordIsStrong", "ClinicNum", "ProvNum", "DateTLastLogin"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("UserNum") DO NOTHING
        """
        
        db.execute_batch(sql, users)
        self.data_store['users'] = [u[0] for u in users]
        self.data_store['counts']['userod'] = len(users)
    
    def _generate_clinics(self, db):
        """Generate clinic locations"""
        clinics = []
        
        for i in range(1, self.config.num_clinics + 1):
            clinics.append((
                i,  # ClinicNum
                f'Clinic {i} - {fake.city()}',  # Description
                f'C{i}',  # Abbr
                i,  # ItemOrder
                fake.street_address(),  # Address
                fake.city(),  # City
                fake.state_abbr(),  # State
                fake.zipcode(),  # Zip
                fake.phone_number(),  # Phone
                to_opendental_boolean(False),  # IsHidden
            ))
        
        sql = """
            INSERT INTO raw.clinic (
                "ClinicNum", "Description", "Abbr", "ItemOrder", 
                "Address", "City", "State", "Zip", "Phone", "IsHidden"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("ClinicNum") DO NOTHING
        """
        
        db.execute_batch(sql, clinics)
        self.data_store['clinics'] = [c[0] for c in clinics]
        self.data_store['counts']['clinic'] = len(clinics)
    
    def _generate_providers(self, db):
        """Generate dental providers"""
        providers = []
        
        # Specialty codes (numeric IDs)
        SPECIALTY_GENERAL = 1
        SPECIALTY_HYGIENIST = 2
        SPECIALTY_ORTHODONTIST = 3
        SPECIALTY_PERIODONTIST = 4
        
        for i in range(1, self.config.num_providers + 1):
            is_hygienist = i % 4 == 0  # Every 4th provider is a hygienist
            
            if is_hygienist:
                specialty_id = SPECIALTY_HYGIENIST
                suffix = 'RDH'
            else:
                # Mix of general dentists and specialists
                specialty_id = random.choice([SPECIALTY_GENERAL, SPECIALTY_GENERAL, SPECIALTY_ORTHODONTIST])
                suffix = 'DDS'
            
            fname = fake.first_name()
            lname = fake.last_name()
            
            providers.append((
                i,  # ProvNum
                f'Dr. {lname}' if not is_hygienist else fname,  # Abbr
                lname,  # LName
                fname,  # FName
                '',  # MI
                suffix,  # Suffix
                specialty_id,  # Specialty (numeric ID)
                to_opendental_boolean(False),  # IsHidden
                to_opendental_boolean(False),  # IsSecondary
                fake.ssn(),  # NationalProvID (fake NPI)
                datetime.now(),  # DateTStamp
            ))
        
        sql = """
            INSERT INTO raw.provider (
                "ProvNum", "Abbr", "LName", "FName", "MI", "Suffix", 
                "Specialty", "IsHidden", "IsSecondary", "NationalProvID", "DateTStamp"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("ProvNum") DO NOTHING
        """
        
        db.execute_batch(sql, providers)
        self.data_store['providers'] = [p[0] for p in providers]
        self.data_store['providers_hygienist'] = [p[0] for p in providers if p[6] == 2]  # Specialty ID 2 = Hygienist
        self.data_store['providers_dentist'] = [p[0] for p in providers if p[6] != 2]  # All non-hygienist providers
        self.data_store['counts']['provider'] = len(providers)
    
    def _generate_operatories(self, db):
        """Generate operatories (dental chairs/rooms)"""
        operatories = []
        
        for i in range(1, self.config.num_operatories + 1):
            clinic_id = random.choice(self.data_store['clinics'])
            prov_dentist = random.choice(self.data_store['providers_dentist'])
            prov_hyg = random.choice(self.data_store['providers_hygienist']) if random.random() < 0.5 else None
            
            operatories.append((
                i,  # OperatoryNum
                f'Op {i}',  # OpName
                f'OP{i}',  # Abbrev
                i,  # ItemOrder
                clinic_id,  # ClinicNum
                prov_dentist,  # ProvDentist
                prov_hyg,  # ProvHygienist
                to_opendental_boolean(i % 5 == 0),  # IsHygiene (every 5th is hygiene)
                to_opendental_boolean(False),  # IsHidden
                datetime.now(),  # DateTStamp
            ))
        
        sql = """
            INSERT INTO raw.operatory (
                "OperatoryNum", "OpName", "Abbrev", "ItemOrder", "ClinicNum",
                "ProvDentist", "ProvHygienist", "IsHygiene", "IsHidden", "DateTStamp"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("OperatoryNum") DO NOTHING
        """
        
        db.execute_batch(sql, operatories)
        self.data_store['operatories'] = [o[0] for o in operatories]
        self.data_store['counts']['operatory'] = len(operatories)
    
    def _generate_procedure_codes(self, db):
        """Generate procedure codes (CDT codes) - uses production codes if available"""
        import json
        import os
        
        # Try to load procedure codes from production extraction
        proc_codes_file = os.path.join(
            os.path.dirname(__file__), 
            '../data/procedure_codes.json'
        )
        
        if os.path.exists(proc_codes_file):
            logger.info(f"Loading procedure codes from production: {proc_codes_file}")
            with open(proc_codes_file, 'r') as f:
                prod_codes = json.load(f)
            
            proc_codes = []
            code_mapping = {}
            
            for code_data in prod_codes:
                descript = code_data.get('Descript') or ''
                abbr_desc = code_data.get('AbbrDesc') or (descript[:50] if descript else '')
                proc_codes.append((
                    code_data['CodeNum'],
                    code_data.get('ProcCode', ''),
                    descript,
                    abbr_desc,
                    code_data.get('ProcCat', 0),
                    to_opendental_boolean(code_data.get('IsHygiene', False)),
                    to_opendental_boolean(code_data.get('IsProsth', False)),
                    datetime.fromisoformat(code_data['DateTStamp']) if code_data.get('DateTStamp') else datetime.now(),
                ))
                code_mapping[code_data.get('ProcCode', '')] = code_data['CodeNum']
            
            logger.info(f"Loaded {len(proc_codes)} procedure codes from production")
        else:
            # Fallback to hardcoded CDT codes
            logger.info("Production procedure codes not found, using hardcoded CDT codes")
            proc_codes = []
            code_mapping = {}
            
            for i, code_data in enumerate(CDT_CODES, start=1):
                proc_codes.append((
                    i,  # CodeNum
                    code_data['code'],  # ProcCode
                    code_data['desc'],  # Descript
                    code_data['desc'][:50],  # AbbrDesc
                    code_data['category'],  # ProcCat
                    to_opendental_boolean(code_data['code'].startswith('D1')),  # IsHygiene
                    to_opendental_boolean(False),  # IsProsth
                    datetime.now(),  # DateTStamp
                ))
                code_mapping[code_data['code']] = i
        
        sql = """
            INSERT INTO raw.procedurecode (
                "CodeNum", "ProcCode", "Descript", "AbbrDesc", "ProcCat",
                "IsHygiene", "IsProsth", "DateTStamp"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("CodeNum") DO NOTHING
        """
        
        db.execute_batch(sql, proc_codes)
        self.data_store['procedure_codes'] = code_mapping
        self.data_store['cdt_codes'] = CDT_CODES  # Keep for backward compatibility
        self.data_store['counts']['procedurecode'] = len(proc_codes)
    
    def _generate_definitions(self, db):
        """Generate definition lookup values"""
        definitions = []
        
        # Payment types (Category 1)
        payment_types = [
            (1, 1, 71, 'Check', '', 0, False),
            (2, 1, 69, 'Cash', '', 0, False),
            (3, 1, 70, 'Credit Card', '', 0, False),
            (4, 1, 72, 'Refund', '', 0, False),
            (5, 1, 0, 'Administrative', '', 0, False),
        ]
        
        # Procedure status (Category 4)
        proc_status = [
            (10, 4, 1, 'Treatment Planned', '1', 0, False),
            (11, 4, 2, 'Completed', '2', 0, False),
            (12, 4, 3, 'Admin/Documentation', '3', 0, False),
            (13, 4, 6, 'Ordered/Planned', '6', 0, False),
        ]
        
        # Treatment areas (Category 5)
        treatment_areas = [
            (20, 5, 1, 'Anterior', '1', 0, False),
            (21, 5, 2, 'Posterior', '2', 0, False),
            (22, 5, 3, 'Full Mouth', '3', 0, False),
        ]
        
        # Fee schedule types (Category 6)
        fee_types = [
            (30, 6, 1, 'Standard', '1', 0, False),
            (31, 6, 2, 'Insurance', '2', 0, False),
        ]
        
        # Adjustment types (Category 1 - following OpenDental pattern)
        adjustment_types = [
            (186, 1, 186, 'Senior Discount', '', 0, False),
            (188, 1, 188, 'Insurance Writeoff', '', 0, False),
            (474, 1, 474, 'Provider Discount', '', 0, False),
            (475, 1, 475, 'Provider Discount 2', '', 0, False),
            (472, 1, 472, 'Employee Discount', '', 0, False),
            (485, 1, 485, 'Employee Discount 2', '', 0, False),
            (9, 1, 9, 'Cash Discount', '', 0, False),
            (185, 1, 185, 'Cash Discount 2', '', 0, False),
        ]
        
        definitions = payment_types + proc_status + treatment_areas + fee_types + adjustment_types
        
        sql = """
            INSERT INTO raw.definition (
                "DefNum", "Category", "ItemOrder", "ItemName", "ItemValue", "ItemColor", "IsHidden"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("DefNum") DO NOTHING
        """
        
        db.execute_batch(sql, definitions)
        self.data_store['counts']['definition'] = len(definitions)
    
    def _generate_fee_schedules(self, db):
        """Generate fee schedules"""
        fee_scheds = [
            (1, 'Standard Fees', 0, 1, to_opendental_boolean(False), datetime.now()),
            (2, 'PPO Fees', 0, 2, to_opendental_boolean(False), datetime.now()),
            (3, 'Medicaid Fees', 0, 3, to_opendental_boolean(False), datetime.now()),
        ]
        
        sql = """
            INSERT INTO raw.feesched (
                "FeeSchedNum", "Description", "FeeSchedType", "ItemOrder", "IsHidden", "SecDateTEdit"
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT ("FeeSchedNum") DO NOTHING
        """
        
        db.execute_batch(sql, fee_scheds)
        self.data_store['fee_schedules'] = [1, 2, 3]
        self.data_store['counts']['feesched'] = len(fee_scheds)
    
    def _generate_fees(self, db):
        """Generate fee amounts for all procedure codes"""
        fees = []
        fee_id = 1
        
        for sched_id in [1, 2, 3]:
            for code_data in CDT_CODES:
                code_num = self.data_store['procedure_codes'][code_data['code']]
                
                # Vary fees slightly by schedule
                base_fee = code_data['fee']
                if sched_id == 2:  # PPO - 10% less
                    fee_amt = base_fee * 0.90
                elif sched_id == 3:  # Medicaid - 30% less
                    fee_amt = base_fee * 0.70
                else:  # Standard
                    fee_amt = base_fee
                
                fees.append((
                    fee_id,  # FeeNum
                    sched_id,  # FeeSched
                    code_num,  # CodeNum
                    fee_amt,  # Amount
                    to_opendental_boolean(False),  # UseDefaultFee
                    to_opendental_boolean(False),  # UseDefaultCov
                    datetime.now(),  # SecDateEntry
                    datetime.now(),  # SecDateTEdit
                ))
                fee_id += 1
        
        sql = """
            INSERT INTO raw.fee (
                "FeeNum", "FeeSched", "CodeNum", "Amount", 
                "UseDefaultFee", "UseDefaultCov", "SecDateEntry", "SecDateTEdit"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("FeeNum") DO NOTHING
        """
        
        db.execute_batch(sql, fees)
        self.data_store['counts']['fee'] = len(fees)


def to_opendental_boolean(value: bool) -> bool:
    """Convert Python boolean to PostgreSQL boolean"""
    return value