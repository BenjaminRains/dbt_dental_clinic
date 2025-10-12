"""
Supporting Data Generator
=========================

Generates supporting/ancillary data:
- Communication Logs (calls, texts, emails)
- Documents (document metadata)
- Referrals (referral sources)
- Referral Attachments (patient-referral links)
- Tasks (office workflow tasks) - optional

This module creates realistic supporting data that enhances the
clinical and financial data but is not critical for core analytics.
"""

import random
from datetime import datetime, timedelta
from typing import Dict, List
from faker import Faker

fake = Faker()


# Communication types (ID, Name)
COMM_TYPES = [
    (1, 'APPT_REMINDER'),
    (2, 'RECALL_REMINDER'),
    (3, 'BALANCE_REMINDER'),
    (4, 'INSURANCE_VERIFICATION'),
    (5, 'PATIENT_INQUIRY'),
    (6, 'REFERRAL_SENT'),
    (7, 'REFERRAL_RECEIVED'),
    (8, 'TREATMENT_CONSULT'),
    (9, 'EMERGENCY_CALL'),
    (10, 'GENERAL'),
]

# Communication modes
COMM_MODES = [
    (1, 'Phone'),
    (2, 'Email'),
    (3, 'Text'),
    (4, 'Mail'),
    (5, 'In Person'),
]

# Document categories
DOC_CATEGORIES = [
    (1, 'Patient Forms'),
    (2, 'Radiographs'),
    (3, 'Photos'),
    (4, 'Correspondence'),
    (5, 'Lab'),
    (6, 'Statements'),
    (7, 'Treatment Plans'),
    (8, 'Insurance'),
]

# Referral types
REFERRAL_TYPES = [
    'Orthodontist',
    'Oral Surgeon',
    'Periodontist',
    'Endodontist',
    'Prosthodontist',
    'Pediatric Dentist',
    'Marketing Source',
    'Patient Referral',
    'Insurance Company',
    'Online Review',
]


class SupportingGenerator:
    """Generates supporting data"""
    
    def __init__(self, config, id_gen, data_store):
        self.config = config
        self.id_gen = id_gen
        self.data_store = data_store
    
    def generate(self, db):
        """Generate all supporting data"""
        self._generate_referrals(db)
        self._generate_referral_attachments(db)
        self._generate_communication_logs(db)
        self._generate_documents(db)
        # Optional: tasks, sheets, etc.
    
    def _generate_referrals(self, db):
        """Generate referral sources"""
        referrals = []
        
        # Specialty codes (numeric IDs - matching provider specialties)
        SPECIALTY_GENERAL = 1
        SPECIALTY_HYGIENIST = 2
        SPECIALTY_ORTHODONTIST = 3
        SPECIALTY_PERIODONTIST = 4
        SPECIALTY_ORAL_SURGEON = 5
        SPECIALTY_ENDODONTIST = 6
        SPECIALTY_PROSTHODONTIST = 7
        SPECIALTY_PEDIATRIC = 8
        SPECIALTY_NONE = 0  # Non-doctor referrals
        
        for i, ref_type in enumerate(REFERRAL_TYPES, start=1):
            # Generate referral name based on type
            if ref_type == 'Patient Referral':
                ref_name = fake.name()
            elif ref_type in ['Marketing Source', 'Online Review', 'Insurance Company']:
                ref_name = ref_type
            else:
                # Specialist referrals
                ref_name = f"Dr. {fake.last_name()} - {ref_type}"
            
            is_doctor = ref_type not in ['Marketing Source', 'Patient Referral', 'Online Review', 'Insurance Company']
            
            # Map referral type to specialty ID
            specialty_map = {
                'Orthodontist': SPECIALTY_ORTHODONTIST,
                'Oral Surgeon': SPECIALTY_ORAL_SURGEON,
                'Periodontist': SPECIALTY_PERIODONTIST,
                'Endodontist': SPECIALTY_ENDODONTIST,
                'Prosthodontist': SPECIALTY_PROSTHODONTIST,
                'Pediatric Dentist': SPECIALTY_PEDIATRIC,
            }
            specialty_id = specialty_map.get(ref_type, SPECIALTY_NONE)
            
            referrals.append((
                i,  # ReferralNum
                ref_name,  # LName (full name stored here)
                '',  # FName
                '',  # MName
                to_opendental_boolean(is_doctor),  # IsDoctor
                specialty_id,  # Specialty (numeric ID)
                fake.ssn() if is_doctor else '',  # NationalProvID
                fake.street_address()[:100] if is_doctor else '',  # Address
                fake.city() if is_doctor else '',  # City
                fake.state_abbr() if is_doctor else '',  # State
                fake.zipcode()[:10] if is_doctor else '',  # Zip (max 10 chars)
                ''.join(filter(str.isdigit, fake.phone_number()))[:10] if is_doctor else '',  # Telephone (digits only, max 10)
                to_opendental_boolean(False),  # IsHidden
                to_opendental_boolean(False),  # NotPerson
                '',  # Title
                fake.email() if is_doctor else '',  # EMail
                to_opendental_boolean(is_doctor),  # IsPreferred
                datetime.now(),  # DateTStamp
            ))
        
        sql = """
            INSERT INTO raw.referral (
                "ReferralNum", "LName", "FName", "MName", "IsDoctor", "Specialty",
                "NationalProvID", "Address", "City", "ST", "Zip", "Telephone",
                "IsHidden", "NotPerson", "Title", "EMail", "IsPreferred", "DateTStamp"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("ReferralNum") DO NOTHING
        """
        
        db.execute_batch(sql, referrals)
        self.data_store['referrals'] = [r[0] for r in referrals]
        self.data_store['counts']['referral'] = len(referrals)
        print(f"✓ Generated {len(referrals)} referrals")
    
    def _generate_referral_attachments(self, db):
        """Generate patient-referral links"""
        refattaches = []
        refattach_id = 1
        
        # 40% of patients have a referral source
        patients_with_referrals = random.sample(
            self.data_store['patients'],
            int(len(self.data_store['patients']) * 0.40)
        )
        
        for patient_id in patients_with_referrals:
            # Select random referral
            referral_id = random.choice(self.data_store['referrals'])
            
            # Referral date (at or before first visit)
            ref_date = self.config.start_date + timedelta(
                days=random.randint(0, (self.config.end_date - self.config.start_date).days)
            )
            
            # Referral type (False=RefFrom, True=RefTo)
            is_ref_to = False  # Most are "referred from"
            
            refattaches.append((
                refattach_id,  # RefAttachNum
                referral_id,  # ReferralNum
                patient_id,  # PatNum
                0,  # ItemOrder
                ref_date.date(),  # RefDate
                to_opendental_boolean(is_ref_to),  # RefType (boolean: False=from, True=to)
                '',  # Note
                0,  # ProvNum
                datetime.now(),  # DateTStamp
            ))
            refattach_id += 1
            
            # Some patients also get referred out (5%)
            if random.random() < 0.05:
                specialist_id = random.choice([r for r in self.data_store['referrals'] if r <= 6])  # Only specialists
                
                refattaches.append((
                    refattach_id,
                    specialist_id,
                    patient_id,
                    1,
                    ref_date.date() + timedelta(days=random.randint(30, 180)),
                    to_opendental_boolean(True),  # RefType=True means RefTo
                    'Referred for specialty care',
                    random.choice(self.data_store['providers_dentist']),
                    datetime.now(),
                ))
                refattach_id += 1
        
        if refattaches:
            sql = """
                INSERT INTO raw.refattach (
                    "RefAttachNum", "ReferralNum", "PatNum", "ItemOrder", "RefDate",
                    "RefType", "Note", "ProvNum", "DateTStamp"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("RefAttachNum") DO NOTHING
            """
            db.execute_batch(sql, refattaches)
        
        self.data_store['counts']['refattach'] = len(refattaches)
        print(f"✓ Generated {len(refattaches)} referral attachments")
    
    def _generate_communication_logs(self, db):
        """Generate communication logs"""
        commlogs = []
        commlog_id = 1
        
        # Generate 1-3 comm logs per patient
        for patient_id in self.data_store['patients']:
            num_comms = random.randint(1, 3)
            
            for _ in range(num_comms):
                # Communication date (within date range)
                comm_date = self.config.start_date + timedelta(
                    days=random.randint(0, (self.config.end_date - self.config.start_date).days)
                )
                
                # Communication type
                comm_type_id, comm_type_name = random.choice(COMM_TYPES)
                comm_mode_id, comm_mode_name = random.choice(COMM_MODES)
                
                # Generate note based on type
                notes = {
                    'APPT_REMINDER': 'Appointment reminder sent',
                    'RECALL_REMINDER': 'Due for recall appointment',
                    'BALANCE_REMINDER': f'Patient balance: ${random.randint(50, 500)}',
                    'INSURANCE_VERIFICATION': 'Insurance benefits verified',
                    'PATIENT_INQUIRY': fake.sentence(),
                    'REFERRAL_SENT': 'Referral sent to specialist',
                    'REFERRAL_RECEIVED': 'Referral received from referring dentist',
                    'TREATMENT_CONSULT': 'Treatment consultation completed',
                    'EMERGENCY_CALL': 'Emergency call received',
                    'GENERAL': fake.sentence(),
                }
                
                note = notes.get(comm_type_name, fake.sentence())
                
                commlogs.append((
                    commlog_id,  # CommlogNum
                    patient_id,  # PatNum
                    comm_date,  # CommDateTime
                    comm_mode_id,  # CommType (1=Phone, 2=Email, etc.)
                    note,  # Note
                    random.choice(self.data_store['users']),  # UserNum
                    comm_type_id,  # CommSource (numeric ID)
                    0,  # SentOrReceived (0=Sent)
                    datetime.now(),  # DateTimeEnd
                    datetime.now(),  # DateTStamp
                ))
                commlog_id += 1
        
        if commlogs:
            sql = """
                INSERT INTO raw.commlog (
                    "CommlogNum", "PatNum", "CommDateTime", "CommType", "Note",
                    "UserNum", "CommSource", "SentOrReceived", "DateTimeEnd", "DateTStamp"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("CommlogNum") DO NOTHING
            """
            db.execute_batch(sql, commlogs)
        
        self.data_store['counts']['commlog'] = len(commlogs)
        print(f"✓ Generated {len(commlogs)} communication logs")
    
    def _generate_documents(self, db):
        """Generate document metadata (not actual files)"""
        documents = []
        document_id = 1
        
        # Generate 1-2 documents per patient
        for patient_id in self.data_store['patients']:
            num_docs = random.randint(1, 2)
            
            for _ in range(num_docs):
                # Document category
                doc_category_id, doc_category_name = random.choice(DOC_CATEGORIES)
                
                # Document date
                doc_date = self.config.start_date + timedelta(
                    days=random.randint(0, (self.config.end_date - self.config.start_date).days)
                )
                
                # Generate filename based on category
                file_extensions = {
                    'Patient Forms': 'pdf',
                    'Radiographs': 'jpg',
                    'Photos': 'jpg',
                    'Correspondence': 'pdf',
                    'Lab': 'pdf',
                    'Statements': 'pdf',
                    'Treatment Plans': 'pdf',
                    'Insurance': 'pdf',
                }
                
                ext = file_extensions.get(doc_category_name, 'pdf')
                filename = f"{patient_id}_{doc_category_name.replace(' ', '_')}_{fake.uuid4()[:8]}.{ext}"
                
                # ImgType: 0=Document, 1=Radiograph, 2=Photo, 3=File
                img_type = 1 if doc_category_name == 'Radiographs' else (2 if doc_category_name == 'Photos' else 0)
                
                documents.append((
                    document_id,  # DocNum
                    patient_id,  # PatNum
                    filename,  # FileName
                    doc_date.date(),  # DateCreated
                    doc_category_id,  # DocCategory
                    fake.sentence()[:100],  # Description
                    '',  # Note
                    to_opendental_boolean(False),  # IsFlipped
                    0,  # DegreesRotated
                    '',  # ToothNumbers
                    img_type,  # ImgType (0=Document, 1=Radiograph, 2=Photo)
                    datetime.now(),  # DateTStamp
                ))
                document_id += 1
        
        if documents:
            sql = """
                INSERT INTO raw.document (
                    "DocNum", "PatNum", "FileName", "DateCreated", "DocCategory",
                    "Description", "Note", "IsFlipped", "DegreesRotated", "ToothNumbers",
                    "ImgType", "DateTStamp"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("DocNum") DO NOTHING
            """
            db.execute_batch(sql, documents)
        
        self.data_store['counts']['document'] = len(documents)
        print(f"✓ Generated {len(documents)} documents")


def to_opendental_boolean(value: bool) -> bool:
    """Convert Python boolean to PostgreSQL boolean"""
    return value
