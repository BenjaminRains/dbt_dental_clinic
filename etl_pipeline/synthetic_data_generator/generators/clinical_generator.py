"""
Clinical Data Generator
=======================

Generates clinical encounter data:
- Appointment Types
- Appointments (15,000)
- Procedures (20,000)
- Recalls (hygiene recall tracking)
- Recall Types
- Recall Triggers

This module simulates realistic patient care patterns including:
- New patient comprehensive exams
- Hygiene recall cycles (every 6 months)
- Restorative treatment
- Emergency visits
"""

import random
from datetime import datetime, timedelta, time
from typing import Dict, List, Tuple
from faker import Faker

fake = Faker()


# Appointment type definitions
# IMPORTANT: Names must match expected values in fact_appointment accepted_values test:
# ['Patient', 'NewPatient', 'Hygiene', 'Prophy', 'Perio', 'Restorative', 'Crown', 'SRP', 'Denture', 'Other', 'Unknown']
APPOINTMENT_TYPES = [
    {'name': 'NewPatient', 'pattern': 'XX', 'color': '0', 'duration': 60},  # Comprehensive Exam -> NewPatient
    {'name': 'Patient', 'pattern': 'X', 'color': '0', 'duration': 30},  # Periodic Exam -> Patient
    {'name': 'Other', 'pattern': 'E', 'color': '255', 'duration': 45},  # Emergency -> Other
    {'name': 'Hygiene', 'pattern': 'HH', 'color': '16711680', 'duration': 60},  # Hygiene Adult -> Hygiene
    {'name': 'Prophy', 'pattern': 'H', 'color': '16711680', 'duration': 45},  # Hygiene Child -> Prophy
    {'name': 'Crown', 'pattern': 'CCC', 'color': '65280', 'duration': 90},  # Crown Prep -> Crown
    {'name': 'Crown', 'pattern': 'CS', 'color': '65280', 'duration': 60},  # Crown Seat -> Crown (duplicate name OK)
    {'name': 'Restorative', 'pattern': 'F', 'color': '8421504', 'duration': 45},  # Filling -> Restorative
    {'name': 'Restorative', 'pattern': 'RRR', 'color': '255', 'duration': 120},  # Root Canal -> Restorative (duplicate name OK)
    {'name': 'Other', 'pattern': 'EXT', 'color': '255', 'duration': 60},  # Extraction -> Other
    {'name': 'Denture', 'pattern': 'DD', 'color': '16776960', 'duration': 60},  # Denture Delivery -> Denture
    {'name': 'Other', 'pattern': 'CON', 'color': '0', 'duration': 30},  # Consultation -> Other
    {'name': 'Patient', 'pattern': 'FU', 'color': '8421504', 'duration': 30},  # Follow-up -> Patient (duplicate name OK)
    {'name': 'SRP', 'pattern': 'SRP', 'color': '16711680', 'duration': 60},  # SRP Quad -> SRP
    {'name': 'Perio', 'pattern': 'PM', 'color': '16711680', 'duration': 60},  # Perio Maintenance -> Perio
]


# OpenDental appointment statuses (MUST MATCH main.py)
APPOINTMENT_STATUS = {
    'SCHEDULED': 1,
    'COMPLETED': 2,
    'UNKNOWN': 3,
    'BROKEN': 5,
    'UNSCHEDULED': 6
}

# Procedure statuses (MUST MATCH main.py)
PROCEDURE_STATUS = {
    'TREATMENT_PLANNED': 1,
    'COMPLETED': 2,
    'ADMIN': 3,
    'EXISTING_PRIOR': 4,
    'REFERRED': 5,
    'ORDERED': 6,
    'CONDITION': 7,
    'IN_PROGRESS': 8
}


class ClinicalGenerator:
    """Generates clinical appointment and procedure data"""
    
    def __init__(self, config, id_gen, data_store):
        self.config = config
        self.id_gen = id_gen
        self.data_store = data_store
        self.patient_visit_dates = {}  # Track when patients were seen
    
    def generate(self, db):
        """Generate all clinical data"""
        self._generate_recall_types(db)
        self._generate_appointment_types(db)
        self._generate_appointments_and_procedures(db)
        self._generate_recalls(db)
        self._generate_recall_triggers(db)
    
    def _generate_recall_types(self, db):
        """Generate recall type definitions"""
        recall_types = [
            (1, 'Prophy', 6, 'P', 'D1110', to_opendental_boolean(False)),
            (2, 'Perio', 3, 'PM', 'D4910', to_opendental_boolean(False)),
            (3, 'Child Prophy', 6, 'CP', 'D1120', to_opendental_boolean(False)),
            (4, 'Fluoride', 6, 'F', 'D1206', to_opendental_boolean(False)),
        ]
        
        sql = """
            INSERT INTO raw.recalltype (
                "RecallTypeNum", "Description", "DefaultInterval", "TimePattern",
                "Procedures", "AppendToSpecial"
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT ("RecallTypeNum") DO NOTHING
        """
        
        db.execute_batch(sql, recall_types)
        self.data_store['recall_types'] = [r[0] for r in recall_types]
        self.data_store['counts']['recalltype'] = len(recall_types)
        print(f"✓ Generated {len(recall_types)} recall types")
    
    def _generate_appointment_types(self, db):
        """Generate appointment type definitions"""
        appt_types = []
        
        for i, appt_type in enumerate(APPOINTMENT_TYPES, start=1):
            appt_types.append((
                i,  # AppointmentTypeNum
                appt_type['name'],  # AppointmentTypeName
                appt_type['color'],  # AppointmentTypeColor (int4)
                i,  # ItemOrder
                to_opendental_boolean(False),  # IsHidden
                appt_type['pattern'],  # Pattern
            ))
        
        sql = """
            INSERT INTO raw.appointmenttype (
                "AppointmentTypeNum", "AppointmentTypeName", "AppointmentTypeColor",
                "ItemOrder", "IsHidden", "Pattern"
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT ("AppointmentTypeNum") DO NOTHING
        """
        
        db.execute_batch(sql, appt_types)
        self.data_store['appointment_types'] = [a[0] for a in appt_types]
        self.data_store['counts']['appointmenttype'] = len(appt_types)
        print(f"✓ Generated {len(appt_types)} appointment types")
    
    def _generate_appointments_and_procedures(self, db):
        """Generate appointments and linked procedures"""
        appointments = []
        procedures = []
        
        appt_id = 1
        proc_id = 1
        
        # Calculate how many appointments per patient on average
        avg_appts_per_patient = self.config.num_appointments / len(self.data_store['patients'])
        
        # Generate appointments for each patient
        for patient_id in self.data_store['patients']:
            # Determine number of appointments for this patient (1-8 visits over 2 years)
            num_appts = max(1, int(random.gauss(avg_appts_per_patient, avg_appts_per_patient * 0.3)))
            num_appts = min(num_appts, 8)
            
            patient_appts = []
            
            for appt_idx in range(num_appts):
                # Generate appointment date
                if appt_idx == 0:
                    # First visit (random within date range)
                    appt_date = self._random_business_date(
                        self.config.start_date,
                        self.config.end_date
                    )
                else:
                    # Subsequent visits (3-6 months after last visit)
                    last_appt_date = patient_appts[-1]['date']
                    months_between = random.randint(3, 9)
                    appt_date = last_appt_date + timedelta(days=30 * months_between)
                    
                    # Don't schedule beyond end date
                    if appt_date > self.config.end_date:
                        break
                
                # Determine appointment status
                if appt_date > datetime.now():
                    status = APPOINTMENT_STATUS['SCHEDULED']
                else:
                    status = random.choices(
                        [APPOINTMENT_STATUS['COMPLETED'], APPOINTMENT_STATUS['BROKEN']],
                        weights=[self.config.appointment_completion_rate, 1 - self.config.appointment_completion_rate]
                    )[0]
                
                # Select appointment type
                if appt_idx == 0:
                    # First visit - new patient
                    appt_type_idx = 0  # NewPatient
                elif appt_idx % 2 == 0:
                    # Regular hygiene visit
                    appt_type_idx = 3  # Hygiene
                else:
                    # Restorative treatment
                    appt_type_idx = random.choice([5, 7, 8])  # Crown/Restorative/Restorative
                
                appt_type = APPOINTMENT_TYPES[appt_type_idx]
                appt_type_num = appt_type_idx + 1
                
                # Select provider and operatory
                if appt_type['name'] in ['Hygiene', 'Prophy', 'SRP', 'Perio']:
                    provider_id = random.choice(self.data_store['providers_hygienist'])
                else:
                    provider_id = random.choice(self.data_store['providers_dentist'])
                
                operatory_id = random.choice(self.data_store['operatories'])
                clinic_id = random.choice(self.data_store['clinics'])
                
                # Generate appointment datetime
                appt_datetime = self._add_business_hours(appt_date)
                
                # Appointment flow timestamps (if completed)
                if status == APPOINTMENT_STATUS['COMPLETED']:
                    arrived = appt_datetime + timedelta(minutes=random.randint(-5, 10))
                    seated = arrived + timedelta(minutes=random.randint(5, 20))
                    dismissed = seated + timedelta(minutes=appt_type['duration'])
                else:
                    arrived = None
                    seated = None
                    dismissed = None
                
                # Create appointment
                appointments.append((
                    appt_id,  # AptNum
                    patient_id,  # PatNum
                    status,  # AptStatus
                    appt_type['pattern'],  # Pattern
                    operatory_id,  # Op
                    '',  # Note
                    provider_id,  # ProvNum
                    0,  # ProvHyg
                    appt_datetime,  # AptDateTime
                    to_opendental_boolean(False),  # IsNewPatient
                    clinic_id,  # ClinicNum
                    to_opendental_boolean(False),  # IsHygiene
                    datetime.now(),  # DateTStamp
                    arrived,  # DateTimeArrived
                    seated,  # DateTimeSeated
                    dismissed,  # DateTimeDismissed
                    appt_type_num,  # AppointmentTypeNum
                ))
                
                patient_appts.append({
                    'id': appt_id,
                    'date': appt_date,
                    'status': status
                })
                
                # Generate procedures for completed appointments
                if status == APPOINTMENT_STATUS['COMPLETED']:
                    appt_procedures = self._generate_procedures_for_appointment(
                        proc_id, appt_id, patient_id, provider_id, clinic_id,
                        appt_date, appt_type_idx
                    )
                    procedures.extend(appt_procedures)
                    proc_id += len(appt_procedures)
                
                appt_id += 1
                
                # Stop if we've reached target appointment count
                if appt_id > self.config.num_appointments:
                    break
            
            # Track patient visit history
            self.patient_visit_dates[patient_id] = [a['date'] for a in patient_appts if a['status'] == APPOINTMENT_STATUS['COMPLETED']]
            
            if appt_id > self.config.num_appointments:
                break
        
        # Insert appointments
        sql_appt = """
            INSERT INTO raw.appointment (
                "AptNum", "PatNum", "AptStatus", "Pattern", "Op", "Note",
                "ProvNum", "ProvHyg", "AptDateTime", "IsNewPatient", "ClinicNum",
                "IsHygiene", "DateTStamp", "DateTimeArrived", "DateTimeSeated",
                "DateTimeDismissed", "AppointmentTypeNum"
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT ("AptNum") DO NOTHING
        """
        
        db.execute_batch(sql_appt, appointments)
        self.data_store['appointments'] = [a[0] for a in appointments]
        self.data_store['counts']['appointment'] = len(appointments)
        print(f"✓ Generated {len(appointments)} appointments")
        
        # Insert procedures
        if procedures:
            sql_proc = """
                INSERT INTO raw.procedurelog (
                    "ProcNum", "PatNum", "AptNum", "ProcDate", "ProcFee",
                    "ToothNum", "Priority", "ProcStatus", "ProvNum", "ClinicNum",
                    "CodeNum", "Surf", "UnitQty", "BaseUnits", "DateEntryC",
                    "DateTStamp", "DateComplete", "SecDateEntry"
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT ("ProcNum") DO NOTHING
            """
            
            db.execute_batch(sql_proc, procedures)
        
        self.data_store['procedures'] = [(p[0], p[1], p[10], p[4]) for p in procedures]  # (ProcNum, PatNum, CodeNum, Fee)
        self.data_store['counts']['procedurelog'] = len(procedures)
        print(f"✓ Generated {len(procedures)} procedures")
    
    def _generate_procedures_for_appointment(
        self, proc_id: int, appt_id: int, patient_id: int, provider_id: int,
        clinic_id: int, proc_date: datetime, appt_type_idx: int
    ) -> List[Tuple]:
        """Generate procedures for an appointment based on type"""
        procedures = []
        
        # Map appointment types to procedure codes
        # Note: Appointment type names now match expected values (NewPatient, Patient, Hygiene, etc.)
        if appt_type_idx == 0:  # NewPatient
            proc_codes = ['D0150', 'D0210', 'D0274']  # Exam, FMX, Bitewings
        elif appt_type_idx == 1:  # Patient
            proc_codes = ['D0120', 'D0274']  # Periodic exam, Bitewings
        elif appt_type_idx == 2:  # Other (Emergency)
            proc_codes = [random.choice(['D0140', 'D7140', 'D9110'])]  # Limited exam, extraction, or palliative
        elif appt_type_idx == 3:  # Hygiene
            proc_codes = ['D1110', 'D0120']  # Cleaning, exam
            if random.random() < 0.3:
                proc_codes.append('D1208')  # Fluoride
        elif appt_type_idx == 4:  # Prophy
            proc_codes = ['D1120', 'D0120']  # Child cleaning, exam
            if random.random() < 0.3:
                proc_codes.append('D1206')  # Child fluoride
        elif appt_type_idx == 5:  # Crown
            proc_codes = ['D2750', 'D2950']  # Crown, core buildup
        elif appt_type_idx == 6:  # Crown (duplicate)
            proc_codes = ['D2750']  # Crown delivery
        elif appt_type_idx == 7:  # Restorative
            proc_codes = [random.choice(['D2140', 'D2150', 'D2391', 'D2392'])]  # Fillings
        elif appt_type_idx == 8:  # Restorative (Root Canal)
            proc_codes = [random.choice(['D3310', 'D3320', 'D3330'])]  # Root canals
        elif appt_type_idx == 9:  # Other (Extraction)
            proc_codes = ['D7140']
        elif appt_type_idx == 10:  # Denture
            proc_codes = [random.choice(['D5110', 'D5120', 'D5213', 'D5214'])]  # Dentures
        elif appt_type_idx == 11:  # Other (Consultation)
            proc_codes = ['D9310']  # Consultation
        elif appt_type_idx == 12:  # Patient (Follow-up)
            proc_codes = ['D0120']  # Periodic exam
        elif appt_type_idx == 13:  # SRP
            proc_codes = ['D4341']  # SRP per quadrant
        elif appt_type_idx == 14:  # Perio
            proc_codes = ['D4910']  # Perio maintenance
        else:
            proc_codes = ['D0120']  # Default
        
        # Generate each procedure
        for code in proc_codes:
            if code not in self.data_store['procedure_codes']:
                continue  # Skip if code not in our set
            
            code_num = self.data_store['procedure_codes'][code]
            
            # Get fee from fee schedule
            proc_fee = self._get_procedure_fee(code)
            
            # Tooth number (if applicable)
            tooth_num = self._random_tooth_number() if random.random() < 0.5 else ''
            
            # Surface (for fillings)
            surf = random.choice(['', 'M', 'O', 'D', 'B', 'MOD', 'DO']) if 'D2' in code else ''
            
            # Create realistic mix of procedure statuses for treatment acceptance metrics
            # Business logic: 
            # - Status 1 (Treatment Planned) = Presented but not accepted
            # - Status 6 (Ordered/Planned) = Presented but not accepted  
            # - Status 2 (Completed) = Accepted (but NOT counted as "presented")
            # Target: ~70-80% acceptance rate means we need MORE "presented" than "accepted"
            # Critical: Status 2 procedures are NOT counted as "presented", so we need enough status 1/6
            # Distribution: 70% presented (status 1/6), 30% accepted (status 2) = 30/70 = 43% acceptance rate
            # OR better: 75% presented, 25% accepted = 25/75 = 33% acceptance rate (too low)
            # Best: 65% presented, 35% accepted = 35/65 = 54% acceptance rate
            # OR: 60% presented, 40% accepted = 40/60 = 67% acceptance rate (realistic)
            rand = random.random()
            if rand < 0.40:
                # 40% completed (accepted) - these show as accepted but NOT as presented
                proc_status = PROCEDURE_STATUS['COMPLETED']
            elif rand < 0.70:
                # 30% treatment planned (presented, not accepted) - these are the denominator
                proc_status = PROCEDURE_STATUS['TREATMENT_PLANNED']
            else:
                # 30% ordered/planned (presented, not accepted) - these are also the denominator
                proc_status = PROCEDURE_STATUS['ORDERED']
            
            procedures.append((
                proc_id,  # ProcNum
                patient_id,  # PatNum
                appt_id,  # AptNum
                proc_date.date(),  # ProcDate
                proc_fee,  # ProcFee
                tooth_num,  # ToothNum
                0,  # Priority (0=normal)
                proc_status,  # ProcStatus (mix of 1, 2, 6 for realistic acceptance rates)
                provider_id,  # ProvNum
                clinic_id,  # ClinicNum
                code_num,  # CodeNum
                surf,  # Surf
                1,  # UnitQty
                0,  # BaseUnits
                proc_date.date(),  # DateEntryC
                datetime.now(),  # DateTStamp
                proc_date.date() if proc_status == PROCEDURE_STATUS['COMPLETED'] else proc_date.date(),  # DateComplete (use proc_date for all, NULL handled by dbt)
                proc_date,  # SecDateEntry (timestamp)
            ))
            proc_id += 1
        
        return procedures
    
    def _generate_recalls(self, db):
        """Generate hygiene recall records"""
        recalls = []
        recall_id = 1
        
        # Generate recalls for patients who have had hygiene visits
        for patient_id, visit_dates in self.patient_visit_dates.items():
            if not visit_dates:
                continue
            
            # Last visit date
            last_visit = max(visit_dates)
            
            # Due date (6 months after last visit)
            due_date = last_visit + timedelta(days=180)
            
            # Only create recall if due date is in the future or recent past
            if due_date > (datetime.now() - timedelta(days=180)):
                recall_type = random.choice(self.data_store['recall_types'])
                
                recalls.append((
                    recall_id,  # RecallNum
                    patient_id,  # PatNum
                    due_date.date(),  # DateDue
                    last_visit.date(),  # DatePrevious
                    recall_type,  # RecallTypeNum
                    6,  # RecallInterval (months)
                    to_opendental_boolean(False),  # IsDisabled
                    '',  # Note
                    datetime.now(),  # DateTStamp
                ))
                recall_id += 1
        
        if recalls:
            sql = """
                INSERT INTO raw.recall (
                    "RecallNum", "PatNum", "DateDue", "DatePrevious", "RecallTypeNum",
                    "RecallInterval", "IsDisabled", "Note", "DateTStamp"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("RecallNum") DO NOTHING
            """
            
            db.execute_batch(sql, recalls)
        
        self.data_store['counts']['recall'] = len(recalls)
        print(f"✓ Generated {len(recalls)} recall records")
    
    def _generate_recall_triggers(self, db):
        """Generate recall trigger links (links recall types to procedure codes)"""
        triggers = []
        trigger_id = 1
        
        # Link recall types to trigger procedures
        recall_trigger_map = [
            (1, 'D1110'),  # Prophy triggered by cleaning
            (1, 'D1120'),  # Prophy triggered by child cleaning
            (2, 'D4910'),  # Perio triggered by perio maintenance
            (3, 'D1120'),  # Child prophy triggered by child cleaning
            (4, 'D1206'),  # Fluoride triggered by child fluoride
        ]
        
        for recall_type_num, code in recall_trigger_map:
            if code in self.data_store['procedure_codes']:
                code_num = self.data_store['procedure_codes'][code]
                
                triggers.append((
                    trigger_id,  # RecallTriggerNum
                    recall_type_num,  # RecallTypeNum
                    code_num,  # CodeNum
                ))
                trigger_id += 1
        
        if triggers:
            sql = """
                INSERT INTO raw.recalltrigger (
                    "RecallTriggerNum", "RecallTypeNum", "CodeNum"
                ) VALUES (%s, %s, %s)
                ON CONFLICT ("RecallTriggerNum") DO NOTHING
            """
            
            db.execute_batch(sql, triggers)
        
        self.data_store['counts']['recalltrigger'] = len(triggers)
        print(f"✓ Generated {len(triggers)} recall triggers")
    
    def _random_business_date(self, start: datetime, end: datetime) -> datetime:
        """Generate random date on a weekday"""
        while True:
            random_date = start + timedelta(
                days=random.randint(0, (end - start).days)
            )
            # Monday = 0, Sunday = 6
            if random_date.weekday() < 5:  # Monday-Friday
                return random_date
    
    def _add_business_hours(self, date: datetime) -> datetime:
        """Add business hours time to a date"""
        # Random time between 8 AM and 5 PM
        hour = random.randint(8, 16)
        minute = random.choice([0, 15, 30, 45])
        return datetime.combine(date.date(), time(hour, minute))
    
    def _get_procedure_fee(self, code: str) -> float:
        """Get fee for procedure code"""
        # Find the code in CDT_CODES (imported from foundation_generator)
        from generators.foundation_data_generator import CDT_CODES
        
        for code_data in CDT_CODES:
            if code_data['code'] == code:
                # Apply fee schedule variance
                base_fee = code_data['fee']
                if random.random() < self.config.standard_fee_match_rate:
                    return base_fee  # Exact match
                else:
                    return base_fee * random.uniform(0.90, 1.10)  # ±10% variance
        
        return 50.0  # Default fee
    
    def _random_tooth_number(self) -> str:
        """Generate random tooth number (1-32)"""
        return str(random.randint(1, 32))


def to_opendental_boolean(value: bool) -> bool:
    """Convert Python boolean to PostgreSQL boolean"""
    return value
