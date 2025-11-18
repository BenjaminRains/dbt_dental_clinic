# api/models/hygiene.py
from pydantic import BaseModel
from typing import Optional

class HygieneRetentionSummary(BaseModel):
    """KPI summary for Hygiene Retention dashboard"""
    recall_current_percent: float  # Recall Current %
    hyg_pre_appointment_percent: float  # Hyg Pre-Appointment Any %
    hyg_patients_seen: int  # Hyg Patients Seen
    hyg_pts_reappntd: int  # Hyg Pts Re-appntd
    recall_overdue_percent: float  # Recall Overdue %
    not_on_recall_percent: float  # Not on Recall %
    
    class Config:
        from_attributes = True

