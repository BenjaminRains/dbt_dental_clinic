from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class AppointmentSummary(BaseModel):
    date: str
    provider_name: str
    total_appointments: int
    completed_appointments: int
    no_show_appointments: int
    broken_appointments: int
    new_patient_appointments: int
    hygiene_appointments: int
    unique_patients: int
    completion_rate: float
    no_show_rate: float
    cancellation_rate: float
    utilization_rate: float
    scheduled_production: float
    completed_production: float

class AppointmentDetail(BaseModel):
    appointment_id: int
    patient_id: int
    provider_name: str
    appointment_date: str
    appointment_time: str
    appointment_type: str
    appointment_status: str
    is_completed: bool
    is_no_show: bool
    is_broken: bool
    scheduled_production_amount: float
    appointment_length_minutes: int

class AppointmentCreate(BaseModel):
    patient_id: int
    provider_id: int
    appointment_date: str
    appointment_time: str
    appointment_type: str
    appointment_length_minutes: int
    scheduled_production_amount: Optional[float] = 0.0

class AppointmentUpdate(BaseModel):
    appointment_status: Optional[str] = None
    is_completed: Optional[bool] = None
    is_no_show: Optional[bool] = None
    is_broken: Optional[bool] = None
    scheduled_production_amount: Optional[float] = None
    appointment_length_minutes: Optional[int] = None
