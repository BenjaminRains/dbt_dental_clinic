# api/models/patient.py
from pydantic import BaseModel
from typing import Optional, List, Dict
from datetime import datetime

class Patient(BaseModel):
    patient_id: int
    first_name: str
    last_name: str
    birth_date: Optional[datetime] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    
    class Config:
        orm_mode = True