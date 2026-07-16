from datetime import date
from typing import List, Optional

from pydantic import BaseModel


class LatestCollectionsDate(BaseModel):
    """Most recent payment_date in mart_daily_payments with collection activity."""

    payment_date: Optional[date]
    has_data: bool

    class Config:
        from_attributes = True


class DailyCollectionsKPI(BaseModel):
    """Daily net collections from mart_daily_payments (validated vs OD Daily Payments)."""

    payment_date: date
    net_collections_amount: float
    patient_payment_amount: float
    insurance_payment_amount: float
    payment_count: int
    has_data: bool

    class Config:
        from_attributes = True


class LatestProductionDate(BaseModel):
    """Most recent production_date in mart_daily_production_by_procedure with activity."""

    production_date: Optional[date]
    has_data: bool

    class Config:
        from_attributes = True


class DailyProductionKPI(BaseModel):
    """Day rollup from mart_daily_production_by_procedure (validated vs OD Production by Procedure)."""

    production_date: date
    total_fees: float
    procedure_quantity: int
    procedure_code_count: int
    has_data: bool

    class Config:
        from_attributes = True


class DailyProductionByCodeRow(BaseModel):
    """One procedure-code row for a production_date."""

    production_date: date
    procedure_code: str
    procedure_description: Optional[str] = None
    procedure_category: Optional[str] = None
    procedure_quantity: int
    average_fee: float
    total_fees: float

    class Config:
        from_attributes = True


class DailyProductionByCodeResponse(BaseModel):
    """By-code detail for a production_date."""

    production_date: date
    rows: List[DailyProductionByCodeRow]
    has_data: bool

    class Config:
        from_attributes = True
