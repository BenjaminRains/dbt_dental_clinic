from datetime import date
from typing import Optional

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
