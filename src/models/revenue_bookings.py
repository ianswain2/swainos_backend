from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel


class BookingRecord(BaseModel):
    id: str
    external_id: Optional[str] = None
    itinerary_id: Optional[str] = None
    booking_number: Optional[str] = None
    booking_type: Optional[str] = None
    supplier_id: Optional[str] = None
    service_name: Optional[str] = None
    service_start_date: Optional[date] = None
    service_end_date: Optional[date] = None
    location_country: Optional[str] = None
    location_city: Optional[str] = None
    gross_amount: Optional[Decimal] = None
    net_amount: Optional[Decimal] = None
    commission_amount: Optional[Decimal] = None
    currency_code: Optional[str] = None
    is_deleted: Optional[bool] = None
    confirmation_number: Optional[str] = None
    synced_at: Optional[datetime] = None


class CustomerPaymentRecord(BaseModel):
    id: str
    external_id: Optional[str] = None
    itinerary_id: Optional[str] = None
    payment_date: Optional[date] = None
    amount: Optional[Decimal] = None
    currency_code: Optional[str] = None
    payment_status: Optional[str] = None
    received_at: Optional[datetime] = None


class SupplierInvoiceRecord(BaseModel):
    id: str
    external_id: Optional[str] = None
    supplier_id: Optional[str] = None
    invoice_date: Optional[date] = None
    due_date: Optional[date] = None
    total_amount: Optional[Decimal] = None
    paid_amount: Optional[Decimal] = None
    currency_code: Optional[str] = None
    invoice_status: Optional[str] = None
    payment_status: Optional[str] = None
    paid_date: Optional[date] = None
    synced_at: Optional[datetime] = None


class ItineraryTrendRecord(BaseModel):
    period_start: date
    created_count: int
    closed_count: int
    travel_start_count: int
    travel_end_count: int


class ItineraryLeadFlowRecord(BaseModel):
    period_start: date
    created_count: int
    closed_won_count: int
    closed_lost_count: int
