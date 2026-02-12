from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional

from src.shared.base import BaseSchema


class SalesforceBooking(BaseSchema):
    external_id: str
    booking_number: Optional[str] = None
    is_deleted: Optional[bool] = None
    service_start_date: Optional[date] = None
    service_end_date: Optional[date] = None
    gross_amount: Optional[Decimal] = None
    net_amount: Optional[Decimal] = None
    currency_code: Optional[str] = None


class SalesforceCustomerPayment(BaseSchema):
    external_id: str
    itinerary_id: Optional[str] = None
    payment_date: Optional[date] = None
    amount: Optional[Decimal] = None
    currency_code: Optional[str] = None


class SalesforceSupplierInvoice(BaseSchema):
    external_id: str
    invoice_date: Optional[date] = None
    due_date: Optional[date] = None
    total_amount: Optional[Decimal] = None
    currency_code: Optional[str] = None
