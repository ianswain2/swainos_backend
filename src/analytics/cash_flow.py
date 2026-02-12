from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Dict, Iterable, List, Tuple

from src.models.revenue_bookings import CustomerPaymentRecord, SupplierInvoiceRecord
from src.schemas.revenue_bookings import CashFlowSummary, CashFlowTimeseriesPoint


def calculate_cashflow_summary(
    payments_in: Iterable[CustomerPaymentRecord],
    payments_out: Iterable[SupplierInvoiceRecord],
) -> List[CashFlowSummary]:
    totals: Dict[str, Tuple[Decimal, Decimal]] = defaultdict(lambda: (Decimal("0"), Decimal("0")))
    for payment in payments_in:
        if payment.currency_code and payment.amount is not None:
            cash_in, cash_out = totals[payment.currency_code]
            totals[payment.currency_code] = (cash_in + payment.amount, cash_out)
    for invoice in payments_out:
        if invoice.currency_code:
            cash_out_amount = invoice.paid_amount or Decimal("0")
            cash_in, cash_out = totals[invoice.currency_code]
            totals[invoice.currency_code] = (cash_in, cash_out + cash_out_amount)

    summaries: List[CashFlowSummary] = []
    for currency_code, (cash_in, cash_out) in totals.items():
        summaries.append(
            CashFlowSummary(
                currency_code=currency_code,
                cash_in_total=cash_in,
                cash_out_total=cash_out,
                net_cash_total=cash_in - cash_out,
            )
        )
    return summaries


def calculate_cashflow_timeseries(
    payments_in: Iterable[CustomerPaymentRecord],
    payments_out: Iterable[SupplierInvoiceRecord],
) -> List[CashFlowTimeseriesPoint]:
    buckets: Dict[date, Tuple[Decimal, Decimal]] = defaultdict(lambda: (Decimal("0"), Decimal("0")))
    for payment in payments_in:
        if payment.payment_date and payment.amount is not None:
            cash_in, cash_out = buckets[payment.payment_date]
            buckets[payment.payment_date] = (cash_in + payment.amount, cash_out)
    for invoice in payments_out:
        if invoice.currency_code and invoice.paid_date and invoice.paid_amount is not None:
            cash_in, cash_out = buckets[invoice.paid_date]
            buckets[invoice.paid_date] = (cash_in, cash_out + invoice.paid_amount)

    points = []
    for period_start in sorted(buckets.keys()):
        cash_in, cash_out = buckets[period_start]
        points.append(
            CashFlowTimeseriesPoint(
                period_start=period_start,
                cash_in=cash_in,
                cash_out=cash_out,
                net_cash=cash_in - cash_out,
            )
        )
    return points
