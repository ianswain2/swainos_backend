from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from src.core.supabase import SupabaseClient
from src.schemas.debt_service import (
    DebtBalanceSnapshot,
    DebtCovenantSnapshot,
    DebtFacility,
    DebtFacilityTerm,
    DebtOverviewItem,
    DebtPaymentRecord,
    DebtScenarioSummary,
    DebtSchedulePoint,
)


class DebtServiceRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()

    def list_facilities(self) -> List[DebtFacility]:
        rows, _ = self.client.select(
            table="debt_facilities",
            select="*",
            filters=[("status", "eq.active")],
            order="origination_date.asc",
        )
        return [DebtFacility.model_validate(row) for row in rows]

    def get_facility(self, facility_id: str) -> Optional[DebtFacility]:
        rows, _ = self.client.select(
            table="debt_facilities",
            select="*",
            filters=[("id", f"eq.{facility_id}")],
            limit=1,
        )
        if not rows:
            return None
        return DebtFacility.model_validate(rows[0])

    def get_latest_term(self, facility_id: str) -> Optional[DebtFacilityTerm]:
        rows, _ = self.client.select(
            table="debt_facility_terms",
            select="*",
            filters=[("facility_id", f"eq.{facility_id}")],
            order="effective_start_date.desc",
            limit=1,
        )
        if not rows:
            return None
        return DebtFacilityTerm.model_validate(rows[0])

    def get_term_for_date(self, facility_id: str, as_of_date: date) -> Optional[DebtFacilityTerm]:
        rows, _ = self.client.select(
            table="debt_facility_terms",
            select="*",
            filters=[
                ("facility_id", f"eq.{facility_id}"),
                ("effective_start_date", f"lte.{as_of_date.isoformat()}"),
            ],
            order="effective_start_date.desc",
            limit=25,
        )
        if not rows:
            return None
        for row in rows:
            parsed = DebtFacilityTerm.model_validate(row)
            if parsed.effective_end_date is None or parsed.effective_end_date >= as_of_date:
                return parsed
        return None

    def list_overview_items(self) -> List[DebtOverviewItem]:
        rows, _ = self.client.select(
            table="v_debt_service_overview",
            select="*",
            order="facility_name.asc",
        )
        return [DebtOverviewItem.model_validate(row) for row in rows]

    def list_schedule(
        self,
        facility_id: str,
        start_date: Optional[date],
        end_date: Optional[date],
        limit: int = 240,
    ) -> List[DebtSchedulePoint]:
        filters: List[Tuple[str, str]] = [("facility_id", f"eq.{facility_id}")]
        if start_date:
            filters.append(("due_date", f"gte.{start_date.isoformat()}"))
        if end_date:
            filters.append(("due_date", f"lte.{end_date.isoformat()}"))
        rows, _ = self.client.select(
            table="debt_payment_schedule",
            select="*",
            filters=filters,
            order="due_date.asc",
            limit=limit,
        )
        return [DebtSchedulePoint.model_validate(row) for row in rows]

    def upsert_schedule_rows(self, rows: List[Dict[str, Any]]) -> List[DebtSchedulePoint]:
        if not rows:
            return []
        inserted = self.client.insert(
            table="debt_payment_schedule",
            payload=rows,
            upsert=True,
            on_conflict="facility_id,due_date",
        )
        return [DebtSchedulePoint.model_validate(row) for row in inserted]

    def list_payments(self, facility_id: str, limit: int = 240) -> List[DebtPaymentRecord]:
        rows, _ = self.client.select(
            table="debt_payments_actual",
            select="*",
            filters=[("facility_id", f"eq.{facility_id}")],
            order="payment_date.desc",
            limit=limit,
        )
        return [DebtPaymentRecord.model_validate(row) for row in rows]

    def insert_payment(self, payload: Dict[str, Any]) -> DebtPaymentRecord:
        inserted = self.client.insert(
            table="debt_payments_actual",
            payload=payload,
        )
        return DebtPaymentRecord.model_validate(inserted[0])

    def get_latest_snapshot(self, facility_id: str) -> Optional[DebtBalanceSnapshot]:
        rows, _ = self.client.select(
            table="debt_balance_snapshots",
            select="*",
            filters=[("facility_id", f"eq.{facility_id}")],
            order="as_of_date.desc",
            limit=1,
        )
        if not rows:
            return None
        return DebtBalanceSnapshot.model_validate(rows[0])

    def upsert_snapshot(self, payload: Dict[str, Any]) -> DebtBalanceSnapshot:
        inserted = self.client.insert(
            table="debt_balance_snapshots",
            payload=payload,
            upsert=True,
            on_conflict="facility_id,as_of_date",
        )
        return DebtBalanceSnapshot.model_validate(inserted[0])

    def get_next_schedule_after_date(self, facility_id: str, as_of_date: date) -> Optional[DebtSchedulePoint]:
        rows, _ = self.client.select(
            table="debt_payment_schedule",
            select="*",
            filters=[
                ("facility_id", f"eq.{facility_id}"),
                ("due_date", f"gt.{as_of_date.isoformat()}"),
            ],
            order="due_date.asc",
            limit=1,
        )
        if not rows:
            return None
        return DebtSchedulePoint.model_validate(rows[0])

    def create_scenario(
        self,
        facility_id: str,
        scenario_name: str,
        start_date: date,
    ) -> str:
        inserted = self.client.insert(
            table="debt_scenarios",
            payload={
                "facility_id": facility_id,
                "scenario_name": scenario_name,
                "start_date": start_date.isoformat(),
            },
        )
        return str(inserted[0]["id"])

    def insert_scenario_events(self, scenario_id: str, events: List[Dict[str, Any]]) -> None:
        if not events:
            return
        rows = [{**event, "scenario_id": scenario_id} for event in events]
        self.client.insert(table="debt_scenario_events", payload=rows)

    def update_scenario_result(
        self,
        scenario_id: str,
        payoff_date: Optional[date],
        total_interest_amount: Decimal,
        total_principal_amount: Decimal,
        total_interest_delta_amount: Decimal,
        payoff_date_delta_days: Optional[int],
        metadata: Dict[str, Any],
    ) -> None:
        self.client.update(
            table="debt_scenarios",
            payload={
                "payoff_date": payoff_date.isoformat() if payoff_date else None,
                "total_interest_amount": str(total_interest_amount),
                "total_principal_amount": str(total_principal_amount),
                "total_interest_delta_amount": str(total_interest_delta_amount),
                "payoff_date_delta_days": payoff_date_delta_days,
                "metadata": metadata,
            },
            filters=[("id", f"eq.{scenario_id}")],
        )

    def list_scenarios(self, facility_id: str, limit: int = 50) -> List[DebtScenarioSummary]:
        rows, _ = self.client.select(
            table="debt_scenarios",
            select=(
                "id,facility_id,scenario_name,scenario_type,start_date,is_baseline,payoff_date,"
                "total_interest_amount,total_principal_amount,total_interest_delta_amount,payoff_date_delta_days,"
                "created_at,updated_at"
            ),
            filters=[("facility_id", f"eq.{facility_id}")],
            order="created_at.desc",
            limit=limit,
        )
        return [DebtScenarioSummary.model_validate(row) for row in rows]

    def list_latest_covenant_snapshots(self) -> List[DebtCovenantSnapshot]:
        rows, _ = self.client.select(
            table="debt_covenant_snapshots",
            select=(
                "covenant_id,facility_id,as_of_date,measured_value,threshold_value,is_in_compliance,note,"
                "debt_covenants!inner(covenant_code,covenant_name,metric_name,comparison_operator)"
            ),
            order="as_of_date.desc",
            limit=500,
        )

        latest_by_covenant: Dict[str, DebtCovenantSnapshot] = {}
        for row in rows:
            covenant_id = str(row.get("covenant_id"))
            if covenant_id in latest_by_covenant:
                continue
            covenant_info = row.get("debt_covenants") or {}
            latest_by_covenant[covenant_id] = DebtCovenantSnapshot.model_validate(
                {
                    "covenant_id": covenant_id,
                    "facility_id": row.get("facility_id"),
                    "covenant_code": covenant_info.get("covenant_code"),
                    "covenant_name": covenant_info.get("covenant_name"),
                    "metric_name": covenant_info.get("metric_name"),
                    "threshold_value": row.get("threshold_value"),
                    "comparison_operator": covenant_info.get("comparison_operator"),
                    "as_of_date": row.get("as_of_date"),
                    "measured_value": row.get("measured_value"),
                    "is_in_compliance": row.get("is_in_compliance"),
                    "note": row.get("note"),
                }
            )
        return list(latest_by_covenant.values())
