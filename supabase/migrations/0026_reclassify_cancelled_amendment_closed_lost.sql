-- Reclassify statuses for closed-lost alignment.
-- Business rule update:
--   - Cancelled -> closed_lost
--   - Amendment Merged -> closed_lost

update public.itinerary_status_reference
set
  pipeline_category = 'Closed - Lost',
  pipeline_bucket = 'closed_lost',
  updated_at = now()
where status_value in ('Cancelled', 'Amendment Merged');

-- Refresh materialized views that rely on itinerary_status_reference classification.
refresh materialized view public.mv_itinerary_revenue_monthly;
refresh materialized view public.mv_itinerary_revenue_weekly;
refresh materialized view public.mv_itinerary_deposit_monthly;
refresh materialized view public.mv_itinerary_consortia_monthly;
refresh materialized view public.mv_itinerary_trade_agency_monthly;
refresh materialized view public.mv_itinerary_consortia_actuals_monthly;
refresh materialized view public.mv_itinerary_trade_agency_actuals_monthly;
