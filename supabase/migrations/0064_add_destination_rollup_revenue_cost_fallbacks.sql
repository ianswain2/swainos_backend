-- Forward-only migration:
-- Add revenue/cost fallback logic for destination booked rollups so sparse pricing exports
-- do not force misleading negative margin from zero-revenue rows.

drop materialized view if exists public.mv_itinerary_destination_booked_monthly;

create materialized view public.mv_itinerary_destination_booked_monthly as
with status_map as (
  select
    lower(trim(coalesce(status_value, ''))) as status_key,
    lower(trim(coalesce(pipeline_bucket, ''))) as pipeline_bucket,
    coalesce(is_filter_out, false) as is_filter_out
  from public.itinerary_status_reference
),
scoped_items as (
  select
    date_trunc('month', ii.service_start_date)::date as period_start,
    (date_trunc('month', ii.service_start_date)::date + interval '1 month - 1 day')::date as period_end,
    coalesce(nullif(trim(ii.location_country), ''), 'Unknown') as location_country,
    coalesce(nullif(trim(ii.location_city), ''), 'Unspecified') as location_city,
    ii.itinerary_id,
    ii.quantity,
    ii.profit_margin_percent,
    coalesce(ii.is_cancelled, false) as is_cancelled,
    coalesce(ii.is_deleted, false) as is_deleted,
    coalesce(
      nullif(ii.total_price, 0),
      nullif(ii.subtotal_price, 0),
      case
        when ii.unit_price is not null and ii.quantity is not null then ii.unit_price * ii.quantity
        else null
      end
    ) as derived_revenue_amount,
    coalesce(
      nullif(ii.total_cost, 0),
      nullif(ii.subtotal_cost, 0),
      case
        when ii.unit_cost is not null and ii.quantity is not null then ii.unit_cost * ii.quantity
        else null
      end
    ) as derived_cost_amount,
    coalesce(
      ii.gross_margin,
      case
        when (
          coalesce(
            nullif(ii.total_price, 0),
            nullif(ii.subtotal_price, 0),
            case
              when ii.unit_price is not null and ii.quantity is not null then ii.unit_price * ii.quantity
              else null
            end
          ) is not null
          and
          coalesce(
            nullif(ii.total_cost, 0),
            nullif(ii.subtotal_cost, 0),
            case
              when ii.unit_cost is not null and ii.quantity is not null then ii.unit_cost * ii.quantity
              else null
            end
          ) is not null
        )
          then
            coalesce(
              nullif(ii.total_price, 0),
              nullif(ii.subtotal_price, 0),
              case
                when ii.unit_price is not null and ii.quantity is not null then ii.unit_price * ii.quantity
                else null
              end
            )
            -
            coalesce(
              nullif(ii.total_cost, 0),
              nullif(ii.subtotal_cost, 0),
              case
                when ii.unit_cost is not null and ii.quantity is not null then ii.unit_cost * ii.quantity
                else null
              end
            )
        else null
      end
    ) as derived_margin_amount
  from public.itinerary_items ii
  inner join public.itineraries i
    on i.id = ii.itinerary_id
  left join status_map sm
    on sm.status_key = lower(trim(coalesce(i.itinerary_status, '')))
  where ii.service_start_date is not null
    and coalesce(sm.is_filter_out, false) = false
    and (
      coalesce(sm.pipeline_bucket, '') = 'closed_won'
      or lower(trim(coalesce(i.itinerary_status, ''))) = 'confirmed'
    )
),
rolled as (
  select
    period_start,
    period_end,
    location_country,
    location_city,
    count(*)::bigint as total_item_count,
    count(*) filter (where is_cancelled) ::bigint as cancelled_item_count,
    count(*) filter (where is_deleted) ::bigint as deleted_item_count,
    count(*) filter (where not is_cancelled and not is_deleted) ::bigint as active_item_count,
    count(distinct itinerary_id) filter (where not is_cancelled and not is_deleted) ::bigint as booked_itinerary_count,
    coalesce(sum(coalesce(quantity, 0)) filter (where not is_cancelled and not is_deleted), 0) as booked_quantity,
    coalesce(sum(coalesce(derived_cost_amount, 0)) filter (where not is_cancelled and not is_deleted), 0) as booked_total_cost,
    coalesce(sum(coalesce(derived_revenue_amount, 0)) filter (where not is_cancelled and not is_deleted), 0) as booked_total_price,
    coalesce(
      sum(coalesce(derived_margin_amount, 0)) filter (where not is_cancelled and not is_deleted),
      0
    ) as booked_gross_margin,
    coalesce(
      avg(profit_margin_percent) filter (where not is_cancelled and not is_deleted),
      0
    ) as booked_avg_profit_margin_percent
  from scoped_items
  group by 1, 2, 3, 4
)
select
  period_start,
  period_end,
  location_country,
  location_city,
  total_item_count,
  cancelled_item_count,
  deleted_item_count,
  active_item_count,
  booked_itinerary_count,
  booked_quantity,
  booked_total_cost,
  booked_total_price,
  booked_gross_margin,
  booked_avg_profit_margin_percent,
  case
    when booked_total_price > 0 then booked_gross_margin / booked_total_price
    else 0
  end as booked_margin_pct
from rolled;

create unique index if not exists idx_mv_itinerary_destination_booked_monthly_unique
  on public.mv_itinerary_destination_booked_monthly(period_start, location_country, location_city);

create index if not exists idx_mv_itinerary_destination_booked_monthly_country_period
  on public.mv_itinerary_destination_booked_monthly(location_country, period_start);

create index if not exists idx_mv_itinerary_destination_booked_monthly_city_period
  on public.mv_itinerary_destination_booked_monthly(location_city, period_start);
