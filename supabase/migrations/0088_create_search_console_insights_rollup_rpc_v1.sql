-- Supabase-first Search Console marts for snapshot-serving insights.

create or replace function public.marketing_search_console_insights_rollup_v1(
  p_days_back integer,
  p_country_scope text default 'all',
  p_device_scope text default 'all'
)
returns jsonb
language sql
stable
as $$
with scoped_daily as (
  select *
  from public.marketing_search_console_daily
  where country_scope = coalesce(nullif(trim(p_country_scope), ''), 'all')
    and device_scope = coalesce(nullif(trim(p_device_scope), ''), 'all')
),
as_of as (
  select max(snapshot_date) as as_of_date
  from scoped_daily
),
windowed as (
  select
    as_of_date,
    greatest(coalesce(p_days_back, 30), 1) as days_back,
    (as_of_date - (greatest(coalesce(p_days_back, 30), 1) - 1) * interval '1 day')::date as current_start,
    as_of_date as current_end,
    (as_of_date - (greatest(coalesce(p_days_back, 30), 1) * 2 - 1) * interval '1 day')::date as previous_start,
    (as_of_date - greatest(coalesce(p_days_back, 30), 1) * interval '1 day')::date as previous_end
  from as_of
  where as_of_date is not null
),
current_rollup as (
  select
    coalesce(sum(d.clicks), 0)::numeric as clicks,
    coalesce(sum(d.impressions), 0)::numeric as impressions,
    coalesce(sum(d.average_position * d.impressions), 0)::numeric as position_weight
  from scoped_daily d
  join windowed w on d.snapshot_date between w.current_start and w.current_end
),
previous_rollup as (
  select
    coalesce(sum(d.clicks), 0)::numeric as clicks,
    coalesce(sum(d.impressions), 0)::numeric as impressions,
    coalesce(sum(d.average_position * d.impressions), 0)::numeric as position_weight
  from scoped_daily d
  join windowed w on d.snapshot_date between w.previous_start and w.previous_end
),
query_rollup as (
  select
    q.query,
    bool_or(q.is_branded) as is_branded,
    coalesce(sum(q.clicks), 0)::numeric as clicks,
    coalesce(sum(q.impressions), 0)::numeric as impressions,
    coalesce(sum(q.average_position * q.impressions), 0)::numeric as position_weight
  from public.marketing_search_console_query_daily q
  join windowed w on q.snapshot_date between w.current_start and w.current_end
  where q.country_scope = coalesce(nullif(trim(p_country_scope), ''), 'all')
    and q.device_scope = coalesce(nullif(trim(p_device_scope), ''), 'all')
  group by q.query
),
page_rollup as (
  select
    p.page_path,
    coalesce(sum(p.clicks), 0)::numeric as clicks,
    coalesce(sum(p.impressions), 0)::numeric as impressions,
    coalesce(sum(p.average_position * p.impressions), 0)::numeric as position_weight
  from public.marketing_search_console_page_daily p
  join windowed w on p.snapshot_date between w.current_start and w.current_end
  where p.country_scope = coalesce(nullif(trim(p_country_scope), ''), 'all')
    and p.device_scope = coalesce(nullif(trim(p_device_scope), ''), 'all')
  group by p.page_path
),
page_query_rollup as (
  select
    pq.page_path,
    pq.query,
    coalesce(sum(pq.clicks), 0)::numeric as clicks,
    coalesce(sum(pq.impressions), 0)::numeric as impressions,
    coalesce(sum(pq.average_position * pq.impressions), 0)::numeric as position_weight
  from public.marketing_search_console_page_query_daily pq
  join windowed w on pq.snapshot_date between w.current_start and w.current_end
  where pq.country_scope = coalesce(nullif(trim(p_country_scope), ''), 'all')
    and pq.device_scope = coalesce(nullif(trim(p_device_scope), ''), 'all')
  group by pq.page_path, pq.query
),
country_rollup as (
  select
    c.country as label,
    coalesce(sum(c.clicks), 0)::numeric as clicks,
    coalesce(sum(c.impressions), 0)::numeric as impressions,
    coalesce(sum(c.average_position * c.impressions), 0)::numeric as position_weight
  from public.marketing_search_console_country_daily c
  join windowed w on c.snapshot_date between w.current_start and w.current_end
  where coalesce(nullif(trim(p_country_scope), ''), 'all') = 'all'
  group by c.country
),
device_rollup as (
  select
    d.device as label,
    coalesce(sum(d.clicks), 0)::numeric as clicks,
    coalesce(sum(d.impressions), 0)::numeric as impressions,
    coalesce(sum(d.average_position * d.impressions), 0)::numeric as position_weight
  from public.marketing_search_console_device_daily d
  join windowed w on d.snapshot_date between w.current_start and w.current_end
  where coalesce(nullif(trim(p_country_scope), ''), 'all') = 'all'
  group by d.device
),
opportunity_rows as (
  select
    row_number() over (
      order by case when bucket = 'low_ctr' then 1 else 2 end, impressions desc, query asc
    ) as rank_no,
    bucket,
    query,
    clicks,
    impressions,
    ctr,
    average_position
  from (
    select
      'low_ctr'::text as bucket,
      q.query,
      q.clicks,
      q.impressions,
      case when q.impressions > 0 then q.clicks / q.impressions else 0 end::numeric as ctr,
      case when q.impressions > 0 then q.position_weight / q.impressions else 0 end::numeric as average_position
    from query_rollup q
    where q.impressions >= 100
      and (case when q.impressions > 0 then q.clicks / q.impressions else 0 end) < 0.03

    union all

    select
      'near_breakout'::text as bucket,
      q.query,
      q.clicks,
      q.impressions,
      case when q.impressions > 0 then q.clicks / q.impressions else 0 end::numeric as ctr,
      case when q.impressions > 0 then q.position_weight / q.impressions else 0 end::numeric as average_position
    from query_rollup q
    where q.impressions >= 50
      and (case when q.impressions > 0 then q.position_weight / q.impressions else 0 end) between 4 and 20
  ) u
),
challenge_rows as (
  select
    row_number() over (order by pq.impressions desc, pq.page_path asc, pq.query asc) as rank_no,
    pq.page_path,
    pq.query,
    pq.clicks,
    pq.impressions,
    case when pq.impressions > 0 then pq.clicks / pq.impressions else 0 end::numeric as ctr,
    case when pq.impressions > 0 then pq.position_weight / pq.impressions else 0 end::numeric as average_position
  from page_query_rollup pq
  where pq.impressions >= 30
    and (case when pq.impressions > 0 then pq.clicks / pq.impressions else 0 end) < 0.025
)
select
  case
    when (select as_of_date from as_of) is null then
      jsonb_build_object(
        'as_of_date', null,
        'freshness_days', null,
        'query_row_count', 0,
        'overview', jsonb_build_object(
          'total_clicks', 0,
          'total_impressions', 0,
          'average_ctr', 0,
          'average_position', 0,
          'clicks_delta_pct', null,
          'impressions_delta_pct', null,
          'ctr_delta_pct', null,
          'position_delta', null
        ),
        'top_queries', '[]'::jsonb,
        'top_pages', '[]'::jsonb,
        'country_breakdown', '[]'::jsonb,
        'device_breakdown', '[]'::jsonb,
        'opportunities', '[]'::jsonb,
        'challenges', '[]'::jsonb
      )
    else
      jsonb_build_object(
        'as_of_date', (select as_of_date from windowed),
        'freshness_days', ((current_date - (select as_of_date from windowed))::integer),
        'query_row_count', (select count(*) from query_rollup),
        'overview', jsonb_build_object(
          'total_clicks', (select clicks from current_rollup),
          'total_impressions', (select impressions from current_rollup),
          'average_ctr',
            case
              when (select impressions from current_rollup) > 0
                then (select clicks from current_rollup) / (select impressions from current_rollup)
              else 0
            end,
          'average_position',
            case
              when (select impressions from current_rollup) > 0
                then (select position_weight from current_rollup) / (select impressions from current_rollup)
              else 0
            end,
          'clicks_delta_pct',
            case
              when (select clicks from previous_rollup) > 0
                then ((select clicks from current_rollup) - (select clicks from previous_rollup))
                  / (select clicks from previous_rollup)
              else null
            end,
          'impressions_delta_pct',
            case
              when (select impressions from previous_rollup) > 0
                then ((select impressions from current_rollup) - (select impressions from previous_rollup))
                  / (select impressions from previous_rollup)
              else null
            end,
          'ctr_delta_pct',
            case
              when (select impressions from previous_rollup) > 0 and (select clicks from previous_rollup) > 0
                then (
                  (
                    (select clicks from current_rollup)
                    / nullif((select impressions from current_rollup), 0)
                  )
                  - (
                    (select clicks from previous_rollup)
                    / nullif((select impressions from previous_rollup), 0)
                  )
                )
                / (
                  (select clicks from previous_rollup)
                  / nullif((select impressions from previous_rollup), 0)
                )
              else null
            end,
          'position_delta',
            case
              when (select impressions from previous_rollup) > 0
                then (
                  (
                    (select position_weight from current_rollup)
                    / nullif((select impressions from current_rollup), 0)
                  ) - (
                    (select position_weight from previous_rollup)
                    / nullif((select impressions from previous_rollup), 0)
                  )
                )
              else null
            end
        ),
        'top_queries',
          coalesce((
            select jsonb_agg(
              jsonb_build_object(
                'query', q.query,
                'clicks', q.clicks,
                'impressions', q.impressions,
                'ctr', case when q.impressions > 0 then q.clicks / q.impressions else 0 end,
                'average_position',
                  case when q.impressions > 0 then q.position_weight / q.impressions else 0 end,
                'is_branded', q.is_branded
              )
              order by q.clicks desc, q.impressions desc, q.query asc
            )
            from (select * from query_rollup order by clicks desc, impressions desc limit 25) q
          ), '[]'::jsonb),
        'top_pages',
          coalesce((
            select jsonb_agg(
              jsonb_build_object(
                'page_path', p.page_path,
                'clicks', p.clicks,
                'impressions', p.impressions,
                'ctr', case when p.impressions > 0 then p.clicks / p.impressions else 0 end,
                'average_position',
                  case when p.impressions > 0 then p.position_weight / p.impressions else 0 end
              )
              order by p.clicks desc, p.impressions desc, p.page_path asc
            )
            from (select * from page_rollup order by clicks desc, impressions desc limit 25) p
          ), '[]'::jsonb),
        'country_breakdown',
          coalesce((
            select jsonb_agg(
              jsonb_build_object(
                'label', c.label,
                'clicks', c.clicks,
                'impressions', c.impressions,
                'ctr', case when c.impressions > 0 then c.clicks / c.impressions else 0 end,
                'average_position',
                  case when c.impressions > 0 then c.position_weight / c.impressions else 0 end
              )
              order by c.clicks desc, c.impressions desc, c.label asc
            )
            from (select * from country_rollup order by clicks desc, impressions desc limit 12) c
          ), '[]'::jsonb),
        'device_breakdown',
          coalesce((
            select jsonb_agg(
              jsonb_build_object(
                'label', d.label,
                'clicks', d.clicks,
                'impressions', d.impressions,
                'ctr', case when d.impressions > 0 then d.clicks / d.impressions else 0 end,
                'average_position',
                  case when d.impressions > 0 then d.position_weight / d.impressions else 0 end
              )
              order by d.clicks desc, d.impressions desc, d.label asc
            )
            from (select * from device_rollup order by clicks desc, impressions desc limit 8) d
          ), '[]'::jsonb),
        'opportunities',
          coalesce((
            select jsonb_agg(
              jsonb_build_object(
                'opportunity_id',
                  case
                    when o.bucket = 'low_ctr' then 'low-ctr-query-' || o.rank_no::text
                    else 'near-breakout-query-' || o.rank_no::text
                  end,
                'title',
                  case
                    when o.bucket = 'low_ctr' then 'Improve CTR on high-impression query'
                    else 'Push query into top positions'
                  end,
                'summary',
                  case
                    when o.bucket = 'low_ctr'
                      then quote_literal(o.query) || ' has strong demand but weak click-through.'
                    else quote_literal(o.query) || ' is close to top rankings and can be improved with focused page updates.'
                  end,
                'query', o.query,
                'clicks', o.clicks,
                'impressions', o.impressions,
                'ctr', o.ctr,
                'average_position', o.average_position,
                'priority_score',
                  case
                    when o.bucket = 'low_ctr' then least(100::numeric, o.impressions / 20)
                    else greatest(10::numeric, 25 - o.average_position)
                  end,
                'recommended_action',
                  case
                    when o.bucket = 'low_ctr'
                      then 'Refresh title/meta and align snippet value proposition with search intent.'
                    else 'Strengthen the mapped destination page with intent-specific copy and internal links.'
                  end
              )
              order by o.rank_no
            )
            from (select * from opportunity_rows order by rank_no limit 10) o
          ), '[]'::jsonb),
        'challenges',
          coalesce((
            select jsonb_agg(
              jsonb_build_object(
                'challenge_id', 'page-query-ctr-gap-' || c.rank_no::text,
                'title', 'Page underperforms for high-demand intent',
                'summary',
                  c.page_path || ' is receiving impressions but not converting demand into clicks.',
                'page_path', c.page_path,
                'query', c.query,
                'clicks', c.clicks,
                'impressions', c.impressions,
                'ctr', c.ctr,
                'average_position', c.average_position,
                'severity_score', least(100::numeric, c.impressions / 15),
                'recommended_action',
                  'Rework on-page headings and SERP snippets, then validate search-intent alignment against top competing pages.'
              )
              order by c.rank_no
            )
            from (select * from challenge_rows order by rank_no limit 10) c
          ), '[]'::jsonb)
      )
  end;
$$;

grant execute on function public.marketing_search_console_insights_rollup_v1(integer, text, text) to authenticated;
grant execute on function public.marketing_search_console_insights_rollup_v1(integer, text, text) to service_role;
