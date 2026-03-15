-- US-first Search Console workspace rollups and page profile drill-down RPCs.

create or replace function public.marketing_search_console_us_workspace_v1(
  p_days_back integer
)
returns jsonb
language sql
stable
as $$
with us_rollup as (
  select public.marketing_search_console_insights_rollup_v1(
    greatest(coalesce(p_days_back, 30), 1),
    'United States',
    'all'
  ) as payload
),
params as (
  select
    greatest(coalesce(p_days_back, 30), 1) as days_back,
    (us_rollup.payload ->> 'as_of_date')::date as as_of_date
  from us_rollup
),
windowed as (
  select
    as_of_date,
    days_back,
    (as_of_date - (days_back - 1) * interval '1 day')::date as start_date
  from params
  where as_of_date is not null
),
market_totals as (
  select
    case
      when lower(country) in ('united states', 'usa') then 'United States'
      when lower(country) in ('australia', 'aus') then 'Australia'
      when lower(country) in ('new zealand', 'nzl') then 'New Zealand'
      when lower(country) in ('south africa', 'zaf') then 'South Africa'
      else country
    end as market_label,
    coalesce(sum(clicks), 0)::numeric as clicks,
    coalesce(sum(impressions), 0)::numeric as impressions,
    coalesce(sum(average_position * impressions), 0)::numeric as position_weight
  from public.marketing_search_console_country_daily
  join windowed on snapshot_date between windowed.start_date and windowed.as_of_date
  where lower(country) in ('united states', 'usa', 'australia', 'aus', 'new zealand', 'nzl', 'south africa', 'zaf')
  group by 1
),
us_query_rollup as (
  select
    query,
    bool_or(is_branded) as is_branded,
    coalesce(sum(clicks), 0)::numeric as clicks,
    coalesce(sum(impressions), 0)::numeric as impressions,
    coalesce(sum(average_position * impressions), 0)::numeric as position_weight
  from public.marketing_search_console_query_daily
  join windowed on snapshot_date between windowed.start_date and windowed.as_of_date
  where country_scope = 'United States'
    and device_scope = 'all'
  group by query
),
classified_queries as (
  select
    query,
    is_branded,
    clicks,
    impressions,
    case when impressions > 0 then clicks / impressions else 0 end::numeric as ctr,
    case when impressions > 0 then position_weight / impressions else 0 end::numeric as average_position,
    array_length(regexp_split_to_array(trim(query), '\s+'), 1) as word_count,
    query ~ '[^\x00-\x7F]' as has_non_ascii,
    query ~* '(สล็อต|บาคาร่า|รับ100|ฝาก|พนัน|casino|bet)' as has_spam_pattern,
    query ~* '(travel|tour|trip|vacation|holiday|itinerary|honeymoon|luxury|safari|fiji|botswana|australia|new zealand|south africa|africa|island|where is|best time|things to do)' as has_travel_pattern
  from us_query_rollup
  where trim(query) <> ''
),
travel_queries as (
  select
    query,
    clicks,
    impressions,
    ctr,
    average_position,
    is_branded,
    case
      when is_branded then 'brand'
      when query ~* '(where is|best time|things to do|itinerary|honeymoon)' then 'long_tail_intent'
      when query ~* '(fiji|botswana|australia|new zealand|south africa|africa|island)' then 'destination_intent'
      when query ~* '(travel|tour|trip|vacation|holiday|safari|luxury)' then 'core_travel_intent'
      else 'other_travel_intent'
    end as intent_bucket,
    case
      when coalesce(word_count, 0) <= 2 then 'short_tail'
      else 'long_tail'
    end as term_type,
    case
      when average_position <= 3 then '1-3'
      when average_position <= 10 then '4-10'
      when average_position <= 20 then '11-20'
      else '21+'
    end as position_band
  from classified_queries
  where has_non_ascii = false
    and has_spam_pattern = false
    and has_travel_pattern = true
),
intent_buckets as (
  select
    intent_bucket as bucket_label,
    count(*)::integer as query_count,
    coalesce(sum(clicks), 0)::numeric as clicks,
    coalesce(sum(impressions), 0)::numeric as impressions
  from travel_queries
  group by intent_bucket
),
position_bands as (
  select
    position_band as band_label,
    count(*)::integer as query_count,
    coalesce(sum(clicks), 0)::numeric as clicks,
    coalesce(sum(impressions), 0)::numeric as impressions
  from travel_queries
  group by position_band
)
select
  case
    when (select as_of_date from params) is null then
      jsonb_build_object(
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
        'as_of_date', null,
        'freshness_days', null,
        'top_queries', '[]'::jsonb,
        'top_pages', '[]'::jsonb,
        'opportunities', '[]'::jsonb,
        'challenges', '[]'::jsonb,
        'query_row_count', 0,
        'market_benchmarks', '[]'::jsonb,
        'query_intent_buckets', '[]'::jsonb,
        'position_band_summary', '[]'::jsonb
      )
    else
      jsonb_build_object(
        'overview', (select payload -> 'overview' from us_rollup),
        'as_of_date', (select payload -> 'as_of_date' from us_rollup),
        'freshness_days', (select payload -> 'freshness_days' from us_rollup),
        'top_queries', coalesce((
          select jsonb_agg(
            jsonb_build_object(
              'query', query,
              'clicks', clicks,
              'impressions', impressions,
              'ctr', ctr,
              'average_position', average_position,
              'is_branded', is_branded,
              'intent_bucket', intent_bucket,
              'term_type', term_type,
              'position_band', position_band
            )
            order by clicks desc, impressions desc, query asc
          )
          from (select * from travel_queries order by clicks desc, impressions desc limit 25) ranked
        ), '[]'::jsonb),
        'top_pages', (select payload -> 'top_pages' from us_rollup),
        'opportunities', coalesce((
          select jsonb_agg(
            jsonb_build_object(
              'opportunity_id', o ->> 'opportunity_id',
              'title', o ->> 'title',
              'summary', o ->> 'summary',
              'page_path', o ->> 'page_path',
              'query', o ->> 'query',
              'clicks', o -> 'clicks',
              'impressions', o -> 'impressions',
              'ctr', o -> 'ctr',
              'average_position', o -> 'average_position',
              'priority_score', o -> 'priority_score',
              'recommended_action', o ->> 'recommended_action',
              'opportunity_type',
                case
                  when (o ->> 'opportunity_id') like 'low-ctr-query-%' then 'low_ctr'
                  when (o ->> 'opportunity_id') like 'near-breakout-query-%' then 'near_breakout'
                  when coalesce((o ->> 'average_position')::numeric, 0) > 12 then 'destination_gap'
                  else 'page_refresh'
                end
            )
          )
          from jsonb_array_elements(coalesce((select payload -> 'opportunities' from us_rollup), '[]'::jsonb)) o
        ), '[]'::jsonb),
        'challenges', coalesce((
          select jsonb_agg(
            jsonb_build_object(
              'challenge_id', c ->> 'challenge_id',
              'title', c ->> 'title',
              'summary', c ->> 'summary',
              'page_path', c ->> 'page_path',
              'query', c ->> 'query',
              'clicks', c -> 'clicks',
              'impressions', c -> 'impressions',
              'ctr', c -> 'ctr',
              'average_position', c -> 'average_position',
              'severity_score', c -> 'severity_score',
              'recommended_action', c ->> 'recommended_action',
              'challenge_type',
                case
                  when (c ->> 'challenge_id') like 'page-query-ctr-gap-%' then 'page_ctr_gap'
                  when coalesce((c ->> 'average_position')::numeric, 0) > 15 then 'ranking_drop'
                  when coalesce((c ->> 'ctr')::numeric, 0) < 0.01 then 'coverage_gap'
                  else 'intent_mismatch'
                end
            )
          )
          from jsonb_array_elements(coalesce((select payload -> 'challenges' from us_rollup), '[]'::jsonb)) c
        ), '[]'::jsonb),
        'query_row_count', (select count(*) from travel_queries),
        'market_benchmarks', coalesce((
          select jsonb_agg(
            jsonb_build_object(
              'market_label', market_label,
              'clicks', clicks,
              'impressions', impressions,
              'ctr', case when impressions > 0 then clicks / impressions else 0 end,
              'average_position',
                case when impressions > 0 then position_weight / impressions else 0 end
            )
            order by
              case market_label
                when 'United States' then 1
                when 'Australia' then 2
                when 'New Zealand' then 3
                when 'South Africa' then 4
                else 5
              end
          )
          from market_totals
        ), '[]'::jsonb),
        'query_intent_buckets', coalesce((
          select jsonb_agg(
            jsonb_build_object(
              'bucket_label', bucket_label,
              'query_count', query_count,
              'clicks', clicks,
              'impressions', impressions,
              'average_ctr', case when impressions > 0 then clicks / impressions else 0 end
            )
            order by clicks desc, impressions desc, bucket_label asc
          )
          from intent_buckets
        ), '[]'::jsonb),
        'position_band_summary', coalesce((
          select jsonb_agg(
            jsonb_build_object(
              'band_label', band_label,
              'query_count', query_count,
              'clicks', clicks,
              'impressions', impressions,
              'average_ctr', case when impressions > 0 then clicks / impressions else 0 end
            )
            order by
              case band_label
                when '1-3' then 1
                when '4-10' then 2
                when '11-20' then 3
                else 4
              end
          )
          from position_bands
        ), '[]'::jsonb)
      )
  end;
$$;


create or replace function public.marketing_search_console_page_profile_v1(
  p_days_back integer,
  p_page_path text
)
returns jsonb
language sql
stable
as $$
with params as (
  select
    greatest(coalesce(p_days_back, 30), 1) as days_back,
    nullif(trim(p_page_path), '') as page_path
),
as_of as (
  select max(snapshot_date) as as_of_date
  from public.marketing_search_console_page_daily
  where country_scope = 'United States'
    and device_scope = 'all'
),
windowed as (
  select
    as_of_date,
    days_back,
    (as_of_date - (days_back - 1) * interval '1 day')::date as start_date
  from as_of
  cross join params
  where as_of_date is not null
),
daily as (
  select
    p.snapshot_date,
    coalesce(sum(p.clicks), 0)::numeric as clicks,
    coalesce(sum(p.impressions), 0)::numeric as impressions,
    coalesce(sum(p.average_position * p.impressions), 0)::numeric as position_weight
  from public.marketing_search_console_page_daily p
  join windowed on p.snapshot_date between windowed.start_date and windowed.as_of_date
  join params on p.page_path = params.page_path
  where p.country_scope = 'United States'
    and p.device_scope = 'all'
  group by p.snapshot_date
),
overview as (
  select
    coalesce(sum(clicks), 0)::numeric as total_clicks,
    coalesce(sum(impressions), 0)::numeric as total_impressions,
    coalesce(sum(position_weight), 0)::numeric as total_position_weight
  from daily
),
query_branding as (
  select
    q.query,
    bool_or(q.is_branded) as is_branded
  from public.marketing_search_console_query_daily q
  join windowed on q.snapshot_date between windowed.start_date and windowed.as_of_date
  where q.country_scope = 'United States'
    and q.device_scope = 'all'
  group by q.query
),
query_rows as (
  select
    p.query,
    coalesce(b.is_branded, false) as is_branded,
    coalesce(sum(p.clicks), 0)::numeric as clicks,
    coalesce(sum(p.impressions), 0)::numeric as impressions,
    coalesce(sum(p.average_position * p.impressions), 0)::numeric as position_weight
  from public.marketing_search_console_page_query_daily p
  join windowed on p.snapshot_date between windowed.start_date and windowed.as_of_date
  join params on p.page_path = params.page_path
  left join query_branding b on b.query = p.query
  where p.country_scope = 'United States'
    and p.device_scope = 'all'
  group by p.query, b.is_branded
),
market_rows as (
  select
    case
      when lower(country_scope) = 'united states' then 'United States'
      when lower(country_scope) = 'australia' then 'Australia'
      when lower(country_scope) = 'new zealand' then 'New Zealand'
      when lower(country_scope) = 'south africa' then 'South Africa'
      else country_scope
    end as market_label,
    coalesce(sum(clicks), 0)::numeric as clicks,
    coalesce(sum(impressions), 0)::numeric as impressions,
    coalesce(sum(average_position * impressions), 0)::numeric as position_weight
  from public.marketing_search_console_page_daily p
  join windowed on p.snapshot_date between windowed.start_date and windowed.as_of_date
  join params on p.page_path = params.page_path
  where lower(p.country_scope) in ('united states', 'australia', 'new zealand', 'south africa')
    and p.device_scope = 'all'
  group by 1
)
select
  case
    when (select page_path from params) is null then
      jsonb_build_object(
        'page_path', '',
        'as_of_date', null,
        'overview', jsonb_build_object(
          'total_clicks', 0,
          'total_impressions', 0,
          'average_ctr', 0,
          'average_position', 0
        ),
        'daily_trend', '[]'::jsonb,
        'top_queries', '[]'::jsonb,
        'market_benchmarks', '[]'::jsonb
      )
    else
      jsonb_build_object(
        'page_path', (select page_path from params),
        'as_of_date', (select as_of_date from as_of),
        'overview', jsonb_build_object(
          'total_clicks', (select total_clicks from overview),
          'total_impressions', (select total_impressions from overview),
          'average_ctr',
            case
              when (select total_impressions from overview) > 0
                then (select total_clicks from overview) / (select total_impressions from overview)
              else 0
            end,
          'average_position',
            case
              when (select total_impressions from overview) > 0
                then (select total_position_weight from overview) / (select total_impressions from overview)
              else 0
            end
        ),
        'daily_trend', coalesce((
          select jsonb_agg(
            jsonb_build_object(
              'snapshot_date', snapshot_date,
              'clicks', clicks,
              'impressions', impressions,
              'ctr', case when impressions > 0 then clicks / impressions else 0 end,
              'average_position', case when impressions > 0 then position_weight / impressions else 0 end
            )
            order by snapshot_date asc
          )
          from daily
        ), '[]'::jsonb),
        'top_queries', coalesce((
          select jsonb_agg(
            jsonb_build_object(
              'query', query,
              'clicks', clicks,
              'impressions', impressions,
              'ctr', case when impressions > 0 then clicks / impressions else 0 end,
              'average_position', case when impressions > 0 then position_weight / impressions else 0 end,
              'is_branded', is_branded
            )
            order by clicks desc, impressions desc, query asc
          )
          from (select * from query_rows order by clicks desc, impressions desc limit 20) ranked
        ), '[]'::jsonb),
        'market_benchmarks', coalesce((
          select jsonb_agg(
            jsonb_build_object(
              'market_label', market_label,
              'clicks', clicks,
              'impressions', impressions,
              'ctr', case when impressions > 0 then clicks / impressions else 0 end,
              'average_position',
                case when impressions > 0 then position_weight / impressions else 0 end
            )
            order by
              case market_label
                when 'United States' then 1
                when 'Australia' then 2
                when 'New Zealand' then 3
                when 'South Africa' then 4
                else 5
              end
          )
          from market_rows
        ), '[]'::jsonb)
      )
  end;
$$;

grant execute on function public.marketing_search_console_us_workspace_v1(integer) to authenticated;
grant execute on function public.marketing_search_console_us_workspace_v1(integer) to service_role;
grant execute on function public.marketing_search_console_page_profile_v1(integer, text) to authenticated;
grant execute on function public.marketing_search_console_page_profile_v1(integer, text) to service_role;
