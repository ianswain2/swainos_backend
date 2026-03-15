do $$
declare
  current_definition text;
  updated_definition text;
begin
  select pg_get_functiondef('public.refresh_travel_trade_rollups_v1()'::regprocedure)
    into current_definition;

  updated_definition := regexp_replace(
    current_definition,
    'i\.created_at,\s*i\.travel_end_date,',
    'i.created_at,
    i.travel_start_date,
    i.travel_end_date,',
    'n'
  );

  updated_definition := regexp_replace(
    updated_definition,
    'date_trunc\(''month'',\s*ts\.travel_end_date\)',
    'date_trunc(''month'', coalesce(ts.travel_start_date, ts.travel_end_date))',
    'gn'
  );

  updated_definition := regexp_replace(
    updated_definition,
    'where\s+ts\.travel_end_date\s+is\s+not\s+null',
    'where coalesce(ts.travel_start_date, ts.travel_end_date) is not null',
    'n'
  );

  if updated_definition <> current_definition then
    execute updated_definition;
  end if;
end;
$$;

alter function public.refresh_travel_trade_rollups_v1()
  set statement_timeout = '15min';

select public.refresh_travel_trade_rollups_v1();
