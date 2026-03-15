do $$
declare
  current_definition text;
  updated_definition text;
  canonical_predicate text := 'and nullif(trim(i.consortia), '''') is not null
    and lower(trim(i.consortia)) not in (''not applicable'', ''n/a'', ''na'', ''none'', ''null'')';
begin
  select pg_get_functiondef('public.refresh_travel_trade_rollups_v1()'::regprocedure)
    into current_definition;

  updated_definition := regexp_replace(
    current_definition,
    'and nullif\(trim\(i\.consortia\), ''''\) is not null',
    canonical_predicate,
    'n'
  );

  updated_definition := regexp_replace(
    updated_definition,
    'and\s*\(\s*nullif\(trim\(i\.consortia\), ''''\) is null\s*or lower\(trim\(i\.consortia\)\) in \(''not applicable'', ''n/a'', ''na'', ''none'', ''null''\)\s*\)',
    canonical_predicate,
    'n'
  );

  if updated_definition = current_definition then
    raise exception 'Unable to update consortia trade-classification rule in refresh_travel_trade_rollups_v1()';
  end if;

  execute updated_definition;
end;
$$;

alter function public.refresh_travel_trade_rollups_v1()
  set statement_timeout = '15min';

select public.refresh_travel_trade_rollups_v1();
