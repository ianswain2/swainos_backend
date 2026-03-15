-- Refresh can exceed default statement timeout when reseeding full travel-trade rollups.
alter function public.refresh_travel_trade_rollups_v1()
  set statement_timeout = '15min';
