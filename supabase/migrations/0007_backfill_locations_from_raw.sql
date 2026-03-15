-- Backfill canonical locations from raw ingest

select public.upsert_locations_from_raw();
