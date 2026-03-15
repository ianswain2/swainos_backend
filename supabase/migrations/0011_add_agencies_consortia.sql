-- Add consortia attribute to agencies for analytics segmentation

alter table public.agencies
  add column if not exists consortia text;
