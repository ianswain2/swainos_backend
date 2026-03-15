-- Seed reference data for locations and currencies

insert into public.currencies (currency_code, currency_name, symbol, decimal_places, is_active)
values
  ('USD', 'US Dollar', '$', 2, true),
  ('AUD', 'Australian Dollar', '$', 2, true),
  ('NZD', 'New Zealand Dollar', '$', 2, true),
  ('ZAR', 'South African Rand', 'R', 2, true)
on conflict (currency_code) do nothing;

insert into public.locations (
  country_code,
  country_name,
  region_name,
  city_name,
  latitude,
  longitude,
  timezone,
  is_primary_destination
) values
  ('AU', 'Australia', 'New South Wales', 'Sydney', -33.8688, 151.2093, 'Australia/Sydney', true),
  ('AU', 'Australia', 'Victoria', 'Melbourne', -37.8136, 144.9631, 'Australia/Melbourne', true),
  ('AU', 'Australia', 'Queensland', 'Brisbane', -27.4698, 153.0251, 'Australia/Brisbane', true),
  ('AU', 'Australia', 'Queensland', 'Cairns', -16.9186, 145.7781, 'Australia/Brisbane', true),
  ('AU', 'Australia', 'Western Australia', 'Perth', -31.9505, 115.8605, 'Australia/Perth', true),
  ('NZ', 'New Zealand', 'Auckland', 'Auckland', -36.8485, 174.7633, 'Pacific/Auckland', true),
  ('NZ', 'New Zealand', 'Otago', 'Queenstown', -45.0312, 168.6626, 'Pacific/Auckland', true),
  ('NZ', 'New Zealand', 'Wellington', 'Wellington', -41.2865, 174.7762, 'Pacific/Auckland', true),
  ('ZA', 'South Africa', 'Western Cape', 'Cape Town', -33.9249, 18.4241, 'Africa/Johannesburg', true),
  ('ZA', 'South Africa', 'Gauteng', 'Johannesburg', -26.2041, 28.0473, 'Africa/Johannesburg', true),
  ('ZA', 'South Africa', 'Limpopo', 'Kruger', -23.9884, 31.5547, 'Africa/Johannesburg', true);
