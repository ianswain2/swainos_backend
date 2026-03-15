-- SwainOS initial schema (core + financial + FX + system tables)
-- Generated from SwainOS project specification (Feb 2026)

create extension if not exists "pgcrypto";

-- Core CRM / itinerary tables
create table if not exists public.customers (
  id uuid primary key default gen_random_uuid(),
  salesforce_id text unique,
  full_name text,
  email text,
  phone text,
  country text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists public.agencies (
  id uuid primary key default gen_random_uuid(),
  salesforce_id text unique,
  agency_name text,
  agency_code text,
  contact_email text,
  is_active boolean default true,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists public.advisors (
  id uuid primary key default gen_random_uuid(),
  salesforce_id text unique,
  agency_id uuid references public.agencies(id),
  advisor_name text,
  email text,
  is_active boolean default true,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists public.suppliers (
  id uuid primary key default gen_random_uuid(),
  salesforce_id text unique,
  supplier_name text,
  supplier_code text,
  supplier_type text,
  default_currency text,
  payment_terms_days integer,
  contact_email text,
  contact_phone text,
  address_country text,
  is_active boolean default true,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists public.itineraries (
  id uuid primary key default gen_random_uuid(),
  salesforce_id text unique,
  itinerary_number text,
  itinerary_name text,
  itinerary_status text,
  travel_start_date date,
  travel_end_date date,
  primary_country text,
  primary_region text,
  primary_city text,
  primary_latitude numeric,
  primary_longitude numeric,
  pax_count integer,
  adult_count integer,
  child_count integer,
  gross_amount numeric,
  net_amount numeric,
  commission_amount numeric,
  deposit_received numeric,
  balance_due numeric,
  currency_code text,
  customer_id uuid references public.customers(id),
  agency_id uuid references public.agencies(id),
  advisor_id uuid references public.advisors(id),
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  synced_at timestamptz
);

create table if not exists public.bookings (
  id uuid primary key default gen_random_uuid(),
  salesforce_id text unique,
  itinerary_id uuid references public.itineraries(id),
  booking_number text,
  booking_type text,
  supplier_id uuid references public.suppliers(id),
  service_name text,
  service_start_date date,
  service_end_date date,
  location_country text,
  location_city text,
  pax_count integer,
  gross_amount numeric,
  net_amount numeric,
  commission_amount numeric,
  currency_code text,
  booking_status text,
  confirmation_number text,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  synced_at timestamptz
);

create table if not exists public.itinerary_items (
  id uuid primary key default gen_random_uuid(),
  salesforce_id text unique,
  itinerary_id uuid references public.itineraries(id),
  supplier_id uuid references public.suppliers(id),
  item_type text,
  item_name text,
  item_description text,
  service_start_date date,
  service_end_date date,
  location_country text,
  location_region text,
  location_city text,
  location_latitude numeric,
  location_longitude numeric,
  quantity integer,
  unit_cost numeric,
  total_cost numeric,
  currency_code text,
  confirmation_number text,
  item_status text,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  synced_at timestamptz
);

create table if not exists public.customer_payments (
  id uuid primary key default gen_random_uuid(),
  salesforce_id text unique,
  itinerary_id uuid references public.itineraries(id),
  payment_number text,
  payment_type text,
  payment_method text,
  payment_date date,
  amount numeric,
  currency_code text,
  payment_status text,
  processor_reference text,
  notes text,
  received_at timestamptz,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  synced_at timestamptz
);

-- Financial tables
create table if not exists public.transactions (
  id uuid primary key default gen_random_uuid(),
  quickbooks_id text unique,
  transaction_type text,
  transaction_date date,
  amount numeric,
  currency_code text,
  category text,
  vendor_name text,
  description text,
  is_reconciled boolean default false,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  synced_at timestamptz
);

create table if not exists public.debt_schedules (
  id uuid primary key default gen_random_uuid(),
  debt_name text,
  debt_type text,
  original_principal numeric,
  current_balance numeric,
  interest_rate numeric,
  payment_amount numeric,
  payment_frequency text,
  next_payment_date date,
  maturity_date date,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists public.debt_payments (
  id uuid primary key default gen_random_uuid(),
  debt_schedule_id uuid references public.debt_schedules(id),
  payment_date date,
  principal_amount numeric,
  interest_amount numeric,
  extra_principal numeric,
  balance_after numeric,
  is_paid boolean default false,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists public.budgets (
  id uuid primary key default gen_random_uuid(),
  budget_year integer,
  budget_month integer,
  category text,
  budgeted_amount numeric,
  actual_amount numeric,
  variance_amount numeric,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- Supplier payment tables
create table if not exists public.supplier_invoices (
  id uuid primary key default gen_random_uuid(),
  salesforce_id text unique,
  invoice_number text,
  supplier_id uuid references public.suppliers(id),
  invoice_date date,
  due_date date,
  total_amount numeric,
  currency_code text,
  invoice_status text,
  payment_status text,
  paid_amount numeric,
  paid_date date,
  notes text,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  synced_at timestamptz
);

create table if not exists public.supplier_invoice_lines (
  id uuid primary key default gen_random_uuid(),
  salesforce_id text unique,
  supplier_invoice_id uuid references public.supplier_invoices(id),
  booking_id uuid references public.bookings(id),
  itinerary_id uuid references public.itineraries(id),
  description text,
  service_date date,
  quantity integer,
  unit_price numeric,
  line_amount numeric,
  currency_code text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists public.app_users (
  id uuid primary key references auth.users(id),
  email text,
  full_name text,
  role text,
  preferences jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- FX tables
create table if not exists public.fx_rates (
  id uuid primary key default gen_random_uuid(),
  currency_pair text,
  rate_timestamp timestamptz,
  bid_rate numeric,
  ask_rate numeric,
  mid_rate numeric,
  source text,
  created_at timestamptz default now()
);

create table if not exists public.fx_holdings (
  id uuid primary key default gen_random_uuid(),
  currency_code text unique,
  balance_amount numeric,
  avg_purchase_rate numeric,
  total_purchased numeric,
  total_spent numeric,
  last_transaction_date date,
  last_reconciled_at timestamptz,
  notes text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists public.fx_signals (
  id uuid primary key default gen_random_uuid(),
  currency_code text,
  signal_type text,
  signal_strength text,
  current_rate numeric,
  avg_30d_rate numeric,
  exposure_amount numeric,
  recommended_amount numeric,
  reasoning text,
  generated_at timestamptz,
  expires_at timestamptz,
  was_acted_on boolean default false,
  created_at timestamptz default now()
);

create table if not exists public.fx_transactions (
  id uuid primary key default gen_random_uuid(),
  currency_code text,
  transaction_type text,
  transaction_date date,
  amount numeric,
  exchange_rate numeric,
  usd_equivalent numeric,
  balance_after numeric,
  supplier_invoice_id uuid references public.supplier_invoices(id),
  signal_id uuid references public.fx_signals(id),
  reference_number text,
  notes text,
  entered_by uuid references public.app_users(id),
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- Reference tables
create table if not exists public.locations (
  id uuid primary key default gen_random_uuid(),
  country_code text,
  country_name text,
  region_name text,
  city_name text,
  latitude numeric,
  longitude numeric,
  timezone text,
  is_primary_destination boolean default false
);

create table if not exists public.currencies (
  id uuid primary key default gen_random_uuid(),
  currency_code text unique,
  currency_name text,
  symbol text,
  decimal_places integer default 2,
  is_active boolean default true,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- System tables
create table if not exists public.sync_logs (
  id uuid primary key default gen_random_uuid(),
  source_system text,
  sync_type text,
  started_at timestamptz,
  completed_at timestamptz,
  records_processed integer,
  records_created integer,
  records_updated integer,
  status text,
  error_message text,
  created_at timestamptz default now()
);

create table if not exists public.ai_interactions (
  id uuid primary key default gen_random_uuid(),
  user_id uuid references public.app_users(id),
  interaction_type text,
  prompt text,
  response text,
  tokens_used integer,
  model_name text,
  latency_ms integer,
  created_at timestamptz default now()
);

create table if not exists public.app_settings (
  id uuid primary key default gen_random_uuid(),
  setting_key text unique,
  setting_value jsonb,
  setting_type text,
  description text,
  updated_by uuid references public.app_users(id),
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- Indexes
create index if not exists idx_bookings_travel_start_date on public.bookings(service_start_date);
create index if not exists idx_bookings_booking_status on public.bookings(booking_status);
create index if not exists idx_itineraries_travel_dates on public.itineraries(travel_start_date, travel_end_date);
create index if not exists idx_itineraries_status_dates on public.itineraries(itinerary_status, travel_start_date, travel_end_date);
create index if not exists idx_itineraries_country on public.itineraries(primary_country);
create index if not exists idx_itinerary_items_itinerary on public.itinerary_items(itinerary_id);
create index if not exists idx_itinerary_items_supplier on public.itinerary_items(supplier_id);
create index if not exists idx_itinerary_items_service_date on public.itinerary_items(service_start_date);
create index if not exists idx_fx_rates_currency_timestamp on public.fx_rates(currency_pair, rate_timestamp);
create index if not exists idx_transactions_date on public.transactions(transaction_date);
create index if not exists idx_debt_payments_schedule_date on public.debt_payments(debt_schedule_id, payment_date);
create index if not exists idx_supplier_invoices_due_date on public.supplier_invoices(due_date);
create index if not exists idx_supplier_invoices_status on public.supplier_invoices(payment_status);
create index if not exists idx_supplier_invoices_currency on public.supplier_invoices(currency_code);
create index if not exists idx_supplier_invoice_lines_booking on public.supplier_invoice_lines(booking_id);
create index if not exists idx_bookings_itinerary on public.bookings(itinerary_id);
create index if not exists idx_customer_payments_itinerary on public.customer_payments(itinerary_id);
create index if not exists idx_customer_payments_date_status on public.customer_payments(payment_date, payment_status);
create index if not exists idx_fx_transactions_currency_date on public.fx_transactions(currency_code, transaction_date);
create index if not exists idx_fx_transactions_type on public.fx_transactions(transaction_type);
