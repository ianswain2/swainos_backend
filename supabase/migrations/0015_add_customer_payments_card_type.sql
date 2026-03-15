-- Add card_type to customer_payments for payment method details
alter table if exists public.customer_payments
add column if not exists card_type text;
