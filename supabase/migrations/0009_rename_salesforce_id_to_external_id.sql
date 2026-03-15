-- Rename salesforce_id to external_id for universal external identifiers

alter table public.agencies drop constraint if exists agencies_salesforce_id_key;
alter table public.agencies rename column salesforce_id to external_id;
alter table public.agencies add constraint agencies_external_id_key unique (external_id);

alter table public.suppliers drop constraint if exists suppliers_salesforce_id_key;
alter table public.suppliers rename column salesforce_id to external_id;
alter table public.suppliers add constraint suppliers_external_id_key unique (external_id);

alter table public.itineraries drop constraint if exists itineraries_salesforce_id_key;
alter table public.itineraries rename column salesforce_id to external_id;
alter table public.itineraries add constraint itineraries_external_id_key unique (external_id);

alter table public.bookings drop constraint if exists bookings_salesforce_id_key;
alter table public.bookings rename column salesforce_id to external_id;
alter table public.bookings add constraint bookings_external_id_key unique (external_id);

alter table public.itinerary_items drop constraint if exists itinerary_items_salesforce_id_key;
alter table public.itinerary_items rename column salesforce_id to external_id;
alter table public.itinerary_items add constraint itinerary_items_external_id_key unique (external_id);

alter table public.customer_payments drop constraint if exists customer_payments_salesforce_id_key;
alter table public.customer_payments rename column salesforce_id to external_id;
alter table public.customer_payments add constraint customer_payments_external_id_key unique (external_id);

alter table public.supplier_invoices drop constraint if exists supplier_invoices_salesforce_id_key;
alter table public.supplier_invoices rename column salesforce_id to external_id;
alter table public.supplier_invoices add constraint supplier_invoices_external_id_key unique (external_id);

alter table public.supplier_invoice_lines drop constraint if exists supplier_invoice_lines_salesforce_id_key;
alter table public.supplier_invoice_lines rename column salesforce_id to external_id;
alter table public.supplier_invoice_lines add constraint supplier_invoice_lines_external_id_key unique (external_id);
