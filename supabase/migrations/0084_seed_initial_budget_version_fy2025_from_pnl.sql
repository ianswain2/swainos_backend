-- Forward-only migration:
-- Seed first finance budget version and monthly budget lines from the 2025 P&L export.

with upsert_version as (
  insert into public.budget_versions (
    version_name,
    fiscal_year,
    scenario_name,
    status,
    notes,
    is_locked,
    locked_at
  )
  values (
    'FY2025 Imported Baseline',
    2025,
    'baseline',
    'locked',
    'Seeded from Swain Destinations_Profit and Loss_2025monthly.csv (monthly totals).',
    true,
    now()
  )
  on conflict (fiscal_year, scenario_name, version_name)
  do update set
    status = excluded.status,
    notes = excluded.notes,
    is_locked = excluded.is_locked,
    locked_at = excluded.locked_at,
    updated_at = now()
  returning id
),
month_map as (
  select 1 as month_index, date '2025-01-01' as month_start
  union all select 2, date '2025-02-01'
  union all select 3, date '2025-03-01'
  union all select 4, date '2025-04-01'
  union all select 5, date '2025-05-01'
  union all select 6, date '2025-06-01'
  union all select 7, date '2025-07-01'
  union all select 8, date '2025-08-01'
  union all select 9, date '2025-09-01'
  union all select 10, date '2025-10-01'
  union all select 11, date '2025-11-01'
  union all select 12, date '2025-12-01'
),
account_budget_arrays as (
  select
    source_account_code,
    monthly_amounts
  from (
    values
      (
        '4000000',
        array[
          1281101.03, 885329.96, 909798.98, 490210.66, 514694.17, 420193.46,
          198177.41, 377216.15, 534343.91, 737401.53, 751808.71, 928894.72
        ]::numeric[]
      ),
      (
        '7100000',
        array[
          27948.45, 20681.10, 17912.66, 15511.79, 14894.47, 13645.25,
          15298.64, 16722.35, 16592.20, 19018.44, 19846.17, 21356.26
        ]::numeric[]
      ),
      (
        '5000000',
        array[
          370393.00, 224661.32, 270842.00, 123782.00, 119236.95, 104370.00,
          61542.00, 101902.00, 140222.67, 192566.16, 189480.00, 290758.10
        ]::numeric[]
      ),
      (
        '5000202',
        array[
          0.00, 0.00, 0.00, 0.00, 0.00, 0.00,
          0.00, 0.00, 0.00, -133000.00, -100000.00, 196589.00
        ]::numeric[]
      ),
      (
        '6000000',
        array[
          -69123.95, 61040.66, 78940.97, 103252.49, 35676.05, 76840.50,
          13322.54, 49576.78, 77886.74, 88266.07, 41264.09, 29158.54
        ]::numeric[]
      ),
      (
        '6035000',
        array[
          5154.71, 5155.21, 5156.24, 6316.06, 5155.55, 5223.38,
          5154.71, 5224.47, 5156.98, 5156.64, 5184.06, 5184.96
        ]::numeric[]
      ),
      (
        '6035010',
        array[
          28183.76, 27523.67, 21526.64, 23919.07, 22924.98, 23760.86,
          22966.55, 23528.38, 31910.23, 42958.69, 34025.93, 18339.97
        ]::numeric[]
      ),
      (
        '6090000',
        array[
          301.74, 226.34, 237.35, 401.16, 629.25, 406.73,
          9444.62, 9244.47, 9792.14, 9676.61, 9612.03, 9750.00
        ]::numeric[]
      ),
      (
        '6200000',
        array[
          339423.52, 308935.37, 292486.45, 247700.08, 286489.11, 246673.04,
          236969.63, 260241.06, 201728.69, 282777.37, 273707.60, 281693.50
        ]::numeric[]
      ),
      (
        '6300000',
        array[
          8086.46, 9662.85, 14778.97, 5104.11, 8281.72, 17769.61,
          18392.86, 12169.03, 6569.59, 17433.03, 8334.90, 56957.31
        ]::numeric[]
      )
  ) as seeded(source_account_code, monthly_amounts)
),
expanded_budget_rows as (
  select
    fc.id as financial_category_id,
    m.month_start,
    a.monthly_amounts[m.month_index] as budget_amount
  from account_budget_arrays a
  join public.financial_categories fc
    on fc.source_system = 'quickbooks_online'
   and fc.source_account_code = a.source_account_code
  join month_map m
    on true
)
insert into public.budget_lines (
  budget_version_id,
  month_start,
  financial_category_id,
  currency_code,
  budget_amount,
  notes,
  source_type
)
select
  v.id as budget_version_id,
  e.month_start,
  e.financial_category_id,
  'USD' as currency_code,
  e.budget_amount,
  'Seeded from 2025 monthly P&L export.' as notes,
  'imported' as source_type
from expanded_budget_rows e
cross join upsert_version v
on conflict (budget_version_id, month_start, financial_category_id, currency_code)
do update set
  budget_amount = excluded.budget_amount,
  notes = excluded.notes,
  source_type = excluded.source_type,
  updated_at = now();
