# Silver: `sku_id`

Грейн группы: `sku_id`; `date` исключена из имени группы.

- [`demand_catalog_sku`](demand_catalog_sku/v1/README.md) — полный текущий каталог SKU/card/category/golden/master без окон; DDL/config подготовлены.

- [`demand_sales_daily`](demand_sales_daily/v1/README.md) — точная свёртка прошедшего DQ seller-sales snapshot до SKU; owner DAG и дневной writer подготовлены.

- [`demand_stock_daily`](demand_stock_daily/v1/README.md) — sparse `(date, sku_id)` для положительного active/FBS EOD; owner DAG и range orchestration подготовлены.

- [`sku_daily_dynamic_prices`](sku_daily_dynamic_prices/v1/README.md) — дневные цены SKU по динамическому ценообразованию;
- [`sku_stock_daily`](sku_stock_daily/v1/README.md) — дневной признак активного остатка SKU.
- [`sku_cm2_inputs_daily`](sku_cm2_inputs_daily/v1/README.md) — дневные SKU-входы для Main CM2 и PDP CM2.
