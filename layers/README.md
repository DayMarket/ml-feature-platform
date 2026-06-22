# Feature layers

Repository-managed Iceberg таблицы разделены на два слоя:

- [`silver`](silver/README.md) — переиспользуемые предагрегаты и промежуточные таблицы;
- [`gold`](gold/README.md) — финальные признаки для моделей и downstream-сервисов.

Внутри слоя таблицы сгруппированы по primary key без `date`:
`layers/<layer>/<primary_key_group>/<entity>/vN`. Каждая entity владеет своими
`config.yaml`, DAG, runtime-кодом, migrations и README.
