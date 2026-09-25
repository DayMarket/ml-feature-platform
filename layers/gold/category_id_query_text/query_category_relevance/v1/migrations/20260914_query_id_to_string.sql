-- Iceberg меняет тип колонки только расширяющими промоушенами (int->long,
-- precision у decimal); bigint->string в их число не входит, поэтому колонка
-- пересоздаётся: старая уводится под временное имя query_id_bigint, новая
-- добавляется тем же именем query_id, старая удаляется. Перенос значений не нужен
-- и потому удаление не теряет данных: таблица пуста (0 строк, 0 партиций на
-- 2026-09-14), DAG витрины — заглушка и партиций ещё не писал.
ALTER TABLE {target_table}
RENAME COLUMN IF EXISTS query_id TO query_id_bigint
WHEN SOURCE TYPE IS NOT STRING;

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_id STRING COMMENT 'ID поискового запроса; строка, а не число (в Trino — varchar)';

ALTER TABLE {target_table}
DROP COLUMN IF EXISTS query_id_bigint;
