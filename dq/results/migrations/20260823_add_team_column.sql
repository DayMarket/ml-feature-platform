ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS team STRING COMMENT 'Команда-владелец целевой таблицы из table.meta.team, по умолчанию team:search';
