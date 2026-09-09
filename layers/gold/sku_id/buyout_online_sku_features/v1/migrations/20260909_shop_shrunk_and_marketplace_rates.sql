-- MAD-13695: сглаженная выкупаемость магазина и общая выкупаемость маркетплейса —
-- те же величины, что видела модель невыкупов при обучении (cart_item_signal.sql, k = 30).
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS marketplace_buyout_rate_90d DOUBLE COMMENT 'Общая выкупаемость маркетплейса в штуках за 90 дней (по строкам категорий сигнала) — последний уровень подстановки, к ней стягиваются категория и магазин';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS marketplace_no_show_rate_90d DOUBLE COMMENT 'Общая доля NO SHOW маркетплейса за 90 дней';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS shop_buyout_rate_shrunk_90d DOUBLE COMMENT 'Выкупаемость магазина за 90 дней, стянутая к общей выкупаемости маркетплейса (k = 30) — величина, на которой обучена модель невыкупов; сырая выкупаемость магазина рядом в shop_buyout_rate_90d';
