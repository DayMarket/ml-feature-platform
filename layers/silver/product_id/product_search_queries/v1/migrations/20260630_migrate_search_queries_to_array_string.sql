ALTER TABLE {target_table}
RENAME COLUMN IF EXISTS search_queries TO search_queries_with_installs
WHEN SOURCE TYPE IS NOT ARRAY<STRING>;

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS search_queries ARRAY<STRING> COMMENT 'Top-200 поисковых запросов, в которых product_id встречался среди ranking candidates за последние 14 дней; отсортированы по числу уникальных install_id по убыванию';
