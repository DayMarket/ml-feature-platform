-- Источник кандидатов сменился с дневного предагрегата
-- feature_platform_search_sku_group_id_install_query (uniqs при space = SEARCH_RESULTS) на
-- скользящее 30-дневное окно по silver.search_logs. Смысл колонки тот же (исходный текст
-- запроса), протухло только описание источника в комментарии. Меняются лишь метаданные,
-- данные не трогаются; повторный прогон выставляет тот же комментарий, шаг идемпотентен.
ALTER TABLE {target_table} ALTER COLUMN query_text COMMENT 'Исходный поисковый запрос как service_query из silver.search_logs: corrected_query_text, если он непустой, иначе query_text';
