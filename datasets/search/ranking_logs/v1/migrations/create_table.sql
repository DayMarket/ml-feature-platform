-- Индексация в комментариях колонок ниже: `position` — позиция кандидата в
-- ranking_candidates, 1-based, как и в спеке; индексы внутри массивов
-- model_output[...] и cm2_features[...] — 0-based, Spark array indexing.
-- Это единственное расхождение со спекой, и оно намеренное.
CREATE TABLE IF NOT EXISTS {target_table} (
    collection_date DATE COMMENT 'Дата фактического запуска DAG в UTC (воскресенье); партиция таблицы',
    event_date DATE COMMENT 'Дата события из лога; в партиции ровно 7 значений, воскресенье-суббота',
    fired_at TIMESTAMP COMMENT 'Время запроса к ранжирующему сервису из ranking_analytics_events.fired_at',
    model_name STRING COMMENT 'Имя ранжирующей модели; фиксировано параметром dataset.model_name',
    request_id STRING COMMENT 'ID запроса к ранжирующему сервису; единица сэмплирования',
    install_id STRING COMMENT 'Install ID пользователя',
    search_query STRING COMMENT 'Поисковый запрос как в логе, без нормализации',
    category_id INT COMMENT 'ID категории запроса; для поисковой модели обычно NULL',
    promo_id STRING COMMENT 'Идентификатор промо-конфигурации ранжирования из лога',
    `position` INT COMMENT 'Позиция кандидата в массиве ranking_candidates, 1-based',
    sku_group_id BIGINT COMMENT 'Кандидат: ranking_candidates[position], уровень sku group',
    final_score DOUBLE COMMENT 'Итоговый скор формулы: final_scores[position], равен model_output[position][0]',
    model_probability DOUBLE COMMENT 'Вероятность модели: model_output[position][1]',
    alpha_component DOUBLE COMMENT 'Alpha-составляющая формулы: model_output[position][2]',
    beta_component DOUBLE COMMENT 'Beta-составляющая формулы: model_output[position][3]',
    gamma_component DOUBLE COMMENT 'Gamma-составляющая формулы: model_output[position][4]',
    delta_component DOUBLE COMMENT 'Delta-составляющая формулы: model_output[position][5]',
    dssm_score DOUBLE COMMENT 'external_features.dssm_score[position]',
    linear_score DOUBLE COMMENT 'external_features.linear_score[position]',
    normalized_linear_score DOUBLE COMMENT 'external_features.normalized_linear_score[position]',
    commission_percent DOUBLE COMMENT 'Комиссия в процентах 0-100: cm2_features[position][0]',
    seller_price DOUBLE COMMENT 'Цена продажи, на которой считалась формула: cm2_features[position][1]',
    logistics_fee DOUBLE COMMENT 'Логистический сбор: cm2_features[position][2]',
    cpi_cost DOUBLE COMMENT 'CPI-cost: cm2_features[position][3]',
    cpm_bid DOUBLE COMMENT 'Размер CPM-ставки: cm2_features[position][4]',
    cpo_percent DOUBLE COMMENT 'Процент CPO-ставки: cm2_features[position][5]',
    vat_rate DOUBLE COMMENT 'Коэффициент НДС, по умолчанию 1.12: cm2_features[position][6]',
    items_quantity DOUBLE COMMENT 'Количество товаров для расчёта: cm2_features[position][7]',
    alpha DOUBLE COMMENT 'Коэффициент alpha запроса: common_external_features[alpha]',
    beta DOUBLE COMMENT 'Коэффициент beta запроса: common_external_features[beta]',
    gamma DOUBLE COMMENT 'Коэффициент gamma запроса: common_external_features[gamma]',
    delta DOUBLE COMMENT 'Коэффициент delta запроса: common_external_features[delta]',
    sku_group_age_days INT COMMENT 'Возраст sku group в днях на event_date: event_date минус дата создания самого старого sku группы',
    product_rating DOUBLE COMMENT 'Средний рейтинг sku group из feature_platform_sku_group_feedback_base_stats',
    total_reviews_count BIGINT COMMENT 'Число опубликованных отзывов sku group из feature_platform_sku_group_feedback_base_stats',
    frequency_group STRING COMMENT 'Группа частотности запроса HF/MF/LF; LF, если запрос не найден в справочнике',
    users_total BIGINT COMMENT 'Число пользователей запроса за 30 дней; NULL, если запрос не найден',
    query_rank BIGINT COMMENT 'Ранг запроса по частотности; NULL, если запрос не найден'
)
USING iceberg
COMMENT 'Training dataset v1: развёрнутый лог ранжирования запрос x кандидат для подбора параметров формулы'
PARTITIONED BY (collection_date)
-- write.distribution-mode = 'none': Iceberg 1.5.2 default для unsorted
-- partitioned table — HASH, а это перед записью репартиционирует по
-- collection_date. У этой таблицы на ран приходится ровно одно значение
-- collection_date, поэтому HASH согнал бы все ~200 млн строк в один reduce-таск
-- на одно ядро. 'none' убирает репартиционирование: каждая input-таска Spark
-- пишет свои файлы сама. Оверхед fanout-памяти, ради которого существует HASH
-- при множестве партиций в одной записи, здесь не возникает — партиция всегда
-- одна.
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false', 'write.distribution-mode' = 'none')
