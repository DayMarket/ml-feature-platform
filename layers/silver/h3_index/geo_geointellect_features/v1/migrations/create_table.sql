CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата расчёта фич (data_interval_end в UTC)',
    h3_index BIGINT COMMENT 'H3-индекс гексагона res 9 (UInt64 источника приведён к BIGINT)',
    h3_string STRING COMMENT 'H3-индекс строкой (h3ToString)',
    region STRING COMMENT 'Регион по geopoint2region центра гексагона',
    population_r0 DOUBLE COMMENT 'Население в радиусе <= 0 колец',
    population_r1 DOUBLE COMMENT 'Население в радиусе <= 1 колец',
    population_r2 DOUBLE COMMENT 'Население в радиусе <= 2 колец',
    population_r3 DOUBLE COMMENT 'Население в радиусе <= 3 колец',
    population_r4 DOUBLE COMMENT 'Население в радиусе <= 4 колец',
    population_r5 DOUBLE COMMENT 'Население в радиусе <= 5 колец',
    pedestrian_traffic_index_r0 DOUBLE COMMENT 'Индекс пешеходного трафика в радиусе <= 0 колец',
    pedestrian_traffic_index_r1 DOUBLE COMMENT 'Индекс пешеходного трафика в радиусе <= 1 колец',
    pedestrian_traffic_index_r2 DOUBLE COMMENT 'Индекс пешеходного трафика в радиусе <= 2 колец',
    pedestrian_traffic_index_r3 DOUBLE COMMENT 'Индекс пешеходного трафика в радиусе <= 3 колец',
    pedestrian_traffic_index_r4 DOUBLE COMMENT 'Индекс пешеходного трафика в радиусе <= 4 колец',
    pedestrian_traffic_index_r5 DOUBLE COMMENT 'Индекс пешеходного трафика в радиусе <= 5 колец',
    population_l5 DOUBLE COMMENT 'Население родительского гексагона уровня 5',
    population_l6 DOUBLE COMMENT 'Население родительского гексагона уровня 6',
    population_l7 DOUBLE COMMENT 'Население родительского гексагона уровня 7',
    population_l8 DOUBLE COMMENT 'Население родительского гексагона уровня 8',
    pedestrian_traffic_index_l5 DOUBLE COMMENT 'Индекс пешеходного трафика родительского гексагона уровня 5',
    pedestrian_traffic_index_l6 DOUBLE COMMENT 'Индекс пешеходного трафика родительского гексагона уровня 6',
    pedestrian_traffic_index_l7 DOUBLE COMMENT 'Индекс пешеходного трафика родительского гексагона уровня 7',
    pedestrian_traffic_index_l8 DOUBLE COMMENT 'Индекс пешеходного трафика родительского гексагона уровня 8'
)
USING iceberg
COMMENT 'Silver: демография и пешеходный трафик (geointellect) по кольцам и родительским уровням H3'
PARTITIONED BY (date)
