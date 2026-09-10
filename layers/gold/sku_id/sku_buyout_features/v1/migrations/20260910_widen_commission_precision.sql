-- PyArrow пишет decimal в FIXED_LEN_BYTE_ARRAY; для precision <= 9 текущий
-- PyIceberg ошибочно ожидает INT32 при сборе parquet-статистик.
ALTER TABLE {target_table}
ALTER COLUMN commission TYPE DECIMAL(19,2)
WHEN SOURCE TYPE IS DECIMAL(5,2);
