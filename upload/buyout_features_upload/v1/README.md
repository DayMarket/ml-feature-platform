# Загрузка признаков модели невыкупов

Публикует признаки аккаунта на грейне `account_id` для модели невыкупов на чекауте.

Отдельный DAG, а не ещё одна модель в `upload/features_service_upload/v1/config.yaml`:
витрина-источник пишется в `06:00 UTC`, а общий upload ранжирования стартует в `04:00 UTC` —
дельта до продюсера получилась бы отрицательной. Код job'а и SparkApplication-шаблон
переиспользуются из `upload/features_service_upload/v1`, здесь лежат только конфиг, DAG
и фабрика — как в `upload/dynamic_pricing_inference_upload/v1`.

## Оркестрация

- DAG: `feature-platform.upload.buyout_features_upload`.
- Расписание: `0 7 * * *` UTC, `start_date=2026-09-09T00:00:00+00:00`, `catchup=False`.
- Владелец `team:buyer`, алерты `buyer`, severity `P3`, webhook `team:buyer`.
- Сенсор: таска `dq` DAG-а `feature-platform.layers.gold.account_id.buyout_online_account_features`
  (`external_task_id="dq"`, delta `60` минут: `D 07:00 - 1ч = D 06:00`). Ждём именно DQ,
  а не весь DAG: успешная запись партиции при упавшей проверке качества не даёт права
  публиковать признаки.

## Источник

- `iceberg.gold.feature_platform_buyout_online_account_features`, партиция `date = {{ ds }}`.
- Ключ сущности `account_id` (`primary_key` без `date`), protobuf `AccountFeatureSet`.

## Kafka

- Connection: `kafka_ranking`, topic `ranking.features.updates`.
- Feature set: `fs_buyout_account_features_v1`, 82 признака.
- Ключ сообщения: `fs_buyout_account_features_v1|<account_id>`.

## Признаки

82 признака на грейне аккаунта — ровно то, что модель берёт из feature store.
Порядок в `config.yaml` задаёт позиции в protobuf-массиве и менять его нельзя без
согласованного изменения на стороне сервиса.

63 признака проекция переносит из витрины-источника без изменений. Остальные 19 —
индикаторы (`last_nonbuyout_*`, `last_pay_*`, `first_dp_*`, `first_completed_order_is_postpaid`,
`has_asof_history`, `is_first_order_ever`) и гео-доли последнего заказа
(`prev_city_*`, `prev_region_*`, `has_prev_city`) — материализованы в самой gold-витрине
(`layers/gold/account_id/buyout_online_account_features/v1`): upload публикует сырые колонки,
без выражений и без join-ов.

Признаки корзины (`cart_*`, `order_hour_tsh`, `delivery_cost_share`) сюда не входят —
они считаются в рантайме по составу корзины и на ключе `account_id` не выгружаются.

`missing-feature-value: 0.0` покрывает и аккаунты без строки в витрине, и NULL внутри строки:
job заполняет NULL нулями перед сериализацией (`na.fill(0.0)`).

## Требует подтверждения владельца сервиса

- Имя схемы `ACCOUNT` в `ranking_service_input.yaml`.
- Топик `ranking.features.updates`: у сервиса невыкупов может быть свой топик, тогда
  меняется только `kafka` в `config.yaml`.
- `executor_instances: 6` — стартовое значение. Сериализация в protobuf идёт Python-UDF
  построчно, поэтому ресурсы стоит перепроверить по первому прогону на полной партиции.
