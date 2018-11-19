-- drop table teste.test_order
-- drop table juvenal.order

USE teste;

CREATE TABLE IF NOT EXISTS database.deliverylines(
    id string,
    scope string,
    event_type string,
    event_time string,
    action_type string,
    payload string
) PARTITIONED BY (partition_namespace string, partition_datetime string)
STORED AS PARQUET;

DESC teste.test_order;

-- nao automatizar, nunca descomentar as linhas de baixo

--USE JUVENAL;
--
--CREATE TABLE IF NOT EXISTS database.order (
--    id string,
--    scope string,
--    event_type string,
--    event_time string,
--    action_type string,
--    payload string
--) PARTITIONED BY (partition_namespace string, partition_datetime string);
--
--DESC juvenal.order;

