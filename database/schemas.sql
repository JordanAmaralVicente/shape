CREATE TABLE `gold.fact_sensor_log` (
  sensor_id INT64,
  asset_id INT64,
  temperature NUMERIC(10, 3),
  vibration NUMERIC(10, 3),
  log_type STRING,
  log_datetime DATETIME
)
PARTITION BY DATE(log_datetime);

CREATE TABLE `gold.dm_assets` (
  asset_id INT64,
  name STRING,
  group_name STRING
);

CREATE TABLE `silver.tbl_equipments` (
  equipment_id INT64,
  group_name STRING,
  name STRING
);
CREATE TABLE `silver.tbl_equipment_sensors_relation` (
  equipment_id INT64,
  sensor_id INT64
);

CREATE TABLE `silver.tbl_equipment_failure_sensors` (
  sensor_code STRING,
  log_type STRING,
  temperature FLOAT64,
  vibration FLOAT64,
  log_datetime TIMESTAMP
);
