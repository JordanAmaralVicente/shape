# Shape - Jordan

This project implements a robust data pipeline for processing sensor data from industrial equipment, designed to run entirely within the Google Cloud Platform (GCP) ecosystem.
Leveraging services like BigQuery, Cloud Storage (GCS), and Dataproc, the pipeline transforms raw sensor logs into a structured, queryable format for analytics and insights into equipment failures.

## Project Files
```
├── answers.sql
├── data/
│   ├── question_4.csv
│   └── question_4_cap.csv
├── database/
│   └── schemas.sql
├── pipe/
│   ├── airflow/
│   │   ├── equipments_dag.py
│   │   └── sensors_failures_dag.py
│   └── spark/
│       ├── gold/
│       │   ├── dm_assets.py
│       │   └── fact_sensors_logs.py
│       └── silver/
│           ├── equipment_failure_sensors.py
│           ├── equipment_sensors_relation.py
│           └── equipments.py
└── res/
    ├── architecture.jpeg
    ├── relation.png
    └── database.png
```

## Architecture
The solution follows a multi-layered architecture, typical for data warehousing, consisting of:
Raw Data Layer: Ingests raw data from various sources.
Silver Layer: Processes and cleanses the raw data, applying initial transformations.
Gold Layer: Stores highly curated and modeled data, optimized for analytical queries.
The pipeline is orchestrated using Apache Airflow (Running on Cloud Composer), with data processing handled by PySpark jobs executed on Dataproc.

![image](https://github.com/JordanAmaralVicente/shape/blob/2b77b38fe83a654ddec0cf952f26ffce68b5053b/res/architecture.jpeg)

### Raw Layer
- Original files were mostly preserved.
- The only change was converting a JSON array into a JSON Lines (JSONL) format, where each line is an individual object.

### Silver Layer
`equipment_failure_sensors.py`: This PySpark script processes raw sensor failure logs. It performs critical transformations including:
- Extracting and merging sensor-related fields (e.g., temperature and vibration values).
- Cleaning and extracting numeric sensor codes and log dates from nested strings.
- Handling 'err' values in sensor readings by converting them to NULL.
- Standardizing date formats to YYYY-MM-DD HH:mm:ss.
- Casting data types for numerical and date fields.
- The processed data is stored in silver.tbl_equipment_failure_sensors in BigQuery.

`equipment_sensors_relation.py`: This PySpark script processes the relationship between equipment and sensors from a CSV file.
- It reads equipment_sensors.csv.
- It prepares the data by selecting equipment_id and sensor_id.
- The processed data is stored in silver.tbl_equipment_sensors_relation in BigQuery.

`equipments.py`: This PySpark script processes raw equipment data from a JSON file.
- It reads equipment.json.
- It prepares the data by selecting equipment_id, name, and group_name.
- The processed data is stored in silver.tbl_equipments in BigQuery.

### Gold Layer
- A star schema was implemented with a central fact table and supporting dimensions:
`fact_sensors_logs`
- log_type remains in the fact table due to insufficient description for a dimension.
- log_datetime is stored as a timestamp, but the BigQuery table is partitioned by date.
- The relationship table is kept only in the silver layer as enrichment

After the dimensional modelling we have the following diagram:
![img](https://github.com/JordanAmaralVicente/shape/blob/2b77b38fe83a654ddec0cf952f26ffce68b5053b/res/relation.png)


![img](https://github.com/JordanAmaralVicente/shape/blob/2b77b38fe83a654ddec0cf952f26ffce68b5053b/res/database.png)

## Solutions
All query answers can be found in answers.sql.

### 1st question
> How many equipment failures happened?
```SQL
SELECT
  COUNT(*) AS qt
FROM `gold.fact_sensor_log` fact
INNER JOIN `gold.dm_assets` dm ON fact.asset_id = dm.asset_id
WHERE
  fact.log_type = 'ERROR'
;
```
Result:
| qt |
|--- |
| 4.749.475 |



### 2nd question
> Which piece of equipment had most failures?

```SQL
SELECT
  COUNT(*) AS qt_failures,
  dm.*
FROM `gold.fact_sensor_log` fact
INNER JOIN `gold.dm_assets` dm ON fact.asset_id = dm.asset_id
WHERE
  fact.log_type = 'ERROR'
GROUP BY ALL
ORDER BY qt_failures DESC
LIMIT 1
;
```
Result:
|qt_failures|asset_id|name|group_name|
|---|---|---|---|
|343800|14|2C195700|VAPQY59S|



### 3rd question
> Find the average amount of failures per asset across equipment groups, ordered by the total number of failures in ascending order.
```SQL
SELECT
  COUNT(*) / COUNT(DISTINCT dm.asset_id) AS avg_failures_per_asset,
  group_name
FROM `gold.fact_sensor_log` fact
INNER JOIN `gold.dm_assets` dm ON fact.asset_id = dm.asset_id
WHERE
  log_type = 'ERROR'
GROUP BY dm.group_name
ORDER BY COUNT(*) ASC
```
Result:
|avg_failures_per_asset|group_name|
|---|---|
|340549.0|Z9K1SAP4|
|339070.5|PA92NCXZ|
|340365.5|9N127Z5P|
|340818.0|NQWPA8D3|
|341183.0|VAPQY59S|
|336217.25|FGHQWR2Q|

### 4th question
> For each asset, rank the sensors which present the most number of failures, and also include the equipment group in the output
```SQL
WITH failures_per_sensor AS (
    SELECT
        COUNT(*) AS failures,
        sensor_id,
        asset_id
    FROM `gold.fact_sensor_log` fact
    WHERE log_type = 'ERROR'
    GROUP BY sensor_id, asset_id
)

SELECT
    RANK() OVER (
        PARTITION BY
            failures_per_sensor.asset_id
        ORDER BY failures DESC
    ) AS rnk,
    failures,
    dm.asset_id,
    dm.name,
    dm.group_name,
FROM failures_per_sensor
INNER JOIN `gold.dm_assets` dm ON failures_per_sensor.asset_id = dm.asset_id
ORDER BY failures_per_sensor.asset_id, rnk
;
```
Result at: `data/question_4.csv`
