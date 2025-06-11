-- Question 1
SELECT
  COUNT(*)
FROM `gold.fact_sensor_log` fact
INNER JOIN `gold.dm_assets` dm ON fact.asset_id = dm.asset_id
WHERE
  fact.log_type = 'ERROR'
;

# Result: 4.749.475




-- Question 2 --
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
/* 
 Equipment 2C195700 with ID 14
 qt_failures  asset_id    name       group_name
   343800       14       2C195700     VAPQY59S
*/



-- Question 3 --
SELECT
  COUNT(*) / COUNT(DISTINCT dm.asset_id) AS avg_failures_per_asset,
  group_name
FROM `gold.fact_sensor_log` fact
INNER JOIN `gold.dm_assets` dm ON fact.asset_id = dm.asset_id
WHERE
  log_type = 'ERROR'
GROUP BY dm.group_name
ORDER BY COUNT(*) ASC
;

/* avg_failures_per_asset	group_name
340549.0	Z9K1SAP4
339070.5	PA92NCXZ
340365.5	9N127Z5P
340818.0	NQWPA8D3
341183.0	VAPQY59S
336217.25	FGHQWR2Q
*/




-- Question 4 --
WITH
failures_per_sensor AS (
  SELECT
    COUNT(*) AS failures,
    sensor_id,
    asset_id
  FROM `gold.fact_sensor_log` fact
  WHERE log_type = 'ERROR'
  GROUP BY sensor_id, asset_id
)
SELECT
  RANK() OVER (PARTITION BY failures_per_sensor.asset_id ORDER BY failures DESC) rnk,
  failures,
  dm.asset_id,
  dm.name,
  dm.group_name,
FROM failures_per_sensor
INNER JOIN `gold.dm_assets` dm ON failures_per_sensor.asset_id = dm.asset_id
ORDER BY failures_per_sensor.asset_id, rnk
;

-- Results: attached file data/question_4--