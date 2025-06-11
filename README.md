# Shape Digital <> Jordan

Olá, este é o resultado do meu teste técnico para a Shape Digital. Que consiste na criação de um pipeline de dados para
processamento de dados de sensores em equipamentos.

## Arquitetura dos arquivos
```
* answers.sql
+ data/
    - question_4.csv
    - question_4_cap.csv
+ database/
    - schemas.sql
+ pipe/
    + airflow/
        - equipments_dag.py
        - sensors_failures_dag.py
    + spark/
        + gold/
            - dm_assets.py
            - fact_sensors_logs.py
        + silver/
            - equipment_failure_sensors.py
            - equipment_sensors_relation.py
            - equipments.py
```

## Descrição da solução
O atual projeto foi realizado em ambiente de nuvem GCP. Por esse motivo, são utilizados serviços como Bigquery, GCS e Dataproc. A escolha do GCP foi por familiaridade e também devido à facilidade de configuração de serviços se comparado a um ambiente local (o que foi presumido não ser o core do teste).

![image](res\architecture.jpeg)

Quanto às especificadades de cada camada:

Para os arquivos originais, a única mudança realizada foi a troca do arquivo JSON/ARRAY para JSONL em que cada linha representa um objeto diferente. Além disso, nenhuma outra mudança relevante foi realizada.

Para a camada de dados processados, o único dado que exigiu mais tratamento foi o arquivo de logs com os dados dos sensores. Para ele, foram feitas transformações para tratar converter data para formato únic, remover id do sensor entre caracteres, e buscar as informações de temperatura e vibração.

Para a última camada, foi escolhido uma modelagem de star-schema no seguinte modelo:
![img](res\relation.png)

E aqui vale alguns destaques:
- escolhi manter o log_type na fato pois não havia descritivos suficientes para criar uma dimensal única de log type
- log_datetime foi mantido em datetime, mas a tabela está particionada internamente no bigquery por date referente ao datetime
- A tabela de relação ficou mantida apenas na etapa anterior que serviu de enriquemente para os dados de log (coluna asset_id)

![](res\database.png)

## Soluções
Arquivo de soluções também presente na raíz do projeto

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
Resultado:
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
Resultado:
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
Resultado:
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
Resultado: `data/question_4.csv`
