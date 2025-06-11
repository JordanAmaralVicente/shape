import argparse
from pyspark.sql.functions import col
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType, DecimalType, TimestampType


class DataProcessor:
    def __init__(self, gcp_project_id, bq_stg_bucket, gcs_data_bucket) -> None:
        self.gcp_project_id = gcp_project_id
        self.bq_stg_bucket = bq_stg_bucket
        self.gcs_data_bucket = gcs_data_bucket
        self.source_table = f'{gcp_project_id}.silver.tbl_equipment_failure_sensors'
        self.source_equipments_enrich_table = f'{gcp_project_id}.silver.tbl_equipment_sensors_relation'
        self.target_table = f'{gcp_project_id}.gold.fact_sensor_log'
        self.spark = None


    def _build_spark_session(self):
        spark = SparkSession.builder \
            .appName('Process Sensors Failures') \
            .getOrCreate()
        
        spark.conf.set('parentProject', self.gcp_project_id)

        return spark


    def _read_bigquery_data(self, table) -> DataFrame:
        return self.spark.read.format('bigquery') \
            .option('table', table) \
            .load()

    
    def _join_sensors_and_equipments(self, df_sensors: DataFrame, df_equipments: DataFrame) -> DataFrame:
        return df_sensors.log('logs') \
            .join(
                df_equipments.alias('assets'),
                col('logs.sensor_code') == col('assets.sensor_id')
            )

    def _cast_fields_types(self, df: DataFrame) -> DataFrame:
        return df \
            .withColumn('sensor_code', col('sensor_code').cast(IntegerType())) \
            .withColumn('equipment_id', col('equipment_id').cast(IntegerType())) \
            .withColumn('temperature', col('temperature').cast(DecimalType(10, 3))) \
            .withColumn('vibration', col('vibration').cast(DecimalType(10, 3))) \
            .withColumn('log_datetime', col('log_datetime').cast(TimestampType()))

    def _prepare_data(self, df: DataFrame) -> DataFrame:
        return df.select(
            col('sensor_code').alias('sensor_id'),
            col('equipment_id').alias('assset_id'),
            col('temperature'),
            col('vibration'),
            col('log_type'),
            col('log_datetime')
        )


    def _write_data(self, df: DataFrame):
        df.write.format('bigquery') \
            .option('table', self.target_table) \
            .option('temporaryGcsBucket', self.bq_stg_bucket) \
            .mode('overwrite') \
            .save()


    def process_data(self):
        self.spark = self._build_spark_session()

        df_source = self._read_bigquery_data(self.source_table)
        df_equipments = self._read_bigquery_data(self.self.source_equipments_enrich_table)
        joined_df = self._join_sensors_and_equipments(df_source, df_equipments)
        
        df_prepared = (joined_df
                       .transform(self._cast_fields_types)
                       .transform(self._prepare_data))

        self._write_data(df_prepared)


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('--gcp_project_id')
    parser.add_argument('--big_query_stg_bucket', default = 'bq_running_proccess')

    parser.add_argument('--gcs_data_project_bucket', default = 'shape-digital')

    return parser.parse_args()


def main():
    try:
        args = parse_arguments()

        DataProcessor(
            gcp_project_id = args.gcp_project_id,
            bq_stg_bucket = args.big_query_stg_bucket,
            gcs_data_bucket = args.gcs_data_project_bucket
        ).process_data()

    except Exception as e:
        print(f'Data pipeline error: {str(e)}')


if __name__ == '__main__':
    main()
