import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import (
    regexp_replace, split, regexp_extract, col, when, date_format, to_timestamp
)


class DataProcessor:
    def __init__(self, gcp_project_id, bq_stg_bucket, gcs_data_bucket) -> None:
        self.gcp_project_id = gcp_project_id
        self.bq_stg_bucket = bq_stg_bucket
        self.gcs_data_bucket = gcs_data_bucket
        self.source_path = f'gs://{gcs_data_bucket}/data/raw/equipment-failure-sensors/*.log'
        self.target_table = f'{gcp_project_id}.silver.tbl_equipment_failure_sensors'
        self.spark = None
    
    def _build_spark_session(self):
        spark = SparkSession.builder \
            .appName('Process Equipment Failure Sensors') \
            .getOrCreate()
        
        spark.conf.set('parentProject', self.gcp_project_id)

        return spark
    
    def _raw_data_schema(self):
        return StructType([
                StructField('log_date', StringType()),
                StructField('log_type', StringType()),
                StructField('sensor_code', StringType()),
                StructField('temperature_label', StringType()),
                StructField('merged_value', StringType()),
                StructField('vibration_value', StringType())
            ])
    
    def _read_raw_data(self) -> DataFrame:
        return self.spark.read \
            .option('delimiter', '\t') \
            .schema(self._raw_data_schema()) \
            .csv(self.source_path)

    def _extract_merged_fields(self, df: DataFrame) -> DataFrame:
        return df \
            .withColumn('vibration_value', regexp_replace('vibration_value', '\)', '')) \
            .withColumn('temperature_label', regexp_replace('temperature_label','\(','')) \
            .withColumn('temperature_value', split('merged_value', ',')[0]) \
            .withColumn('vibration_label', split('merged_value', ',')[1]) \
            .drop('merged_value')

    def _extract_inner_characters_fields(self, df: DataFrame) -> DataFrame:
        return df \
            .withColumn('sensor_code',
                regexp_extract('sensor_code', 'sensor\[(\d+)\]:', 1)
            ).withColumn('log_date', regexp_extract('log_date', '\[(.*?)\]', 1))


    def _transform_err_values(self, df: DataFrame) -> DataFrame:
        return df \
            .withColumn('vibration_value',
                when(
                    col('vibration_value') == 'err', None) \
                    .otherwise(col('vibration_value')
                )) \
            .withColumn('temperature_value',
                when(
                    col('temperature_value') == 'err', None) \
                    .otherwise(col('temperature_value')
            ))

    def _parse_date_field(self, df: DataFrame) -> DataFrame:
        return df \
            .withColumn('log_date',
                when(
                    col('log_date').rlike(r'(\d{4})\/(\d{2})\/(\d{1,2})'),
                    date_format(
                        to_timestamp(col('log_date'), 'yyyy/MM/d'),
                        'yyyy-MM-d H:m:s'
                    )
                ).otherwise(col('log_date'))
            ) \
            .withColumn(
                'log_date',
                date_format(
                    to_timestamp(col('log_date'), 'yyyy-MM-d H:m:s'),
                    'yyyy-MM-dd HH:mm:ss'
                )
            )

    def _prepare_data(self, df: DataFrame) -> DataFrame:
        return df.select(
            col('sensor_code'),
            col('log_type'),
            col('temperature_value').cast(DoubleType()).alias('temperature'),
            col('vibration_value').cast(DoubleType()).alias('vibration'),
            col('log_date')
        )

    def _write_data(self, df: DataFrame):
        df.write.format('bigquery') \
            .option('table', self.target_table) \
            .option('temporaryGcsBucket', self.bq_stg_bucket) \
            .mode('overwrite') \
            .save()
        
    def process_data(self):
        self.spark = self._build_spark_session()

        df_raw = self._read_raw_data()
        df_processed = (df_raw
                        .transform(self._extract_merged_fields)
                        .transform(self._extract_inner_characters_fields)
                        .transform(self._transform_err_values)
                        .transform(self._parse_date_field)
                        .transform(self._prepare_data))

        self._write_data(df_processed)


def parse_arguments():
    parser = argparse.ArgumentParser('Processamento de dados de falhas em sensores')

    parser.add_argument('--gcp_project_id')
    parser.add_argument('--big_query_stg_bucket', default = 'bq_running_proccess')

    parser.add_argument('--gcs_data_project_bucket', default = 'shape-digital')

    return parser.parse_args()


def main():
    try:
        args = parse_arguments()

        DataProcessor(
            gcp_project_id=args.gcp_project_id,
            bq_stg_bucket=args.big_query_stg_bucket,
            gcs_data_bucket=args.gcs_data_project_bucket
        ).process_data()

    except Exception as e:
        print(f'Datapipeline não foi executado corretamente: {str(e)}')


if __name__ == '__main__':
    main()
