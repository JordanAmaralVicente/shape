import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql.functions import (
    regexp_replace, split, regexp_extract, col, when, date_format, to_timestamp
)


class DataProcessor:
    def __init__(self, gcp_project_id, bq_stg_bucket, gcs_data_bucket) -> None:
        self.gcp_project_id = gcp_project_id
        self.bq_stg_bucket = bq_stg_bucket
        self.gcs_data_bucket = gcs_data_bucket
        self.source_path = f'gs://{gcs_data_bucket}/data/raw/equipment-sensors/equipment_sensors.csv'
        self.target_table = f'{gcp_project_id}.silver.tbl_equipment_sensors_relation'
        self.spark = None


    def _build_spark_session(self):
        spark = SparkSession.builder \
            .appName('Process Relation between sensors and equipment data') \
            .getOrCreate()
        
        spark.conf.set('parentProject', self.gcp_project_id)

        return spark


    def _raw_data_schema(self):
        return StructType([
                StructField('equipment_id', IntegerType()),
                StructField('sensor_id', IntegerType())
            ])


    def _read_raw_data(self) -> DataFrame:
        return self.spark.read \
            .option('delimiter', ',') \
            .option('header', True) \
            .schema(self._raw_data_schema()) \
            .csv(self.source_path)


    def _prepare_data(self, df: DataFrame) -> DataFrame:
        return df.select(
            col('equipment_id'),
            col('sensor_id')
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
        df_processed = self._prepare_data(df_raw)

        self._write_data(df_processed)


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
        print(f'Datapipeline não foi executado corretamente: {str(e)}')


if __name__ == '__main__':
    main()
