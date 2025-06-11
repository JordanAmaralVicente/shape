import argparse
from pyspark.sql.functions import col
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


class DataProcessor:
    def __init__(self, gcp_project_id, bq_stg_bucket, gcs_data_bucket) -> None:
        self.gcp_project_id = gcp_project_id
        self.bq_stg_bucket = bq_stg_bucket
        self.gcs_data_bucket = gcs_data_bucket
        self.source_table = f'{gcp_project_id}.silver.tbl_equipments'
        self.target_table = f'{gcp_project_id}.gold.dm_assets'
        self.spark = None


    def _build_spark_session(self):
        spark = SparkSession.builder \
            .appName('Process equipments data') \
            .getOrCreate()
        
        spark.conf.set('parentProject', self.gcp_project_id)

        return spark


    def _data_schema(self):
        return StructType([
                StructField('equipment_id', IntegerType()),
                StructField('name', StringType()),
                StructField('group_name', StringType())
            ])


    def _read_raw_data(self) -> DataFrame:
        return self.spark.read.format('bigquery') \
            .option('table', self.source_table) \
            .load()


    def _prepare_data(self, df: DataFrame) -> DataFrame:
        return df.select(
            col('equipment_id').alias('asset_id'),
            col('name'),
            col('group_name')
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
        print(f'Data pipeline error: {str(e)}')


if __name__ == '__main__':
    main()
