# from airflow.hooks.postgres_hook import PostgresHook
from airflow.sdk import BaseOperator, Variable


class StageToRedshiftOperator(BaseOperator):
    def __init__(self, conn_id, s3_bucket, stg_s3_folder_mapping, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.s3_bucket = s3_bucket
        self.stg_s3_folder_mapping = stg_s3_folder_mapping

    def execute(self, context):
        s3_bucket = self.s3_bucket

        self.log.info(f'StageToRedshiftOperator not implemented yet --- {len(s3_bucket)}')