from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseOperator, Variable
from helpers import SqlQueries, RedshiftVariableManager
import boto3


class StageToRedshiftOperator(BaseOperator):
    def __init__(
            self, 
            conn_id, 
            s3_bucket, 
            s3_dir, 
            staging_table, 
            include_filter, 
            staging_col_mapping_config, 
            filter_key='events.json',
            **kwargs
        ):
        super().__init__(**kwargs)
        self.s3 = boto3.client('s3')
        self.redshift_var_mgr = RedshiftVariableManager()
        
        self.conn_id = conn_id
        self.s3_bucket = s3_bucket
        self.s3_dir = s3_dir
        self.staging_table = staging_table
        self.include_filter = include_filter
        self.staging_col_mapping_config = staging_col_mapping_config
        self.filter_key = filter_key

    def execute(self, context):
        paginator = self.s3.get_paginator('list_objects_v2')
        contents = []
        pg_hook = PostgresHook(postgres_conn_id='redshift')

        for page in paginator.paginate(Bucket=f'{self.s3_bucket}'):
            contents.extend(page['Contents'])

        filtered_s3_subdir = list(
            set(
                [
                    obj['Key'][obj['Key'].index('/') + 1:obj['Key'].rindex('/')] for obj in contents
                    if obj['Key'].endswith(self.filter_key) == self.include_filter and not obj['Key'].endswith('_json_path.json')
                ]
            )
        )

        self.log.info(f"Starting COPY from S3 to Redshift staging table '{self.staging_table}'.")

        for subdir in filtered_s3_subdir[0:1]:                      # TO BE REMOVED: COPY THE OBJECTS FROM THE FIRST DIRECTORY FOR TESTING/DEBUGGING ONLY
            s3_bucket_path = f's3://{self.s3_bucket}'
            src_path_data = f'{s3_bucket_path}/{self.s3_dir}/{subdir}/'
            src_path_mapping = f'{s3_bucket_path}/{self.staging_col_mapping_config}.json'

            iam_role = self.redshift_var_mgr.get_iam_role('iam_role')

            copy_sql = SqlQueries.copy_s3_to_staging(
                self.staging_table,
                src_path_data,
                iam_role,
                src_path_mapping
            )

            pg_hook.run(copy_sql)

        self.log.info(f"COPY from S3 to Redshift staging table '{self.staging_table}' completed successfully.")

