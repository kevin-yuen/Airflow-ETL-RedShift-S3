from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseOperator
from airflow.exceptions import AirflowException

class LoadFactOperator(BaseOperator):
    def __init__(self,
                 conn_id,
                 ds_name,
                 sql,
                 **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.ds_name = ds_name
        self.sql = sql

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        insert_sql_stmt = f'INSERT INTO {self.ds_name} {self.sql}'
        records = pg_hook.get_records(self.sql)

        if len(records) == 0:
            raise AirflowException('No song records found in events staging.')
        
        pg_hook.run(insert_sql_stmt)
        self.log.info(f'{len(records)} inserted into {self.ds_name}.')
