from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseOperator
from airflow.exceptions import AirflowException
from helpers import SqlQueries


class StagingDataQualityOperator(BaseOperator):
    def __init__(self, conn_id, dq_type, ds_name, ds_columns, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.dq_type = dq_type
        self.ds_name = ds_name
        self.ds_columns = ds_columns

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)

        for dq_type in self.dq_type:
            match dq_type:
                case 'row_count_check':
                    row_cnt_sql = SqlQueries.check_row_cnt(ds=self.ds_name)
                    row_cnt_result = pg_hook.get_first(row_cnt_sql)[0]

                    if row_cnt_result == 0:
                        raise AirflowException(f'{self.ds_name} staging has 0 row.')
                    else:
                        self.log.info(f'{self.ds_name} staging has rows.')
                case 'null_count_check':
                    where_cond_stmts = ' OR '.join([f'{col} IS NULL' for col in self.ds_columns])
                    null_cnt_sql = SqlQueries.check_row_cnt(ds=self.ds_name, cond_stmt=where_cond_stmts)   # check nulls on critical columns
                    null_cnt_result = pg_hook.get_first(null_cnt_sql)[0]

                    if null_cnt_result > 0:
                        raise AirflowException(f'{self.ds_name} staging has nulls.')
                        
                    self.log.info(f'{self.ds_name} staging has 0 nulls.')
                case 'uniqueness_count_check':
                    cleaned_cols = f"{', '.join(self.ds_columns)}"
                    uniqueness_cnt_sql = SqlQueries.check_uniqueness(self.ds_name, cleaned_cols)
                    uniqueness_cnt_result = pg_hook.get_first(uniqueness_cnt_sql)[0]

                    if uniqueness_cnt_result > 1:
                        raise AirflowException(f'{self.ds_name} staging has failed the column uniqueness check.')
                    
                    self.log.info(f'{self.ds_name} staging has passed the column uniqueness check.')
