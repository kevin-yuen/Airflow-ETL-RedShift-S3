from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseOperator
from airflow.exceptions import AirflowException
from helpers import SqlQueries


class FactDataQualityOperator(BaseOperator):
    def __init__(self, conn_id, dq_type, ds_name, ds_columns, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.dq_type = dq_type
        self.ds_name = ds_name
        self.ds_columns = ds_columns

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)

        for dq_type in self.dq_type:
            row_cnt_fact_sql = SqlQueries.check_row_cnt(ds=self.ds_name)
            row_cnt_fact_result = pg_hook.get_first(row_cnt_fact_sql)[0]

            match dq_type:
                case 'reasonableness_check':
                    nextsong_events_cnt_sql = SqlQueries.check_row_cnt(
                        ds='events_staging',
                        cond_stmt="page='NextSong'")
                    
                    nextsong_events_cnt = pg_hook.get_first(nextsong_events_cnt_sql)[0]

                    if row_cnt_fact_result == 0:
                        raise AirflowException('Fact table has zero records.')
                    elif row_cnt_fact_result > nextsong_events_cnt:
                        raise AirflowException('Fact table contains more records than NextSong events. Possible duplication.')
                    
                    self.log.info(f'{self.ds_name} fact record count is within the expected range relative to NextSong events.')
                case 'uniqueness_cnt_check_fact':
                    distinct_cnt_fact_sql = SqlQueries.check_row_cnt(
                        ds=self.ds_name,
                        cnt_method='DISTINCT(songplay_id)'
                    )

                    distinct_fact_cnt = pg_hook.get_first(distinct_cnt_fact_sql)[0]

                    if row_cnt_fact_result != distinct_fact_cnt:
                        raise AirflowException('Fact table seems to contain duplication.')
                    
                    self.log.info(f'{self.ds_name} fact contains unique identifiers.')
                case 'non_null_check':
                    where_cond_stmts = ' OR '.join([f'{col} IS NULL' for col in self.ds_columns])
                    null_cnt_sql = SqlQueries.check_row_cnt(ds=self.ds_name, cond_stmt=where_cond_stmts)   # check nulls on critical columns
                    null_cnt_result = pg_hook.get_first(null_cnt_sql)[0]

                    if null_cnt_result > 0:
                        raise AirflowException(f'{self.ds_name} fact has nulls.')
                        
                    self.log.info(f'{self.ds_name} fact has 0 nulls.')
