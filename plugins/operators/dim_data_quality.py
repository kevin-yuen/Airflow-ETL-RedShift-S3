from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseOperator
from airflow.exceptions import AirflowException
from helpers import SqlQueries


class DimDataQualityOperator(BaseOperator):
    def __init__(self, conn_id, dq_config, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.dq_config = dq_config

    def _run_dq_check(self, ds, dq_types, pg_hook, pk):
        sql_queries = SqlQueries()

        for dq in dq_types:
            if dq == 'dim_row_count_check':
                row_count_sql = sql_queries.check_row_cnt(ds=ds)
                row_count_result = pg_hook.get_first(row_count_sql)[0]

                if row_count_result == 0:
                    raise AirflowException(f'{ds} has 0 record.')
                
                self.log.info(f'{ds} has {row_count_result} records.')
            else:
                null_pk_cnt_sql = sql_queries.check_row_cnt(ds=ds, cond_stmt=f'{pk} IS NULL')
                null_pk_cnt_result = pg_hook.get_first(null_pk_cnt_sql)[0]

                if null_pk_cnt_result > 0:
                    raise AirflowException(f'{ds} has null primary keys.')
                
                self.log.info(f'All records in {ds} have a primary key.')

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)

        for dim_table, dq_attr in self.dq_config.items():
            pk = dq_attr['pk']
            dq_types = dq_attr['dq_types']

            self._run_dq_check(dim_table, dq_types, pg_hook, pk)
