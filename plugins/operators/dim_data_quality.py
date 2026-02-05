from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseOperator
from airflow.exceptions import AirflowException
from helpers import SqlQueries


class DimDataQualityOperator(BaseOperator):
    def __init__(self, conn_id, dq_config, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.dq_config = dq_config

    def _run_dq_check(ds, dq_types, pg_hook, log_object, pk):
        sql_queries = SqlQueries()
        logger = log_object

        for dq in dq_types:
            if dq == 'dim_row_count_check':
                row_count_sql = sql_queries.check_row_cnt(ds=ds)
                row_count_result = pg_hook.get_first(row_count_sql)[0]

                if row_count_result == 0:
                    raise AirflowException(f'{ds} has 0 record.')
                
                logger.info(f'{ds} has {row_count_result} records.')
            elif dq == 'dim_null_pk_check':
                null_pk_cnt_sql = sql_queries.check_row_cnt(ds=ds, cond_stmt=f'{pk} IS NULL')
                null_pk_cnt_result = pg_hook.get_first(null_pk_cnt_sql)[0]

                if null_pk_cnt_result > 0:
                    raise AirflowException(f'{ds} has null primary keys.')
                
                logger.info(f'All records in {ds} have a primary key.')
            elif dq == 'dim_pk_uniqueness_check':
                unique_pk_cnt_sql = sql_queries.check_uniqueness(ds, pk)
                unique_pk_cnt_result = pg_hook.get_first(unique_pk_cnt_sql)[0]

                if unique_pk_cnt_result > 0:
                    raise AirflowException(f'Not all primary keys are unique in {ds}.')
                
                logger.info(f'All primary keys are unique in {ds}.')
            else:       # foreign-key integrity check
                fk_integrity_cnt_sql = sql_queries.check_fk_integrity('songplays_fact', ds, pk, pk)
                fk_integrity_cnt_result = pg_hook.get_first(fk_integrity_cnt_sql)[0]

                if fk_integrity_cnt_result > 0:
                    raise AirflowException(f'Fact table contains references to missing dimension records from {ds}.')
                
                logger.info(f'All references in fact table correspond to dimension records in {ds}.')

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)

        for dim_table, dq_attr in self.dq_config.items():
            pk = dq_attr['pk']
            dq_types = dq_attr['dq_types']

            self._run_dq_check(dim_table, dq_types, pg_hook, self.log, pk)
