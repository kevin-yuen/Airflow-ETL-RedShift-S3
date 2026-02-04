from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from helpers.sql_queries import SqlQueries

class CreateFactOperator(SQLExecuteQueryOperator):
    def execute(self, context):
        sql_queries = SqlQueries()

        drop_table = context['params']['drop_table']
        stg_table = self.parameters['dataset_name']

        if drop_table:
            self.sql = sql_queries.drop_table(stg_table) + self.sql

        statements = [s.strip() for s in self.sql.split(';') if s.strip()]
        hook = self.get_hook(conn_id=self.conn_id)

        for stmt in statements:
            self.log.info(f'Executing statement: {stmt}')
            hook.run(stmt)

