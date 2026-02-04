from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from helpers import SqlQueries

class CreateStageOperator(SQLExecuteQueryOperator):
    def execute(self, context):
        sql_queries = SqlQueries()

        drop_table = context['params']['drop_table']
        stg_table = self.parameters['dataset_name']

        self.sql = sql_queries.drop_table(stg_table) + self.sql if drop_table else self.sql + sql_queries.truncate_table(stg_table)

        statements = [s.strip() for s in self.sql.split(';') if s.strip()]
        hook = self.get_hook(conn_id=self.conn_id)

        for stmt in statements:
            self.log.info(f'Executing statement: {stmt}')
            hook.run(stmt)
