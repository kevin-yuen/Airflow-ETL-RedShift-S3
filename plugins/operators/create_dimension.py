from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from helpers.sql_queries import SqlQueries

class CreateDimensionOperator(SQLExecuteQueryOperator):
    def execute(self, context):
        sql_queries = SqlQueries()

        drop_table = context['params']['drop_table']
        dim_table = self.parameters['dataset_name']
        load_mode = self.parameters['mode']

        if drop_table:
            self.sql = sql_queries.drop_table(dim_table) + self.sql
        else:
            if load_mode == 'truncate-load':
                self.sql = self.sql + sql_queries.truncate_table(dim_table)
        
        statements = [s.strip() for s in self.sql.split(';') if s.strip()]
        hook = self.get_hook(conn_id=self.conn_id)

        for stmt in statements:
            self.log.info(f'Executing statement: {stmt}')
            hook.run(stmt)
