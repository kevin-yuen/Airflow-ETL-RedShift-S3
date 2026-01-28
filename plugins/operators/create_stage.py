from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

class CreateStageOperator(SQLExecuteQueryOperator):
    def execute(self, context):
        statements = [sql_stmt.strip() for sql_stmt in self.sql.split(';') if sql_stmt.strip()]
        hook = self.get_hook(conn_id=self.conn_id)

        for stmt in statements:
            self.log.info(f'Executing statement: {stmt}')
            hook.run(stmt)

