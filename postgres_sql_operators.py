from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.providers.postgres.hooks.postgres import PostgresHook

class PostgreSQLCountRowsOperator(BaseOperator):
    """
    Custom operator to count all rows in a PostgreSQL table.

    :param postgres_conn_id: PostgreSQL connection ID
    :param table_name: Name of the table to count rows from
    """
    template_fields = ('table_name', 'postgres_conn_id')
    ui_color = '#87CEEB'

    def __init__(
            self,
            postgres_conn_id: str,
            table_name: str,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name

    def execute(self, context: Context) -> int:
        """
        Executes the row count operation.

        :param context: The Airflow task context
        :return: The number of rows in the specified table
        """
        run_id = context.get('run_id', 'unknown')
        self.log.info(f"Counting rows in table '{self.table_name}' for run_id: {run_id}")

        try:
            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

            sql = f'SELECT COUNT(*) FROM "{self.table_name}";'
            self.log.info(f"Executing SQL: {sql}")

            result = hook.get_first(sql)

            if result and len(result) > 0:
                row_count = result[0]
                self.log.info(f"Table '{self.table_name}' has {row_count} rows.")
                return row_count
            else:
                self.log.warning(f"Query returned no result for table '{self.table_name}'.")
                return 0

        except Exception as e:
            self.log.error(f"An error occurred while counting rows in table '{self.table_name}': {str(e)}")
            raise