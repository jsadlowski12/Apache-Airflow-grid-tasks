import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random

POSTGRES_CONN_ID = "my_postgres_con"

dag_configs = {
    'js_dag_1': {'schedule_interval': None,
                 "start_date": datetime(2025, 8, 6),
                 'table_name': 'table_1'},
    'js_dag_2': {'schedule_interval': None,
                 "start_date": datetime(2025, 8, 6),
                 'table_name': 'table_2'},
    'js_dag_3': {'schedule_interval': None,
                 "start_date": datetime(2025, 8, 6),
                 'table_name': 'table_3'}
}


def create_dag(dag_id, schedule_interval, start_date, table_name):
    @dag(dag_id=dag_id, schedule=schedule_interval, start_date=start_date, catchup=False)
    def _new_dag():
        @task
        def log_table_processing_information():
            logging.info(f"{dag_id} start processing tables in table: {table_name}")
            return table_name

        @task.branch()
        def check_table_exists(postgres_conn_id: str, table_name: str):
            hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            sql = """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = %s
                );
            """
            result = hook.get_first(sql, parameters=(table_name,))
            logging.info(f"Checking for table: {table_name}. Exists: {result[0]}")

            if result and result[0]:
                return "insert_new_row"
            else:
                return "create_table"

        @task.bash()
        def get_current_user():
            return "whoami"

        @task(trigger_rule=TriggerRule.NONE_FAILED)
        def query_the_table(**kwargs):
            run_id = kwargs['run_id']
            logging.info(f"Querying table {table_name} for run_id: {run_id}")

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            sql = f"SELECT COUNT(*) FROM {table_name};"
            result = hook.get_first(sql)
            logging.info(f"Table {table_name} has {result[0]} rows")
            return run_id

        @task
        def insert_new_row(**kwargs):
            ti = kwargs['ti']
            user_name = ti.xcom_pull(task_ids='get_current_user')

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            custom_id = random.randint(1, 1000000)
            current_time = datetime.now()
            user_name_cleaned = user_name.strip() if user_name else 'unknown'

            sql = f"""
                INSERT INTO {table_name} (custom_id, user_name, timestamp)
                VALUES (%s, %s, %s);
            """
            hook.run(sql, parameters=(custom_id, user_name_cleaned, current_time))

        create_table = SQLExecuteQueryOperator(
            task_id="create_table",
            conn_id=POSTGRES_CONN_ID,
            sql=f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    custom_id INTEGER NOT NULL,  
                    user_name VARCHAR(50) NOT NULL,  
                    timestamp TIMESTAMP NOT NULL
                );
            """
        )

        log_task = log_table_processing_information()
        current_user = get_current_user()
        check_if_table_exists = check_table_exists(POSTGRES_CONN_ID, table_name)
        insert_row = insert_new_row()
        query_row = query_the_table()

        log_task >> current_user >> check_if_table_exists
        check_if_table_exists >> [insert_row, create_table]
        [insert_row, create_table] >> query_row

    return _new_dag()


for dag_id, config in dag_configs.items():
    dag_instance = create_dag(
        dag_id=dag_id,
        schedule_interval=config["schedule_interval"],
        start_date=config["start_date"],
        table_name=config["table_name"]
    )
    globals()[dag_id] = dag_instance