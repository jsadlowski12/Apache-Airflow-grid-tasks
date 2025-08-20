from airflow.decorators import dag, task_group
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import json

TRIGGER_FILE_PATH = "/opt/airflow/run/run.txt"
TRIGGERED_DAG_ID = "js_dag_1"
SLACK_CONN_ID = "slack_conn"

def send_simple_slack_notification(**context):
    slack_connection = BaseHook.get_connection(SLACK_CONN_ID)
    extra_options = json.loads(slack_connection.extra)
    slack_token = extra_options['token']

    dag_id = context['dag_run'].dag_id
    logical_date = context['dag_run'].logical_date.strftime("%Y-%m-%d %H:%M:%S UTC")

    message_text = f"Hello from your app! :tada:\nDAG: {dag_id}\nLogical Date: {logical_date}"

    client = WebClient(token=slack_token)

    try:
        response = client.chat_postMessage(
            channel="#all-testairflow",
            text=message_text
        )
    except SlackApiError as e:
        assert e.response["error"]

@dag(dag_id="trigger_when_file_run_present",
     schedule=None,
     start_date=datetime(2025, 8, 6))
def trigger_when_file_run_present():
    path = Variable.get("file_to_trigger_execution_of_js_dag", default_var=TRIGGER_FILE_PATH)

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=path
    )

    trigger_target_dag = TriggerDagRunOperator(
        task_id="trigger_target_dag",
        trigger_dag_id=TRIGGERED_DAG_ID,
        logical_date="{{ data_interval_end }}",
        wait_for_completion=False
    )

    @task_group
    def process_results():
        check_the_status_of_triggered_dag = ExternalTaskSensor(
            task_id="check_the_status_of_triggered_dag",
            external_task_id=None,
            external_dag_id=TRIGGERED_DAG_ID,
            mode="reschedule",
            poke_interval=30,
        )

        def print_xcom_and_context(**context):
            ti = context['ti']
            xcom_value = ti.xcom_pull(
                dag_id= TRIGGERED_DAG_ID,
                task_ids='query_the_table'
            )

            print(f"XCom message: {xcom_value}")
            print(f"Task context keys: {list(context.keys())}")

        print_results_of_the_dag_run = PythonOperator(
            task_id="print_results_of_the_dag_run",
            python_callable=print_xcom_and_context
        )

        remove_trigger_file = BashOperator(
            task_id="remove_trigger_file",
            bash_command=f"rm -f {path}",
        )

        create_timestamp_file = BashOperator(
            task_id="create_timestamp_file",
            bash_command="touch /opt/airflow/run/finished/{{ ts_nodash }}"
        )

        send_slack_message = PythonOperator(
            task_id="send_slack_completion_message",
            python_callable=send_simple_slack_notification
        )

        (check_the_status_of_triggered_dag >> print_results_of_the_dag_run
         >> remove_trigger_file >> create_timestamp_file >> send_slack_message)

    wait_for_file >> trigger_target_dag >> process_results()

globals()["trigger_when_file_run_present"] = trigger_when_file_run_present()