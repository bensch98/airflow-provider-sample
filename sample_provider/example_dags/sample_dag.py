from datetime import timedelta
import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from sample_provider.operators.sample_operator import SampleOperator
from sample_provider.sensors.sample_sensor import SampleSensor


default_args = {
  'owner': 'airflow'
}

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['example'])
def sample_worfklow():

  task_get_op = Sample_Operator(
    task_id='get_op',
    sample_conn_id='conn_sample',
    method=''
  )

  task_sensor = Sample_Sensor(
    task_id='sensor',
    sample_conn_id='conn_sample',
    endpoint=''
  )

  task_get_op >> task_sensor

sample_workflow_dag = sample_workflow()
