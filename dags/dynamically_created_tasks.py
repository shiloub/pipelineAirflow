import json
import os
from datetime import datetime, timedelta
import pandas as pd

import pandas as pd
from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from airflow.models.xcom_arg import XComArg
import psycopg2

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 7, 16, 17, 0),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

def makexcom():
    return (["banane", "pomme", "pluie"])


def printxcom(n, **kwargs):
    print(kwargs["ti"].xcom_pull(task_ids='makexcom')[n - 1])
    
def return3():
    return (3)

with DAG(dag_id="dynamically_created_task_dag",
         schedule_interval="@daily",
         default_args=default_args,
         template_searchpath=[f"{os.environ['AIRFLOW_HOME']}"],
         catchup=False) as dag:
   
    start = DummyOperator(
        task_id = "start",
        dag=dag
    )
    
    makexcom = PythonOperator(
        task_id = "makexcom",
        python_callable = makexcom,
        dag=dag
    )
    
    make_number_of_tasks = PythonOperator(
        task_id = "makenumberoftasks",
        python_callable = return3,
        dag = dag
    )
    
    
    with TaskGroup("group_1", dag=dag) as group: 
        
        task_nb = int(XComArg(make_number_of_tasks))
        
        # task_nb = number
        # for i in range(3):
        for i in range(task_nb):
            task = PythonOperator(
                task_id = f"task_{i + 1}",
                python_callable = printxcom,
                op_args = [i + 1],
                provide_context = True,
                dag=dag
            )
    
    end = DummyOperator(
        task_id = "end",
        dag=dag
    )
start >> make_number_of_tasks >> makexcom >> group >> end




    # transform_data = PythonOperator(
    #     task_id = "Transform_data",
    #     python_callable = make_xcom_,
    #     provide_context = True,
    #     dag=dag
    # )


