import json
import os
from datetime import datetime, timedelta
import pandas as pd

import pandas as pd
from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.hooks.base import BaseHook
import psycopg2

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 7, 16, 17, 0),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "axel.sirota@gmail.com",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5)
}

raw_data_path = "/opt/airflow/data/data.csv"
transformed_data_path = "/opt/airflow/data/transformed.csv"

def loadData(**kwargs):
    from sqlalchemy import create_engine
    df = pd.read_csv(transformed_data_path, encoding='ISO-8859-1')
    df['invoicedate'] = pd.to_datetime(df['invoicedate'])
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/mydb")
    print(df.shape)
    df.to_sql("invoices", engine, if_exists="append", index=False)


def transformData(**kwargs):
    df = pd.read_csv(raw_data_path, encoding='ISO-8859-1')
    df.drop("InvoiceNo", axis=1, inplace=True)
    df.drop("Description", axis=1, inplace=True)
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%m/%d/%Y %H:%M')
    # Reformater les Dates au format 'yyyy-mm-dd hh:mm' compatible avec {{ ds }}
    df['InvoiceDate'] = df['InvoiceDate'] + pd.DateOffset(years= 14, months=3)
    df = df.rename(columns={
        "StockCode": "stockcode",
        "Quantity": "quantity",
        "InvoiceDate": "invoicedate",
        "UnitPrice": "unitprice",
        "CustomerID": "customerid",
        "Country": "country"
    })
    df.to_csv(transformed_data_path, index=False)

def choose_branch(**kwargs):
    query_result = kwargs['ti'].xcom_pull(task_ids='check_data_in_invoices')

    if query_result:
        return 'make_rapport'
    else:
        return 'make_null_rapport'


with DAG(dag_id="invoices_dag",
         schedule_interval="* * * * *",
         default_args=default_args,
         template_searchpath=[f"{os.environ['AIRFLOW_HOME']}"],
         catchup=False) as dag:
   


    isNewDataAvailable = FileSensor(
        task_id = "Check_new_data",
        fs_conn_id = "data_path",
        poke_interval = 30,
        timeout = 600,
        filepath=raw_data_path,
        mode="poke",
        dag=dag
    )


    transform_data = PythonOperator(
        task_id = "Transform_data",
        python_callable = transformData,
        provide_context = True,
        dag=dag
    )

    load_data = PythonOperator(
        task_id = "load_data",
        python_callable = loadData,
        provide_context = True,
        dag=dag
    )

    create_table_invoices = SQLExecuteQueryOperator(
    task_id="Create_table_invoices",
    conn_id="postgres_mydb",
        sql=[
        '''
            DROP TABLE IF EXISTS invoices;
            CREATE TABLE invoices (
            stockcode VARCHAR(50),
            quantity integer,
            invoicedate TIMESTAMP,
            unitprice decimal,
            customerid integer,
            country VARCHAR (50)
        );''',
    ]
    )
    create_table_rapport = SQLExecuteQueryOperator(
    task_id="Create_table_rapport",
    conn_id="postgres_mydb",
        sql=[
        '''
            CREATE TABLE IF NOT EXISTS rapport (
            invoicedate TIMESTAMP,
            customers_count integer,
            total_sold integer,
            total_rev decimal,
            mean_price decimal,
            mean_basket decimal
        );''',
    ]
    )

    branch_task = BranchPythonOperator(
    task_id="branch_task",
    python_callable=choose_branch,
    provide_context=True,
    dag=dag,
    )
    
    check_data_query = SQLExecuteQueryOperator(
        task_id = "check_data_in_invoices",
        conn_id = "postgres_mydb",
        sql = """
    SELECT 1
    FROM invoices
    WHERE DATE_TRUNC('minute', "invoicedate") = DATE_TRUNC('minute', TIMESTAMP '{{ ts }}')
    LIMIT 1;
    """,
    dag=dag
    )


    make_null_rapport = SQLExecuteQueryOperator(
        task_id = "make_null_rapport",
        conn_id="postgres_mydb",
        sql="""
    INSERT INTO rapport (
        invoicedate,
        customers_count,
        total_sold,
        total_rev,
        mean_price,
        mean_basket
    )
    VALUES (
        DATE_TRUNC('minute', NOW() - INTERVAL '1 minute'),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    );
    """,
    dag=dag,
)

    make_rapport = SQLExecuteQueryOperator(
    task_id="make_rapport",
    conn_id="postgres_mydb",
        sql=[
        '''
         WITH "t1" AS (
            SELECT
                "invoicedate",
                "stockcode",
                "customerid",
                COUNT (*) as nb_commandes,
                SUM ("quantity") as total_sold,
                "unitprice",
                (SUM ("quantity") * "unitprice") as earned
                FROM invoices
                WHERE DATE_TRUNC('minute', "invoicedate") = DATE_TRUNC('minute', TIMESTAMP '{{ ts }}')
                group by "stockcode", "unitprice", "customerid", "invoicedate"
                )
        INSERT INTO rapport (
            invoicedate,
            customers_count,
            total_sold,
            total_rev,
            mean_price,
            mean_basket
        )
        SELECT 
            invoicedate,
            count(distinct "customerid") as customers_count,
            sum(total_sold) as total_sold,
            sum(earned) as total_rev,
            case
                when sum(total_sold) = 0 then 0
                else round((sum(earned) / sum(total_sold))::numeric, 2)
            end as mean_price,
            case
                when count(distinct "customerid") = 0 then 0
                else round((sum(earned) / count(distinct "customerid"))::numeric, 2)
            end as mean_basket
        FROM "t1"
        GROUP BY invoicedate
        RETURNING * ;''',
    ],
    dag=dag
    )

    # is_monday_task = BranchPythonOperator(
    #     task_id = 'is_monday',
    #     python_callable=is_monday,
    #     provide_context=True
    # )


    
    # is_new_data_available >> transform_data
    # transform_data >> create_table >> save_into_db
    # save_into_db >> notify_data_science_team



    isNewDataAvailable >> create_table_invoices >> transform_data
    transform_data >> load_data >> create_table_rapport >> check_data_query >> branch_task
    branch_task >> [make_rapport, make_null_rapport]



    # save_into_db >> create_report