from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from include.eczachly.trino_queries import execute_trino_query, run_trino_query_dq_check
from airflow.models import Variable
from databricks import sql
import os

polygon_api_key = os.getenv("POLYGON_TOKEN")
server_hostname = os.getenv("DATABRICKS_HOST")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")

schema = 'jakebuto'
@dag(
    description="Takes polygon data in and cumulates it",
    default_args={
        "owner": schema,
        "start_date": datetime(2025, 12, 17),
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2025, 12, 17),
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False,
)
def starter_dag():
    maang_stocks = ['AAPL', 'AMZN', 'NFLX', 'GOOGL', 'META']
    production_table = f'{schema}.user_web_events_cumulated'
    staging_table = production_table + '_stg_{{ ds_nodash }}'
    cumulative_table = f'{schema}.your_table_name'
    yesterday_ds = '{{ yesterday_ds }}'
    ds = '{{ ds }}'

    # todo figure out how to load data from polygon into Iceberg
    def load_data_from_polygon(table):
        pass

    # TODO create schema for daily stock price summary table
    create_daily_step = PythonOperator(
        task_id="create_daily_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f""""""
        }
    )

    # TODO create the schema for your staging table
    create_staging_step = PythonOperator(
        task_id="create_staging_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f""""""
        }
    )


    # todo make sure you load into the staging table not production
    load_to_staging_step = PythonOperator(
        task_id="load_to_staging_step",
        python_callable=load_data_from_polygon,
        op_kwargs={
            'table': """"""
        }
    )

    # TODO figure out some nice data quality checks
    run_dq_check = PythonOperator(
        task_id="run_dq_check",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f""""""
        }
    )


    # todo make sure you clear out production to make things idempotent
    clear_step = PythonOperator(
        task_id="clear_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f""""""
        }
    )

    exchange_data_from_staging = PythonOperator(
        task_id="exchange_data_from_staging",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': """
                          INSERT INTO {production_table}
                          SELECT * FROM {staging_table} 
                          WHERE ds = DATE('{ds}')
                      """.format(production_table=production_table,
                                 staging_table=staging_table,
                                 ds='{{ ds }}')
        }
    )

    # TODO do not forget to clean up
    drop_staging_table = PythonOperator(
        task_id="drop_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': """"""
        }
    )

    # TODO create the schema for your cumulative table
    create_cumulative_step = PythonOperator(
        task_id="create_cumulative_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f""""""
        }
    )

    clear_cumulative_step = PythonOperator(
        task_id="clear_cumulative_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f""""""
        }
    )

    # TODO make sure you create array metrics for the last 7 days of stock prices
    cumulate_step = PythonOperator(
        task_id="cumulate_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': """"""
        }
    )

    # TODO figure out the right dependency chain

starter_dag()
