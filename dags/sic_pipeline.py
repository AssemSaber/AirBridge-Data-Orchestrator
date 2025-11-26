from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.decorators import task_group
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'flight_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule="0 23 * * *",
    catchup=False,
    tags=['flight']
) as dag:
    
    # test_task = SnowflakeOperator(
    #     task_id='test_snowflake',
    #     snowflake_conn_id='snowflake_conn',
    #     sql="""
    #         SELECT ' Connection OK' AS message, CURRENT_TIMESTAMP() AS ts
    #     """
    # )

    raw_data = SnowflakeOperator(
        task_id='raw_data',
        snowflake_conn_id='snowflake_conn',
        sql="""
            COPY INTO flights
            FROM @GP.RAW.kafka_stream
            FILE_FORMAT = (TYPE = PARQUET)
            PATTERN='.*\.parquet'
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """
    )
    last_data_coming=BashOperator(
        task_id='last_data_coming',
        bash_command=""" dbt source freshness --project-dir /opt/airflow/dbt  """
    )
    run_staging_models=BashOperator(
        task_id='run_staging',
        bash_command=""" dbt run --model staging.*  --project-dir /opt/airflow/dbt """
    ) 
    @task_group(group_id='flight_mart')
    def flight_mart():
        dim_destination=BashOperator(
            task_id='dim_destination',
            bash_command=""" dbt run --select dim_destination --project-dir /opt/airflow/dbt """
        )
        dim_origin=BashOperator(
            task_id='dim_origin',
            bash_command=""" dbt run --select dim_origin --project-dir /opt/airflow/dbt """
        )
        dim_flight_status=BashOperator(
            task_id='dim_flight_status',
            bash_command=""" dbt run --select dim_flight_status --project-dir /opt/airflow/dbt """
        )
        fact_flights=BashOperator(
            task_id='fact_flights',
            bash_command=""" dbt run --select fact_flights --project-dir /opt/airflow/dbt """
        )
        [dim_destination,dim_origin,dim_flight_status]>>fact_flights

    test_modeling=BashOperator(
        task_id='test-dimensional-modeling',
        bash_command= """ dbt test --select test_type:generic  --project-dir /opt/airflow/dbt"""
    )

    raw_data>>last_data_coming>>run_staging_models>>flight_mart()>>test_modeling

