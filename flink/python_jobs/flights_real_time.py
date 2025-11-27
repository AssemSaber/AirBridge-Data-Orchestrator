from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import logging

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)  # Increase for production
    env.enable_checkpointing(60000)  # Checkpoint every 60 seconds
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    tbl_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    # ------------------------------
    # Kafka Source - Flight Data >> docker compose exec jobmanager flink run -py /opt/flink/python_jobs/flights_real_time.py
    # ------------------------------
    # >>>>>>>> docker compose jobmanager exec flink run -py /opt/flink/python_jobs/kafka-python/flinkist_of_all.py
    tbl_env.execute_sql("""
        CREATE TABLE flight_events (
            `year` INT,
            `month` INT,
            `day_of_month` INT,
            day_of_week INT,
            fl_date STRING,
            op_unique_carrier STRING,
            op_carrier_fl_num STRING,
            origin STRING,
            origin_city_name STRING,
            origin_state_nm STRING,
            dest STRING,
            dest_city_name STRING,
            dest_state_nm STRING,
            crs_dep_time INT,
            dep_time DOUBLE,
            dep_delay DOUBLE,
            taxi_out DOUBLE,
            wheels_off DOUBLE,
            wheels_on DOUBLE,
            taxi_in DOUBLE,
            crs_arr_time INT,
            arr_time DOUBLE,
            arr_delay DOUBLE,
            cancelled DOUBLE,
            cancellation_code STRING,
            diverted DOUBLE,
            crs_elapsed_time DOUBLE,
            actual_elapsed_time DOUBLE,
            air_time DOUBLE,
            distance DOUBLE,
            carrier_delay DOUBLE,
            weather_delay DOUBLE,
            nas_delay DOUBLE,
            security_delay DOUBLE,
            late_aircraft_delay DOUBLE,
            proctime AS PROCTIME(),
            event_time AS TO_TIMESTAMP(fl_date, 'yyyy-MM-dd'),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' MINUTE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'mysql-server.GP.flights',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'mysql-server.GP.flights',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)
    
    # connection with actual table  
    tbl_env.execute_sql("""
        CREATE TABLE route_performance_pg (
            window_end TIMESTAMP(3),
            origin STRING,
            dest STRING,
            route STRING,
            flight_count BIGINT,
            avg_delay DOUBLE,
            cancellation_rate DOUBLE,
            avg_taxi_out DOUBLE,
            avg_taxi_in DOUBLE,
            avg_air_time DOUBLE,
            PRIMARY KEY (window_end, route) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'route_performance',
            'username' = 'airflow',
            'password' = 'airflow',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    logger.info("Starting Route Performance streaming job...")
    tbl_env.execute_sql("""
        INSERT INTO route_performance_pg
        SELECT
            TUMBLE_END(proctime, INTERVAL '30' second) AS window_end,
            origin,
            dest,
            CONCAT(origin, '-', dest) AS route,
            COUNT(*) AS flight_count,
            AVG(COALESCE(arr_delay, 0)) AS avg_delay,
            SUM(CAST(cancelled AS DOUBLE)) / NULLIF(COUNT(*), 0) * 100 AS cancellation_rate,
            AVG(COALESCE(taxi_out, 0)) AS avg_taxi_out,
            AVG(COALESCE(taxi_in, 0)) AS avg_taxi_in,
            AVG(COALESCE(air_time, 0)) AS avg_air_time
        FROM flight_events
        GROUP BY origin, dest, TUMBLE(proctime, INTERVAL '30' second)
    """)



if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        raise