from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import logging

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(60000)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    tbl_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    # ------------------------------
    # Kafka Source - Raw JSON format (not Debezium)
    # ------------------------------
    logger.info("Creating Kafka source table...")
    tbl_env.execute_sql("""
        CREATE TABLE flight_events_raw (
            payload ROW<
                id BIGINT,
                `year` BIGINT,
                `month` BIGINT,
                day_of_month BIGINT,
                day_of_week BIGINT,
                fl_date BIGINT,
                op_unique_carrier STRING,
                op_carrier_fl_num DOUBLE,
                origin STRING,
                origin_city_name STRING,
                origin_state_nm STRING,
                dest STRING,
                dest_city_name STRING,
                dest_state_nm STRING,
                crs_dep_time BIGINT,
                dep_time DOUBLE,
                dep_delay DOUBLE,
                taxi_out DOUBLE,
                wheels_off DOUBLE,
                wheels_on DOUBLE,
                taxi_in DOUBLE,
                crs_arr_time BIGINT,
                arr_time DOUBLE,
                arr_delay DOUBLE,
                cancelled INT,
                cancellation_code STRING,
                diverted INT,
                crs_elapsed_time DOUBLE,
                actual_elapsed_time DOUBLE,
                air_time DOUBLE,
                distance DOUBLE,
                carrier_delay INT,
                weather_delay INT,
                nas_delay INT,
                security_delay INT,
                late_aircraft_delay INT
            >
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'mysql-server.GP.flights',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-flight-consumer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)
    
    # Create a view to flatten the payload
    logger.info("Creating flattened view...")
    tbl_env.execute_sql("""
        CREATE VIEW flight_events AS
        SELECT 
            payload.id,
            payload.`year`,
            payload.`month`,
            payload.day_of_month,
            payload.day_of_week,
            payload.fl_date,
            payload.op_unique_carrier,
            payload.op_carrier_fl_num,
            payload.origin,
            payload.origin_city_name,
            payload.origin_state_nm,
            payload.dest,
            payload.dest_city_name,
            payload.dest_state_nm,
            payload.crs_dep_time,
            payload.dep_time,
            payload.dep_delay,
            payload.taxi_out,
            payload.wheels_off,
            payload.wheels_on,
            payload.taxi_in,
            payload.crs_arr_time,
            payload.arr_time,
            payload.arr_delay,
            payload.cancelled,
            payload.cancellation_code,
            payload.diverted,
            payload.crs_elapsed_time,
            payload.actual_elapsed_time,
            payload.air_time,
            payload.distance,
            payload.carrier_delay,
            payload.weather_delay,
            payload.nas_delay,
            payload.security_delay,
            payload.late_aircraft_delay,
            PROCTIME() as proctime
        FROM flight_events_raw
        WHERE payload.origin IS NOT NULL 
          AND payload.dest IS NOT NULL
    """)
    
    # Debug: Print sink
    logger.info("Creating print sink for debugging...")
    tbl_env.execute_sql("""
        CREATE TABLE print_debug (
            origin STRING,
            dest STRING,
            flight_count BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """)
    
    # Comment out debug job for now - we'll use StatementSet instead
    # logger.info("Starting debug print job...")
    # debug_job = tbl_env.execute_sql(...)
    logger.info("Skipping debug job - will insert directly to PostgreSQL")
    
    # PostgreSQL Sink
    logger.info("Creating PostgreSQL sink...")
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
            TUMBLE_END(proctime, INTERVAL '30' SECOND) AS window_end,
            origin,
            dest,
            CONCAT(origin, '-', dest) AS route,
            COUNT(*) AS flight_count,
            AVG(COALESCE(arr_delay, 0.0)) AS avg_delay,
            CAST(SUM(cancelled) AS DOUBLE) / NULLIF(COUNT(*), 0) * 100 AS cancellation_rate,
            AVG(COALESCE(taxi_out, 0.0)) AS avg_taxi_out,
            AVG(COALESCE(taxi_in, 0.0)) AS avg_taxi_in,
            AVG(COALESCE(air_time, 0.0)) AS avg_air_time
        FROM flight_events
        GROUP BY origin, dest, TUMBLE(proctime, INTERVAL '30' SECOND)
    """)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        raise