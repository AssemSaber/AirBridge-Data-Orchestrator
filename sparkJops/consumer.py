from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *


spark = SparkSession.builder \
        .appName("KafkaSparkConsumer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages",                    # keep in mind all of them written also in spark-submit              
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"  
                "io.delta:delta-core_2.12:2.2.0"         
                "org.apache.commons:commons-pool2:2.12.0") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", "AKIAQ6Z3CAROZUIIDEMG") \
        .config("spark.hadoop.fs.s3a.secret.key", "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") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .getOrCreate()   
                                # docker compose exec spark-master  spark-submit   --master spark://spark-master:7077   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.commons:commons-pool2:2.12.0,io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.529   /opt/airflow/sparkJops/consumer.py
                                # org.apache.spark:spark-avro_2.12:3.3.0 >> that is for avro if u want add in spark.jar.packages and also in spark-submit

spark.sparkContext.setLogLevel("ERROR")


mySchema = StructType([
    StructField("schema", StringType(), True),
    StructField("payload", StructType([
        StructField("id", LongType(), True),
        StructField("year", LongType(), True),
        StructField("month", LongType(), True),
        StructField("day_of_month", LongType(), True),
        StructField("day_of_week", LongType(), True),
        StructField("fl_date", LongType(), True),
        StructField("op_unique_carrier", StringType(), True),
        StructField("op_carrier_fl_num", DoubleType(), True),
        StructField("origin", StringType(), True),
        StructField("origin_city_name", StringType(), True),
        StructField("origin_state_nm", StringType(), True),
        StructField("dest", StringType(), True),
        StructField("dest_city_name", StringType(), True),
        StructField("dest_state_nm", StringType(), True),
        StructField("crs_dep_time", LongType(), True),
        StructField("dep_time", DoubleType(), True),
        StructField("dep_delay", DoubleType(), True),
        StructField("taxi_out", DoubleType(), True),
        StructField("wheels_off", DoubleType(), True),
        StructField("wheels_on", DoubleType(), True),
        StructField("taxi_in", DoubleType(), True),
        StructField("crs_arr_time", LongType(), True),
        StructField("arr_time", DoubleType(), True),
        StructField("arr_delay", DoubleType(), True),
        StructField("cancelled", IntegerType(), True),
        StructField("cancellation_code", StringType(), True),
        StructField("diverted", IntegerType(), True),
        StructField("crs_elapsed_time", DoubleType(), True),
        StructField("actual_elapsed_time", DoubleType(), True),
        StructField("air_time", DoubleType(), True),
        StructField("distance", DoubleType(), True),
        StructField("carrier_delay", IntegerType(), True),
        StructField("weather_delay", IntegerType(), True),
        StructField("nas_delay", IntegerType(), True),
        StructField("security_delay", IntegerType(), True),
        StructField("late_aircraft_delay", IntegerType(), True),
        StructField("__deleted", StringType(), True),
        StructField("__op", StringType(), True),
        StructField("__source_ts_ms", LongType(), True)
    ]), True)
])


topics = ["mysql-server.GP.flights"]

df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", ",".join(topics)) \
    .option("failOnDataLoss", "false")\
    .option("startingOffsets", "earliest") \
    .load()


df_parsed = df_raw.select(F.col("value").cast("string").alias("value")) \
    .withColumn("jsonData", F.from_json("value", mySchema)) \
    .select("jsonData.*") \
    .withColumn("failure_count", F.lit(0))


def extract_payload(df):
    try:
        return df.select(
            F.col("payload.id").alias("id"),
            F.col("payload.year").alias("year"),
            F.col("payload.month").alias("month"),
            F.col("payload.day_of_month").alias("day_of_month"),
            F.col("payload.day_of_week").alias("day_of_week"),
            F.col("payload.fl_date").alias("fl_date"),
            F.col("payload.op_unique_carrier").alias("op_unique_carrier"),
            F.col("payload.op_carrier_fl_num").alias("op_carrier_fl_num"),
            F.col("payload.origin").alias("origin"),
            F.col("payload.origin_city_name").alias("origin_city_name"),
            F.col("payload.origin_state_nm").alias("origin_state_nm"),
            F.col("payload.dest").alias("dest"),
            F.col("payload.dest_city_name").alias("dest_city_name"),
            F.col("payload.dest_state_nm").alias("dest_state_nm"),
            F.col("payload.crs_dep_time").alias("crs_dep_time"),
            F.col("payload.dep_time").alias("dep_time"),
            F.col("payload.dep_delay").alias("dep_delay"),
            F.col("payload.taxi_out").alias("taxi_out"),
            F.col("payload.wheels_off").alias("wheels_off"),
            F.col("payload.wheels_on").alias("wheels_on"),
            F.col("payload.taxi_in").alias("taxi_in"),
            F.col("payload.crs_arr_time").alias("crs_arr_time"),
            F.col("payload.arr_time").alias("arr_time"),
            F.col("payload.arr_delay").alias("arr_delay"),
            F.col("payload.cancelled").alias("cancelled"),
            F.col("payload.cancellation_code").alias("cancellation_code"),
            F.col("payload.diverted").alias("diverted"),
            F.col("payload.crs_elapsed_time").alias("crs_elapsed_time"),
            F.col("payload.actual_elapsed_time").alias("actual_elapsed_time"),
            F.col("payload.air_time").alias("air_time"),
            F.col("payload.distance").alias("distance"),
            F.col("payload.carrier_delay").alias("carrier_delay"),
            F.col("payload.weather_delay").alias("weather_delay"),
            F.col("payload.nas_delay").alias("nas_delay"),
            F.col("payload.security_delay").alias("security_delay"),
            F.col("payload.late_aircraft_delay").alias("late_aircraft_delay"),
            F.col("payload.__deleted").cast("boolean").alias("isDelete"),
            F.col("payload.__op").alias("operation"),
            F.col("payload.__source_ts_ms").alias("event_time"),
            F.current_timestamp().alias("ingestion_time"),
            F.col("failure_count")
        )
    except:
        # If parsing fails, increment failure_count
        return df.withColumn("failure_count", F.col("failure_count") + 1)

df_clean = extract_payload(df_parsed)


df_success = df_clean.filter(F.col("failure_count") < 3)
df_dlq     = df_clean.filter(F.col("failure_count") >= 3) \
    .selectExpr("to_json(struct(*)) AS value")  # Kafka needs 'value' column


# write to s3 in case of successed processing
df_success.writeStream \
    .format("parquet") \
    .option("path", "s3a://kafka-staging-abdelrahman-2025/kafka/flights") \
    .option("checkpointLocation", "s3a://kafka-staging-abdelrahman-2025/kafka/checkpoints/flights") \
    .outputMode("append") \
    .trigger(processingTime="3 seconds") \
    .start()

#  Write DLQ to Kafka that rite to s3 in case of failed processing more than 3 times
# -----------------------------
df_dlq.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "flight-data-dlq") \
    .option("checkpointLocation", "s3a://kafka-staging-abdelrahman-2025/kafka/checkpoints/dlq") \
    .outputMode("append") \
    .trigger(processingTime="3 seconds") \
    .start()

spark.streams.awaitAnyTermination()

# ---------+-------------------+--------+--------+-------------+-------------+---------+--------------+-------------------+--------+---------+-------------+--------------------+
# | id|year|month|day_of_month|day_of_week|      fl_date|op_unique_carrier|op_carrier_fl_num|origin|    origin_city_name|origin_state_nm|dest|      dest_city_name| dest_state_nm|crs_dep_time|dep_time|dep_delay|taxi_out|wheels_off|wheels_on|taxi_in|crs_arr_time|arr_time|arr_delay|cancelled|cancellation_code|diverted|crs_elapsed_time|actual_elapsed_time|air_time|distance|carrier_delay|weather_delay|nas_delay|security_delay|late_aircraft_delay|isDelete|operation|   event_time|      ingestion_time|
# +---+----+-----+------------+-----------+-------------+-----------------+-----------------+------+--------------------+---------------+----+--------------------+--------------+------------+--------+---------+--------+----------+---------+-------+------------+--------+---------+---------+-----------------+--------+----------------+-------------------+--------+--------+-------------+-------------+---------+--------------+-------------------+--------+---------+-------------+--------------------+
# |  5|2024|    2|          16|          5|1708041600000|               WN|            862.0|   BWI|       Baltimore, MD|       Maryland| MYR|    Myrtle Beach, SC|South Carolina|        1340|  1333.0|     -7.0|    16.0|    1349.0|   1505.0|    4.0|        1510|  1509.0|     -1.0|        0|             null|       0|            90.0|               96.0|    76.0|   399.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# |  6|2024|    4|          15|          1|1713139200000|               WN|           2358.0|   SAN|       San Diego, CA|     California| PHX|         Phoenix, AZ|       Arizona|         715|   708.0|     -7.0|    13.0|     721.0|    812.0|    4.0|         830|   816.0|    -14.0|        0|             null|       0|            75.0|               68.0|    51.0|   304.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# |  7|2024|   11|          27|          3|1732665600000|               G4|            163.0|   MLB|       Melbourne, FL|        Florida| ABE|Allentown/Bethleh...|  Pennsylvania|         933|   924.0|     -9.0|    12.0|     936.0|   1153.0|    5.0|        1202|  1158.0|     -4.0|        0|             null|       0|           149.0|              154.0|   137.0|   914.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# |  8|2024|    8|          31|          6|1725062400000|               AS|           1269.0|   BNA|       Nashville, TN|      Tennessee| PDX|        Portland, OR|        Oregon|        1855|  1850.0|     -5.0|    21.0|    1911.0|   2125.0|    3.0|        2155|  2128.0|    -27.0|        0|             null|       0|           300.0|              278.0|   254.0|  1973.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# |  9|2024|    4|          19|          5|1713484800000|               OO|           3438.0|   LAX|     Los Angeles, CA|     California| SJC|        San Jose, CA|    California|         820|   814.0|     -6.0|    29.0|     843.0|    932.0|    5.0|         939|   937.0|     -2.0|        0|             null|       0|            79.0|               83.0|    49.0|   308.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# | 10|2024|    2|           8|          4|1707350400000|               OO|           5654.0|   SFO|   San Francisco, CA|     California| BFL|     Bakersfield, CA|    California|        2235|  2227.0|     -8.0|    17.0|    2244.0|   2324.0|    3.0|        2350|  2327.0|    -23.0|        0|             null|       0|            75.0|               60.0|    40.0|   238.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# | 11|2024|    9|          19|          4|1726704000000|               G4|           1510.0|   DSM|      Des Moines, IA|           Iowa| AUS|          Austin, TX|         Texas|        1501|  1548.0|     47.0|    12.0|    1600.0|   1736.0|    7.0|        1707|  1743.0|     36.0|        0|             null|       0|           126.0|              115.0|    96.0|   813.0|            0|            0|        0|             0|                 36|   false|        c|1763922253000|2025-11-23 18:24:...|
# | 12|2024|    3|          28|          4|1711584000000|               AA|           2050.0|   MIA|           Miami, FL|        Florida| LGA|        New York, NY|      New York|        2039|  2115.0|     36.0|    48.0|    2203.0|     20.0|    5.0|        2337|    25.0|     48.0|        0|             null|       0|           178.0|              190.0|   137.0|  1096.0|           19|            0|       12|             0|                 17|   false|        c|1763922253000|2025-11-23 18:24:...|
# | 13|2024|    6|          14|          5|1718323200000|               AA|           1911.0|   RIC|        Richmond, VA|       Virginia| CLT|       Charlotte, NC|North Carolina|         635|   639.0|      4.0|    27.0|     706.0|    758.0|    7.0|         759|   805.0|      6.0|        0|             null|       0|            84.0|               86.0|    52.0|   257.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# | 14|2024|   10|          26|          6|1729900800000|               WN|           1102.0|   TPA|           Tampa, FL|        Florida| BNA|       Nashville, TN|     Tennessee|        1925|  1911.0|    -14.0|     6.0|    1917.0|   1944.0|    4.0|        2015|  1948.0|    -27.0|        0|             null|       0|           110.0|               97.0|    87.0|   612.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# | 15|2024|    8|          21|          3|1724198400000|               AA|           2832.0|   LAX|     Los Angeles, CA|     California| PHL|    Philadelphia, PA|  Pennsylvania|         905|   903.0|     -2.0|    13.0|     916.0|   1656.0|   11.0|        1725|  1707.0|    -18.0|        0|             null|       0|           320.0|              304.0|   280.0|  2402.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# | 16|2024|    3|          23|          6|1711152000000|               UA|           2432.0|   FSD|     Sioux Falls, SD|   South Dakota| DEN|          Denver, CO|      Colorado|         905|   859.0|     -6.0|    10.0|     909.0|    924.0|   23.0|        1002|   947.0|    -15.0|        0|             null|       0|           117.0|              108.0|    75.0|   483.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# | 17|2024|    7|          21|          7|1721520000000|               UA|           1926.0|   IAD|      Washington, DC|       Virginia| DEN|          Denver, CO|      Colorado|        1610|  1612.0|      2.0|    12.0|    1624.0|   1736.0|   14.0|        1814|  1750.0|    -24.0|        0|             null|       0|           244.0|              218.0|   192.0|  1452.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# | 18|2024|   12|          18|          3|1734480000000|               UA|           1101.0|   DEN|          Denver, CO|       Colorado| IAD|      Washington, DC|      Virginia|         800|   754.0|     -6.0|    18.0|     812.0|   1258.0|    4.0|        1319|  1302.0|    -17.0|        0|             null|       0|           199.0|              188.0|   166.0|  1452.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# | 19|2024|   10|          11|          5|1728604800000|               DL|           3146.0|   ATL|         Atlanta, GA|        Georgia| MSY|     New Orleans, LA|     Louisiana|        1540|  1537.0|     -3.0|    10.0|    1547.0|   1554.0|    6.0|        1612|  1600.0|    -12.0|        0|             null|       0|            92.0|               83.0|    67.0|   425.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# | 20|2024|    4|          12|          5|1712880000000|               WN|           1124.0|   SRQ|Sarasota/Bradento...|        Florida| BWI|       Baltimore, MD|      Maryland|        1810|  1806.0|     -4.0|    10.0|    1816.0|   2014.0|   11.0|        2035|  2025.0|    -10.0|        0|             null|       0|           145.0|              139.0|   118.0|   880.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# | 21|2024|    7|          24|          3|1721779200000|               OO|           4055.0|   DTW|         Detroit, MI|       Michigan| OMA|           Omaha, NE|      Nebraska|         832|   827.0|     -5.0|    14.0|     841.0|    926.0|    3.0|         935|   929.0|     -6.0|        0|             null|       0|           123.0|              122.0|   105.0|   651.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# | 22|2024|    9|           8|          7|1725753600000|               F9|           2920.0|   TPA|           Tampa, FL|        Florida| CLE|       Cleveland, OH|          Ohio|        1805|  1817.0|     12.0|    31.0|    1848.0|   2054.0|   11.0|        2041|  2105.0|     24.0|        0|             null|       0|           156.0|              168.0|   126.0|   927.0|           12|            0|       12|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# | 23|2024|   11|          25|          1|1732492800000|               UA|            469.0|   IAH|         Houston, TX|          Texas| ORD|         Chicago, IL|      Illinois|         720|   715.0|     -5.0|    21.0|     736.0|    937.0|    9.0|        1002|   946.0|    -16.0|        0|             null|       0|           162.0|              151.0|   121.0|   925.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# | 24|2024|   10|           6|          7|1728172800000|               DL|            461.0|   DFW|Dallas/Fort Worth...|          Texas| LGA|        New York, NY|      New York|         600|   556.0|     -4.0|    14.0|     610.0|   1002.0|    5.0|        1032|  1007.0|    -25.0|        0|             null|       0|           212.0|              191.0|   172.0|  1389.0|            0|            0|        0|             0|                  0|   false|        c|1763922253000|2025-11-23 18:24:...|
# +---+----+-----+------------+-----------+-------------+-----------------+-----------------+------+--------------------+---------------+----+--------------------+--------------+------------+--------+---------+--------+----------+---------+-------+------------+--------+---------+---------+-----------------+--------+----------------+-------------------+--------+--------+-------------+-------------+---------+--------------+-------------------+--------+---------+-------------+--------------------+
# only showing top 20 rows