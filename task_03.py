from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, from_unixtime, struct, to_json
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
from colorama import Fore, Style, init
import os

# Initialize colored logging
init(autoreset=True)

# Define topic names
my_name = "IOT"
topic_name_in = f"{my_name}_iot_sensors_data"
alerts_topic_name = f"{my_name}_iot_alerts"

# Packages for working with Kafka
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Create SparkSession with custom configurations
print(f"{Fore.CYAN}Starting Spark session...")
spark = (SparkSession.builder
         .appName("IoT_Sensors_Aggregation")
         .master("local[*]")
         .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
         .config("spark.sql.adaptive.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print(f"{Fore.GREEN}Spark session started successfully.")

# JSON schema for Kafka data
iot_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)  # Initially DOUBLE
])

# Schema for CSV file with alert conditions
alerts_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("humidity_min", DoubleType(), True),
    StructField("humidity_max", DoubleType(), True),
    StructField("temperature_min", DoubleType(), True),
    StructField("temperature_max", DoubleType(), True),
    StructField("code", StringType(), True),
    StructField("message", StringType(), True)
])

# Read alert conditions from CSV
alerts_conditions_path = "alerts_conditions.csv"
print(f"{Fore.CYAN}Loading alert conditions from {alerts_conditions_path}...")
alerts_df = spark.read.csv(alerts_conditions_path, schema=alerts_schema, header=True)
print(f"{Fore.GREEN}Alert conditions loaded successfully.")

# Read stream data from Kafka
print(f"{Fore.CYAN}Connecting to Kafka topic: {topic_name_in}...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("subscribe", topic_name_in) \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";'
    ) \
    .load()
print(f"{Fore.GREEN}Connected to Kafka topic: {topic_name_in}")

# Deserialize data and transform according to schema
print(f"{Fore.CYAN}Parsing and transforming data from Kafka...")
iot_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), iot_schema).alias("data")) \
    .select(
        col("data.id"),
        col("data.temperature"),
        col("data.humidity"),
        from_unixtime(col("data.timestamp").cast("long")).cast("timestamp").alias("timestamp")  # Convert to TIMESTAMP
    )
print(f"{Fore.GREEN}Data parsed and transformed successfully.")

# Aggregation: Sliding window (1 minute) with a 30-second interval
print(f"{Fore.CYAN}Starting data aggregation pipeline...")
agg_df = iot_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds")) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )
print(f"{Fore.GREEN}Aggregation pipeline created successfully.")

# Apply alert conditions
print(f"{Fore.CYAN}Applying alert conditions...")
alerts = agg_df.crossJoin(alerts_df) \
    .filter(
        (col("avg_temperature") > col("temperature_min")) &
        (col("avg_temperature") < col("temperature_max")) |
        (col("avg_humidity") > col("humidity_min")) &
        (col("avg_humidity") < col("humidity_max"))
    ) \
    .select(
        "window",
        "avg_temperature",
        "avg_humidity",
        "code",
        "message"
    )
print(f"{Fore.GREEN}Alert conditions applied successfully.")

# Stream alert results to console
print(f"{Fore.CYAN}Starting to stream alerts to console...")
alerts_query = alerts.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Prepare data for Kafka
alerts_to_kafka = alerts.select(
    to_json(
        struct(
            col("window"),
            col("avg_temperature"),
            col("avg_humidity"),
            col("code"),
            col("message")
        )
    ).alias("value")
)

# Write alerts to Kafka
print(f"{Fore.CYAN}Streaming alerts to Kafka topic: {alerts_topic_name}...")
alerts_kafka_query = alerts_to_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", alerts_topic_name) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";'
    ) \
    .option("checkpointLocation", "/tmp/kafka_alerts_checkpoint") \
    .start()

# End streaming

print(f"{Fore.GREEN}Data successfully written to topic: {alerts_topic_name}")
