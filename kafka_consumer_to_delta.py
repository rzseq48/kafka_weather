from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()


# Kafka topic
topic = os.getenv('KAFKA_TOPIC')
KAFKA_HOST=os.getenv('KAFKA_HOST')
KAFKA_PORT=os.getenv('KAFKA_PORT')

# Define Delta table path
DELTA_TABLE_PATH = os.getenv('DELTA_TABLE_PATH')

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaDeltaLakeConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,io.delta:delta-core_2.12:1.0.0") \
    .getOrCreate()

# Define schema for Kafka message value
schema = StructType().add("current", StringType())

# Kafka configuration
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
kafka_topic = os.getenv('KAFKA_TOPIC')

# Define Kafka consumer options
kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap_servers,
    "subscribe": kafka_topic,
    "startingOffsets": "latest"
}

# Read from Kafka as a streaming DataFrame
raw_stream_df = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# Extract value as JSON string
json_df = raw_stream_df.selectExpr("CAST(value AS STRING)")

# Parse JSON data into structured DataFrame
parsed_df = json_df.withColumn("weather_data", from_json(col("value"), schema)) \
    .select("weather_data.current")

# Write parsed DataFrame to Delta Lake table
query = parsed_df \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", os.path.join(DELTA_TABLE_PATH, "_checkpoint")) \
    .start(DELTA_TABLE_PATH)

query.awaitTermination()
