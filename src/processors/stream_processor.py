import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sha2, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Set local Hadoop environment for Windows compatibility
current_dir = os.getcwd()
hadoop_home = os.path.join(current_dir, 'hadoop')
hadoop_bin = os.path.join(hadoop_home, 'bin')

os.environ['HADOOP_HOME'] = hadoop_home
# Dynamically add hadoop/bin to the system PATH so the JVM can locate hadoop.dll
os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')

# Configure professional logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "aml_transactions"

def get_spark_session():
    """
    Initializes and returns a SparkSession configured for Kafka streaming.
    Utilizes local mode for development to streamline the testing process.
    """
    return SparkSession.builder \
        .appName("AML_Transaction_Processor") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

def process_stream():
    """
    Reads streaming data from Kafka, applies data masking (SHA-256) for PDPA compliance,
    and outputs the cleaned data to the console. Implements graceful shutdown.
    """
    spark = get_spark_session()
    
    # Suppress verbose Spark logging to focus on our application logs
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Spark Session initialized. Connecting to Kafka stream...")

    # Define the schema matching the PaySim generator output
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("sender_account", StringType(), True),
        StructField("receiver_account", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("channel", StringType(), True)
    ])

    # Read the data stream from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON payload into structured DataFrame columns
    parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    # Apply Data Engineering Transformations (Hashing for Privacy)
    processed_stream = parsed_stream \
        .withColumn("sender_hashed", sha2(col("sender_account"), 256)) \
        .withColumn("receiver_hashed", sha2(col("receiver_account"), 256)) \
        .withColumn("processed_at", current_timestamp()) \
        .drop("sender_account", "receiver_account")

    # Output to console for testing Phase 3
    query = processed_stream.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    logger.info("Streaming query started. Awaiting data... (Press Ctrl+C to stop)")
    
    # Implement Graceful Shutdown to prevent Windows file lock exceptions
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Interrupt signal received. Stopping streaming query gracefully...")
        query.stop()
        spark.stop()
        logger.info("Spark Session closed successfully. No data lost.")

if __name__ == "__main__":
    process_stream()