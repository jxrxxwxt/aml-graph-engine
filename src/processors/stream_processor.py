import os
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sha2
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from neo4j import GraphDatabase

# --- Windows Compatibility Setup ---
current_dir = os.getcwd()
hadoop_home = os.path.join(current_dir, 'hadoop')
hadoop_bin = os.path.join(hadoop_home, 'bin')

# Ensure Hadoop environment is set BEFORE Spark session starts
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# System Configurations
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "aml_transactions"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "aml_graph_pass"

class Neo4jSink:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def write_batch(self, df, epoch_id):
        records = [row.asDict() for row in df.collect()]
        if not records: return

        cypher_query = """
        UNWIND $batch AS txn
        MERGE (s:Account {account_id: txn.sender_id})
        MERGE (r:Account {account_id: txn.receiver_id})
        CREATE (s)-[:TRANSFERRED_TO {
            amount: txn.amount,
            currency: txn.currency,
            timestamp: txn.timestamp,
            transaction_id: txn.transaction_id
        }]->(r)
        """
        with self.driver.session() as session:
            session.run(cypher_query, batch=records)
        logger.info(f"Batch {epoch_id}: Ingested {len(records)} txns to Neo4j.")

def process_stream():
    # Force creation of a NEW Spark session to ensure Jars are loaded
    spark = SparkSession.builder \
        .appName("AML_Final_Job") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.streaming.checkpointLocation", "spark-warehouse/checkpoints/neo4j-sink") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("New Spark Session initialized with Kafka Support.")

    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("sender_account", StringType(), True),
        StructField("receiver_account", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    try:
        # Source with failOnDataLoss=false to handle Kafka retention issues
        raw_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", TOPIC_NAME) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        processed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
            .select(from_json(col("json_str"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("sender_id", sha2(col("sender_account"), 256)) \
            .withColumn("receiver_id", sha2(col("receiver_account"), 256))

        neo4j_sink = Neo4jSink(NEO4J_URI, NEO4J_USER, NEO4J_PASS)

        query = processed_stream.writeStream \
            .foreachBatch(neo4j_sink.write_batch) \
            .start()

        logger.info("Pipeline is RUNNING. Waiting for data...")
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Stopping...")
        query.stop()
        spark.stop()

if __name__ == "__main__":
    process_stream()