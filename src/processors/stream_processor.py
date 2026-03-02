import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sha2, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from neo4j import GraphDatabase

# Set local Hadoop environment for Windows compatibility
current_dir = os.getcwd()
hadoop_home = os.path.join(current_dir, 'hadoop')
hadoop_bin = os.path.join(hadoop_home, 'bin')

os.environ['HADOOP_HOME'] = hadoop_home
os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')

# Configure professional logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# System Configurations
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "aml_transactions"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "aml_graph_pass"

class Neo4jSink:
    """
    Manages the connection to Neo4j and handles batch writing via Cypher.
    """
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def write_batch(self, df, epoch_id):
        """
        Writes each micro-batch of Spark stream into Neo4j using Cypher UNWIND.
        This is highly efficient as it processes multiple records in one network trip.
        """
        records = [row.asDict() for row in df.collect()]
        if not records:
            return

        # Cypher query to create nodes and relationships efficiently
        cypher_query = """
        UNWIND $batch AS txn
        MERGE (s:Account {account_id: txn.sender_id})
        MERGE (r:Account {account_id: txn.receiver_id})
        CREATE (s)-[:TRANSFERRED_TO {
            amount: txn.amount,
            currency: txn.currency,
            timestamp: txn.timestamp,
            transaction_id: txn.transaction_id,
            channel: txn.channel
        }]->(r)
        """
        
        with self.driver.session() as session:
            session.run(cypher_query, batch=records)
        
        logger.info(f"Batch {epoch_id}: Successfully ingested {len(records)} transactions into Neo4j.")

def get_spark_session():
    """
    Initializes SparkSession with only Kafka dependency.
    """
    return SparkSession.builder \
        .appName("AML_Graph_Storage_Job") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

def process_stream():
    """
    Consumes Kafka stream, masks PII, and sinks data into Neo4j via Python driver.
    """
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Spark Session initialized. Connecting to stream...")

    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("sender_account", StringType(), True),
        StructField("receiver_account", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("channel", StringType(), True)
    ])

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .load()

    processed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("sender_id", sha2(col("sender_account"), 256)) \
        .withColumn("receiver_id", sha2(col("receiver_account"), 256))

    # Initialize Neo4j Sink
    neo4j_sink = Neo4jSink(NEO4J_URI, NEO4J_USER, NEO4J_PASS)

    # Use foreachBatch to handle the writing process via Python
    query = processed_stream.writeStream \
        .foreachBatch(neo4j_sink.write_batch) \
        .option("checkpointLocation", "spark-warehouse/checkpoints/neo4j-sink") \
        .start()

    logger.info("Graph ingestion started via Python Driver. Monitoring Neo4j data flow...")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
        query.stop()
        neo4j_sink.close()
        spark.stop()

if __name__ == "__main__":
    process_stream()