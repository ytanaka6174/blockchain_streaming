"""
    This is a PySpark Streaming job to consume blockchain events from Kafka and write to PostgreSQL

    Usage:
        docker exec spark /opt/spark/bin/spark-submit \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
            /opt/spark-apps/blockchain_consumer.py
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    conv,
    current_timestamp,
    element_at,
    expr,
    coalesce,
    lit,
    concat_ws,
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration from environment with defaults
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transaction")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "blockchain_data")
POSTGRES_USER = os.getenv("POSTGRES_USER", "user_1")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "user_1")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/opt/checkpoints/blockchain")

# JDBC URL for PostgreSQL
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Schema matching the Kafka message format from blockchain events
BLOCKCHAIN_EVENT_SCHEMA = StructType(
    [
        StructField("address", StringType(), True),
        StructField("blockNumber", StringType(), True),
        StructField("transactionHash", StringType(), True),
        StructField("logIndex", StringType(), True),
        StructField("topics", ArrayType(StringType()), True),
        StructField("data", StringType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    """Create SparkSession with required configurations."""
    return (
        SparkSession.builder.appName("BlockchainEventConsumer")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def parse_and_transform(df):
    """Parse JSON and transform to PostgreSQL schema."""
    return (
        df.select(
            from_json(col("value").cast("string"), BLOCKCHAIN_EVENT_SCHEMA).alias(
                "event"
            )
        )
        .select("event.*")
        .withColumn(
            "block_number",
            conv(expr("substring(blockNumber, 3)"), 16, 10).cast("bigint"),
        )
        .withColumn(
            "log_index",
            coalesce(
                conv(expr("substring(logIndex, 3)"), 16, 10).cast("int"),
                lit(0),
            ),
        )
        .withColumn("event_signature", element_at(col("topics"), 1))
        .withColumnRenamed("address", "contract_address")
        .withColumnRenamed("transactionHash", "transaction_hash")
        .withColumnRenamed("data", "raw_data")
        # Convert topics array to PostgreSQL array format
        .withColumn("topics", concat_ws(",", col("topics")))
        .withColumn("ingested_at", current_timestamp())
        .select(
            "contract_address",
            "block_number",
            "transaction_hash",
            "log_index",
            "event_signature",
            "topics",
            "raw_data",
            "ingested_at",
        )
    )


def write_to_postgres(batch_df, batch_id: int) -> None:
    """Write a micro-batch to PostgreSQL."""
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: Empty batch, skipping")
        return

    record_count = batch_df.count()
    logger.info(f"Batch {batch_id}: Writing {record_count} records to PostgreSQL")

    jdbc_properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    (
        batch_df.write.jdbc(
            url=JDBC_URL,
            table="events",
            mode="append",
            properties=jdbc_properties,
        )
    )

    logger.info(f"Batch {batch_id}: Successfully wrote to PostgreSQL")


def main() -> None:
    """Main entry point for the streaming job."""
    logger.info("Starting Blockchain Event Consumer")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}, Topic: {KAFKA_TOPIC}")
    logger.info(f"PostgreSQL: {JDBC_URL}")
    logger.info(f"Checkpoint: {CHECKPOINT_LOCATION}")

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Transform the data
    transformed_df = parse_and_transform(kafka_df)

    # Write to PostgreSQL using foreachBatch
    query = (
        transformed_df.writeStream.foreachBatch(write_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info("Streaming query started, waiting for termination...")

    # Wait for termination
    query.awaitTermination()

if __name__ == "__main__":
    main()
