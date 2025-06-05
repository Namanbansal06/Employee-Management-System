from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# Load reserved words from JSON file
with open("/home/naman/Downloads/capstone/bootcamp-project/data/marked_word.json", "r") as file:
    reserved_words = json.load(file)

# Create regex pattern from reserved words (case insensitive)
pattern = "|".join([word.lower() for word in reserved_words])

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaToPostgresBatch10") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,"
            "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema for incoming Kafka messages with flag column
schema = StructType() \
    .add("sender", StringType()) \
    .add("receiver", StringType()) \
    .add("message", StringType()) \
    .add("timestamp", LongType()) \
    .add("flag", BooleanType())  # Add flag column directly to schema

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "capstone") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse Kafka message as JSON and clean nulls
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .na.drop(subset=["sender", "receiver", "message", "timestamp"])

# Add the flag column based on reserved words (case-insensitive)
df_flagged = df_parsed.withColumn(
    "flag",
    when(lower(col("message")).rlike(pattern), True).otherwise(False)
)

# Write to PostgreSQL in batches of 10 records
def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        print(f"Skipping batch {batch_id} (empty batch).")
        return

    print(f"Processing batch {batch_id} with {batch_df.count()} records")

    batch_size = 10
    total_rows = batch_df.count()

    # Use monotonically_increasing_id to simulate row numbers
    batch_df = batch_df.withColumn("row_id", monotonically_increasing_id())

    for start in range(0, total_rows, batch_size):
        end = start + batch_size
        chunk_df = batch_df.filter((col("row_id") >= start) & (col("row_id") < end)).drop("row_id")

        # print(f"Writing records {start + 1} to {min(end, total_rows)}")

        chunk_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres_capstone") \
            .option("dbtable", "kafka_messages") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()


# Start the streaming job and apply foreachBatch
query = df_flagged.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/home/naman/Downloads/capstone/bootcamp-project/kafka/checkpoint/") \
    .trigger(processingTime='10 seconds') \
    .start()

query.awaitTermination()
