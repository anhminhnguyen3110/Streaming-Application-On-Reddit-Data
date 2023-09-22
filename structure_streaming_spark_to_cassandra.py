from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, col, from_unixtime, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, BooleanType, FloatType
import uuid

def make_uuid():
    return udf(lambda: str(uuid.uuid1()), StringType())()

spark = SparkSession.builder \
    .appName("StreamProcessor") \
    .config('spark.driver.host', 'localhost') \
    .config('spark.cassandra.connection.host', '35.201.0.188') \
    .config('spark.cassandra.connection.port', '9042') \
    .config('spark.cassandra.output.consistency.level', 'ONE') \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

comment_schema = StructType([
    StructField("id", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("author", StringType(), nullable=True),
    StructField("body", StringType(), nullable=True),
    StructField("subreddit", StringType(), nullable=True),
    StructField("upvotes", IntegerType(), nullable=True),
    StructField("downvotes", IntegerType(), nullable=True),
    StructField("over_18", BooleanType(), nullable=True),
    StructField("timestamp", IntegerType(), nullable=True),
    StructField("permalink", StringType(), nullable=True),
    StructField("score", IntegerType(), nullable=True),
])

KAFKA_BOOTSTRAP_SERVERS = "host.docker.internal:29092"
KAFKA_TOPIC = "Subreddit_Comments"

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df.printSchema()


df = df.withColumn(
    "comment_json",
    from_json(df["value"].cast("string"),comment_schema)
)

parsed_df = df.withColumn(
    "comment_json",
    from_json(df["value"].cast("string"), comment_schema)
)

output_df = parsed_df.select(
       col("comment_json.id").alias("id"),
       col("comment_json.name").alias("name"),
       col("comment_json.author").alias("author"),
       col("comment_json.body").alias("body"),
       col("comment_json.subreddit").alias("subreddit"),
       col("comment_json.upvotes").alias("upvotes"),
       col("comment_json.downvotes").alias("downvotes"),
       col("comment_json.over_18").alias("over_18"),
       col("comment_json.timestamp").alias("timestamp"),
       col("comment_json.permalink").alias("permalink"),
       col("comment_json.score").alias("score"),
    ) \
    .withColumn("uuid", make_uuid()) \
    .withColumn("api_timestamp", from_unixtime(col("timestamp").cast(FloatType()))) \
    .withColumn("ingest_timestamp", current_timestamp()) \
    .drop("timestamp")

output_df.writeStream \
    .option("checkpointLocation", "/tmp/check_point/") \
    .option("failOnDataLoss", "false") \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="comments", keyspace="reddit") \
    .start()

spark.streams.awaitAnyTermination()