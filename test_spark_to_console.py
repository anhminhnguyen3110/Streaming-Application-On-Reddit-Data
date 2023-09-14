from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, col
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, BooleanType
import uuid

def make_uuid():
    return udf(lambda: str(uuid.uuid1()), StringType())()

spark = SparkSession.builder \
    .appName("StreamProcessor") \
    .master("local[*]") \
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
    # StructField("score", IntegerType(), nullable=True),
])

KAFKA_BOOTSTRAP_SERVERS = "host.docker.internal:29092"
KAFKA_TOPIC = "Subreddit_AskReddit_Comments"
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

df.printSchema()


df = df.withColumn(
    "comment_json",
    from_json(df["value"].cast("string"),comment_schema)
)

df.writeStream \
  .outputMode("update") \
  .format("console") \
  .option("truncate", False) \
  .start() \
      
spark.streams.awaitAnyTermination()
  
# # Select the fields from the struct
# df = df.select(
#     col("comment_json.id").alias("id"),
#     col("comment_json.name").alias("name"),
#     col("comment_json.author").alias("author"),
#     col("comment_json.body").alias("body"),
#     col("comment_json.subreddit").alias("subreddit"),
#     col("comment_json.upvotes").alias("upvotes"),
#     col("comment_json.downvotes").alias("downvotes"),
#     col("comment_json.over_18").alias("over_18"),
#     col("comment_json.timestamp").alias("timestamp"),
#     col("comment_json.permalink").alias("permalink"),
#     col("comment_json.score").alias("score"),
#     col("comment_json.is_submitter").alias("is_submitter")
# )



# # Write the resulting DataFrame to the console with the custom trigger
# query = df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("numRows", 1000) \
#     .trigger(processingTime='5 seconds') \
#     .start()

# query.awaitTermination()