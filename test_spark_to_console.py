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