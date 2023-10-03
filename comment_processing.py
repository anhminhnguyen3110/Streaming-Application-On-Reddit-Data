from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, col, from_unixtime, avg, current_timestamp, regexp_replace
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, BooleanType, FloatType
import uuid
import nltk
import demoji
import re
from nltk.sentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
nltk.download('vader_lexicon')


def make_uuid():
    return udf(lambda: str(uuid.uuid1()), StringType())()

def analyze_sentiment(text):
    analyzer = SentimentIntensityAnalyzer()
    sentiment = analyzer.polarity_scores(text)
    return sentiment['compound']

sentiment_udf = udf(analyze_sentiment, FloatType())

def polarity_detection(text):
    return TextBlob(text).sentiment.polarity

polarity_detection_udf = udf(polarity_detection, FloatType())

def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity

subjectivity_detection_udf = udf(subjectivity_detection, FloatType())

spark = SparkSession.builder \
    .appName("StreamCommentProcessor") \
    .config('spark.driver.host', 'localhost') \
    .config('spark.cassandra.connection.host', '35.244.104.22') \
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
    StructField("over_18", BooleanType(), nullable=True),
    StructField("timestamp", IntegerType(), nullable=True),
    StructField("permalink", StringType(), nullable=True),
])

KAFKA_BOOTSTRAP_SERVERS = "host.docker.internal:29092"
KAFKA_TOPIC = "Subreddit_Comments"

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribePattern", "Subreddit_Comments_*") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()
    
df.printSchema()

parsed_df = df.withColumn(
    "comment_json",
    from_json(df["value"].cast("string"), comment_schema)
)

def preprocess_text(text):
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'[^\x00-\x7F]+', '', text)
    text = re.sub(r'[\n\r]', ' ', text)
    text = re.sub(r'\n\n', ' ', text)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'#', '', text)
    text = re.sub(r'RT', '', text)
    text = re.sub(r':', '', text)
    text = re.sub(r'<a href="' , '', text)
    text = re.sub(r'  ', ' ', text)
    text = demoji.replace(text, '')
    return text

preprocess_text_udf = udf(preprocess_text, StringType())

output_df = parsed_df.select(
       col("comment_json.id").alias("id"),
       col("comment_json.name").alias("name"),
       col("comment_json.author").alias("author"),
       col("comment_json.body").alias("body"),
       col("comment_json.subreddit").alias("subreddit"),
       col("comment_json.upvotes").alias("upvotes"),
       col("comment_json.over_18").alias("over_18"),
       col("comment_json.timestamp").alias("timestamp"),
       col("comment_json.permalink").alias("permalink"),
    ) \
    .withColumn("uuid", make_uuid()) \
    .withColumn("api_timestamp", from_unixtime(col("timestamp").cast(FloatType()))) \
    .withColumn("ingest_timestamp", current_timestamp()) \
    .withColumn("body", preprocess_text_udf(col("body"))) \
    .withColumn("sentiment_score_compound", sentiment_udf(col("body"))) \
    .withColumn("sentiment_score_polarity", polarity_detection_udf(col("body"))) \
    .withColumn("sentiment_score_subjectivity", subjectivity_detection_udf(col("body"))) \
    .drop("timestamp")

KAFKA_NEGATIVE_COMMENTS_TOPIC = "Negative_Comments"

def send_to_kafka_and_cassandra(current_df, epoch_id):
    threshold = 0.0  # Adjust the threshold as needed
    negative_comments = current_df.filter(col("sentiment_score_compound") < threshold)
    if not negative_comments.isEmpty():
        negative_comments.selectExpr(
            "CAST(uuid AS STRING) AS key",
            "to_json(struct(*)) AS value"
        ) \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_NEGATIVE_COMMENTS_TOPIC) \
        .option("checkpointLocation", "/tmp/check_point/negative_comments/") \
        .mode("append") \
        .save()
    
    current_df.write \
        .option("checkpointLocation", "/tmp/check_point/comments/") \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="comments", keyspace="reddit") \
        .mode("append") \
        .save()
    
output_df.writeStream \
    .option("failOnDataLoss", "false") \
    .outputMode("append") \
    .foreachBatch(send_to_kafka_and_cassandra) \
    .start()
    
summary_df = output_df.withWatermark("ingest_timestamp", "2 minutes").groupBy("subreddit") \
    .agg(
        avg("upvotes").alias("upvotes_avg"),
        avg("sentiment_score_compound").alias("sentiment_score_compound_avg"),
        avg("sentiment_score_polarity").alias("sentiment_score_polarity_avg"),
        avg("sentiment_score_subjectivity").alias("sentiment_score_subjectivity_avg")
    ) \
    .withColumn("uuid", make_uuid()) \
    .withColumn("ingest_timestamp", current_timestamp())

summary_df.writeStream.trigger(processingTime="2 minutes") \
    .foreachBatch(
        lambda batchDF, batchID: batchDF.write.format("org.apache.spark.sql.cassandra") \
            .option("checkpointLocation", "/tmp/check_point/summary_comments/") \
            .options(table="subreddit_sentiment_avg", keyspace="reddit") \
            .mode("append").save()
    ).outputMode("update").start()
    
spark.streams.awaitAnyTermination()