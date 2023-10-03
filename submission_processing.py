from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, col, from_unixtime, current_timestamp, regexp_replace, concat_ws
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, FloatType, DoubleType, ArrayType
import uuid
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
import demoji
import re
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

KAFKA_BOOTSTRAP_SERVERS = "host.docker.internal:29092"

spark = SparkSession.builder \
    .appName("StreamSubmissionProcessor") \
    .config('spark.driver.host', 'localhost') \
    .config('spark.cassandra.connection.host', '35.244.104.22') \
    .config('spark.cassandra.connection.port', '9042') \
    .config('spark.cassandra.output.consistency.level', 'ONE') \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

submission_schema = StructType([
    StructField("id", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("author", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("subreddit", StringType(), nullable=True),
    StructField("upvotes", IntegerType(), nullable=True),
    StructField("upvote_ratio", DoubleType(), nullable=True),
    StructField("timestamp", IntegerType(), nullable=True),
    StructField("permalink", StringType(), nullable=True),
    StructField("submission_url", StringType(), nullable=True),
    StructField("num_comments", IntegerType(), nullable=True),
    StructField("num_reports", IntegerType(), nullable=True),
    StructField("num_duplicates", IntegerType(), nullable=True),
    StructField("insertion_timestamp", IntegerType(), nullable=True),
    StructField("comments", ArrayType(StringType()), nullable=True)
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribePattern", "Subreddit_Submissions_*") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()
    
df.printSchema()

parsed_df = df.withColumn(
    "submission_json",
    from_json(df["value"].cast("string"), submission_schema)
)

def preprocessing(df, column):
    df = df.filter(col(column).isNotNull())
    df = df.withColumn(column, regexp_replace(column, r'http\S+', ''))
    df = df.withColumn(column, regexp_replace(column, r'[^\x00-\x7F]+', ''))
    df = df.withColumn(column, regexp_replace(column, r'[\n\r]', ' '))
    df = df.withColumn(column, regexp_replace(column, r'\n\n', ' '))
    df = df.withColumn(column, regexp_replace(column, '@\w+', ''))
    df = df.withColumn(column, regexp_replace(column, '#', ''))
    df = df.withColumn(column, regexp_replace(column, 'RT', ''))
    df = df.withColumn(column, regexp_replace(column, ':', ''))
    df = df.withColumn(column, regexp_replace(column, '<a href="' , ''))
    
    return df

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

def analyze_sentiment(text):
    analyzer = SentimentIntensityAnalyzer()
    sentiment = analyzer.polarity_scores(text)
    return sentiment['compound']

def percent_of_negative_comment(texts):
    text_list = texts.split("|----------|")
    count = 0
    for text in text_list:
        sentiment_score = analyze_sentiment(preprocess_text(text))
        if sentiment_score < 0:
            count += 1
    return count / len(text_list)

percent_of_negative_comment_udf = udf(percent_of_negative_comment ,FloatType())

output_df = parsed_df.select(
    col("submission_json.id").alias("id"),
    col("submission_json.name").alias("name"),
    col("submission_json.comments").alias("comments"),
    col("submission_json.author").alias("author"),
    col("submission_json.title").alias("title"),
    col("submission_json.subreddit").alias("subreddit"),
    col("submission_json.upvotes").alias("upvotes"),
    col("submission_json.upvote_ratio").alias("upvote_ratio"),
    col("submission_json.timestamp").alias("timestamp"),
    col("submission_json.permalink").alias("permalink"),
    col("submission_json.submission_url").alias("submission_url"),
    col("submission_json.num_comments").alias("num_comments"),
    col("submission_json.num_reports").alias("num_reports"),
    col("submission_json.num_duplicates").alias("num_duplicates"),
    col("submission_json.insertion_timestamp").alias("insertion_timestamp")
) \
.withColumn("uuid", make_uuid()) \
.withColumn("ingest_timestamp", current_timestamp()) \
.withColumn("api_timestamp", from_unixtime(col("timestamp").cast(FloatType()))) \
.withColumn("comments", concat_ws("|----------|", col("comments"))) \
.withColumn("comments", preprocess_text_udf(col("comments"))) \
.withColumn("title", preprocess_text_udf(col("title"))) \
.withColumn("sentiment_score_compound", sentiment_udf(col("title"))) \
.withColumn("sentiment_score_polarity", polarity_detection_udf(col("title"))) \
.withColumn("sentiment_score_subjectivity", subjectivity_detection_udf(col("title"))) \
.withColumn("negative_comment_rate", percent_of_negative_comment_udf(col("comments"))) \
.drop("num_reports") \
.drop("num_duplicates") \
.drop("insertion_timestamp") \
.drop("timestamp")


def send_to_kafka_and_cassandra(current_df, epoch_id):
    threshold = 0.5
    records_above_threshold = current_df.filter(col("negative_comment_rate") >= threshold)
    if not records_above_threshold.isEmpty():
        records_above_threshold.selectExpr(
            "CAST(uuid AS STRING) AS key",
            "to_json(struct(*)) AS value"
        ) \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", "Negative_Submissions") \
        .option("checkpointLocation", "/tmp/check_point/negative_submission/") \
        .mode("append") \
        .save()
    
    current_df.write \
        .option("checkpointLocation", "/tmp/check_point/submission/") \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="submissions", keyspace="reddit") \
        .mode("append") \
        .save()

output_df.writeStream \
    .option("failOnDataLoss", "false") \
    .outputMode("append") \
    .foreachBatch(send_to_kafka_and_cassandra) \
    .start() \
    .awaitTermination()    