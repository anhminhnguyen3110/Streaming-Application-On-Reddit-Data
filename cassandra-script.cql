CREATE KEYSPACE IF NOT EXISTS reddit 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE reddit;

DROP TABLE IF EXISTS reddit.comments;

CREATE TABLE IF NOT EXISTS comments(
    uuid uuid,
    id text,
    name text,
    author text,
    body text,
    subreddit text,
    upvotes int,
    downvotes int,
    over_18 boolean,
    permalink text,
    api_timestamp timestamp,
    ingest_timestamp timestamp,
    sentiment_score_compound float,
    sentiment_score_polarity float,
    sentiment_score_subjectivity float,
    score float,
    PRIMARY KEY((id), api_timestamp)
)
WITH CLUSTERING ORDER BY (api_timestamp DESC);

CREATE INDEX IF NOT EXISTS ON comments (uuid);

DESCRIBE reddit
DESCRIBE reddit.comments;

DROP TABLE IF EXISTS reddit.subreddit_sentiment_avg;
CREATE TABLE IF NOT EXISTS subreddit_sentiment_avg(
    uuid uuid,
    subreddit text,
    sentiment_score_compound_avg double,
    sentiment_score_polarity_avg double,
    sentiment_score_subjectivity_avg double,
    upvotes_avg double,
    downvotes_avg double,
    score_avg double,
    ingest_timestamp timestamp,
    PRIMARY KEY((subreddit),ingest_timestamp))
WITH CLUSTERING ORDER BY (ingest_timestamp DESC);
 CREATE INDEX IF NOT EXISTS ON subreddit_sentiment_avg (uuid);