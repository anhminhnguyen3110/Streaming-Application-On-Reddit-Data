DROP KEYSPACE reddit;
CREATE KEYSPACE IF NOT EXISTS reddit 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE reddit;

DROP TABLE IF EXISTS comments;
CREATE TABLE IF NOT EXISTS comments (
    uuid UUID,
    id TEXT,
    name TEXT,
    author TEXT,
    body TEXT,
    subreddit TEXT,
    upvotes INT,
    over_18 BOOLEAN,
    permalink TEXT,
    api_timestamp TIMESTAMP,
    ingest_timestamp TIMESTAMP,
    sentiment_score_compound FLOAT,
    sentiment_score_polarity FLOAT,
    sentiment_score_subjectivity FLOAT,
    is_toxic BOOLEAN, 
    PRIMARY KEY ((id), api_timestamp)
)
WITH CLUSTERING ORDER BY (api_timestamp DESC);
CREATE INDEX IF NOT EXISTS ON comments (uuid);

DROP TABLE IF EXISTS subreddit_sentiment_avg;
CREATE TABLE IF NOT EXISTS subreddit_sentiment_avg (
    uuid UUID,
    subreddit TEXT,
    sentiment_score_compound_avg FLOAT,
    sentiment_score_polarity_avg FLOAT,
    sentiment_score_subjectivity_avg FLOAT,
    upvotes_avg FLOAT,
    ingest_timestamp TIMESTAMP,
    PRIMARY KEY ((subreddit), ingest_timestamp)
)
WITH CLUSTERING ORDER BY (ingest_timestamp DESC);
CREATE INDEX IF NOT EXISTS ON subreddit_sentiment_avg (uuid);


DROP TABLE IF EXISTS submissions;
CREATE TABLE IF NOT EXISTS submissions (
    uuid UUID,
    id TEXT,
    name TEXT,
    author TEXT,
    title TEXT,
    subreddit TEXT,
    submission_url TEXT,
    upvotes INT,
    upvote_ratio FLOAT,
    permalink TEXT,
    num_comments INT,
    api_timestamp TIMESTAMP,
    ingest_timestamp TIMESTAMP,
    sentiment_score_compound FLOAT,
    sentiment_score_polarity FLOAT,
    sentiment_score_subjectivity FLOAT,
    sentiment_score_compound_of_comments FLOAT,
    sentiment_score_polarity_of_comments FLOAT,
    sentiment_score_subjectivity_of_comments FLOAT,
    negative_comment_rate FLOAT,
    comments TEXT,
    PRIMARY KEY ((id), api_timestamp)
)
WITH CLUSTERING ORDER BY (api_timestamp DESC);
CREATE INDEX IF NOT EXISTS ON submissions (uuid);
