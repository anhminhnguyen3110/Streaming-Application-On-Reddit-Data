from kafka_producer import KafkaProducerAdapter
from kafka_topic import KafkaTopicManager
import praw
import threading
from config import Setting
import time

# Define the subreddit list
subreddit_list = ['Coronavirus']

class RedditProducer:

    def __init__(self):
        # Initialize the Reddit client
        self.reddit = self.__get_reddit_client__()
    def __get_reddit_client__(self):
        try:
            reddit = praw.Reddit(
                client_id=Setting.CLIENT_ID,
                client_secret=Setting.CLIENT_SECRET,
                user_agent=Setting.USER_AGENT,
                username=Setting.USERNAME,
                password=Setting.PASSWORD,
            )
            return reddit
        except Exception as e:
            print("Error creating Reddit client:", str(e))
            return None

    def start_stream(self, subreddit_name):
        subreddit = self.reddit.subreddit(subreddit_name)

        topic_manager = KafkaTopicManager(f"Subreddit_{subreddit_name}_Submissions")
        topic_name = topic_manager.create_or_get_topic()
        if topic_name is None:
            print("Topic creation or retrieval failed. Check Kafka broker connectivity.")
            return None

        producer = KafkaProducerAdapter(topic=topic_name)  # Use the created topic_name

        submission: praw.models.Submission
        for submission in subreddit.stream.submissions():
            try:
                submission_json = {
					"id": submission.id,
					"name": submission.name,
					"author": submission.author.name,
					"title": submission.title,
					"body": submission.selftext,
					"subreddit": submission.subreddit.display_name,
					"upvotes": submission.ups,
					"downvotes": submission.downs,
					"over_18": submission.over_18,
					"timestamp": submission.created_utc,
					"permalink": submission.permalink,
					"score": submission.score,
					"awards": submission.all_awardings,
					"num_comments": submission.num_comments,
					"num_crossposts": submission.num_crossposts,
					"num_reports": submission.num_reports,
					"num_duplicates": submission.num_duplicates,
					"num_gilded": submission.gilded,
					"num_stickied": submission.stickied,
					"num_archived": submission.archived,
					"num_locked": submission.locked,
					"time_stamp": int(submission.created_utc),
     			}
                producer.send_message(submission_json)
                print(f"After format subreddit: {subreddit_name}, comment: {submission_json}")
                # Wait for 1 second before the next iteration
                time.sleep(0.5)
            except Exception as e:
                print("An error occurred:", str(e))

    def start_streaming_threads(self):
        threads = []
        for subreddit_name in subreddit_list:
            thread = threading.Thread(target=self.start_stream, args=(subreddit_name,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

if __name__ == "__main__":
    reddit_producer = RedditProducer()
    reddit_producer.start_streaming_threads()
