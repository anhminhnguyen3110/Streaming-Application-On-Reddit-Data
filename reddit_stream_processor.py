from kafka_producer import KafkaProducerAdapter
from kafka_topic import KafkaTopicManager
import praw
import threading
from json import dumps
from config import Setting

# Define the subreddit list
subreddit_list = ['AskReddit']

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
        
        topic_manager = KafkaTopicManager(subreddit_name)
        topic_name = topic_manager.create_or_get_topic()        
        if topic_name is None:
            print("Topic creation or retrieval failed. Check Kafka broker connectivity.")
            return None
        producer = KafkaProducerAdapter(topic=subreddit_name)
        
        comment: praw.models.Comment
        for comment in subreddit.stream.comments(skip_existing=True):
            try:
                comment_json = {
                    "id": comment.id,
                    "name": comment.name,
                    "author": comment.author.name,
                    "body": comment.body,
                    "subreddit": comment.subreddit.display_name,
                    "upvotes": comment.ups,
                    "downvotes": comment.downs,
                    "over_18": comment.over_18,
                    "timestamp": comment.created_utc,
                    "permalink": comment.permalink,
                    "link_id": comment.link_id,
                    "score": comment.score,
                    "awards": comment.all_awardings,
                    "is_submitter": comment.is_submitter,
                    "stickied": comment.stickied,
                }
                producer.send_message(comment_json)
                print(f"After format subreddit: {subreddit_name}, comment: {comment_json}")
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
