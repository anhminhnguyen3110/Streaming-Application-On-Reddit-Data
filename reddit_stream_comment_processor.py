from kafka_producer import KafkaProducerAdapter
from kafka_topic import KafkaTopicManager
import praw
import threading
from config import Setting
import time

subreddit_list = [
    "AskReddit",
    "funny",
    "gaming",
    "aww",
    "worldnews",
    "india",
    "usa",
    "unitedkingdom",
    "australia",
    "russia",
    "China",
]


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

        topic_manager = KafkaTopicManager(f"Subreddit_Comments_{subreddit_name}")

        topic_name = topic_manager.create_or_get_topic()
        if topic_name is None:
            print(
                "Topic creation or retrieval failed. Check Kafka broker connectivity."
            )
            return None

        producer = KafkaProducerAdapter(topic=topic_name)  # Use the created topic_name

        comment: praw.models.Comment
        for comment in subreddit.stream.comments(skip_existing=True):
            try:
                comment_json = {
                    "id": comment.id,
                    "name": comment.name,
                    "author": comment.author.name,
                    "body": comment.body,
                    "subreddit": comment.subreddit.display_name,
                    "over_18": comment.over_18,
                    "timestamp": comment.created_utc,
                    "permalink": comment.permalink,
                    "upvotes": comment.score,
                    "is_submitter": comment.is_submitter,
                    "timestamp": int(comment.created_utc),
                    "created_utc": comment.created_utc,
                    "distinguished": comment.distinguished,
                    "edited": comment.edited,
                    "parent_id": comment.parent_id,
                    "saved": comment.saved,
                    "stickied": comment.stickied,
                }
                producer.send_message(comment_json)
                print(
                    f"{subreddit_name}, comment_id: {comment_json['id']}, comment_author: {comment_json['author']}"
                )
                # Wait for 2 second before the next iteration
                time.sleep(10)
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
