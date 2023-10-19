from kafka_producer import KafkaProducerAdapter
from kafka_topic import KafkaTopicManager
import praw
import threading
from config import Setting
import time

# Define the subreddit list
subreddit_list = ["AskReddit"]


def preprocess(text: str):
    clean_text = demoji.replace(text, "")
    if clean_text != text:
        print("Emoji found in text:", text)

    # Remove URLs
    clean_text = re.sub(r"'\w+|\$[\d\.]+|http\s+'", "", clean_text)
    return clean_text


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

        topic_submission_manager = KafkaTopicManager(
            f"Subreddit_Submissions_{subreddit_name}"
        )
        topic_submission_name = topic_submission_manager.create_or_get_topic()

        if topic_submission_name is None:
            print(
                "Topic creation or retrieval failed. Check Kafka broker connectivity."
            )
            return None

        submission_producer = KafkaProducerAdapter(
            topic=topic_submission_name
        )  # Use the created topic_submission_name

        submission: praw.models.Submission
        for submission in subreddit.stream.submissions():
            try:
                submission_json = {
                    "id": submission.id,
                    "name": submission.name,
                    "author": submission.author.name,
                    "title": submission.title,
                    "subreddit": submission.subreddit.display_name,
                    "upvotes": submission.ups,
                    "upvote_ratio": submission.upvote_ratio,
                    "timestamp": int(submission.created_utc),
                    "permalink": submission.permalink,
                    "submission_url": submission.url,
                    "num_comments": submission.num_comments,
                    "num_reports": submission.num_reports,
                    "num_duplicates": submission.num_duplicates,
                    "insertion_timestamp": int(time.time()),
                }
                comment_list = []
                if submission.num_comments > 0:
                    for comment in submission.comments:
                        comment_json = {
                            "id": comment.id,
                            "name": comment.name,
                            "author": comment.author.name,
                            "body": comment.body,
                            "subreddit": comment.subreddit.display_name,
                            "timestamp": int(comment.created_utc),
                            "permalink": comment.permalink,
                            "insertion_timestamp": int(time.time()),
                            "submission_id": submission.id,
                        }
                        comment_list.append(comment_json["body"])
                submission_json["comments"] = comment_list
                submission_producer.send_message(submission_json)
                print(
                    f"{subreddit_name}, submission_id: {submission_json['id']}, submission_author: {submission_json['author']} with timestamp: {submission_json['timestamp']}"
                )
                time.sleep(25)
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
