from config import Setting
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

retention_ms = 86400000

topic_configs = {
    "retention.ms": str(retention_ms)
}

class KafkaTopicManager:
    def __init__(self, subreddit_name):
        self.subreddit_name = subreddit_name
        self.admin_client = KafkaAdminClient(bootstrap_servers=Setting.KAFKA_BOOTSTRAP_SERVER)

    def topic_exists(self):
        topics = self.admin_client.list_topics()
        return self.subreddit_name in topics

    def create_or_get_topic(self):
        if self.topic_exists():
            print(f"Topic '{self.subreddit_name}' already exists.")
        else:
            try:
                new_topic = NewTopic(name=self.subreddit_name, num_partitions=Setting.PARTITION_NUMBER_PER_TOPIC, replication_factor=Setting.REPLICA_FACTOR, topic_configs=topic_configs)
                self.admin_client.create_topics([new_topic])
                print(f"Topic '{self.subreddit_name}' created successfully.")
            except TopicAlreadyExistsError:
                print(f"Topic '{self.subreddit_name}' already exists (concurrent creation).")

        return self.subreddit_name if self.topic_exists() else None

if __name__ == "__main__":
    subreddit_name = "your_subreddit_name2"
    topic_manager = KafkaTopicManager(subreddit_name)
    topic_name = topic_manager.create_or_get_topic()

    if topic_name:
        # Now you can use the 'topic_name' for producing and consuming messages
        producer = KafkaProducer(bootstrap_servers=Setting.KAFKA_BOOTSTRAP_SERVER)
        consumer = KafkaConsumer(topic_name, group_id='subreddit_consumer', bootstrap_servers=Setting.KAFKA_BOOTSTRAP_SERVER)

        print(f"Subscribed to topic '{topic_name}'")

        # Add your Kafka message production and consumption logic here
        producer.send(topic_name, key=b'key', value=b'Hello, Kafka!')
        for message in consumer:
            print(f"Received message: {message.value.decode('utf-8')}")
    else:
        print("Topic creation or retrieval failed. Check Kafka broker connectivity.")
