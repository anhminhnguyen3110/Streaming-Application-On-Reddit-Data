from kafka import KafkaProducer
import json
from config import Setting


class KafkaProducerAdapter:
    def __init__(self, topic: str, bootstrap_servers=Setting.KAFKA_BOOTSTRAP_SERVER):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def send_message(self, message):
        try:
            self.producer.send(self.topic, value=message)
            self.producer.flush()
            return True  # Message sent successfully
        except Exception as e:
            print(f"Error sending message to Kafka: {str(e)}")
            return False  # Message sending failed

    def close(self):
        self.producer.close()


# Example usage:
if __name__ == "__main__":
    bootstrap_servers = Setting.KAFKA_BOOTSTRAP_SERVER
    topic_name = "your_subreddit_name"

    producer = KafkaProducerAdapter(bootstrap_servers, topic_name)

    message = {"key": "value", "foo": "bar"}

    success = producer.send_message(message)

    if success:
        print("Message sent successfully")
    else:
        print("Failed to send message")

    producer.close()
