from kafka import KafkaConsumer, KafkaProducer
import json
from producer import producer
from consumer import consumer

bootstrap_servers = ['localhost:9092']

def transfer_messages(source_topic, target_topic):
    """Consumes messages from a source topic, processes them (if needed),
       and produces them to a target topic."""
    for msg in consumer:
        data = msg.value  # Accessing the decoded JSON data
        try:
            producer.send(target_topic, data)
            producer.flush()
            print(f"Transferred message from topic '{source_topic}' to topic '{target_topic}'")
        except Exception as e:
            print(f"Error transferring message: {e}")
if __name__ == '__main__':
    source_topic = 'source'
    target_topic = 'target'
    transfer_messages(source_topic, target_topic)
