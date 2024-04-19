from kafka import KafkaConsumer
import json
# Kafka broker address
bootstrap_servers = ['localhost:9092']

consumer = KafkaConsumer('target',
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

def consume_messages():
    """Consumes messages from a Kafka topic and prints them."""
    try:
      for msg in consumer:
        print(f"Received message from '2':",json.loads(msg.value))
    except (KeyboardInterrupt,SystemExit):
       print("Consumer stopped.")
    except Exception as e:
       print(f"Error consuming message : {e}")
if __name__ == '__main__':
    consume_messages()
