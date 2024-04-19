import json
from kafka import KafkaProducer

#  Kafka broker address
bootstrap_servers = ['localhost:9092']

# Creating a Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                          value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def send_json_data_from_file(topic, data):
    producer.send(topic, data)
    producer.flush()
    print(f"Sent message to topic '{topic}' from the file")
    
if __name__ == '__main__':
    topic_name = 'source'
    path= '/root/kafka_2.12-3.7.0/Kafka/data.json'

    json_list=[]
    with open(path) as file:
        for jsonObj in file:
            jsonDict = json.loads(jsonObj)
            json_list.append(jsonDict)
    # print("json_list",json_list)
    for item in json_list:
        items= json.dumps(item)
        send_json_data_from_file(topic_name, items)
