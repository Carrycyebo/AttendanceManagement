from kafka import KafkaConsumer
import json

def create_kafka_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers="zhao:9092",
        group_id="test",
        auto_offset_reset='latest'
    )

def consume_messages(consumer, topic_name, socketio):
    print(f"Starting to consume messages from topic: {topic_name}")
    for message in consumer:
        try:
            data = json.loads(message.value.decode('utf-8'))
            socketio.emit(f'{topic_name}_message', {'data': data})
        except json.JSONDecodeError as e:
            print(f"JSON decode error for topic {topic_name}: {e}")
        except Exception as e:
            print(f"Error processing Kafka message from {topic_name}: {e}")
