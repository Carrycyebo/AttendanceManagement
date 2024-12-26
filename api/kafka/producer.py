from api.kafka.util import create_producer, close_producer

## @秋风起  在这个文件下写

def producer():
    # 创建Kafka Producer
    producer = create_producer()

    # 发送消息
    producer.send('my_topic', b'Hello, Kafka!')
    close_producer(producer)
