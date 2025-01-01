from kafka import KafkaConsumer

def get_kafka_consumer():
    """
    获取 Kafka 消费者实例
    """
    consumer = KafkaConsumer(
        'attendance_summary',  # 订阅的 Kafka Topic
        bootstrap_servers="zhao:9092",  # Kafka 集群地址
        group_id="test",  # Kafka 消费者组 ID
        auto_offset_reset='earliest'  # 从最早的消息开始消费
    )
    for msg in consumer:
        print((msg.value).decode('utf8'))

    # return consumer
get_kafka_consumer()