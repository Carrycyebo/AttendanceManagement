from kafka import KafkaConsumer

def get_kafka_consumer():
    """
    获取 Kafka 消费者实例
    """
    consumer = KafkaConsumer(
        'attendance',  # 订阅的 Kafka Topic
        bootstrap_servers="zxlu:9092",  # Kafka 集群地址
        group_id="test",  # Kafka 消费者组 ID
        auto_offset_reset='earliest'  # 从最早的消息开始消费
    )
    for msg in consumer:
        data = (msg.value).decode('utf8').split('\t')
        print(f"班级: {data[0]}, 学生: {data[1]}, 课程: {data[2]}, 学号: {data[3]}, 分数: {data[4]}, 状态: {data[5]}")

    # return consumer
get_kafka_consumer()