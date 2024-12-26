from kafka import KafkaProducer
from conf import getConf

config = getConf.conf()

def create_producer(topic) -> KafkaProducer:
    # 获取Kafka连接信息
    bootstrap_servers = config.get('kafka', 'bootstrap_servers')

    # 创建Kafka Producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    return producer

def close_producer(producer):
    # 关闭Producer
    producer.close()

