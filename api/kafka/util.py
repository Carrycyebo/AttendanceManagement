from kafka import KafkaProducer
from conf import getConf

conf = getConf.conf()

def create_producer() -> KafkaProducer:
    # 获取Kafka连接信息
    bootstrap_servers = conf.get("kafka", "bootstrap_servers")

    # 创建Kafka Producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    return producer

def get_producer_topic():
    return conf.get("kafka", "producer_topic")

def close_producer(producer):
    # 关闭Producer
    producer.close()



