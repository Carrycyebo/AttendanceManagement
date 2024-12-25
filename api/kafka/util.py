from configparser import ConfigParser
from kafka import KafkaProducer

def create_producer():
    # 创建ConfigParser实例
    config = ConfigParser()

    # 读取配置文件
    config.read('default.ini')

    # 获取Kafka连接信息
    bootstrap_servers = config.get('kafka', 'bootstrap_servers')

    # 创建Kafka Producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    return producer

def close_producer(producer):
    # 关闭Producer
    producer.close()
