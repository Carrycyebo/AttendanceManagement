#初始化__init__.py文件，使其成为包
def init():
    from api.kafka import util
    from api.kafka import producer
    from api.kafka import consumer
    from api.kafka import config
    from api.kafka import kafka
    from api.kafka import kafka_producer
    from api.kafka import kafka_consumer