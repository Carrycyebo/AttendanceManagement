from kafka import KafkaProducer
import time,random
from util import create_producer, close_producer

## @秋风起  在这个文件下写

def run_producer():
    # 创建Kafka Producer
    # producer = create_producer()
    producer = KafkaProducer(bootstrap_servers='zhao:9092')

    # 发送消息
    for line in range(1, 10000000):
        gender = random.randint(0, 2)
        num = random.randint(1, 10)
        genders = str(gender) + ',' + str(num)
        print('生产的数据为:' + genders)
        time.sleep(1)
        producer.send('test', genders.encode('utf8'))

run_producer()