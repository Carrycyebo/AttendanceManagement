from api.kafka.util import create_producer, get_producer_topic
import time, random
from kafka import KafkaProducer

## @秋风起  在这个文件下写

# Kafka 配置
# bootstrap_servers = ['118.31.166.152:9092']
# topic = 'test'

# 创建 KafkaProducer 实例
producer = create_producer()
topic = get_producer_topic()

def send_data(i):
    className = ["210711", "210712", "210713", "210714", "210715", "210716", "210717", "210718", "210719", "210720",
                 "210721"]
    na = random.randint(0, 10)
    studentID = className[na]

    # Name
    names = ['Tank', 'Jack', 'Marry', 'Tom', 'Jesson']
    name_1 = random.randint(0, 4)
    name = names[name_1] + str(name_1)

    # Class
    classs = ['Hadoop', 'Java', 'Python', 'Linux', 'Mysql']
    name_class = random.randint(0, 4)
    className = classs[name_class]

    # Time
    num_class = [12, 34, 56, 78]
    num = num_class[random.randint(0, 3)]
    time_str = str(random.randint(2020, 2021)) + str(random.randint(1, 12)) + str(random.randint(1, 29)) + str(
        random.randint(1, 24)) + str(num)
    status = "A" if i % 13 != 0 else "L"

    return str(studentID) + '\t' + str(name) + '\t' + str(className) + '\t' + str(time_str) + '\t' + str(status) + '\n'

def run_producer():
    # 创建Kafka Producer
    # producer = create_producer()
    # producer = KafkaProducer(bootstrap_servers='zhao:9092')
    
    print("Kafka Producer 启动成功")
    
    i = 0
    while True:
        # 生成随机数据
        data = send_data(i)

        # 发送数据到 Kafka
        producer.send(topic, value=data.encode('utf-8'))

        # 打印发送的数据
        print(f"Sent{i}: {data}")

        # 每 5 秒发送一次
        time.sleep(5)
        i += 1
