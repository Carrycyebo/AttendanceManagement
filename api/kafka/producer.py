from api.kafka.util import create_producer, get_producer_topic
import time, random
from kafka import KafkaProducer

## @秋风起  在这个文件下写

# Kafka 配置
# bootstrap_servers = ['118.31.166.152:9092']
# topic = 'test'

# 创建 KafkaProducer 实例
import json
import random
producer = create_producer()
topic = get_producer_topic()

def send_data():
    #状态和课程列表
    state_array = ["A", "L"]  # A: 出勤, L: 缺勤
    course_array = ["Hadoop", "Spark", "Flink", "Hive", "HBase", "Kafka"]
    
    data_batch = []  # List to store the batch of data

    # 在循环中生成5个数据,作为一个记录
    for i in range(5):
        #将出勤概率设置为百分之八十，缺勤为百分之二十
        state = "A" if random.random() < 0.8 else "L"
        score = 100 if state == "A" else 0
        course = random.choice(course_array)

        #从列表中随机选择一名学生
        student = get_random_student()

        if student:  # 检查学生是空还是没有
            data_batch.append('\t'.join([student[0], student[1], course, student[2], str(score), state]))
        else:
            print("生成失败")
    
    return data_batch

def run_producer():
    print("Kafka Producer 启动成功")

    i = 0
    while True:
        #随机生成数据
        data_batch = send_data()

        if data_batch:  #批处理不为空时
            for data in data_batch:
                producer.send(topic, value=data.encode('utf-8'))

            # 输出已经发送的数据
            print(f"Sent Batch {i}: {data_batch}")
        else:
            print(f"Batch {i} is empty. Skipping sending.")

        # 等5s
        time.sleep(5)
        i += 1


def get_random_student():
    # 读取学生信息文件
    try:
        with open('api\kafka\students.json', 'r', encoding='utf-8') as f:
            students = json.load(f)

        # 从学生列表中随机选择一个学生
        return random.choice(students)

    except FileNotFoundError:
        print("学生信息文件未找到，请检查文件路径！")
        return None
    except json.JSONDecodeError:
        print("学生信息文件格式错误！")
        return None