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
    # State and Course List
    state_array = ["A", "L"]  # A: 出勤, L: 缺勤
    course_array = ["Hadoop", "Spark", "Flink", "Hive", "HBase", "Kafka"]
    
    data_batch = []  # List to store the batch of data

    # Generate 5 records in a batch
    for i in range(5):
        # Weighted random choice for state, "A" has higher probability than "L"
        state = "A" if random.random() < 0.8 else "L"  # 80% chance for "A" and 20% for "L"
        
        course = random.choice(course_array)

        # Randomly choose a student from the list
        student = get_random_student()

        if student:  # Check if student is not None or empty
            # Format data as required
            data_batch.append('\t'.join([student[0], student[1], course, student[2], state]))
        else:
            print("Failed to get random student, skipping record.")
    
    return data_batch

def run_producer():
    print("Kafka Producer 启动成功")

    i = 0
    while True:
        # Generate a batch of 5 random data records
        data_batch = send_data()

        if data_batch:  # Only send data if the batch is not empty
            # Send the batch to Kafka
            for data in data_batch:
                producer.send(topic, value=data.encode('utf-8'))

            # Print sent data
            print(f"Sent Batch {i}: {data_batch}")
        else:
            print(f"Batch {i} is empty. Skipping sending.")

        # Sleep before sending the next batch of data
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