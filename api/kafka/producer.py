from apiutil import create_producer, close_producer
import time,random


## @秋风起  在这个文件下写

def run_producer():
    # 创建Kafka Producer
    producer = create_producer()
    # producer = KafkaProducer(bootstrap_servers='zhao:9092')
    import time
    import random
    from kafka import KafkaProducer

    # Kafka 配置
    bootstrap_servers = ['localhost:9092']
    topic = 'attendance'

    # 创建 KafkaProducer 实例
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: str(v).encode('utf-8'))

    # 随机数据生成器
    classes = ["101", "102", "103", "104", "105"]
    student_names = ["张三", "李四", "王五", "赵六", "钱七"]
    courses = ["数学", "英语", "物理", "化学", "生物"]
    attendance_status = ["L", "A"]  # L: 缺勤, A: 出勤

    def generate_random_data():
        class_id = random.choice(classes)
        student_name = random.choice(student_names)
        course_name = random.choice(courses)
        student_id = f"S{random.randint(1000, 9999)}"  # 学号 S+4位随机数
        status = random.choice(attendance_status)

        # 格式化数据，用制表符 \t 分隔
        data = f"{class_id}\t{student_name}\t{course_name}\t{student_id}\t{status}"
        return data

    def send_data():
        while True:
            # 生成随机数据
            data = generate_random_data()

            # 发送数据到 Kafka
            producer.send(topic, value=data)

            # 打印发送的数据
            print(f"Sent: {data}")

            # 每 5 秒发送一次
            time.sleep(5)

    if __name__ == "__main__":
        send_data()

    for line in range(1, 10000000):
        gender = random.randint(0, 2)
        num = random.randint(1, 10)
        genders = str(gender) + ',' + str(num)
        print('生产的数据为:' + genders)
        time.sleep(1)
        producer.send('test', genders.encode('utf8'))
