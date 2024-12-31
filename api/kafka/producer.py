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
    # List of students with their IDs
    students = [
        ["220713", "James", "202234071301"],
        ["220713", "Mary", "202234071302"],
        ["220713", "John", "202234071303"],
        ["220713", "Patricia", "202234071304"],
        ["220713", "Robert", "202234071305"],
        ["220713", "Jennifer", "202234071306"],
        ["220713", "Michael", "202234071307"],
        ["220713", "Linda", "202234071308"],
        ["220713", "William", "202234071309"],
        ["220713", "Elizabeth", "202234071310"],
        ["220713", "David", "202234071311"],
        ["220713", "Barbara", "202234071312"],
        ["220713", "Richard", "202234071313"],
        ["220713", "Susan", "202234071314"],
        ["220713", "Joseph", "202234071315"],
        ["220713", "Jessica", "202234071316"],
        ["220713", "Thomas", "202234071317"],
        ["220713", "Sarah", "202234071318"],
        ["220713", "Charles", "202234071319"],
        ["220713", "Karen", "202234071320"],
        ["220714", "Christopher", "202234071401"],
        ["220714", "Nancy", "202234071402"],
        ["220714", "Daniel", "202234071403"],
        ["220714", "Lisa", "202234071404"],
        ["220714", "Matthew", "202234071405"],
        ["220714", "Margaret", "202234071406"],
        ["220714", "Anthony", "202234071407"],
        ["220714", "Betty", "202234071408"],
        ["220714", "Mark", "202234071409"],
        ["220714", "Sandra", "202234071410"],
        ["220714", "Paul", "202234071411"],
        ["220714", "Ashley", "202234071412"],
        ["220714", "Steven", "202234071413"],
        ["220714", "Dorothy", "202234071414"],
        ["220714", "Andrew", "202234071415"],
        ["220714", "Kimberly", "202234071416"],
        ["220714", "Kenneth", "202234071417"],
        ["220714", "Emily", "202234071418"],
        ["220714", "Joshua", "202234071419"],
        ["220714", "Donna", "202234071420"],
        ["220715", "Kevin", "202234071501"],
        ["220715", "Michelle", "202234071502"],
        ["220715", "Brian", "202234071503"],
        ["220715", "Carol", "202234071504"],
        ["220715", "George", "202234071505"],
        ["220715", "Amanda", "202234071506"],
        ["220715", "Edward", "202234071507"],
        ["220715", "Melissa", "202234071508"],
        ["220715", "Ronald", "202234071509"],
        ["220715", "Deborah", "202234071510"],
        ["220715", "Timothy", "202234071511"],
        ["220715", "Stephanie", "202234071512"],
        ["220715", "Jason", "202234071513"],
        ["220715", "Rebecca", "202234071514"],
        ["220715", "Jeffrey", "202234071515"],
        ["220715", "Laura", "202234071516"],
        ["220715", "Ryan", "202234071517"],
        ["220715", "Helen", "202234071518"],
        ["220715", "Jacob", "202234071519"],
        ["220715", "Sharon", "202234071520"],
        ["220716", "Anna", "202234071601"],
        ["220716", "Paul", "202234071602"],
        ["220716", "Sandra", "202234071603"],
        ["220716", "Jacob", "202234071604"],
        ["220716", "Hannah", "202234071605"],
        ["220716", "David", "202234071606"],
        ["220716", "Sophia", "202234071607"],
        ["220716", "Emily", "202234071608"],
        ["220716", "Joshua", "202234071609"],
        ["220716", "Ella", "202234071610"],
        ["220716", "Benjamin", "202234071611"],
        ["220716", "Lucas", "202234071612"],
        ["220716", "Liam", "202234071613"],
        ["220716", "Amelia", "202234071614"],
        ["220716", "James", "202234071615"],
        ["220716", "Charlotte", "202234071616"],
        ["220716", "William", "202234071617"],
        ["220716", "Oliver", "202234071618"],
        ["220716", "Mia", "202234071619"],
        ["220716", "Sophia", "202234071620"]
    ]
    
    # Select a random student from the list
    return random.choice(students)