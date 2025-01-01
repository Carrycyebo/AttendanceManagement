from flask import Flask
from flask_socketio import SocketIO
from conf import getConf
from api.rounter.page import page_bp
from kafka import KafkaConsumer
import threading
import json
import pymysql
import time

# 获取配置信息
conf = getConf.conf()

# 创建 Flask 应用
app = Flask(__name__, template_folder='web/templates', static_folder='web/static')
app.config['SECRET_KEY'] = 'secret!'

# 创建 SocketIO 实例
socketio = SocketIO(app)

# 注册蓝图
app.register_blueprint(page_bp)

# 创建 Kafka 消费者
def create_kafka_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers="zhao:9092",
        group_id="test",
        auto_offset_reset='latest'
    )

# 消费 Kafka 消息并发送到 WebSocket
def consume_messages(consumer, topic_name, socketio):
    print(f"Starting to consume messages from topic: {topic_name}")
    for message in consumer:
        try:
            data = json.loads(message.value.decode('utf-8'))  # 确保正确解码消息
            socketio.emit(f'{topic_name}_message', {'data': data})
        except json.JSONDecodeError as e:
            print(f"JSON decode error for topic {topic_name}: {e}")
        except Exception as e:
            print(f"Error processing Kafka message from {topic_name}: {e}")

# 后台线程，消费 Kafka 消息并通过 WebSocket 发送到前端
def background_thread(socketio):
    try:
        # 创建消费者实例
        consumer_1 = create_kafka_consumer('attendance_summary')
        consumer_2 = create_kafka_consumer('class_attendance')
        consumer_3 = create_kafka_consumer('course_count')

        # 启动线程消费各个 Kafka topic 消息
        threading.Thread(target=consume_messages, args=(consumer_1, 'attendance_summary', socketio), daemon=True).start()
        threading.Thread(target=consume_messages, args=(consumer_2, 'class_attendance', socketio), daemon=True).start()
        threading.Thread(target=consume_messages, args=(consumer_3, 'course_count', socketio), daemon=True).start()
    except Exception as e:
        print(f"Error in background_thread: {e}")

# 获取 MySQL 连接
def get_mysql_connection():
    return pymysql.connect(
        host='43.140.205.103',  # MySQL 地址
        user='AttendanceManagement',  # 用户名
        password='cen5CjQpeSKxAWSZ',  # 密码
        database='AttendanceManagement',  # 数据库
        charset='utf8mb4'
    )

# 查询 MySQL 中的数据
def query_mysql_data():
    connection = get_mysql_connection()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    # 查询 course_attendance 数据
    course_attendance_query = "SELECT * FROM course_attendance LIMIT 10;"
    cursor.execute(course_attendance_query)
    course_attendance_data = cursor.fetchall()

    # 查询 student_attendance 数据
    student_attendance_query = "SELECT * FROM student_attendance LIMIT 10;"
    cursor.execute(student_attendance_query)
    student_attendance_data = cursor.fetchall()

    # 关闭连接
    cursor.close()
    connection.close()

    return course_attendance_data, student_attendance_data

# 每隔 5 秒查询一次 MySQL 数据并通过 WebSocket 推送到前端
def query_and_push_data():
    while True:
        course_attendance_data, student_attendance_data = query_mysql_data()

        # 推送数据到 WebSocket
        socketio.emit('course_attendance_data', {'data': course_attendance_data})
        socketio.emit('student_attendance_data', {'data': student_attendance_data})

        # 等待 5 秒
        time.sleep(5)

# 启动查询任务
def start_query_task():
    thread = threading.Thread(target=query_and_push_data)
    thread.daemon = True
    thread.start()

# WebSocket 连接事件
@socketio.on('connect')
def handle_connect():
    print("Client connected")
    socketio.emit('connected', {'data': 'You are successfully connected!'})

# WebSocket 断开连接事件
@socketio.on('disconnect')
def handle_disconnect():
    print("Client disconnected")

# 启动 Flask 应用
def start_flask():
    host = conf.get('server', 'host')
    port = conf.getint('server', 'port')
    socketio.run(app, host=host, port=port, debug=False)

if __name__ == "__main__":
    # 启动查询任务
    start_query_task()

    # 启动后台线程监听 Kafka
    thread = threading.Thread(target=background_thread, args=(socketio,))
    thread.daemon = True  # 守护线程，在主线程退出时退出
    thread.start()

    # 启动 Flask 应用
    start_flask()
