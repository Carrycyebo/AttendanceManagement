import time
import threading
from api.service.mysql_service import query_mysql_data
from api.service.kafka_service import create_kafka_consumer, consume_messages

def query_and_push_data(socketio):
    while True:
        course_attendance_data, student_attendance_data = query_mysql_data()
        socketio.emit('course_attendance_data', {'data': course_attendance_data})
        socketio.emit('student_attendance_data', {'data': student_attendance_data})
        time.sleep(10)

def background_thread(socketio):
    try:
        consumer_1 = create_kafka_consumer('attendance_summary')
        consumer_2 = create_kafka_consumer('class_attendance')
        consumer_3 = create_kafka_consumer('course_count')

        threading.Thread(target=consume_messages, args=(consumer_1, 'attendance_summary', socketio), daemon=True).start()
        threading.Thread(target=consume_messages, args=(consumer_2, 'class_attendance', socketio), daemon=True).start()
        threading.Thread(target=consume_messages, args=(consumer_3, 'course_count', socketio), daemon=True).start()
    except Exception as e:
        print(f"Error in background_thread: {e}")

def start_services(socketio):
    threading.Thread(target=query_and_push_data, args=(socketio,), daemon=True).start()
    threading.Thread(target=background_thread, args=(socketio,), daemon=True).start()
