from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import logging
from logging.handlers import RotatingFileHandler
import requests


# 创建SparkSession
spark = SparkSession.builder.appName("AttendanceStatistics").getOrCreate()

# 创建StreamingContext，设置批处理间隔为2秒
ssc = StreamingContext(spark.sparkContext, 2)

# 从Kafka读取数据
kafkaParams = {"bootstrap.servers": "118.31.166.152:9092", "group.id": "attendance - management"}
lines = KafkaUtils.createDirectStream(ssc, ["your_kafka_topic"], kafkaParams)


# 假设数据格式为：student_id,class_id,course_id,attendance_status
# 出勤状态为1表示出勤，0表示缺勤

# 解析每行数据
def parse_line(line):
    fields = line.split(",")
    return (fields[0], fields[1], fields[2], int(fields[3]))


parsed_lines = lines.map(lambda x: parse_line(x[1]))

# 统计所有学生出勤和缺勤数量总和
total_attendance = parsed_lines.map(lambda x: x[3]).reduce(lambda x, y: x + y)
total_absence = parsed_lines.map(lambda x: 1 - x[3]).reduce(lambda x, y: x + y)

# 统计各个班级的出勤和缺勤数量
class_attendance = parsed_lines.map(lambda x: (x[1], x[3])).reduceByKey(lambda x, y: x + y)
class_absence = parsed_lines.map(lambda x: (x[1], 1 - x[3])).reduceByKey(lambda x, y: x + y)

# 统计所有课程的数量（这里简单统计不同课程的数量）
course_count = parsed_lines.map(lambda x: x[2]).distinct().count()


# 打印统计结果
def print_results(time):
    print("-" * 20)
    print("Time: ", time)
    print("Total Attendance: ", total_attendance)
    print("Total Absence: ", total_absence)
    print("Class Attendance: ", class_attendance.collect())
    print("Class Absence: ", class_absence.collect())
    print("Total Courses: ", course_count)

    # 将统计结果发送到服务器（假设服务器有接收数据的接口）
    data = {
        "total_attendance": total_attendance,
        "total_absence": total_absence,
        "class_attendance": dict(class_attendance.collect()),
        "class_absence": dict(class_absence.collect()),
        "total_courses": course_count
    }
    try:
        response = requests.post("http://0.0.0.0:8080/your_endpoint", json=data)
        print("Data sent to server with status code: ", response.status_code)
    except Exception as e:
        print("Failed to send data to server: ", str(e))


# 配置日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = RotatingFileHandler("logs/app.log", maxBytes=10 * 1024 * 1024, backupCount=3)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


total_attendance.pprint()
total_absence.pprint()
class_attendance.pprint()
class_absence.pprint()
ssc.start()
ssc.awaitTermination()