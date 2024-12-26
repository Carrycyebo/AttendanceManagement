from pyspark import SparkConf, SparkContext

# 创建Spark配置和上下文
conf = SparkConf().setAppName("AttendanceCount").setMaster("local[*]")  # 根据实际情况调整master配置，local[*]表示本地多线程模式
sc = SparkContext(conf=conf)

# 读取数据文件创建RDD，这里假设数据文件名为student_attendance.txt，且与代码在同一目录下，根据实际情况调整
data_rdd = sc.textFile("student_attendance.txt")

# 对每行数据进行处理，提取班级号和出勤情况，映射为(班级号, (出勤数, 缺勤数))的形式
mapped_rdd = data_rdd.map(lambda line: line.split(",")).map(lambda fields: (fields[0], 1 if fields[2].strip() == "出勤" else 0)) \
   .map(lambda x: (x[0], (x[1], 1 - x[1])))

# 按班级号进行聚合，将同一个班级的出勤数和缺勤数分别累加
reduced_rdd = mapped_rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# 收集结果并打印
results = reduced_rdd.collect()
for result in results:
    class_number = result[0]
    attendance_count = result[1][0]
    absence_count = result[1][1]
    print(f"班级号: {class_number}，出勤人数: {attendance_count}，缺勤人数: {absence_count}")

# 关闭Spark上下文
sc.stop()