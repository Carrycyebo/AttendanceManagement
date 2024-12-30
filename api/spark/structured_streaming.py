import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk1.8.0_351'
os.environ['HADOOP_HOME'] = 'C:\\hadoop-2.8.1'

if __name__ == '__main__':
    # 1- 创建 SparkSession 对象
    spark = SparkSession.builder \
        .config("spark.sql.shuffle.partitions", 1) \
        .appName('ss_read_kafka_process') \
        .master('local[*]') \
        .getOrCreate()

    # 2- 读取 Kafka 数据流
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "zhao:9092") \
        .option("subscribePattern", "test") \
        .load()

    # 3- 解析数据
    parsed_stream = kafka_stream.selectExpr("cast(value as string) as value") \
        .withColumn("班级号", F.split(F.col("value"), "\t")[0]) \
        .withColumn("姓名", F.split(F.col("value"), "\t")[1]) \
        .withColumn("课程", F.split(F.col("value"), "\t")[2]) \
        .withColumn("学号", F.split(F.col("value"), "\t")[3]) \
        .withColumn("状态", F.split(F.col("value"), "\t")[4]) \
        .drop("value")

    # 4- 统计逻辑

    # 4.1 实时统计所有学生出勤和缺勤数量总和
    attendance_summary = parsed_stream.groupBy("状态") \
        .count() \
        .withColumnRenamed("count", "数量")

    # 4.2 实时统计各个班级号的出勤和缺勤数量
    class_attendance = parsed_stream.groupBy("班级号", "状态") \
        .count() \
        .withColumnRenamed("count", "数量")

    # 4.3 实时统计所有课程的数量
    course_count = parsed_stream.groupBy("课程") \
        .count() \
        .withColumnRenamed("count", "课程数量")

    # 5- 输出流
    attendance_query = attendance_summary.writeStream \
        .outputMode("complete") \
        .format("console") \
        .trigger(processingTime="2 seconds") \
        .option("truncate", "false") \
        .start()

    class_query = class_attendance.writeStream \
        .outputMode("complete") \
        .format("console") \
        .trigger(processingTime="2 seconds") \
        .option("truncate", "false") \
        .start()

    course_query = course_count.writeStream \
        .outputMode("complete") \
        .format("console") \
        .trigger(processingTime="2 seconds") \
        .option("truncate", "false") \
        .start()

    # 6- 等待终止
    spark.streams.awaitAnyTermination()
