import os
import pymysql
# 从 pyspark.sql 模块导入 SparkSession 类，用于创建和管理 Spark 会话
from pyspark.sql import SparkSession
from api.db.util import get_db
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.streaming import DataStreamWriter

# 设置 Hadoop 环境变量 支持spark HDFS 连接
# os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk1.8.0_351'
os.environ['JAVA_HOME'] = 'D:\\envs\\java\\jdk1.8.0_441'
os.environ['HADOOP_HOME'] = 'C:\\hadoop-2.8.1'

#定义写入MySQL的函数
def write_to_mysql(batch_df, batch_id):
    # 将 DataFrame 转换为 Pandas DataFrame 并将数据插入到 MySQL 数据库
    pandas_df = batch_df.toPandas()

    # 连接 MySQL 数据库
    connection = get_db()

    # 创建数据库游标对象
    cursor = connection.cursor()

    # 遍历 Pandas DataFrame 的每一行，插入数据到 MySQL
    for _, row in pandas_df.iterrows():
        course = row['course']
        status = row['status']
        count = row['count']

        # 插入语句
        # 如果该课程记录已存在，更新count
        sql = """
        INSERT INTO course_attendance (course, status, count)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE count = count + VALUES(count)
        """

        # 执行插入语句
        cursor.execute(sql, (course, status, count))
    
    # 提交事务并关闭连接
    connection.commit()
    cursor.close()
    connection.close()

# def write_course_stats(batch_df, batch_id):
#     """
#     写入课程统计数据到新表
#     """
#     # 课程维度统计
#     course_stats = batch_df.groupBy("course", "status")\
#                          .agg(F.sum("count").alias("total_count"))
    
#     connection = get_db()
#     cursor = connection.cursor()
    
#     for row in course_stats.collect():
#         sql = """
#         INSERT INTO course_stats 
#         (course, attendance_count, absence_count)
#         VALUES (%s, %s, %s)
#         ON DUPLICATE KEY UPDATE
#             attendance_count = attendance_count + VALUES(attendance_count),
#             absence_count = absence_count + VALUES(absence_count)
#         """
#         # 根据状态更新不同字段
#         if row['status'] == 'A':
#             cursor.execute(sql, (
#                 row['course'], row['total_count'], 0
#             ))
#         else:
#             cursor.execute(sql, (
#                 row['course'], 0, row['total_count']
#             ))
    
#     connection.commit()
#     cursor.close()
#     connection.close()


def run_spark_sql():
    #启动 Spark Structured Streaming，读取 Kafka 流数据，进行处理后将数据推送到 MySQL。
    # 1- 创建 SparkSession,与spark交互
    # 创建一个 SparkSession 实例，设置应用名称为 'KafkaSparkSQL'，并启用 Hive 支持
    spark = SparkSession.builder \
        .config("spark.sql.shuffle.partitions", 1) \
        .appName('ss_kafka_push_to_mysql') \
        .master('local[*]') \
        .config("spark.executor.processTreeMetrics.enabled", "false") \
        .getOrCreate()

    # 2- 读取 Kafka 数据流
    # 通过 Spark Structured Streaming 读取 Kafka 数据流。
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "zxlu:9092") \
        .option("subscribePattern", "attendance") \
        .load() # 读取数据流

    # 3- 解析Kafka数据并使用英文列名
    parsed_stream = kafka_stream.selectExpr("cast(value as string) as value", "timestamp") \
        .withColumn("class_id", F.split(F.col("value"), "\t")[0]) \
        .withColumn("name", F.split(F.col("value"), "\t")[1]) \
        .withColumn("course", F.split(F.col("value"), "\t")[2]) \
        .withColumn("student_id", F.split(F.col("value"), "\t")[3]) \
        .withColumn("score", F.split(F.col("value"), "\t")[4]) \
        .withColumn("status", F.split(F.col("value"), "\t")[5]) \
        .drop("value")

    # 4- 定义时间窗口，并使用窗口聚合
    # 将数据按课程和状态进行分组，并计算每个时间窗口内每个课程和状态的出勤人数。
    windowed_stream = parsed_stream \
        .groupBy(
            F.window(parsed_stream.timestamp, "2 seconds"),  # 2分钟的时间窗口
            parsed_stream.course,
            parsed_stream.status
        ) \
        .agg(F.count("*").alias("count")) # 计算每组的计数（出勤人数）

    # 5- 使用 foreachBatch 将数据推送到 MySQL
    # 使用 foreachBatch 操作将处理后的数据写入到 MySQL 中。
    windowed_stream.writeStream \
        .foreachBatch(write_to_mysql) \
        .outputMode("update") \
        .trigger(processingTime="2 seconds") \
        .start()  # 启动流任务
        
    # windowed_stream.writeStream \
    # .foreachBatch(write_course_stats) \
    # .outputMode("update") \
    # .trigger(processingTime="2 seconds") \
    # .start()

    # 等待流任务结束
    spark.streams.awaitAnyTermination()

if __name__ == '__main__':
    # 程序入口，当脚本作为主程序运行时执行以下代码
    run_spark_sql()