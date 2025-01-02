import os
import pymysql
from pyspark.sql import SparkSession
from api.db.util import get_db
import pyspark.sql.functions as F

# os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk1.8.0_351'
os.environ['HADOOP_HOME'] = 'C:\\hadoop-2.8.1'

# 将结果写入 MySQL 的函数
def write_to_mysql(batch_df, batch_id):
    # 将 DataFrame 转换为 Pandas DataFrame
    pandas_df = batch_df.toPandas()

    # 连接 MySQL 数据库
    connection = get_db()

    cursor = connection.cursor()

    # 插入数据到 MySQL
    for _, row in pandas_df.iterrows():
        class_id = row['class_id']
        student_id = row['student_id']
        student_name = row['student_name']
        status = row['status']
        count = row['count']

        # 插入语句
        sql = """
        INSERT INTO student_attendance (class_id, student_id, student_name, status, count)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE count = count + VALUES(count)
        """
        cursor.execute(sql, (class_id, student_id, student_name, status, count))
    
    # 提交事务并关闭连接
    connection.commit()
    cursor.close()
    connection.close()

def run_spark_RDD():
    # 1- 创建 SparkSession
    spark = SparkSession.builder \
        .config("spark.sql.shuffle.partitions", 1) \
        .appName('ss_kafka_push_to_mysql') \
        .master('local[*]') \
        .getOrCreate()

    # 2- 读取 Kafka 数据流
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "zhao:9092") \
        .option("subscribePattern", "test") \
        .load()

    # 3- 解析 Kafka 数据
    parsed_stream = kafka_stream.selectExpr("cast(value as string) as value", "timestamp") \
        .withColumn("class_id", F.split(F.col("value"), "\t")[0]) \
        .withColumn("student_name", F.split(F.col("value"), "\t")[1]) \
        .withColumn("course", F.split(F.col("value"), "\t")[2]) \
        .withColumn("student_id", F.split(F.col("value"), "\t")[3]) \
        .withColumn("status", F.split(F.col("value"), "\t")[4]) \
        .drop("value")

    # 4- 数据统计 定义时间窗口
    # 按照 class_id, student_id, student_name, status 分组并统计数量
    attendance_counts = parsed_stream \
        .groupBy(
            F.window(parsed_stream.timestamp, "2 seconds"),
            parsed_stream.class_id,
            parsed_stream.student_id,
            parsed_stream.student_name,
            parsed_stream.status
        ) \
        .count()
    
    # attendance_counts = parsed_stream.groupBy("class_id", "student_id", "student_name", "status") \
    #     .count()

    # 5- 使用 foreachBatch 将数据推送到 MySQL
    attendance_counts.writeStream \
        .foreachBatch(write_to_mysql) \
        .outputMode("update") \
        .trigger(processingTime="2 seconds") \
        .start()

    # 等待流任务结束
    spark.streams.awaitAnyTermination()

if __name__ == '__main__':
    run_spark_RDD()