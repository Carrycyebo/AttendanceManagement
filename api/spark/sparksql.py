import os
import pymysql
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.streaming import DataStreamWriter

# os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk1.8.0_351'
os.environ['HADOOP_HOME'] = 'C:\\hadoop-2.8.1'

def write_to_mysql(batch_df, batch_id):
    # 将 DataFrame 转换为 Pandas DataFrame
    pandas_df = batch_df.toPandas()

    # 连接 MySQL 数据库
    connection = pymysql.connect(
        host='43.140.205.103',
        user='AttendanceManagement',
        password='cen5CjQpeSKxAWSZ',
        database='AttendanceManagement'
    )

    cursor = connection.cursor()

    # 插入数据到 MySQL
    for _, row in pandas_df.iterrows():
        course = row['course']
        status = row['status']
        count = row['count']

        # 插入语句
        sql = """
        INSERT INTO course_attendance (course, status, count)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE count = count + VALUES(count)
        """

        cursor.execute(sql, (course, status, count))
    
    # 提交事务并关闭连接
    connection.commit()
    cursor.close()
    connection.close()

def run_spark_sql():
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

    # 3- 解析数据并使用英文列名
    parsed_stream = kafka_stream.selectExpr("cast(value as string) as value", "timestamp") \
        .withColumn("class_id", F.split(F.col("value"), "\t")[0]) \
        .withColumn("name", F.split(F.col("value"), "\t")[1]) \
        .withColumn("course", F.split(F.col("value"), "\t")[2]) \
        .withColumn("student_id", F.split(F.col("value"), "\t")[3]) \
        .withColumn("status", F.split(F.col("value"), "\t")[4]) \
        .drop("value")

    # 4- 定义时间窗口，并使用窗口聚合
    windowed_stream = parsed_stream \
        .groupBy(
            F.window(parsed_stream.timestamp, "2 seconds"),  # 2分钟的时间窗口
            parsed_stream.course,
            parsed_stream.status
        ) \
        .agg(F.count("*").alias("count"))

    # 5- 使用 foreachBatch 将数据推送到 MySQL
    windowed_stream.writeStream \
        .foreachBatch(write_to_mysql) \
        .outputMode("update") \
        .trigger(processingTime="2 seconds") \
        .start()

    # 等待流任务结束
    spark.streams.awaitAnyTermination()

if __name__ == '__main__':
    run_spark_sql()