import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk1.8.0_351'
os.environ['HADOOP_HOME'] = 'C:\\hadoop-2.8.1'

if __name__ == '__main__':
    # 1- 创建 SparkSession
    spark = SparkSession.builder \
        .config("spark.sql.shuffle.partitions", 1) \
        .appName('ss_kafka_push') \
        .master('local[*]') \
        .getOrCreate()

    # 2- 读取 Kafka 数据流
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "zhao:9092") \
        .option("subscribePattern", "test") \
        .load()

    # 3- 解析数据
    parsed_stream = kafka_stream.selectExpr("cast(value as string) as value", "timestamp") \
        .withColumn("班级号", F.split(F.col("value"), "\t")[0]) \
        .withColumn("姓名", F.split(F.col("value"), "\t")[1]) \
        .withColumn("课程", F.split(F.col("value"), "\t")[2]) \
        .withColumn("学号", F.split(F.col("value"), "\t")[3]) \
        .withColumn("状态", F.split(F.col("value"), "\t")[4]) \
        .drop("value")

    # 4- 使用时间窗口对数据进行分组聚合
    # 4.1 学生出勤和缺勤数量总和
    attendance_summary = parsed_stream.groupBy(
        F.window("timestamp", "2 seconds").alias("时间窗口"),  # 2 秒窗口
        "状态"
    ).count() \
        .withColumnRenamed("count", "数量") \
        .select("时间窗口", "状态", "数量")

    # 4.2 各个班级号的出勤和缺勤数量
    class_attendance = parsed_stream.groupBy(
        F.window("timestamp", "2 seconds").alias("时间窗口"),
        "班级号", "状态"
    ).count() \
        .withColumnRenamed("count", "数量") \
        .select("时间窗口", "班级号", "状态", "数量")

    # 4.3 所有课程的数量
    course_count = parsed_stream.groupBy(
        F.window("timestamp", "2 seconds").alias("时间窗口"),
        "课程"
    ).count() \
        .withColumnRenamed("count", "课程数量") \
        .select("时间窗口", "课程", "课程数量")

    # 5- 将每组统计结果写入 Kafka
    def write_to_kafka(batch_df, batch_id, topic_name):
        batch_df.selectExpr(
            "cast(null as string) as key",  # Kafka 的 key
            "to_json(struct(*)) as value"  # 将所有列转换为 JSON 格式
        ).write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "zhao:9092") \
            .option("topic", topic_name) \
            .save()

    # 将每组统计结果写入 Kafka topic
    attendance_summary.writeStream \
        .foreachBatch(lambda df, id: write_to_kafka(df, id, "attendance_summary")) \
        .outputMode("update") \
        .trigger(processingTime="2 seconds") \
        .start()

    class_attendance.writeStream \
        .foreachBatch(lambda df, id: write_to_kafka(df, id, "class_attendance")) \
        .outputMode("update") \
        .trigger(processingTime="2 seconds") \
        .start()

    course_count.writeStream \
        .foreachBatch(lambda df, id: write_to_kafka(df, id, "course_count")) \
        .outputMode("update") \
        .trigger(processingTime="2 seconds") \
        .start()

    # 等待流任务结束
    spark.streams.awaitAnyTermination()
