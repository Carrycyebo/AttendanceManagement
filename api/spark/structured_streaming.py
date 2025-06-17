import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# 设置HADOOP_HOME环境变量，指定Hadoop的安装路径，这对于Spark在某些情况下与Hadoop相关功能的交互是必要的
# 注意：这里假设Hadoop 2.8.1版本安装在指定路径下，实际使用中需根据真实安装情况调整路径
os.environ['JAVA_HOME'] = 'D:\\envs\\java\\jdk1.8.0_441'
os.environ['HADOOP_HOME'] = 'C:\\hadoop-2.8.1'

def run_structured_streaming():
    # 1- 创建SparkSession
    # SparkSession是与Spark进行交互的入口点，通过配置相关参数来初始化Spark应用
    # 配置了shuffle分区数为1，应用名称为'ss_kafka_push'，并设置运行模式为本地模式（使用所有可用的本地核心）
    spark = SparkSession.builder \
       .config("spark.sql.shuffle.partitions", 1) \
       .appName('ss_kafka_push') \
       .master('local[*]') \
       .config("spark.executor.processTreeMetrics.enabled", "false") \
       .getOrCreate()

    # 2- 读取Kafka数据流
    # 使用Spark的结构化流功能从Kafka读取数据，指定了Kafka服务器的地址（这里的"zhao:9092"需要根据实际Kafka服务器地址进行替换）
    # 通过"subscribePattern"选项订阅了名为"test"的主题（这里支持通配符订阅模式，可以订阅多个符合模式的主题）
    kafka_stream = spark.readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "zxlu:9092") \
       .option("subscribePattern", "attendance") \
       .load()

    # 3- 解析数据
    # 从Kafka读取的原始数据中进行解析操作，先将消息的"value"字段转换为字符串类型，并取了一个"timestamp"字段（可能是消息自带的时间戳或者其他相关时间信息）
    # 然后通过使用F.split函数按照"\t"（制表符）对"value"字段进行分割，提取出不同的列信息，分别命名为"class_id"、"student_name"、"course"、"student_id"、"status"
    # 最后删除原始的"value"字段，因为已经解析出了具体需要的字段
    parsed_stream = kafka_stream.selectExpr("cast(value as string) as value", "timestamp") \
       .withColumn("class_id", F.split(F.col("value"), "\t")[0]) \
       .withColumn("student_name", F.split(F.col("value"), "\t")[1]) \
       .withColumn("course", F.split(F.col("value"), "\t")[2]) \
       .withColumn("student_id", F.split(F.col("value"), "\t")[3]) \
       .withColumn("score", F.split(F.col("value"), "\t")[4]) \
       .withColumn("status", F.split(F.col("value"), "\t")[5]) \
       .drop("value")

    # 4- 使用时间窗口对数据进行分组聚合
    # 4.1 学生出勤和缺勤数量总和
    # 按照时间窗口（每2秒为一个窗口，通过F.window函数实现）和考勤"status"（出勤或缺勤状态）进行分组
    # 然后对每组进行计数，统计每个时间窗口内不同考勤状态的记录数量，最后将计数结果的列名重命名为"count"，并移除时间窗口字段（如果后续不需要该字段展示等情况）
    attendance_summary = parsed_stream.groupBy(
        F.window("timestamp", "2 seconds").alias("time_window"),  # 2秒窗口
        "status"
    ).count() \
       .withColumnRenamed("count", "count") \
       .drop("time_window")  # 移除时间窗口字段

    # 4.2 各个班级号的出勤和缺勤数量
    # 类似地，按照时间窗口、班级"class_id"以及考勤"status"进行分组，统计每个班级在每个时间窗口内不同考勤状态的记录数量
    # 同样对计数结果列重命名并移除时间窗口字段
    class_attendance = parsed_stream.groupBy(
        F.window("timestamp", "2 seconds").alias("time_window"),
        "class_id", "status"
    ).count() \
       .withColumnRenamed("count", "count") \
       .drop("time_window")  # 移除时间窗口字段

    # 4.3 所有课程的数量
    # 按照时间窗口和"course"（课程）进行分组，统计每个时间窗口内出现的不同课程的数量，将计数结果列重命名为"course_count"并移除时间窗口字段
    course_count = parsed_stream.groupBy(
        F.window("timestamp", "2 seconds").alias("time_window"),
        "course"
    ).count() \
       .withColumnRenamed("count", "course_count") \
       .drop("time_window")  # 移除时间窗口字段

    # 5- 将每组统计结果写入Kafka
    def write_to_kafka(batch_df, batch_id, topic_name):
        """
        这个函数用于将给定的数据帧（batch_df）中的数据写入到Kafka中。
        它将数据帧中的所有列转换为JSON格式，作为Kafka消息的"value"部分，"key"部分设置为空字符串（根据实际需求可能需要调整）。
        然后按照指定的Kafka服务器地址和目标主题进行写入操作。
        """
        batch_df.selectExpr(
            "cast(null as string) as key",  # Kafka的key，这里设置为空字符串，可根据实际情况修改
            "to_json(struct(*)) as value"  # 将所有列转换为JSON格式
        ).write \
           .format("kafka") \
           .option("kafka.bootstrap.servers", "zxlu:9092") \
           .option("topic", topic_name) \
           .save()

    # 将每组统计结果写入Kafka topic
    # 对于学生出勤和缺勤数量总和的统计结果，通过foreachBatch操作，在每个批次的数据处理完成后调用write_to_kafka函数将数据写入"attendance_summary"主题
    # 设置输出模式为"update"，表示只输出更新的数据（适合这种聚合统计的场景），并设置触发间隔为每2秒处理一次数据（与前面的时间窗口等时间设置相匹配）
    attendance_summary.writeStream \
       .foreachBatch(lambda df, id: write_to_kafka(df, id, "attendance_summary")) \
       .outputMode("update") \
       .trigger(processingTime="2 seconds") \
       .start()

    # 对于各个班级号的出勤和缺勤数量统计结果，同样的方式写入"class_attendance"主题
    class_attendance.writeStream \
       .foreachBatch(lambda df, id: write_to_kafka(df, id, "class_attendance")) \
       .outputMode("update") \
       .trigger(processingTime="2 seconds") \
       .start()

    # 对于所有课程的数量统计结果，写入"course_count"主题
    course_count.writeStream \
       .foreachBatch(lambda df, id: write_to_kafka(df, id, "course_count")) \
       .outputMode("update") \
       .trigger(processingTime="2 seconds") \
       .start()

    # 等待流任务结束，使程序保持运行状态，持续处理和输出数据，直到手动停止或者遇到异常等情况导致任务终止
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_structured_streaming()