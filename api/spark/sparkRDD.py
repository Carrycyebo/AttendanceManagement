from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType

# 创建SparkSession，这是在新版本中推荐用于处理数据的入口
spark = SparkSession.builder.appName("AttendanceCount").master("local[*]").getOrCreate()

# Kafka相关配置参数，根据实际情况修改
kafka_bootstrap_servers = "118.31.166.152:9092"
kafka_topic = "students_topic"

# 定义读取的Kafka消息的数据格式（示例中假设消息是简单的文本，按逗号分隔的内容，如果实际是JSON等格式需要相应调整结构定义）
schema = StructType([
    StructField("class_number", StringType(), True),
    StructField("other_field_1", StringType(), True),  # 根据实际消息字段补充完整
    StructField("attendance_status", StringType(), True)
])

# 从Kafka读取数据创建DataFrame
df = spark.readStream \
         .format("kafka") \
         .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
         .option("subscribe", kafka_topic) \
         .load()

# 提取消息中的value部分，并转换为字符串类型（假设消息value是文本），然后按照定义的格式解析为DataFrame结构
df_value = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

# 后续处理可以继续使用DataFrame的操作，比如进行类似之前的转换等
# 以下示例简单将出勤情况转换为数字标记（出勤为1，缺勤为0），并创建临时视图用于后续SQL风格的聚合查询
df_transformed = df_value.withColumn("attendance_status", when(col("attendance_status") == "出勤", 1).otherwise(0))
df_transformed.createTempView("attendance_view")

# 进行聚合查询（示例，按班级号聚合统计出勤人数和缺勤人数，这里使用SQL风格操作，也可以继续使用DataFrame API操作）
result_df = spark.sql("""
    SELECT class_number,
           SUM(CASE WHEN attendance_status = 1 THEN 1 ELSE 0 END) AS attendance_count,
           SUM(CASE WHEN attendance_status = 0 THEN 1 ELSE 0 END) AS absence_count
    FROM attendance_view
    GROUP BY class_number
""")

# 启动流式查询并输出结果（以下简单打印到控制台，可以根据需求配置输出到文件等其他地方）
query = result_df.writeStream \
               .outputMode("complete") \
               .format("console") \
               .start()

query.awaitTermination()