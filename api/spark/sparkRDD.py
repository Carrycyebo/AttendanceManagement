import os
import pymysql
from pyspark.sql import SparkSession
from api.db.util import get_db
import pyspark.sql.functions as F

# 设置Hadoop环境变量，指定Hadoop的安装路径，以便后续Spark能够正确调用Hadoop相关功能
# 注意这里如果Java环境有问题也需要类似设置JAVA_HOME，当前代码中注释掉了相关设置，可按需取消注释启用
os.environ['JAVA_HOME'] = 'D:\\envs\\java\\jdk1.8.0_441'
os.environ['HADOOP_HOME'] = 'C:\\hadoop-2.8.1'

# 定义一个函数，用于将Spark DataFrame中的每一批数据写入到MySQL数据库中
def write_to_mysql(batch_df, batch_id):
    """
    将每一批次的DataFrame数据写入到MySQL数据库的函数

    参数:
    batch_df (DataFrame): 当前批次的Spark DataFrame数据，包含要插入到MySQL的数据
    batch_id: 批次的唯一标识（在这个场景中可能不一定会用到具体的值）
    """
    # 将Spark DataFrame转换为Pandas DataFrame，方便后续按行遍历数据进行插入操作
    # Pandas DataFrame提供了类似Python列表那样方便的按行迭代方式，便于和MySQL的插入语句配合
    pandas_df = batch_df.toPandas()
    # 建立与MySQL数据库的连接，指定主机地址、用户名、密码以及要使用的数据库名称
    connection = get_db()
    # 创建游标对象，用于执行SQL语句
    cursor = connection.cursor()

    # 遍历Pandas DataFrame的每一行数据，提取相应的列值，并构造插入语句插入到MySQL数据库中
    for _, row in pandas_df.iterrows():
        class_id = row['class_id']
        student_id = row['student_id']
        student_name = row['student_name']
        status = row['status']
        count = row['count']

        # 构造插入数据到MySQL数据库的SQL语句
        # 这里使用了ON DUPLICATE KEY UPDATE语句，意味着如果插入的数据行在表中已经存在（根据主键或唯一键判断），
        # 那么就更新count字段的值，将原来的值加上新插入的值，实现数据的累计统计功能
        sql = """
        INSERT INTO student_attendance (class_id, student_id, student_name, status, count)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE count = count + VALUES(count)
        """
        # 使用游标执行SQL语句，将对应的数据插入到数据库中，传入的参数是要插入的具体值
        cursor.execute(sql, (class_id, student_id, student_name, status, count))

    # 提交事务，将之前执行的插入/更新操作持久化到数据库中
    connection.commit()
    # 关闭游标，释放相关资源
    cursor.close()
    # 关闭数据库连接，释放连接资源
    connection.close()

def write_comprehensive_stats(batch_df, batch_id):
        """
        写入综合统计数据到新表
        """
        # 学生维度统计
        student_stats = batch_df.groupBy(
            "student_id", 
            "student_name", 
            "class_id", 
            "status"
        ).agg(F.sum("count").alias("total_count"))
        
        # 使用现有get_db()方法写入
        connection = get_db()
        cursor = connection.cursor()
        
        for row in student_stats.collect():
            sql = """
            INSERT INTO student_stats 
            (student_id, student_name, class_id, 
             attendance_count, absence_count, score)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                attendance_count = attendance_count + VALUES(attendance_count),
                absence_count = absence_count + VALUES(absence_count),
                score = score + VALUES(score)  # 添加这行使score也能累加
            """
            # 根据状态更新不同字段
            if row['status'] == 'A':
                cursor.execute(sql, (
                    row['student_id'], row['student_name'], row['class_id'],
                    row['total_count'], 0, row['total_count']*100
                ))
            else:
                cursor.execute(sql, (
                    row['student_id'], row['student_name'], row['class_id'],
                    0, row['total_count'], 0
                ))
        
        connection.commit()
        cursor.close()
        connection.close()


def run_spark_RDD():
    """
    主函数，用于执行整个Spark任务流程，包括创建SparkSession、读取Kafka数据、解析数据、统计数据以及将数据写入到MySQL数据库
    """

    spark = SparkSession.builder \
       .config("spark.sql.shuffle.partitions", 1) \
       .appName('ss_kafka_push_to_mysql') \
       .master('local[*]') \
       .config("spark.executor.processTreeMetrics.enabled", "false") \
       .getOrCreate()


    kafka_stream = spark.readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "zxlu:9092") \
       .option("subscribePattern", "attendance") \
       .load()


    parsed_stream = kafka_stream.selectExpr("cast(value as string) as value", "timestamp") \
       .withColumn("class_id", F.split(F.col("value"), "\t")[0]) \
       .withColumn("student_name", F.split(F.col("value"), "\t")[1]) \
       .withColumn("course", F.split(F.col("value"), "\t")[2]) \
       .withColumn("student_id", F.split(F.col("value"), "\t")[3]) \
       .withColumn("score", F.split(F.col("value"), "\t")[4]) \
       .withColumn("status", F.split(F.col("value"), "\t")[5]) \
       .drop("value")


    attendance_counts = parsed_stream \
       .groupBy(
            F.window(parsed_stream.timestamp, "2 seconds"),
            parsed_stream.class_id,
            parsed_stream.student_id,
            parsed_stream.student_name,
            parsed_stream.status
        ) \
       .count()


    attendance_counts.writeStream \
       .foreachBatch(write_to_mysql) \
       .outputMode("update") \
       .trigger(processingTime="2 seconds") \
       .start()

       # 在原有writeStream后添加新处理
    attendance_counts.writeStream \
       .foreachBatch(write_comprehensive_stats) \
       .outputMode("update") \
       .trigger(processingTime="2 seconds") \
       .start()

    # 让Spark应用程序等待，直到流任务结束（比如手动停止或者出现异常结束等情况），保持程序运行状态，持续处理数据
    spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    run_spark_RDD()