from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from cf_recommendation import generate_user_based_recommendations
from absent_analysis import get_top_absent_students, get_most_absent_students
from predict_service import predict_absences_for_all_students
from mysql_writer import write_to_mysql
import os

os.environ['JAVA_HOME'] = 'D:\\envs\\java\\jdk1.8.0_441'
os.environ['HADOOP_HOME'] = 'C:\\hadoop-2.8.1'

def main():
    spark = SparkSession.builder \
        .appName("Attendance Offline Analysis") \
        .master('local[*]') \
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .getOrCreate()

    # 定义 schema
    schema = "class STRING, name STRING, course STRING, student_id STRING, score INT, state STRING"

    # 读取 HDFS 上的数据
    df = spark.read \
        .option("delimiter", "\t") \
        .schema(schema) \
        .csv("hdfs://niit01:8020/user/hive/warehouse/attendance_log/*")

    df.cache()  # 缓存提高性能

    print("✅ 数据加载完成，开始分析...")

    # 1. 协同过滤推荐
    print("🔍 正在生成用户课程推荐...")
    user_recs = generate_user_based_recommendations(df).dropDuplicates(["student_id", "course"])
    write_to_mysql(user_recs, "user_recommendations")

    # 2. 课程缺勤 Top5 名单
    print("📊 正在生成课程缺勤 Top5...")
    # 修改后 ✅ 包含所有字段
    top_absent_df = get_top_absent_students(df) \
        .dropDuplicates(["course", "student_id"]) \
        .select("course", "student_id", "absent_count", "absence_rank")  # 确保字段顺序和数量匹配
    write_to_mysql(top_absent_df, "top_absent_per_course")

    # 3. 个性化推荐缺勤名单（前6）
    print("📌 正在生成个性化缺勤推荐...")
    personal_absent_df = user_recs.select("student_id", "course") \
        .limit(6) \
        .withColumn("recommendation_score", lit(0.9)) \
        .dropDuplicates(["student_id", "course"])
    write_to_mysql(personal_absent_df, "personal_absent_recommendations")

    # 4. 缺席最多的前3名学生
    print("🚨 正在生成最常缺勤学生名单...")
    most_absent_df = get_most_absent_students(df).dropDuplicates(["student_id"])
    write_to_mysql(most_absent_df, "most_absent_students")

    # 5. 预测每个学生的缺勤课程
    print("🔮 正在预测学生可能缺席的课程...")
    predicted_absences_df = predict_absences_for_all_students(df, user_recs) \
        .dropDuplicates(["student_id", "course"]) \
        .withColumnRenamed("course", "predicted_course") \
        .withColumn("prediction_score", lit(0.8)) \
        .select("student_id", "predicted_course", "prediction_score")  # 显式指定字段顺序
    write_to_mysql(predicted_absences_df, "student_absence_predictions")

    print("🎉 所有任务完成，结果已写入 MySQL！")
    spark.stop()


if __name__ == "__main__":
    main()