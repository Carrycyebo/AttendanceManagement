# 从 pyspark.sql 模块导入 SparkSession 类，用于创建和管理 Spark 会话
from pyspark.sql import SparkSession
# 从 pyspark.sql.functions 模块导入 col 和 lit 函数
from pyspark.sql.functions import col, lit
# 从 cf_recommendation 模块导入 generate_user_based_recommendations 函数，用于生成用户基于协同过滤的课程推荐
from cf_recommendation import generate_user_based_recommendations
# 从 absent_analysis 模块导入 get_top_absent_students 和 get_most_absent_students 函数，用于获取缺勤相关信息
from absent_analysis import get_top_absent_students, get_most_absent_students
# 从 predict_service 模块导入 predict_absences_for_all_students 函数，用于预测学生缺勤课程
from predict_service import predict_absences_for_all_students
# 从 mysql_writer 模块导入 write_to_mysql 函数，用于将数据写入 MySQL 数据库
from mysql_writer import write_to_mysql
# 导入 os 模块，用于与操作系统进行交互，设置环境变量等操作
import os

os.environ['JAVA_HOME'] = 'D:\\envs\\java\\jdk1.8.0_441'
os.environ['HADOOP_HOME'] = 'C:\\hadoop-2.8.1'

def main():
    """
    主函数，执行离线数据分析任务，包括:
    1. 协同过滤推荐
    2. 课程缺勤Top5分析
    3. 个性化缺勤推荐
    4. 最常缺勤学生分析
    5. 缺勤课程预测
    并将所有结果写入MySQL数据库
    """
    # 此函数是程序的入口，用于执行离线数据分析任务，包括数据加载、协同过滤推荐、缺勤名单生成和预测，并将结果写入 MySQL 数据库
    # 创建一个 SparkSession 实例，用于与 Spark 集群进行交互
    # 创建SparkSession实例，配置应用名称、运行模式及网络相关参数
    # 使用local[*]模式运行，自动检测可用CPU核心数
    spark = SparkSession.builder \
        .appName("Attendance Offline Analysis") \
        .master('local[*]') \
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .getOrCreate()

    # 定义 schema
    schema = "class STRING, name STRING, course STRING, student_id STRING, score INT, state STRING"

    # 读取 HDFS 上的数据
    # 从HDFS读取TSV格式的原始数据，指定schema结构
    # 数据路径指向HDFS上的attendance_log目录
    df = spark.read \
        .option("delimiter", "\t") \
        .schema(schema) \
        .csv("hdfs://niit01:8020/user/hive/warehouse/attendance_log/*")

    df.cache()  # 缓存提高性能

    print("✅ 数据加载完成，开始分析...")

    # 执行协同过滤推荐任务，为用户生成课程推荐
    # 1. 协同过滤推荐
    print("🔍 正在生成用户课程推荐...")
    # 生成基于协同过滤的用户课程推荐
    # 使用ALS算法分析用户行为模式，预测可能感兴趣的课程
    # 去除重复的学生-课程组合保证结果唯一性
    user_recs = generate_user_based_recommendations(df).dropDuplicates(["student_id", "course"])
    write_to_mysql(user_recs, "user_recommendations")

    # 从数据中筛选出缺勤的学生记录，生成课程缺勤 Top5 名单
    # 2. 课程缺勤 Top5 名单
    print("📊 正在生成课程缺勤 Top5...")
    # 修改后 ✅ 包含所有字段
    # 分析每门课程缺勤最多的前5名学生
    # 基于窗口函数按课程分组计算缺勤排名
    # 确保输出字段与MySQL表结构匹配
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

    # 调用预测函数，预测学生可能缺席的课程
    # 5. 预测每个学生的缺勤课程
    print("🔮 正在预测学生可能缺席的课程...")
    # 预测学生可能缺席的课程
    # 结合协同过滤推荐和实际出勤数据
    # 重命名字段以明确其预测性质
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