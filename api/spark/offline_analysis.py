from pyspark.sql import SparkSession
from cf_recommendation import generate_user_based_recommendations, build_course_similarity_matrix
from top_absent import get_top_absent_students
from mysql_writer import write_to_mysql


def main():
    spark = SparkSession.builder \
        .appName("Attendance Offline Analysis") \
        .enableHiveSupport() \
        .getOrCreate()

    # 读取 Hive 表数据
    df = spark.sql("SELECT * FROM default.attendance_log")

    # 1. 用户-课程评分矩阵 & User-Based CF 推荐
    print("生成用户推荐列表...")
    user_recommendations = generate_user_based_recommendations(df)

    # 2. 课程缺勤 Top5 名单
    print("生成课程缺勤 Top5...")
    top_absent_per_course = get_top_absent_students(df)

    # 3. 个性化推荐缺勤学生名单（基于 CF）
    print("生成个性化缺勤推荐...")
    personal_absent_recs = user_recommendations.selectExpr("student_id", "explode(recommendations) as (course, rating)")

    # 4. 写入 MySQL
    print("写入 MySQL...")
    write_to_mysql(top_absent_per_course, "top_absent_per_course")
    write_to_mysql(personal_absent_recs, "personal_absent_recommendations")

    spark.stop()


if __name__ == "__main__":
    main()