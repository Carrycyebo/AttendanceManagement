# 从 pyspark.sql.window 模块导入 Window 类，用于窗口函数操作
from pyspark.sql.window import Window
# 从 pyspark.sql.functions 模块导入所需的函数
from pyspark.sql.functions import count, desc, rank, col

def get_top_absent_students(df):
    # 此函数用于获取每门课程缺勤次数最多的前 5 名学生
    # 过滤出状态为 'L'（缺勤）的记录
    absent_df = df.filter(col("state") == "L") \
        .groupBy("course", "student_id") \
        .agg(count("*").alias("absent_count"))

    # 定义窗口规范，按课程分组，并按缺勤次数降序排序
    window_spec = Window.partitionBy("course").orderBy(desc("absent_count"))
    # 为每个学生在对应课程中的缺勤次数添加排名
    top_absent = absent_df.withColumn("absence_rank", rank().over(window_spec)) \
        .filter(col("absence_rank") <= 5) \
        .select("course", "student_id", "absent_count", col("absence_rank").cast("int"))

    # 返回每门课程缺勤次数最多的前 5 名学生
    return top_absent


def get_most_absent_students(df):
    # 此函数用于获取所有学生中缺勤次数最多的前 3 名学生
    # 统计每个学生总的缺勤次数
    absent_counts = df.filter(col("state") == "L") \
        .groupBy("student_id") \
        .agg(count("*").alias("total_absences")) \
        .orderBy(desc("total_absences")) \
        .limit(3)

    return absent_counts