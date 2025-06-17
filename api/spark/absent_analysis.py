from pyspark.sql.window import Window
from pyspark.sql.functions import count, desc, rank, col

def get_top_absent_students(df):
    absent_df = df.filter(col("state") == "L") \
        .groupBy("course", "student_id") \
        .agg(count("*").alias("absent_count"))

    window_spec = Window.partitionBy("course").orderBy(desc("absent_count"))
    top_absent = absent_df.withColumn("absence_rank", rank().over(window_spec)) \
        .filter(col("absence_rank") <= 5) \
        .select("course", "student_id", "absent_count", col("absence_rank").cast("int"))

    return top_absent


def get_most_absent_students(df):
    # 统计每个学生总的缺勤次数
    absent_counts = df.filter(col("state") == "L") \
        .groupBy("student_id") \
        .agg(count("*").alias("total_absences")) \
        .orderBy(desc("total_absences")) \
        .limit(3)

    return absent_counts