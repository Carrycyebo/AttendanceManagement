from pyspark.sql import functions as F
from pyspark.sql.window import Window


def get_top_absent_students(df):
    absent_df = df.filter(df.state == 'L') \
        .groupBy("course", "student_id") \
        .agg(F.count("*").alias("absent_count"))

    window_spec = Window.partitionBy("course").orderBy(F.desc("absent_count"))
    
    top_absent = absent_df.withColumn("absence_rank", F.rank().over(window_spec)) \
        .filter(F.col("absence_rank") <= 5)

    return top_absent.select("course", "student_id", "absent_count", "absence_rank")