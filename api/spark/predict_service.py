from pyspark.sql.functions import lit, col

def predict_absences_for_all_students(df, user_recs):
    # 去除已经出勤的学生-课程组合
    attended = df.filter(col("state") == "A").select("student_id", "course").distinct()
    predicted_absences = user_recs.join(attended, on=["student_id", "course"], how="left_anti")

    # 添加预测分数
    predicted_absences = predicted_absences.withColumn("prediction_score", lit(0.8))

    return predicted_absences.limit(15)  # 最多返回 15 条预测结果