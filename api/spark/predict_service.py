# 从 pyspark.sql.functions 模块导入 lit 和 col 函数，lit 用于创建常量列，col 用于引用列
from pyspark.sql.functions import lit, col

def predict_absences_for_all_students(df, user_recs):
    # 此函数用于预测所有学生可能缺席的课程，结合用户推荐和实际出勤数据
    # 从输入的数据框 df 中筛选出状态为 'A'（出勤）的记录，提取学生 ID 和课程列，并去重
    # 去除已经出勤的学生-课程组合
    attended = df.filter(col("state") == "A").select("student_id", "course").distinct()
    # 使用左反连接，从用户推荐数据中去除已经出勤的学生-课程组合，得到可能缺席的组合
    predicted_absences = user_recs.join(attended, on=["student_id", "course"], how="left_anti")

    # 为可能缺席的组合添加一个常量列 'prediction_score'，值为 0.8
    # 添加预测分数
    predicted_absences = predicted_absences.withColumn("prediction_score", lit(0.8))

    # 返回最多 15 条可能缺席的预测结果
    return predicted_absences.limit(15)  # 最多返回 15 条预测结果