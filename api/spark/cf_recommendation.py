# 从 pyspark.ml.feature 模块导入 StringIndexer 类，用于将字符串类型的列转换为数值类型
from pyspark.ml.feature import StringIndexer
# 从 pyspark.ml.recommendation 模块导入 ALS 类，用于构建协同过滤推荐模型
from pyspark.ml.recommendation import ALS
# 从 pyspark.sql.functions 模块导入 col 和 explode 函数
from pyspark.sql.functions import col, explode

def generate_user_based_recommendations(df):
    # 此函数用于基于用户的协同过滤算法生成课程推荐
    # 从输入的数据框中选择需要的列，并将 'score' 列重命名为 'rating'，用于后续的推荐模型训练
    # 只保留需要的列，并重命名 score 为 rating
    rating_df = df.select("student_id", "course", "score").withColumnRenamed("score", "rating")

    # 步骤 1：将字符串类型的 'student_id' 列转换为数值类型的 'user_id_numeric' 列，以便模型处理
    # Step 1: 映射 student_id -> user_id_numeric
    user_indexer = StringIndexer(inputCol="student_id", outputCol="user_id_numeric")
    indexed_by_user = user_indexer.fit(rating_df).transform(rating_df)

    # 步骤 2：将字符串类型的 'course' 列转换为数值类型的 'course_id_numeric' 列，以便模型处理
    # Step 2: 映射 course -> course_id_numeric
    course_indexer = StringIndexer(inputCol="course", outputCol="course_id_numeric")
    indexed_rating_df = course_indexer.fit(indexed_by_user).transform(indexed_by_user)

    # 使用 ALS（交替最小二乘法）算法构建协同过滤推荐模型
    # 构建 ALS 模型
    als = ALS(
        maxIter=5,
        regParam=0.01,
        userCol="user_id_numeric",
        itemCol="course_id_numeric",
        ratingCol="rating"
    )

    model = als.fit(indexed_rating_df)

    # 调用训练好的模型，为所有用户生成 5 门课程的推荐
    # 获取所有用户的推荐
    recommendations = model.recommendForAllUsers(5)  # 每个用户推荐5门课

    # 将每个用户的推荐结果展开，方便后续处理
    # 展开推荐结果
    exploded = recommendations.withColumn("exploded", explode("recommendations")) \
        .select("user_id_numeric", col("exploded.course_id_numeric").alias("course_id_numeric"), col("exploded.rating").alias("rating"))

    # 步骤 3：将数值类型的 'course_id_numeric' 列映射回原始的字符串类型的 'course' 名称
    # Step 3: 映射回原始 course 名称
    course_mapping = indexed_rating_df.select("course", "course_id_numeric").dropDuplicates()

    # 合并 course 映射
    final_recs_with_course = exploded.join(course_mapping, on="course_id_numeric", how="inner")

    # 步骤 4：将数值类型的 'user_id_numeric' 列映射回原始的字符串类型的 'student_id'
    # Step 4: 映射回原始 student_id
    user_mapping = indexed_rating_df.select("student_id", "user_id_numeric").dropDuplicates()

    # 合并 student 映射
    final_recs = final_recs_with_course.join(user_mapping, on="user_id_numeric", how="inner") \
        .select("student_id", "course", "rating")

    return final_recs