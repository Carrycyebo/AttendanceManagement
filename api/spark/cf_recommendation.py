from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, explode

def generate_user_based_recommendations(df):
    # 只保留需要的列，并重命名 score 为 rating
    rating_df = df.select("student_id", "course", "score").withColumnRenamed("score", "rating")

    # Step 1: 映射 student_id -> user_id_numeric
    user_indexer = StringIndexer(inputCol="student_id", outputCol="user_id_numeric")
    indexed_by_user = user_indexer.fit(rating_df).transform(rating_df)

    # Step 2: 映射 course -> course_id_numeric
    course_indexer = StringIndexer(inputCol="course", outputCol="course_id_numeric")
    indexed_rating_df = course_indexer.fit(indexed_by_user).transform(indexed_by_user)

    # 构建 ALS 模型
    als = ALS(
        maxIter=5,
        regParam=0.01,
        userCol="user_id_numeric",
        itemCol="course_id_numeric",
        ratingCol="rating"
    )

    model = als.fit(indexed_rating_df)

    # 获取所有用户的推荐
    recommendations = model.recommendForAllUsers(5)  # 每个用户推荐5门课

    # 展开推荐结果
    exploded = recommendations.withColumn("exploded", explode("recommendations")) \
        .select("user_id_numeric", col("exploded.course_id_numeric").alias("course_id_numeric"), col("exploded.rating").alias("rating"))

    # Step 3: 映射回原始 course 名称
    course_mapping = indexed_rating_df.select("course", "course_id_numeric").dropDuplicates()

    # 合并 course 映射
    final_recs_with_course = exploded.join(course_mapping, on="course_id_numeric", how="inner")

    # Step 4: 映射回原始 student_id
    user_mapping = indexed_rating_df.select("student_id", "user_id_numeric").dropDuplicates()

    # 合并 student 映射
    final_recs = final_recs_with_course.join(user_mapping, on="user_id_numeric", how="inner") \
        .select("student_id", "course", "rating")

    return final_recs