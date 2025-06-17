from pyspark.ml.recommendation import ALS


def generate_user_based_recommendations(df):
    rating_df = df.select("student_id", "course", "score").withColumnRenamed("score", "rating")
    als = ALS(
        maxIter=5,
        regParam=0.01,
        userCol="student_id",
        itemCol="course",
        ratingCol="rating"
    )
    model = als.fit(rating_df)
    recommendations = model.recommendForAllUsers(6)  # 每个用户推荐6门课
    return recommendations


def build_course_similarity_matrix(df):
    from pyspark.sql.functions import col
    from pyspark.ml.feature import StringIndexer
    from pyspark.ml.linalg import Vectors
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.recommendation import ALS

    # 构建用户-课程评分矩阵
    rating_df = df.select("student_id", "course", "score").withColumnRenamed("score", "rating")

    # 使用 ALS 训练模型
    als = ALS(
        maxIter=5,
        regParam=0.01,
        userCol="student_id",
        itemCol="course",
        ratingCol="rating"
    )

    model = als.fit(rating_df)

    # 获取物品（课程）因子向量
    item_factors = model.itemFactors

    # 向量相似度计算（余弦相似度）
    def cosine_similarity(vec1, vec2):
        dot = sum(float(x * y) for x, y in zip(vec1, vec2))
        norm1 = sum(x ** 2 for x in vec1) ** 0.5
        norm2 = sum(y ** 2 for y in vec2) ** 0.5
        return dot / (norm1 * norm2) if norm1 * norm2 != 0 else 0

    similarity_udf = F.udf(lambda v1, v2: cosine_similarity(v1.toArray(), v2.toArray()), FloatType())

    # 自连接计算课程相似度
    course_similarity = item_factors.alias("i1").join(
        item_factors.alias("i2"),
        col("i1.id") < col("i2.id")
    ).select(
        col("i1.id").alias("course1"),
        col("i2.id").alias("course2"),
        similarity_udf(col("i1.features"), col("i2.features")).alias("similarity")
    ).orderBy(col("similarity").desc())

    return course_similarity