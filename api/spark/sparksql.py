from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SaveMode

def main():
    # 1. 创建 SparkSession
    spark = SparkSession.builder \
        .appName("MISA") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 2. 读取文本文件并转换为 RDD
    tf = spark.sparkContext.textFile("")
    # 将 RDD 转换为包含 PersonObj 字段的 Row RDD
    personRDD = tf.map(lambda line: line.split(",")) \
                  .map(lambda parts: Row(name=parts[0], age=int(parts[1]), height=int(parts[2])))

    # 3. 将 RDD 转换为 DataFrame
    personDF = spark.createDataFrame(personRDD)

    # 4. 显示 DataFrame 内容
    personDF.show()

    # 5. 配置 MySQL 数据库连接
    url = "jdbc:mysql://"
    properties = {
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": "root",
        "password": "123456"
    }

    # 6. 将 DataFrame 写入 MySQL 数据库
    personDF.write \
        .mode(SaveMode.Append) \
        .jdbc(url, "person_table", properties)

    # 7. 停止 SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
