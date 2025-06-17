# 导入 pymysql 库，用于与 MySQL 数据库建立连接
import pymysql
# 从 pyspark.sql 模块导入 DataFrame 类，用于处理和操作数据框
from pyspark.sql import DataFrame

def write_to_mysql(df: DataFrame, table_name: str):
    # 此函数用于将 Spark DataFrame 写入 MySQL 数据库的指定表中
    # 定义 MySQL 数据库的主机地址
    host = '43.140.205.103'
    # 定义 MySQL 数据库的端口号
    port = 3306
    # 定义连接 MySQL 数据库的用户名
    user = 'AttendanceManagement'
    # 定义连接 MySQL 数据库的密码
    password = 'cen5CjQpeSKxAWSZ'
    # 定义要连接的 MySQL 数据库名
    database = 'AttendanceManagement'

    # 尝试建立与 MySQL 数据库的连接
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset='utf8mb4'
    )
    # 创建一个游标对象，用于执行 SQL 语句
    cursor = conn.cursor()

    try:
        # 尝试执行将数据写入数据库的操作
        # 执行 SQL 语句，获取目标表的所有字段名
        # 自动获取目标表字段名（顺序必须与 DataFrame 一致）
        cursor.execute(f"DESCRIBE {table_name}")
        table_columns = [col[0] for col in cursor.fetchall()]
        df_columns = df.columns

        # 检查目标表的字段数量是否与 DataFrame 的列数量一致
        if len(table_columns) != len(df_columns):
            raise ValueError(f"[ERROR] 表 '{table_name}' 的字段数量不匹配！预期: {len(table_columns)}, 实际: {len(df_columns)}")

        print(f"[INFO] 开始写入表 '{table_name}'，字段匹配成功：{df_columns}")

        # 构建 SQL 插入语句，使用 INSERT IGNORE 避免主键冲突
        # 使用 INSERT IGNORE 避免主键冲突
        placeholders = ','.join(['%s'] * len(df_columns))
        columns_str = ','.join(df_columns)
        sql = f"INSERT IGNORE INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        # 遍历 DataFrame 中的每一行数据
        for row in df.collect():
            try:
                # 尝试执行插入操作
                cursor.execute(sql, tuple(row))
            except Exception as e:
                # 捕获插入过程中可能出现的异常
                print(f"[ERROR] 插入失败：{e}")
                continue

        conn.commit()
        print(f"✅ 已成功写入表：{table_name}")

    finally:
        # 无论操作是否成功，都关闭游标和数据库连接
        cursor.close()
        conn.close()