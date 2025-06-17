import pymysql
from pyspark.sql import DataFrame

def write_to_mysql(df: DataFrame, table_name: str):
    host = '43.140.205.103'
    port = 3306
    user = 'AttendanceManagement'
    password = 'cen5CjQpeSKxAWSZ'
    database = 'AttendanceManagement'

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset='utf8mb4'
    )
    cursor = conn.cursor()

    try:
        # 自动获取目标表字段名（顺序必须与 DataFrame 一致）
        cursor.execute(f"DESCRIBE {table_name}")
        table_columns = [col[0] for col in cursor.fetchall()]
        df_columns = df.columns

        if len(table_columns) != len(df_columns):
            raise ValueError(f"[ERROR] 表 '{table_name}' 的字段数量不匹配！预期: {len(table_columns)}, 实际: {len(df_columns)}")

        print(f"[INFO] 开始写入表 '{table_name}'，字段匹配成功：{df_columns}")

        # 使用 INSERT IGNORE 避免主键冲突
        placeholders = ','.join(['%s'] * len(df_columns))
        columns_str = ','.join(df_columns)
        sql = f"INSERT IGNORE INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        for row in df.collect():
            try:
                cursor.execute(sql, tuple(row))
            except Exception as e:
                print(f"[ERROR] 插入失败：{e}")
                continue

        conn.commit()
        print(f"✅ 已成功写入表：{table_name}")

    finally:
        cursor.close()
        conn.close()