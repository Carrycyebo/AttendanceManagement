import pymysql


def write_to_mysql(df, table_name):
    conn = pymysql.connect(
        host='43.140.205.103',
        port=3306,
        user='AttendanceManagement',
        password='cen5CjQpeSKxAWSZ',
        database='AttendanceManagement',
        charset='utf8mb4'
    )
    cursor = conn.cursor()

    for row in df.collect():
        placeholders = ','.join(['%s'] * len(row))
        sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.execute(sql, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()