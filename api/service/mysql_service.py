from pymysql import connect
import pymysql
from conf import getConf
from api.db.util import get_db

conf = getConf.conf()


def query_mysql_data():
    connection = get_db()
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    course_attendance_query = "SELECT * FROM course_attendance;"
    cursor.execute(course_attendance_query)
    course_attendance_data = cursor.fetchall()

    student_attendance_query = "SELECT * FROM student_attendance;"
    cursor.execute(student_attendance_query)
    student_attendance_data = cursor.fetchall()

    cursor.close()
    connection.close()

    return course_attendance_data, student_attendance_data
