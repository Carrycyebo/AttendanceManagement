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

def get_most_absent_students():
    connection = get_db()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    query = "SELECT * FROM most_absent_students ORDER BY total_absences DESC LIMIT 3;"
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

def get_personal_absent_recommendations():
    connection = get_db()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    query = "SELECT * FROM personal_absent_recommendations LIMIT 6;"
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

def get_student_absence_predictions():
    connection = get_db()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    query = "SELECT * FROM student_absence_predictions;"
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

def get_top_absent_per_course():
    connection = get_db()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    query = "SELECT * FROM top_absent_per_course WHERE absence_rank <= 5;"
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

def get_user_recommendations():
    connection = get_db()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    query = "SELECT * FROM user_recommendations;"
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

def check_user_credentials(username, password):
    connection = get_db()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    query = "SELECT * FROM users WHERE username = %s AND password = %s"
    cursor.execute(query, (username, password))
    user = cursor.fetchone()
    cursor.close()
    connection.close()
    return user is not None

def create_user(username, password):
    connection = get_db()
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    try:
        query = "INSERT INTO users (username, password) VALUES (%s, %s)"
        cursor.execute(query, (username, password))
        connection.commit()
        return True
    except pymysql.err.IntegrityError:
        return False
    finally:
        cursor.close()
        connection.close()
