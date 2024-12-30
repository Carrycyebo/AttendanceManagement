#mysql测试连接
from api.db import util
from api.kafka.producer import run_producer
from run_flask import start_flask

def test_db():
    db = util.get_db()
    cursor = db.cursor()
    sql = "show databases"
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()
    print(cursor.fetchall())
    db.close()


def test_kafka():
    run_producer()

def test_flask():
    start_flask()

if __name__ == '__main__':
    # test_db()
    test_kafka()
    # test_flask()