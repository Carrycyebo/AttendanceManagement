#mysql测试连接
from api.db import util
from api.kafka import util
from api.kafka.util import create_producer


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
    create_producer()


if __name__ == '__main__':
    # test_db()
    test_kafka()