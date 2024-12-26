#mysql测试连接
from api.db import util


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
    pass

test_db()