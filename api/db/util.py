# db unitl

def get_db():
    from db import db
    db.connect()
    return db

def close_db(e=None):
    from db import db
    if db.is_connected():
        db.close()