# db unitl
from conf import getConf
from pymysql import connect

conf = getConf.conf()

def get_db():
    return connect(host=conf.get("database", "host"), 
                   user=conf.get("database", "user"), 
                   port=conf.getint("database", "port"),
                   password=conf.get("database", "password"), 
                   database=conf.get("database", "database"), 
                   charset=conf.get("database", "charset"))
