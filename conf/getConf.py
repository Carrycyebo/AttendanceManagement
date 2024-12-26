import configparser , os

def conf():
    conf = configparser.ConfigParser()

    if os.path.exists('./config/default.ini'):
        conf.read('./config/default.ini')
    else:
        conf.read('./config/default_d.ini')

    return conf
