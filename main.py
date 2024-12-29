from threading import Thread
from flask import Flask, app
from conf import getConf
from api.kafka.producer import run_producer
from api.rounter.page import page_bp


# 获取配置信息
conf = getConf.conf()

# 创建 Flask 应用
app = Flask(__name__,template_folder='web/templates', static_folder='web/static')

# 注册蓝图
app.register_blueprint(page_bp)

# 启动 Flask 的线程
def start_flask():
    app.run(host=conf.get('server', 'host'), port=conf.get('server', 'port'))  # `use_reloader=False` 避免多线程冲突

# 启动 Kafka 数据生产的线程
def start_producer():
    run_producer()  # 模拟数据生产的逻辑

if __name__ == '__main__':
    # 创建线程
    flask_thread = Thread(target=start_flask)
    producer_thread = Thread(target=start_producer)

    # 启动线程
    flask_thread.start()
    producer_thread.start()

    # 等待线程完成
    flask_thread.join()
    producer_thread.join()
