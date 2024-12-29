from threading import Thread
from flask import Flask
from flask_socketio import SocketIO, emit
from conf import getConf
from api.kafka.producer import run_producer
from api.rounter.page import page_bp
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 获取配置信息
conf = getConf.conf()

# 创建 Flask 应用
app = Flask(__name__, template_folder='web/templates', static_folder='web/static')
app.config['SECRET_KEY'] = 'secret!'

# 创建 SocketIO 实例
socketio = SocketIO(app, cors_allowed_origins="*")

# 注册蓝图
app.register_blueprint(page_bp)

# WebSocket 事件处理
@socketio.on('connect')
def handle_connect():
    logger.info("Client connected.")
    emit('message', {'data': 'Connected to WebSocket server!'})

@socketio.on('disconnect')
def handle_disconnect():
    logger.info("Client disconnected.")

@socketio.on('send_data')
def handle_send_data(data):
    logger.info(f"Received data from client: {data}")
    emit('response_data', {'data': f"Processed {data}"}, broadcast=True)

def start_socketio():
    """
    启动 SocketIO 服务
    """
    try:
        logger.info("Starting Flask-SocketIO server...")
        socketio.run(
            app,
            host=conf.get('server', 'host'),
            port=conf.getint('server', 'port'),
            debug=False
        )
    except Exception as e:
        logger.error(f"Error starting Flask-SocketIO server: {e}")

def start_producer():
    """
    启动 Kafka 数据生产逻辑
    """
    try:
        logger.info("Starting Kafka producer...")
        run_producer()
    except Exception as e:
        logger.error(f"Error in Kafka producer: {e}")

if __name__ == '__main__':
    try:
        # 创建线程
        socketio_thread = Thread(target=start_socketio, name="SocketIOThread")
        producer_thread = Thread(target=start_producer, name="KafkaProducerThread")

        # 启动线程
        socketio_thread.start()
        producer_thread.start()

        logger.info("Both Flask-SocketIO server and Kafka producer have started.")

        # 等待线程完成
        socketio_thread.join()
        producer_thread.join()
    except KeyboardInterrupt:
        logger.info("Shutting down application...")
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
