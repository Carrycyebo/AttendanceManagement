import multiprocessing
from run_flask import start_flask
from run_producer import start_producer
from run_spark import start_spark
from flask import Flask
from flask_socketio import SocketIO
from conf import getConf
from api.router.page import page_bp

conf = getConf.conf()

app = Flask(__name__, template_folder='web/templates', static_folder='web/static')
app.config['SECRET_KEY'] = 'secret!'

socketio = SocketIO(app)

app.register_blueprint(page_bp)

@socketio.on('connect')
def handle_connect():
    print("Client connected")
    socketio.emit('connected', {'data': 'You are successfully connected!'})

@socketio.on('disconnect')
def handle_disconnect():
    print("Client disconnected")

if __name__ == '__main__':
    try:
        # 创建进程
        flask_process = multiprocessing.Process(target=start_flask)
        producer_process = multiprocessing.Process(target=start_producer)
        spark_process = multiprocessing.Process(target=start_spark)
        
        # 启动进程
        flask_process.start()
        producer_process.start()
        spark_process.start()

        # 等待进程完成
        flask_process.join()
        producer_process.join()
        spark_process.join()
        

    except KeyboardInterrupt:
        print("Shutting down application...")
