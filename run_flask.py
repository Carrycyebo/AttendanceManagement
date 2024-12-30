from flask import Flask
from flask_socketio import SocketIO
from conf import getConf
from api.rounter.page import page_bp
from api.rounter.websocket import register_websocket_events

# 获取配置信息
conf = getConf.conf()

# 创建 Flask 应用
app = Flask(__name__, template_folder='web/templates', static_folder='web/static')
app.config['SECRET_KEY'] = 'secret!'

# 创建 SocketIO 实例
socketio = SocketIO(app)

# 注册蓝图
app.register_blueprint(page_bp)

# 注册 WebSocket 事件
register_websocket_events(socketio)

# 注册 WebSocket 事件
# @socketio.on('test_connect')
# def handle_test_connect(message):
#     print(f"Received test_connect: {message}")
#     socketio.emit('connected', {'data': 'You are successfully connected!'})

# @socketio.on('send_data')
# def handle_send_data(data):
#     print(f"Received send_data: {data}")
#     response = f"Processed: {data['data']}"
#     socketio.emit('response_data', {'data': response})

def start_flask():
    """
    启动 Flask-SocketIO 服务
    """
    host = conf.get('server', 'host')
    port = conf.getint('server', 'port')
    socketio.run(app, host=host, port=port, debug=False)

if __name__ == "__main__":
    start_flask()
