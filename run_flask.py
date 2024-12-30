from flask import Flask
from flask_socketio import SocketIO
from conf import getConf
from api.rounter.page import page_bp
from api.rounter.websocket import register_websocket_events

# 获取配置信息
conf = getConf.conf()

# 创建 Flask 应用
app = Flask(__name__, template_folder='web/templates', static_folder='web/static')
app.config['SECRET_KEY'] = 'your_secret_key'

# 创建 SocketIO 实例
socketio = SocketIO(app, cors_allowed_origins="*")

# 注册蓝图
app.register_blueprint(page_bp)

# 注册 WebSocket 事件
register_websocket_events(socketio)

def start_flask():
    """
    启动 Flask-SocketIO 服务
    """
    host = conf.get('server', 'host')
    port = conf.getint('server', 'port')
    socketio.run(app, host=host, port=port, debug=False)
