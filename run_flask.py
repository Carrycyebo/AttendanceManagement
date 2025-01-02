from flask import Flask
from flask_socketio import SocketIO
from conf import getConf
from api.router.page import page_bp
from api.service.websocket_service import start_services

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

def start_flask():
    host = conf.get('server', 'host')
    port = conf.getint('server', 'port')
    socketio.run(app, host=host, port=port, debug=False)

if __name__ == "__main__":
    start_services(socketio)
    start_flask()
