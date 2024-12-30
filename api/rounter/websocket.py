import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def register_websocket_events(socketio):
    """
    注册 WebSocket 事件
    """
    @socketio.on('connect')
    def handle_connect():
        logger.info("Client connected.")
        socketio.emit('message', {'data': 'Connected to WebSocket server!'})

    @socketio.on('disconnect')
    def handle_disconnect():
        logger.info("Client disconnected.")

    @socketio.on('send_data')
    def handle_send_data(data):
        logger.info(f"Received data from client: {data}")
        socketio.emit('response_data', {'data': f"Processed {data}"}, broadcast=True)
