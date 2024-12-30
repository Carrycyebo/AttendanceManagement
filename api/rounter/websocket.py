from flask_socketio import SocketIO

def register_websocket_events(socketio):

    @socketio.on('test_connect')
    def handle_test_connect(message):
        print(f"Received test_connect: {message}")
        socketio.emit('connected', {'data': 'You are successfully connected!'})

    @socketio.on('send_data')
    def handle_send_data(data):
        print(f"Received send_data: {data}")
        response = f"Processed: {data['data']}"
        socketio.emit('response_data', {'data': response})
