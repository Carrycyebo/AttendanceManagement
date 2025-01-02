from flask_socketio import SocketIO
from api.kafka.consumer import get_kafka_consumer

# thread = None

# def background_thread():
#     """Example of how to send server generated events to clients."""
#     consumer = get_kafka_consumer()
#     for message in consumer:
#         socketio.emit('123', {'data': message.value})

# def register_websocket_events(socketio):
#     @socketio.on('123')
#     def handle_connect():
#         print('Client connected')
#         global thread
#         if thread is None:
#             thread = socketio.start_background_task(target=background_thread)
#         socketio.emit('123', {'data': 'Connected'})

