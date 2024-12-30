from threading import Thread
from run_flask import start_flask
from run_producer import start_producer

if __name__ == '__main__':
    try:
        # 创建线程
        flask_thread = Thread(target=start_flask, name="FlaskThread")
        producer_thread = Thread(target=start_producer, name="KafkaProducerThread")

        # 启动线程
        flask_thread.start()
        producer_thread.start()

        # 等待线程完成
        flask_thread.join()
        producer_thread.join()
    except KeyboardInterrupt:
        print("Shutting down application...")
