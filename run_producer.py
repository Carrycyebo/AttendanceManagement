from api.kafka.producer import run_producer
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def start_producer():
    """
    启动 Kafka Producer 数据生产
    """
    try:
        logger.info("Starting Kafka producer...")
        run_producer()
    except Exception as e:
        logger.error(f"Error in Kafka producer: {e}")
