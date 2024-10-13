import asyncio
import logging
from kafka_system import KafkaSystem
from handlers import  handle_algo_started
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def main():

    kafka_system = KafkaSystem(
        bootstrap_servers='192.168.1.14:9092',
        group_id='algo-192.168.1.14',
    )

    kafka_system.add_message_handler(handle_algo_started ,"algo_started")
        

    try:
        await kafka_system.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Error running Kafka system: {e}")
    finally:
        logger.info("Shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
