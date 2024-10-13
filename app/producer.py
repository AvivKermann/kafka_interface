import json
import logging
from aiokafka import AIOKafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaProducer:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await self.producer.start()
        logger.info("Producer started successfully")

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            logger.info("Producer stopped")

    async def send_initial_messages(self, topic: str, file_path: str):
        try:
            assert self.producer is not None
            logger.info(f"file path is: {file_path}")
            with open(file_path, 'r') as file:
                message = json.load(file)
                await self.producer.send_and_wait(topic, message)
                logger.info("Sent initial message")

        except FileNotFoundError:
            logger.error("No initial data file provided")
        except AssertionError:
            logger.error("Producer not started")
        except Exception as e:
            logger.error(f"Error sending initial messages: {e}")
            raise
