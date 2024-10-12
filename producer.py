import json
import logging
from typing import List
from aiokafka import AIOKafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaJSONProducer:
    def __init__(self, bootstrap_servers: str, topic: List[str]):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
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

    async def send_initial_messages(self, file_path: str):
        try:
            assert self.producer is not None
            with open(file_path, 'r') as file:
                data = json.load(file)
                if isinstance(data, list):
                    for message in data:
                        await self.producer.send_and_wait(self.topic, message)
                        logger.info(f"Sent initial message: {message}")
                else:
                    await self.producer.send_and_wait(self.topic, data)
                    logger.info(f"Sent initial message: {data}")
            logger.info("All initial messages sent successfully")

        except FileNotFoundError:
            logger.error("No initial data file provided")
        except AssertionError:
            logger.error("Producer not started")
        except Exception as e:
            logger.error(f"Error sending initial messages: {e}")
            raise

