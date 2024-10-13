import asyncio
import logging
from producer import KafkaProducer
from consumer import KafkaConsumer, HANDLER_TYPE

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaSystem:
    def __init__(self, bootstrap_servers: str, group_id: str):
        self.producer = KafkaProducer(bootstrap_servers)
        self.consumer = KafkaConsumer(bootstrap_servers,group_id)
        self.shutdown_event = asyncio.Event()

    def add_message_handler(self, handler: HANDLER_TYPE, topic: str):
        self.consumer.add_message_handler(handler, topic)

    async def start(self):
        logger.info("starting kafka system")
        await self.consumer.start()
        logger.info("started consumer")
        asyncio.create_task(self.consumer.consume_messages())

        await self.shutdown_event.wait()
        await self.consumer.stop()

    async def shutdown(self):
        logger.info("Shutting down...")
        self.shutdown_event.set()

