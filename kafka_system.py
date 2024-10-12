import asyncio
import signal
import logging
from typing import Dict, Any, Callable, Optional, List
from .producer import KafkaJSONProducer
from .consumer import KafkaJSONConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaSystem:
    def __init__(self, bootstrap_servers: str, topic: List[str], group_id: str):
        self.producer = KafkaJSONProducer(bootstrap_servers, topic)
        self.consumer = KafkaJSONConsumer(bootstrap_servers, topic, group_id)
        self.shutdown_event = asyncio.Event()

    def add_message_handler(self, handler: Callable[[Dict[str, Any]], None]):
        self.consumer.add_message_handler(handler)

    async def start(self, initial_data_file: Optional[str] = None):

        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda s, f: asyncio.create_task(self.shutdown()))

        if initial_data_file:
            await self.producer.start()
            await self.producer.send_initial_messages(initial_data_file)
            await self.producer.stop()

        await self.consumer.start()
        asyncio.create_task(self.consumer.consume_messages())

        await self.shutdown_event.wait()
        await self.consumer.stop()

    async def shutdown(self):
        logger.info("Shutting down...")
        self.shutdown_event.set()

