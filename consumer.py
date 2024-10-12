import json
import logging
import asyncio
from aiokafka import AIOKafkaConsumer
from typing import Dict, Any, Callable

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaJSONConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self.running = False
        self.message_handlers: list[Callable[[Dict[str, Any]], None]] = []

    def add_message_handler(self, handler: Callable[[Dict[str, Any]], None]):
        self.message_handlers.append(handler)

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        await self.consumer.start()
        self.running = True
        logger.info("Consumer started successfully")

    async def stop(self):
        self.running = False
        if self.consumer:
            await self.consumer.stop()
            logger.info("Consumer stopped")

    async def consume_messages(self):
        try:
            assert self.consumer is not None
            while self.running:
                async for message in self.consumer:
                    try:
                        logger.info(f"Received message: {message.value}")
                        for handler in self.message_handlers:
                            try:
                                handler(message.value)
                            except Exception as handler_error:
                                logger.error(f"Error in message handler: {handler_error}")
                    except Exception as process_error:
                        logger.error(f"Error processing message: {process_error}")
                        continue

        except AssertionError:
            logger.error("Consumer not started")
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
            if self.running:
                logger.info("Attempting to restart consumer...")
                await self.restart()

    async def restart(self):
        await self.stop()
        await asyncio.sleep(5)
        await self.start()
        asyncio.create_task(self.consume_messages())


