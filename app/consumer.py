import json
import logging
import asyncio
from aiokafka import AIOKafkaConsumer
from typing import Dict, Callable, Union, Any, Coroutine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HANDLER_TYPE = Callable[[Union[Dict, str, None]], Coroutine[Any, Any, None]]

class KafkaConsumer:


    CONSUME_CHANNELS = {
            "algo_started",
            "start_ship_event",
            "publish_ship_event",
            "end_ship_event",
            "add_ship_detections",
            }

    def __init__(self, bootstrap_servers: str, group_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.topics = self.CONSUME_CHANNELS
        self.group_id = group_id
        self.consumer = None
        self.running = False
        self.message_handlers: Dict[str, HANDLER_TYPE] = {}
        self.request_timeout_ms = 50

    def add_message_handler(self, handler: HANDLER_TYPE, topic: str):
            self.message_handlers[topic] = handler
        

    async def start(self):

        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )

        self.consumer.subscribe(self.CONSUME_CHANNELS)
        await self.consumer.start()
        self.running = True
        await self.consumer.getmany(timeout_ms=self.request_timeout_ms)
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
                        logger.info(f"Received message: {message.value} from topic {message.topic}")
                        try:
                            if str(message.topic) == "algo_started":
                                await self.message_handlers[message.topic](self.bootstrap_servers)
                            else:
                                await self.message_handlers[message.topic](message)

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


