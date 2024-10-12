import asyncio
import json
import logging
from kafka_system import KafkaSystem

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def message_handler(message):
    """Example message handler"""
    logger.info(f"Processing message: {message}")
    with open('processed_messages.json', 'a') as f:
        json.dump(message, f)
        f.write('\n')

async def main():
    # Initialize the Kafka system
    kafka_system = KafkaSystem(
        bootstrap_servers='localhost:9092',
        topic=['json-topic'],
        group_id='json-group'
    )

    # Add message handler
    kafka_system.add_message_handler(message_handler)

    # Start the system
    try:
        await kafka_system.start(initial_data_file='initial_data.json')
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Error running Kafka system: {e}")
    finally:
        logger.info("Shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
