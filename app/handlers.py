from producer import KafkaProducer
from typing import Union, Dict
""" This module will define all message handlers for different message consumed from kafka"""

async def handle_algo_started(bootstrep_servers: Union[str, Dict, None]) -> None:
    """ 
    Handle the algo started message
    by sending the params and algo to op-mode
    """

    CAMERA_ALGO_SETTINGS ="./configs/camera_algo_settings.json"
    ALGO_CAMERAS = "./configs/algo_cameras.json"

    # so linter won't go crazy
    assert isinstance(bootstrep_servers, str) 

    producer = KafkaProducer(bootstrep_servers)
    await producer.start()
    await producer.send_initial_messages("camera_algo_settings", CAMERA_ALGO_SETTINGS)
    await producer.send_initial_messages("algo_cameras", ALGO_CAMERAS)
    await producer.stop()


