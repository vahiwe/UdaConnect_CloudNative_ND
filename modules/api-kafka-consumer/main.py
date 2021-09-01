from kafka import KafkaConsumer
from json import loads
import os
import logging
from models import session, Location, Person, func
from geoalchemy2.functions import ST_AsText, ST_Point

KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
KAFKA_PORT = os.environ["KAFKA_PORT"]
KAFKA_HOST = os.environ["KAFKA_HOST"]

logging.basicConfig(
    level=logging.DEBUG,
    filemode='w',
    format='%(name)s - %(levelname)s - %(message)s')

consumer = KafkaConsumer(
    KAFKA_TOPIC,
     bootstrap_servers=[f'{KAFKA_HOST}:{KAFKA_PORT}'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

logging.info("Starting consumer")
for message in consumer:
    logging.info("New Location data {}".format(message.value))
    message_value = message.value
    try:
        location_value = {
            "id": message_value["id"],
            "person_id": message_value["person_id"],
            "coordinate": ST_Point(message_value["latitude"], message_value["longitude"]),
            "creation_time": message_value["creation_time"],
        }
        
        logging.info("Location Data ==> {}".format(location_value))
        location = Location(**location_value)
        session.add(location)
        session.commit()
    
        logging.info("Location created!")
    except Exception as e:
        logging.info("error!!!")
        logging.error("Exception occured: {}".format(e))
        session.rollback()

