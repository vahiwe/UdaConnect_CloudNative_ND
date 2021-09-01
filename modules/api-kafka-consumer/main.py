from kafka import KafkaConsumer
from json import loads
import os
from models import session, Location, Person, func
from geoalchemy2.functions import ST_AsText, ST_Point

KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
KAFKA_PORT = os.environ["KAFKA_PORT"]
KAFKA_HOST = os.environ["KAFKA_HOST"]


# consumer = KafkaConsumer(TOPIC_NAME)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
     bootstrap_servers=[f'{KAFKA_HOST}:{KAFKA_PORT}'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

print("Starting consumer")
for message in consumer:
    print ("New Location data ",message.value)
    message_value = message.value
    try:
        location_value = {
            "id": message_value["id"],
            "person_id": message_value["person_id"],
            "coordinate": ST_Point(message_value["latitude"], message_value["longitude"]),
            "creation_time": message_value["creation_time"],
        }
        
        print("Location Data ==>",location_value)
        location = Location(**location_value)
        session.add(location)
        session.commit()
    
        print("Location created!")
    except Exception as e:
        print("Exception occured: ", e)
        session.rollback()

