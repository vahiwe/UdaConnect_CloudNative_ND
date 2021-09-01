import time, os
from concurrent import futures
import grpc
import location_pb2
import location_pb2_grpc
from models import session, Location
from json import dumps
from kafka import KafkaProducer

KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
KAFKA_PORT = os.environ["KAFKA_PORT"]
KAFKA_HOST = os.environ["KAFKA_HOST"]
producer = KafkaProducer(bootstrap_servers=[f'{KAFKA_HOST}:{KAFKA_PORT}'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

class LocationServicer(location_pb2_grpc.LocationServiceServicer):
    def Get(self, request, context):
        id = request.id
        print("Get transaction of ID ", id)
        location = session.query(Location).filter(Location.id == id).first()
        print("location ===>",location)
        if location is None:
            return location_pb2.LocationSchema(id=id, person_id=None, longitude=None, latitude=None, creation_time=None)
        else:
            return location_pb2.LocationSchema(**{
                "id": location.id,
                "person_id": location.person_id,
                "longitude": location.longitude,
                "latitude": location.latitude,
                "creation_time": location.creation_time.isoformat(),
            })


    def Create(self, request, context):
        print("Received a message!")

        request_value = {
            "id": request.id,
            "person_id": request.person_id,
            "longitude": request.longitude,
            "latitude": request.latitude,
            "creation_time": request.creation_time,
        }
        
        print("request_value ==>",request_value)
        try:
            producer.send(KAFKA_TOPIC, request_value)
        
            print("Location Stored in Kafka!")
            return location_pb2.LocationSchema(**request_value)
        except Exception as e:
            print("Exception occured: ", e)
            return location_pb2.LocationSchema(**{
                "id": None,
                "person_id": None,
                "longitude": None,
                "latitude": None,
                "creation_time": None,
            })


# Initialize gRPC server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
location_pb2_grpc.add_LocationServiceServicer_to_server(LocationServicer(), server)


print("Server starting on port 5005...")
server.add_insecure_port("[::]:5005")
server.start()
# Keep thread alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)
