import time
from concurrent import futures
import grpc
import location_pb2
import location_pb2_grpc
from models import session, Location, Person, func
from geoalchemy2.functions import ST_AsText, ST_Point

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
            "coordinate": ST_Point(request.latitude, request.longitude),
            "creation_time": request.creation_time,
        }
        
        print("request_value ==>",request_value)
        try:
            location = Location(**request_value)
            session.add(location)
            session.commit()
        
            print("Location created!")
            return location_pb2.LocationSchema(**{
                "id": location.id,
                "person_id": location.person_id,
                "longitude": location.longitude,
                "latitude": location.latitude,
                "creation_time": location.creation_time.isoformat(),
            })
        except Exception as e:
            print("Exception occured: ", e)
            session.rollback()
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
