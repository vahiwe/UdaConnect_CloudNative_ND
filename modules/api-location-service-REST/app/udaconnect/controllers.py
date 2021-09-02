import os
from app.udaconnect.models import Location
from app.udaconnect.schemas import (
    LocationSchema,
)
from app.udaconnect.services import LocationService
from flask import request
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource
from kafka import KafkaProducer
from json import dumps

KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
KAFKA_PORT = os.environ["KAFKA_PORT"]
KAFKA_HOST = os.environ["KAFKA_HOST"]
producer = KafkaProducer(bootstrap_servers=[f'{KAFKA_HOST}:{KAFKA_PORT}'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

DATE_FORMAT = "%Y-%m-%d"
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

api = Namespace("UdaConnect Location Service", description="Connections via geolocation.")  # noqa


# TODO: This needs better exception handling
@api.route("/locations")
@api.route("/locations/<location_id>")
@api.param("location_id", "Unique ID for a given Location", _in="query")
class LocationResource(Resource):
    @accepts(schema=LocationSchema)
    def post(self):
        try:
            request_data = request.get_json()
            creation_time = request_data["creation_time"]
            creation_time = creation_time.isoformat()
            request_value = {
                "id": request_data["id"],
                "person_id": request_data["person_id"],
                "longitude": request_data["longitude"],
                "latitude": request_data["latitude"],
                "creation_time": creation_time,
            }
            producer.send(KAFKA_TOPIC, request_value)
            producer.flush()
            return request_value
        except Exception as e:
            return {"error": str(e)}, 400

    @responds(schema=LocationSchema)
    def get(self, location_id) -> Location:
        location: Location = LocationService.retrieve(location_id)
        return location