import grpc
import location_pb2
import location_pb2_grpc
from datetime import datetime

"""
Sample implementation of a writer that can be used to write messages to gRPC.
"""

print("Sending sample payload...")

channel = grpc.insecure_channel("localhost:5005")
stub = location_pb2_grpc.LocationServiceStub(channel)

# Update this with desired payload
order = location_pb2.LocationSchema(
    id=1,
    person_id= 1,
    longitude= "37.5534409999999994",
    latitude= "-122.2905240000000049",
    creation_time=datetime.now().isoformat(),
)


response = stub.Create(order)

print(response)
