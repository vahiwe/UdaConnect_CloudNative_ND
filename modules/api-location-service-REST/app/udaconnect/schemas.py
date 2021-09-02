from app.udaconnect.models import Location
from marshmallow import Schema, fields, pre_load
from datetime import datetime
class MyDateTimeField(fields.DateTime):
    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, datetime):
            return value
        return super()._deserialize(value, attr, data)

class LocationSchema(Schema):
    id = fields.Integer()
    person_id = fields.Integer()
    longitude = fields.String(attribute="longitude")
    latitude = fields.String(attribute="latitude")
    creation_time = MyDateTimeField()

    class Meta:
        model = Location
        datetimeformat = '%Y-%m-%dT%H:%M:%S'

    # Clean up data
    @pre_load
    def process_input(self, data, **kwargs):
        data["creation_time"] = data["creation_time"] if isinstance(data['creation_time'], datetime) else datetime.strptime(data['creation_time'], '%Y-%m-%dT%H:%M:%S')
        return data
