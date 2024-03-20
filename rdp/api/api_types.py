from pydantic import BaseModel

class ValueTypeNoID(BaseModel):
    type_name : str
    type_unit : str

class ValueType(ValueTypeNoID):
    id : int

class ValueNoID(BaseModel):
    value_type_id: int
    time: int
    value: float
    device_id: int 

class Value(ValueNoID):
    id: int

class DeviceNoId(BaseModel):
    device_name: str
    device_description: str
    location_id: int

class DeviceId(DeviceNoId):
    id: int

class LocationNoId(BaseModel):
    location_name: str
    location_description: str

class LocationId(LocationNoId):
    id: int

class ApiDescription(BaseModel):
    description : str = "This is the Api"
    value_type_link : str = "/type"
    value_link : str = "/value"