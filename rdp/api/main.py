from typing import Union, List

from fastapi import FastAPI, HTTPException

from rdp.sensor import Reader
from rdp.crud import create_engine, Crud
from . import api_types as ApiTypes
import logging

logger = logging.getLogger("rdp.api")
app = FastAPI()

@app.get("/")
def read_root() -> ApiTypes.ApiDescription:
    """This url returns a simple description of the api

    Returns:
        ApiTypes.ApiDescription: the Api description in json format 
    """    
    return ApiTypes.ApiDescription()

@app.get("/type/")
def read_types() -> List[ApiTypes.ValueType]:
    """Implements the get of all value types

    Returns:
        List[ApiTypes.ValueType]: list of available valuetypes. 
    """    
    global crud
    return crud.get_value_types()

@app.get("/type/{id}/")
def read_type(id: int) -> ApiTypes.ValueType:
    """returns an explicit value type identified by id

    Args:
        id (int): primary key of the desired value type

    Raises:
        HTTPException: Thrown if a value type with the given id cannot be accessed

    Returns:
        ApiTypes.ValueType: the desired value type 
    """
    global crud
    try:
         return crud.get_value_type(id)
    except crud.NoResultFound:
        raise HTTPException(status_code=404, detail="Item not found") 
    return value_type 

@app.put("/type/{id}/")
def put_type(id, value_type: ApiTypes.ValueTypeNoID) -> ApiTypes.ValueType:
    """PUT request to a special valuetype. This api call is used to change a value type object.

    Args:
        id (int): primary key of the requested value type
        value_type (ApiTypes.ValueTypeNoID): json object representing the new state of the value type. 

    Raises:
        HTTPException: Thrown if a value type with the given id cannot be accessed 

    Returns:
        ApiTypes.ValueType: the requested value type after persisted in the database. 
    """
    global crud
    try:
        crud.add_or_update_value_type(id, value_type_name=value_type.type_name, value_type_unit=value_type.type_unit)
        return read_type(id)
    except crud.NoResultFound:
        raise HTTPException(status_code=404, detail="Item not found")

@app.get("/value/")
def get_values(type_id:int=None, start:int=None, end:int=None) -> List[ApiTypes.Value]:
    """Get values from the database. The default is to return all available values. This result can be filtered.

    Args:
        type_id (int, optional): If set, only values of this type are returned. Defaults to None.
        start (int, optional): If set, only values at least as new are returned. Defaults to None.
        end (int, optional): If set, only values not newer than this are returned. Defaults to None.

    Raises:
        HTTPException: _description_

    Returns:
        List[ApiTypes.Value]: _description_
    """
    global crud
    try:
        values = crud.get_values(type_id, start, end)
        return values
    except crud.NoResultFound:
        raise HTTPException(status_code=404, deltail="Item not found")

@app.get("/device/{id}/")
def read_device(id: int) -> ApiTypes.DeviceId:
    """returns an explicit device identified by id

    Args:
        id (int): primary key of the desired value type

    Raises:
        HTTPException: Thrown if a value type with the given id cannot be accessed

    Returns:
        ApiTypes.ValueType: the desired value type 
    """
    global crud
    try:
         return crud.get_device(id)
    except crud.NoResultFound:
        raise HTTPException(status_code=404, detail="Item not found") 
    return value_type 

@app.get("/devicevalues/{id}/")
def read_device_values(id: int) -> List[ApiTypes.ValueNoID]:
    """Implements the get of all Devices

    Returns:
        List[ApiTypes.ValueNoID]: list of values of the Device. 
    """    
    global crud
    try:
        return crud.get_device_values(id)
    except crud.NoResultFound:
        raise HTTPException(status_code=404, detail="Item not found") 
    return value_type 

@app.get("/devices/")
def read_devices() -> List[ApiTypes.DeviceId]:
    """Implements the get of all Devices

    Returns:
        List[ApiTypes.DeviceId]: list of available Devices. 
    """    
    global crud
    return crud.get_devices()

@app.post("/device/")
def put_device(device_name, device_description) -> ApiTypes.DeviceId:
    """PUT request to a Device. This api call is used to change a Device object.

    Args:
        id (int): primary key of the requested value type
        value_type (ApiTypes.Device): json object representing the new state of the value type. 

    Raises:
        HTTPException: Thrown if a value type with the given id cannot be accessed 

    Returns:
        ApiTypes.Device: the requested value type after persisted in the database. 
    """
    global crud
    try:
        id = crud.add_or_update_device(device_name=device_name, device_description=device_description)
        return read_device(id)
    except crud.NoResultFound:
        raise HTTPException(status_code=404, detail="Item not found")

@app.get("/location/{id}/")
def read_location(id: int) -> ApiTypes.LocationId:
    """Returns an explicit location identified by id

    Args:
        id (int): primary key of the desired location

    Raises:
        HTTPException: Thrown if a location with the given id cannot be accessed

    Returns:
        ApiTypes.Location: the desired location 
    """
    global crud
    try:
         return crud.get_location(id)
    except crud.NoResultFound:
        raise HTTPException(status_code=404, detail="Item not found") 

@app.get("/locationvalues/{id}/")
def read_location_values(id: int) -> List[ApiTypes.ValueNoID]:
    """Implements the get of all values for a Location

    Returns:
        List[ApiTypes.ValueNoID]: list of values of the Location. 
    """    
    global crud
    try:
        return crud.get_location_values(id)
    except crud.NoResultFound:
        raise HTTPException(status_code=404, detail="Item not found") 

@app.get("/locations/")
def read_locations() -> List[ApiTypes.LocationId]:
    """Implements the get of all Locations

    Returns:
        List[ApiTypes.LocationId]: list of available Locations. 
    """    
    global crud
    return crud.get_locations()

@app.post("/location/")
def put_location(location_name: str, location_description: str) -> ApiTypes.LocationId:
    """PUT request to a Location. This api call is used to change a Location object.

    Args:
        location_name (str): name of the location
        location_description (str): description of the location

    Raises:
        HTTPException: Thrown if a location with the given id cannot be accessed 

    Returns:
        ApiTypes.Location: the requested location after persisted in the database. 
    """
    global crud
    try:
        id = crud.add_or_update_location(location_name=location_name, location_description=location_description)
        return read_location(id)
    except crud.NoResultFound:
        raise HTTPException(status_code=404, detail="Item not found")

@app.on_event("startup")
async def startup_event() -> None:
    """start the character device reader
    """    
    logger.info("STARTUP: Sensor reader!")
    global reader, crud
    engine = create_engine("sqlite:///rdb.test.db")
    crud = Crud(engine)
    reader = Reader(crud)
    reader.start()
    logger.debug("STARTUP: Sensor reader completed!")

@app.on_event("shutdown")
async def shutdown_event():
    """stop the character device reader
    """    
    global reader
    logger.debug("SHUTDOWN: Sensor reader!")
    reader.stop()
    logger.info("SHUTDOWN: Sensor reader completed!")
