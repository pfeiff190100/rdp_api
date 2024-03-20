import logging
from typing import List

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.orm import Session

from .model import Base, Value, ValueType, Device, Location


class Crud:
    def __init__(self, engine):
        self._engine = engine
        self.IntegrityError = IntegrityError
        self.NoResultFound = NoResultFound

        Base.metadata.create_all(self._engine)

    def add_or_update_value_type(
        self,
        value_type_id: int = None,
        value_type_name: str = None,
        value_type_unit: str = None,
    ) -> None:
        """update or add a value type

        Args:
            value_type_id (int, optional): ValueType id to be modified (if None a new ValueType is added), Default to None.
            value_type_name (str, optional): Typename wich should be set or updated. Defaults to None.
            value_type_unit (str, optional): Unit of mesarument wich should be set or updated. Defaults to None.

        Returns:
            _type_: _description_
        """
        with Session(self._engine) as session:
            stmt = select(ValueType).where(ValueType.id == value_type_id)
            db_type = None
            for type in session.scalars(stmt):
                db_type = type
            if db_type is None:
                db_type = ValueType(id=value_type_id)
            if value_type_name:
                db_type.type_name = value_type_name
            elif not db_type.type_name:
                db_type.type_name = "TYPE_%d" % value_type_id
            if value_type_unit:
                db_type.type_unit = value_type_unit
            elif not db_type.type_unit:
                db_type.type_unit = "UNIT_%d" % value_type_id
            session.add_all([db_type])
            session.commit()
            return db_type

    def add_value(self, value_time: int, value_type: int, value_value: float, device_id: int) -> None:
        """Add a measurement point to the database.

        Args:
            value_time (int): unix time stamp of the value.
            value_type (int): Valuetype id of the given value. 
            value_value (float): The measurement value as float.
            device_id (int): Device id of the given value.
        """        
        with Session(self._engine) as session:
            stmt = select(ValueType).where(ValueType.id == value_type)
            db_type = self.add_or_update_value_type(value_type)
            db_value = Value(time=value_time, value=value_value, value_type=db_type, device_id=device_id)

            session.add_all([db_type, db_value])
            try:
                session.commit()
            except IntegrityError:
                logging.error("Integrity")
                raise

    def get_values(
        self, value_type_id: int = None, start: int = None, end: int = None
    ) -> List[Value]:
        """Get Values from database.

        The result can be filtered by the following paramater:

        Args:
            value_type_id (int, optional): If set, only value of this given type will be returned. Defaults to None.
            start (int, optional): If set, only values with a timestamp as least as big as start are returned. Defaults to None.
            end (int, optional): If set, only values with a timestamp as most as big as end are returned. Defaults to None.

        Returns:
            List[Value]: _description_
        """
        with Session(self._engine) as session:
            stmt = select(Value)
            if value_type_id is not None:
                stmt = stmt.join(Value.value_type).where(ValueType.id == value_type_id)
            if start is not None:
                stmt = stmt.where(Value.time >= start)
            if end is not None:
                stmt = stmt.where(Value.time <= end)
            stmt = stmt.order_by(Value.time)
            return session.scalars(stmt).all()

    def get_value_type(self, value_type_id: int) -> ValueType:
        """Get a special ValueType

        Args:
            value_type_id (int): the primary key of the ValueType

        Returns:
            ValueType: The ValueType object
        """
        with Session(self._engine) as session:
            stmt = select(ValueType).where(ValueType.id == value_type_id)
            return session.scalars(stmt).one()

    def get_value_types(self) -> List[ValueType]:
        """Get all configured value types

        Returns:
            List[ValueType]: List of ValueType objects. 
        """
        with Session(self._engine) as session:
            stmt = select(ValueType)
            return session.scalars(stmt).all()
    
    def add_or_update_device(self, device_name: str, device_description: str, location_id: int, device_id = None) -> Device:
        """update or add a device

        Args:
            device_id (int, optional): Device id to be modified (if None a new ValueType is added), Default to None.
            device_name (str, optional): Device name wich should be set or updated. Defaults to None.
            device_description (str, optional): Device description wich should be set or updated. Defaults to None.

        Returns:
            _type_: _description_
        """
        with Session(self._engine) as session:
            stmt = select(Device).where(Device.id == device_id)
            db_type = session.scalars(stmt).first()
            if db_type is None:
                db_type = Device(device_name=device_name, device_description=device_description, location_id=location_id)
            if device_name:
                db_type.device_name = device_name
            if device_description:
                db_type.device_description = device_description
            if location_id:
                db_type.location_id = location_id
            session.add(db_type)
            session.commit()
            
            return db_type.id

    def get_devices(self) -> List[Device]:
        """Get all configured devices

        Returns:
            List[ValueType]: List of ValueType objects. 
        """
        with Session(self._engine) as session:
            stmt = select(Device)
            return session.scalars(stmt).all()

    def get_device(self, device_id: int) -> Device:
        """Get a special Device

        Args:
            device (int): the primary key of the Device

        Returns:
            Device: The Device object
        """
        with Session(self._engine) as session:
            stmt = select(Device).where(Device.id == device_id)
            return session.scalars(stmt).one()

    def get_device_values(self, device_id: int) -> List[Value]:
        """Get all values from a Device

        Args:
            device (int): the primary key of the Device

        Returns:
            ValueType: The ValueType object
        """
        with Session(self._engine) as session:
            stmt = select(Value).where(Value.device_id == device_id)
            return session.scalars(stmt).all()

    def add_or_update_location(self, location_name: str, location_description: str, location_id = None) -> Device:
        """update or add a location

        Args:
            location_id (int, optional): Device id to be modified (if None a new ValueType is added), Default to None.
            location_name (str, optional): Location name wich should be set or updated. Defaults to None.
            location_description (str, optional): Location description wich should be set or updated. Defaults to None.

        Returns:
            _type_: _description_
        """
        with Session(self._engine) as session:
            stmt = select(Location).where(Device.id == location_id)
            db_type = session.scalars(stmt).first()
            if db_type is None:
                db_type = Location(location_name=location_name, location_description=location_description)
            if location_name:
                db_type.device_name = location_name
            if location_description:
                db_type.device_description = location_description
            session.add(db_type)
            session.commit()
            
            return db_type.id

    def get_locations(self) -> List[Location]:
        """Get all configured locations

        Returns:
            List[Location]: List of Location objects. 
        """
        with Session(self._engine) as session:
            stmt = select(Location)
            return session.scalars(stmt).all()

    def get_location(self, location_id: int) -> Location:
        """Get a special Location

        Args:
            device (int): the primary key of the Location

        Returns:
            Device: The Device object
        """
        with Session(self._engine) as session:
            stmt = select(Location).where(Location.id == location_id)
            return session.scalars(stmt).one()

    def get_location_values(self, location_id: int) -> List[Value]:
        """Get all values from a Location

        Args:
            location_id (int): the primary key of the Location

        Returns:
            List[Value]: The Value objects associated with the Location
        """
        with Session(self._engine) as session:
            stmt = select(Value).join(Device).where(Device.location_id == location_id)
            return session.scalars(stmt).all()
