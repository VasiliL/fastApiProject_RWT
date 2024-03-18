from pydantic import BaseModel
from datetime import datetime, date
from psycopg2 import sql
from typing import Optional, List
from decimal import Decimal


class MyModel(BaseModel):
    @classmethod
    def generate_select_query(cls):
        column_names = list(cls.__annotations__.keys())
        return sql.SQL("SELECT {}").format(
            sql.SQL(",").join(map(sql.Identifier, column_names))
        )


class Car(MyModel, BaseModel):
    id: int
    description: str
    plate_number: str
    owner: Optional[str]
    vin: Optional[str]
    year: Optional[float]
    engine_hp: Optional[float]
    weight_capacity: Optional[float]
    volume: Optional[float]
    weight_own: Optional[float]
    car_type: str


class Person(MyModel, BaseModel):
    id: int
    fio: str
    company: Optional[str]
    position: Optional[str]


class Invoice(MyModel, BaseModel):
    id: int
    client: str
    route: str
    cargo: str
    weight: Decimal
    price: Decimal
    departure_date: datetime
    arrival_date: datetime


class DriverPlace(MyModel, BaseModel):
    id: Optional[int] = None
    date_place: date
    car_id: int
    driver_id: int
    plate_number: Optional[str] = None
    fio: Optional[str] = None


class Run(MyModel, BaseModel):
    id: Optional[int] = None
    car_id: List[int] | int
    driver_id: Optional[int] = None
    invoice_id: int
    date_departure: date
    invoice_document: Optional[str] = None
    waybill: Optional[str] = None
    weight: Optional[Decimal] = Decimal(0)
    weight_arrival: Optional[Decimal] = Decimal(0)
    date_arrival: Optional[date] = None
    reg_number: Optional[str] = None
    reg_date: Optional[date] = None
    acc_number: Optional[str] = None
    acc_date: Optional[date] = None
    client: Optional[str] = None
    route: Optional[str] = None
    cargo: Optional[str] = None
